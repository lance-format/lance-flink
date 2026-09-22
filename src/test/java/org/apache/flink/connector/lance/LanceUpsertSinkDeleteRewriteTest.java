/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the A4 rewrite of {@code LanceUpsertSink}'s DELETE path: the SQL {@code (k = v) OR ...}
 * predicate is replaced by a key-only {@code mergeInsert} with
 * {@code WhenMatched.Delete} + {@code WhenNotMatched.DoNothing}.
 *
 * <p>Each test targets one concrete defect of the predicate-based implementation, so a regression
 * points at a specific cause rather than merely "delete broke":
 *
 * <ul>
 *   <li>{@code deleteByTimestampKey} / {@code deleteByDateKey} / {@code deleteByBinaryKey} — types
 *       that {@code formatSqlValue} rejected with {@code UnsupportedOperationException}.</li>
 *   <li>{@code deleteByStringKeyContainingSqlMetacharacters} — the injection / quoting surface.</li>
 *   <li>{@code deleteByNonFiniteFloatKey} — {@code NaN}, which has no valid SQL literal.</li>
 *   <li>{@code deleteLargeKeyBatch} — the O(N) predicate growth.</li>
 * </ul>
 *
 * <p>Assertions read a freshly reopened dataset, never the sink's internal handle, so a delete that
 * is applied only to an in-memory snapshot cannot pass.
 */
class LanceUpsertSinkDeleteRewriteTest {

    private static LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(10_000)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    /** Drive the sink end-to-end: open, feed rows, checkpoint-flush, close. */
    private static void runSink(
            String path,
            RowType rowType,
            java.util.List<String> primaryKeys,
            int[] keyIndices,
            java.util.List<GenericRowData> rows) throws Exception {

        LanceUpsertSink sink = new LanceUpsertSink(
                options(path), rowType, primaryKeys, keyIndices);
        sink.open(new Configuration());
        try {
            for (GenericRowData row : rows) {
                sink.invoke(row, null);
            }
            sink.flush();
        } finally {
            sink.close();
        }
    }

    private static long countRows(String path, String filter) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset dataset = Dataset.open(path, allocator)) {
            return filter == null ? dataset.countRows() : dataset.countRows(filter);
        }
    }

    // ------------------------------------------------------------------
    // type coverage: these all threw UnsupportedOperationException before
    // ------------------------------------------------------------------

    @Test
    @DisplayName("TIMESTAMP primary key can be deleted (formatSqlValue had no literal for it)")
    void deleteByTimestampKey(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("ts_key").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6).notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        TimestampData keep = TimestampData.fromEpochMillis(1_700_000_000_000L);
        TimestampData drop = TimestampData.fromEpochMillis(1_700_000_001_000L);

        GenericRowData insertKeep = GenericRowData.ofKind(
                RowKind.INSERT, keep, StringData.fromString("keep"));
        GenericRowData insertDrop = GenericRowData.ofKind(
                RowKind.INSERT, drop, StringData.fromString("drop"));

        runSink(path, rowType, Collections.singletonList("ts"), new int[] {0},
                java.util.Arrays.asList(insertKeep, insertDrop));
        assertThat(countRows(path, null)).isEqualTo(2L);

        GenericRowData deleteDrop = GenericRowData.ofKind(
                RowKind.DELETE, drop, StringData.fromString("drop"));
        runSink(path, rowType, Collections.singletonList("ts"), new int[] {0},
                Collections.singletonList(deleteDrop));

        assertThat(countRows(path, null))
                .as("a TIMESTAMP-keyed row must be deletable")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("DATE primary key can be deleted")
    void deleteByDateKey(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("date_key").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("d", DataTypes.DATE().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        int keep = (int) LocalDate.of(2026, 1, 15).toEpochDay();
        int drop = (int) LocalDate.of(2026, 3, 20).toEpochDay();

        runSink(path, rowType, Collections.singletonList("d"), new int[] {0},
                java.util.Arrays.asList(
                        GenericRowData.ofKind(RowKind.INSERT, keep, StringData.fromString("keep")),
                        GenericRowData.ofKind(RowKind.INSERT, drop, StringData.fromString("drop"))));
        assertThat(countRows(path, null)).isEqualTo(2L);

        runSink(path, rowType, Collections.singletonList("d"), new int[] {0},
                Collections.singletonList(
                        GenericRowData.ofKind(RowKind.DELETE, drop, StringData.fromString("drop"))));

        assertThat(countRows(path, null))
                .as("a DATE-keyed row must be deletable")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("VARBINARY primary key can be deleted")
    void deleteByBinaryKey(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("bin_key").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("k", DataTypes.BYTES().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        byte[] keep = "keep-key".getBytes(StandardCharsets.UTF_8);
        byte[] drop = "drop-key".getBytes(StandardCharsets.UTF_8);

        runSink(path, rowType, Collections.singletonList("k"), new int[] {0},
                java.util.Arrays.asList(
                        GenericRowData.ofKind(RowKind.INSERT, keep, StringData.fromString("keep")),
                        GenericRowData.ofKind(RowKind.INSERT, drop, StringData.fromString("drop"))));
        assertThat(countRows(path, null)).isEqualTo(2L);

        runSink(path, rowType, Collections.singletonList("k"), new int[] {0},
                Collections.singletonList(
                        GenericRowData.ofKind(RowKind.DELETE, drop, StringData.fromString("drop"))));

        assertThat(countRows(path, null))
                .as("a VARBINARY-keyed row must be deletable")
                .isEqualTo(1L);
    }

    // ------------------------------------------------------------------
    // injection surface
    // ------------------------------------------------------------------

    /**
     * Keys containing quotes, an OR-clause and a comment marker. Under string interpolation these
     * are the payloads that either corrupt the predicate or widen it; as Arrow values they are
     * inert. The decisive assertion is that the sibling row survives -- a widened predicate would
     * have deleted both.
     */
    @Test
    @DisplayName("string key with SQL metacharacters deletes exactly one row")
    void deleteByStringKeyContainingSqlMetacharacters(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("injection").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        String nasty = "a' OR '1'='1";
        String sibling = "ordinary";

        runSink(path, rowType, Collections.singletonList("id"), new int[] {0},
                java.util.Arrays.asList(
                        GenericRowData.ofKind(RowKind.INSERT,
                                StringData.fromString(nasty), StringData.fromString("v1")),
                        GenericRowData.ofKind(RowKind.INSERT,
                                StringData.fromString(sibling), StringData.fromString("v2")),
                        GenericRowData.ofKind(RowKind.INSERT,
                                StringData.fromString("x--comment"), StringData.fromString("v3"))));
        assertThat(countRows(path, null)).isEqualTo(3L);

        runSink(path, rowType, Collections.singletonList("id"), new int[] {0},
                Collections.singletonList(GenericRowData.ofKind(RowKind.DELETE,
                        StringData.fromString(nasty), StringData.fromString("v1"))));

        assertThat(countRows(path, null))
                .as("an injection-shaped key must delete exactly its own row, not widen the match")
                .isEqualTo(2L);
        assertThat(countRows(path, "id = 'ordinary'"))
                .as("the sibling row must survive")
                .isEqualTo(1L);
    }

    // ------------------------------------------------------------------
    // non-finite floats
    // ------------------------------------------------------------------

    /**
     * {@code NaN} was explicitly rejected before, because {@code Float.toString(NaN)} produces
     * "NaN" which is not a valid SQL float literal. As an Arrow value it round-trips natively.
     */
    @Test
    @DisplayName("NaN float primary key is deletable (was explicitly rejected before)")
    void deleteByNonFiniteFloatKey(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("nan_key").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("score", DataTypes.FLOAT().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        runSink(path, rowType, Collections.singletonList("score"), new int[] {0},
                java.util.Arrays.asList(
                        GenericRowData.ofKind(RowKind.INSERT,
                                Float.NaN, StringData.fromString("nan-row")),
                        GenericRowData.ofKind(RowKind.INSERT,
                                1.5f, StringData.fromString("normal"))));
        assertThat(countRows(path, null)).isEqualTo(2L);

        runSink(path, rowType, Collections.singletonList("score"), new int[] {0},
                Collections.singletonList(GenericRowData.ofKind(RowKind.DELETE,
                        Float.NaN, StringData.fromString("nan-row"))));

        assertThat(countRows(path, null))
                .as("a NaN-keyed row must be deletable rather than rejected")
                .isEqualTo(1L);
        assertThat(countRows(path, "score = 1.5"))
                .as("the finite-keyed row must survive")
                .isEqualTo(1L);
    }

    // ------------------------------------------------------------------
    // predicate size / batching
    // ------------------------------------------------------------------

    /**
     * 2000 keys in one flush. Previously this produced a single predicate string with 2000 OR
     * branches; now it is one columnar batch.
     */
    @Test
    @DisplayName("a large key batch deletes in one merge (was an O(N) predicate string)")
    void deleteLargeKeyBatch(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("large_batch").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        int total = 4000;
        java.util.List<GenericRowData> inserts = new java.util.ArrayList<>(total);
        for (long i = 0; i < total; i++) {
            inserts.add(GenericRowData.ofKind(
                    RowKind.INSERT, i, StringData.fromString("v" + i)));
        }
        runSink(path, rowType, Collections.singletonList("id"), new int[] {0}, inserts);
        assertThat(countRows(path, null)).isEqualTo(total);

        // Delete every even key: 2000 keys in a single flush.
        java.util.List<GenericRowData> deletes = new java.util.ArrayList<>();
        for (long i = 0; i < total; i += 2) {
            deletes.add(GenericRowData.ofKind(
                    RowKind.DELETE, i, StringData.fromString("v" + i)));
        }
        runSink(path, rowType, Collections.singletonList("id"), new int[] {0}, deletes);

        assertThat(countRows(path, null))
                .as("exactly the even keys must be deleted")
                .isEqualTo(total / 2);
        assertThat(countRows(path, "id = 0")).isZero();
        assertThat(countRows(path, "id = 3998")).isZero();
        assertThat(countRows(path, "id = 1")).isEqualTo(1L);
        assertThat(countRows(path, "id = 3999")).isEqualTo(1L);
    }

    // ------------------------------------------------------------------
    // preserved semantics
    // ------------------------------------------------------------------

    @Test
    @DisplayName("deleting an absent key is a no-op and does not insert the probe row")
    void deleteAbsentKeyDoesNotInsert(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("absent").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        runSink(path, rowType, Collections.singletonList("id"), new int[] {0},
                Collections.singletonList(GenericRowData.ofKind(
                        RowKind.INSERT, 1L, StringData.fromString("only"))));

        runSink(path, rowType, Collections.singletonList("id"), new int[] {0},
                Collections.singletonList(GenericRowData.ofKind(
                        RowKind.DELETE, 999L, StringData.fromString("ghost"))));

        assertThat(countRows(path, null))
                .as("WhenNotMatched.DoNothing must keep this a no-op")
                .isEqualTo(1L);
        assertThat(countRows(path, "id = 999"))
                .as("the key-only probe row must NOT be inserted")
                .isZero();
    }

    @Test
    @DisplayName("NULL primary key is still rejected with a column-named error")
    void nullPrimaryKeyIsRejected(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("null_key").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        LanceUpsertSink sink = new LanceUpsertSink(
                options(path), rowType, Collections.singletonList("id"), new int[] {0});
        sink.open(new Configuration());
        try {
            sink.invoke(GenericRowData.ofKind(
                    RowKind.DELETE, null, StringData.fromString("x")), null);

            assertThatThrownBy(sink::flush)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("NULL primary-key value")
                    .hasMessageContaining("'id'");
        } finally {
            sink.close();
        }
    }

    @Test
    @DisplayName("composite primary key deletes only the exact tuple")
    void deleteByCompositeKey(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("composite").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("region", DataTypes.STRING().notNull()),
                DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        java.util.List<String> pk = java.util.Arrays.asList("region", "id");
        int[] keyIndices = {0, 1};

        runSink(path, rowType, pk, keyIndices, java.util.Arrays.asList(
                GenericRowData.ofKind(RowKind.INSERT,
                        StringData.fromString("eu"), 1L, StringData.fromString("a")),
                GenericRowData.ofKind(RowKind.INSERT,
                        StringData.fromString("us"), 1L, StringData.fromString("b")),
                GenericRowData.ofKind(RowKind.INSERT,
                        StringData.fromString("eu"), 2L, StringData.fromString("c"))));
        assertThat(countRows(path, null)).isEqualTo(3L);

        runSink(path, rowType, pk, keyIndices, Collections.singletonList(
                GenericRowData.ofKind(RowKind.DELETE,
                        StringData.fromString("eu"), 1L, StringData.fromString("a"))));

        assertThat(countRows(path, null))
                .as("only the (eu,1) tuple must be deleted")
                .isEqualTo(2L);
        assertThat(countRows(path, "region = 'us' AND id = 1")).isEqualTo(1L);
        assertThat(countRows(path, "region = 'eu' AND id = 2")).isEqualTo(1L);
    }

    /**
     * Read-your-own-write across flushes: only correct because the sink now adopts
     * {@code MergeInsertResult#dataset()} instead of keeping the handle from {@code open()}.
     */
    @Test
    @DisplayName("upsert then delete then re-insert across flushes within one sink instance")
    void multipleFlushesWithinOneSinkInstance(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("multi_flush").toString();
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();

        LanceUpsertSink sink = new LanceUpsertSink(
                options(path), rowType, Collections.singletonList("id"), new int[] {0});
        sink.open(new Configuration());
        try {
            sink.invoke(GenericRowData.ofKind(
                    RowKind.INSERT, 1L, StringData.fromString("v1")), null);
            sink.invoke(GenericRowData.ofKind(
                    RowKind.INSERT, 2L, StringData.fromString("v2")), null);
            sink.flush();

            sink.invoke(GenericRowData.ofKind(
                    RowKind.DELETE, 1L, StringData.fromString("v1")), null);
            sink.flush();

            sink.invoke(GenericRowData.ofKind(
                    RowKind.UPDATE_AFTER, 2L, StringData.fromString("v2-updated")), null);
            sink.invoke(GenericRowData.ofKind(
                    RowKind.INSERT, 3L, StringData.fromString("v3")), null);
            sink.flush();
        } finally {
            sink.close();
        }

        assertThat(countRows(path, null))
                .as("keys 2 and 3 must remain after delete of 1")
                .isEqualTo(2L);
        assertThat(countRows(path, "id = 1")).isZero();
        assertThat(countRows(path, "payload = 'v2-updated'"))
                .as("the in-place update must be visible")
                .isEqualTo(1L);
        assertThat(countRows(path, "id = 3")).isEqualTo(1L);
    }
}
