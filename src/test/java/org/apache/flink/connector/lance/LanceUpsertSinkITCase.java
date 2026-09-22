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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link LanceUpsertSink}: primary-key upsert (+I/+U) via {@code mergeInsert}
 * and delete (-D) via {@code Dataset#delete}.
 */
class LanceUpsertSinkITCase {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    private static RowType rowType() {
        return new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType(false)),
                new RowType.RowField("name", new VarCharType(true, VarCharType.MAX_LENGTH))));
    }

    private static final String[] PRIMARY_KEYS = {"id"};
    private static final int[] KEY_INDICES = {0};

    private LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(100)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    private GenericRowData row(long id, String name, RowKind kind) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(name));
        row.setRowKind(kind);
        return row;
    }

    private long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    @Test
    @DisplayName("first flush creates the dataset and persists the primary key")
    void firstFlushCreatesDatasetAndPersistsPrimaryKey() throws Exception {
        String path = tempDir.resolve("first").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.invoke(row(2L, "bob", RowKind.INSERT), null);
            sink.flush();
        } finally {
            sink.close();
        }

        assertThat(countRows(path)).isEqualTo(2L);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(PrimaryKeyPersistence.load(ds)).containsExactly("id");
        }
    }

    @Test
    @DisplayName("+I(k) then -D(k) in the same flush leaves k absent")
    void insertThenDeleteSameFlushLeavesKeyAbsent() throws Exception {
        String path = tempDir.resolve("insert_then_delete").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            // Seed one row in a first flush.
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.flush();
            assertThat(countRows(path)).isEqualTo(1L);

            // +I(1) then -D(1) collapse to delete.
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.invoke(row(1L, "alice", RowKind.DELETE), null);
            sink.flush();
        } finally {
            sink.close();
        }

        assertThat(countRows(path)).isZero();
    }

    @Test
    @DisplayName("-D(k) then +I(k) in the same flush leaves k present")
    void deleteThenInsertSameFlushLeavesKeyPresent() throws Exception {
        String path = tempDir.resolve("delete_then_insert").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.flush();
            assertThat(countRows(path)).isEqualTo(1L);

            // -D(1) then +I(1) collapse to upsert of the new value.
            sink.invoke(row(1L, "alice", RowKind.DELETE), null);
            sink.invoke(row(1L, "alice-new", RowKind.INSERT), null);
            sink.flush();
        } finally {
            sink.close();
        }

        assertThat(countRows(path)).isEqualTo(1L);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(ds.countRows("name = 'alice-new'")).isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("+U(k) replaces the previous value via merge-insert")
    void updateAfterReplacesPreviousValue() throws Exception {
        String path = tempDir.resolve("update").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            sink.invoke(row(1L, "old", RowKind.INSERT), null);
            sink.flush();

            sink.invoke(row(1L, "new", RowKind.UPDATE_AFTER), null);
            sink.flush();
        } finally {
            sink.close();
        }

        assertThat(countRows(path)).isEqualTo(1L);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(ds.countRows("name = 'new'")).isEqualTo(1L);
            assertThat(ds.countRows("name = 'old'")).isZero();
        }
    }

    @Test
    @DisplayName("replaying the same upsert batch (checkpoint replay) is idempotent")
    void replaySameUpsertBatchIsIdempotent() throws Exception {
        String path = tempDir.resolve("replay_upsert").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            // First flush (checkpoint 1 completed).
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.invoke(row(2L, "bob", RowKind.INSERT), null);
            sink.flush();
            assertThat(countRows(path)).isEqualTo(2L);

            // Replay the same batch (checkpoint 1 failed and was retried).
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.invoke(row(2L, "bob", RowKind.INSERT), null);
            sink.flush();
        } finally {
            sink.close();
        }

        // Idempotent: replay must not duplicate rows.
        assertThat(countRows(path)).isEqualTo(2L);
    }

    @Test
    @DisplayName("replaying a delete batch (checkpoint replay) is idempotent")
    void replayDeleteBatchIsIdempotent() throws Exception {
        String path = tempDir.resolve("replay_delete").toString();
        LanceUpsertSink sink = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);

        sink.open(new Configuration());
        try {
            sink.invoke(row(1L, "alice", RowKind.INSERT), null);
            sink.flush();
            assertThat(countRows(path)).isEqualTo(1L);

            // First delete flush.
            sink.invoke(row(1L, "alice", RowKind.DELETE), null);
            sink.flush();
            assertThat(countRows(path)).isZero();

            // Replay the delete (checkpoint 2 failed and was retried).
            sink.invoke(row(1L, "alice", RowKind.DELETE), null);
            sink.flush();
        } finally {
            sink.close();
        }

        // Idempotent: replaying a delete of an absent key must remain a no-op.
        assertThat(countRows(path)).isZero();
    }

    @Test
    @DisplayName("two subtasks concurrently write distinct keys to an existing dataset")
    void twoSubtasksConcurrentWriteToExistingDataset() throws Exception {
        String path = tempDir.resolve("concurrent_existing").toString();

        // Seed the dataset with key=1 so it already exists.
        LanceUpsertSink seeder = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);
        seeder.open(new Configuration());
        seeder.invoke(row(1L, "one", RowKind.INSERT), null);
        seeder.flush();
        seeder.close();

        // Two subtasks open at the same base version.
        LanceUpsertSink s1 = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);
        LanceUpsertSink s2 = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);
        s1.open(new Configuration());
        s2.open(new Configuration());

        try {
            s1.invoke(row(2L, "two", RowKind.INSERT), null);
            s1.flush();
            s2.invoke(row(3L, "three", RowKind.INSERT), null);
            s2.flush();
        } finally {
            s1.close();
            s2.close();
        }

        // Both writes must land: no write conflict, no lost row.
        assertThat(countRows(path)).isEqualTo(3L);
    }

    @Test
    @DisplayName("two subtasks concurrently first-write distinct keys without data loss")
    void twoSubtasksConcurrentFirstWrite() throws Exception {
        String path = tempDir.resolve("concurrent_first").toString();

        // Dataset does NOT exist yet. Both subtasks open, then first-write.
        LanceUpsertSink s1 = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);
        LanceUpsertSink s2 = new LanceUpsertSink(options(path), rowType(),
                Arrays.asList(PRIMARY_KEYS), KEY_INDICES);
        s1.open(new Configuration());
        s2.open(new Configuration());

        try {
            s1.invoke(row(1L, "one", RowKind.INSERT), null);
            s1.flush();
            s2.invoke(row(2L, "two", RowKind.INSERT), null);
            s2.flush();
        } finally {
            s1.close();
            s2.close();
        }

        // The second first-write must not clobber the first (regression: Overwrite clobbering).
        assertThat(countRows(path)).isEqualTo(2L);
    }
}
