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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check that the predicate strings produced by {@code LanceDynamicTableSource}'s filter
 * push-down are actually accepted by Lance and select the rows they claim to.
 *
 * <h3>Why a unit test on the string is not enough</h3>
 * <p>PR #76 established that a plausible-looking call into the Lance SDK can be silently wrong
 * ({@code castTo}). The same applies to predicate syntax: asserting that push-down emits
 * {@code date '2026-03-14'} proves only that we render what the docs describe, not that the
 * engine parses it, and not that it compares against a Date32 column the way we assume. Each case
 * below runs the predicate through a real dataset and asserts the resulting row count.
 *
 * <p>Lives in this package rather than {@code ...lance.table} so it can reuse
 * {@link ArrowArrayStreamsTestAccess}, the same {@code VectorSchemaRoot} bridge the sink uses.
 */
class LanceFilterPredicateDialectITCase {

    private static final String[] NAMES = {"alpha", "beta", "gamma"};

    /** Epoch days for 2026-03-14, 2026-03-15, 2026-03-16. */
    private static final int[] DAYS = {20526, 20527, 20528};

    /** 2026-03-14 15:09:26.123 UTC and the same clock time on the two following days. */
    private static final long[] MILLIS = {
        20526L * 86_400_000L + 54_566_123L,
        20527L * 86_400_000L + 54_566_123L,
        20528L * 86_400_000L + 54_566_123L
    };

    private static final Schema SCHEMA = new Schema(Arrays.asList(
            new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("created_date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
            new Field(
                    "created_ts",
                    FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                    null),
            new Field("amount", FieldType.nullable(new ArrowType.Decimal(10, 2, 128)), null),
            new Field("weird name", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    private String seedDataset(Path dir, BufferAllocator allocator) throws Exception {
        String path = dir.resolve("filter_dialect.lance").toString();
        try (Dataset created =
                Dataset.create(allocator, path, SCHEMA, new WriteParams.Builder().build())) {
            assertThat(created.countRows()).isZero();
        }

        try (Dataset target = Dataset.open(path, allocator);
                VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            root.allocateNew();
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            DateDayVector date = (DateDayVector) root.getVector("created_date");
            TimeStampMilliVector ts = (TimeStampMilliVector) root.getVector("created_ts");
            DecimalVector amount = (DecimalVector) root.getVector("amount");
            BigIntVector weird = (BigIntVector) root.getVector("weird name");

            for (int i = 0; i < 3; i++) {
                id.setSafe(i, i + 1);
                name.setSafe(i, NAMES[i].getBytes(StandardCharsets.UTF_8));
                date.setSafe(i, DAYS[i]);
                ts.setSafe(i, MILLIS[i]);
                amount.setSafe(i, new BigDecimal("1234.5" + i));
                weird.setSafe(i, 100L + i);
            }
            root.setRowCount(3);

            MergeInsertParams params =
                    new MergeInsertParams(Collections.singletonList("id"))
                            .withMatchedUpdateAll()
                            .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
            ArrowArrayStreamsTestAccess.mergeInsert(target, params, allocator, root);
        }

        try (Dataset check = Dataset.open(path, allocator)) {
            assertThat(check.countRows())
                    .as("harness sanity: three rows must be visible before any predicate runs")
                    .isEqualTo(3L);
        }
        return path;
    }

    private long countMatching(String path, BufferAllocator allocator, String predicate) {
        try (Dataset dataset = Dataset.open(path, allocator)) {
            return dataset.countRows(predicate);
        }
    }

    @Test
    @DisplayName("typed DATE predicate parses and selects the right rows")
    void dateLiteralIsAcceptedByLance(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            assertThat(countMatching(path, allocator, "`created_date` = date '2026-03-14'"))
                    .as("typed date literal must select exactly the first row")
                    .isEqualTo(1L);
            assertThat(countMatching(path, allocator, "`created_date` > date '2026-03-14'"))
                    .isEqualTo(2L);
        }
    }

    @Test
    @DisplayName("typed TIMESTAMP predicate parses and selects the right row")
    void timestampLiteralIsAcceptedByLance(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            String predicate = "`created_ts` = timestamp(3) '2026-03-14 15:09:26.123000000'";
            assertThat(countMatching(path, allocator, predicate))
                    .as("typed timestamp literal must select exactly the first row")
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("typed DECIMAL predicate parses and selects the right row")
    void decimalLiteralIsAcceptedByLance(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            assertThat(countMatching(path, allocator, "`amount` = decimal(10,2) '1234.50'"))
                    .as("typed decimal literal must select exactly the first row")
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("backtick-quoted identifier with a space resolves")
    void backtickQuotedIdentifierIsAcceptedByLance(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            assertThat(countMatching(path, allocator, "`weird name` = 100"))
                    .as("a spaced column name is addressable only when backtick-quoted")
                    .isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("backtick quoting is harmless on an ordinary identifier")
    void backtickQuotingOrdinaryIdentifierStillWorks(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            assertThat(countMatching(path, allocator, "`name` = 'alpha'")).isEqualTo(1L);
            assertThat(countMatching(path, allocator, "`id` >= 2")).isEqualTo(2L);
        }
    }

    /**
     * The shapes Calcite produces for {@code IN} and {@code BETWEEN}, confirming neither needs a
     * dedicated branch in the converter.
     */
    @Test
    @DisplayName("OR chain and range conjunction parse as IN / BETWEEN equivalents")
    void expandedInAndBetweenShapesAreAccepted(@TempDir Path dir) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            String path = seedDataset(dir, allocator);

            assertThat(countMatching(path, allocator, "(`name` = 'alpha') OR (`name` = 'gamma')"))
                    .isEqualTo(2L);
            assertThat(countMatching(path, allocator, "(`id` >= 2) AND (`id` <= 3)"))
                    .isEqualTo(2L);
        }
    }
}
