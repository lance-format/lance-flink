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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for append-mode {@link LanceSink} concurrency.
 *
 * <p>Locks in that two subtasks concurrently first-writing to a not-yet-existing dataset do not
 * clobber each other (regression: {@code Overwrite} on first write silently dropped the other
 * subtask's rows).
 */
class LanceSinkConcurrencyITCase {

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
                new RowType.RowField("content", new VarCharType(true, VarCharType.MAX_LENGTH))));
    }

    private LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(100)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    private GenericRowData row(long id, String content) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(content));
        return row;
    }

    private long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    @Test
    @DisplayName("two append subtasks concurrently first-write distinct rows without data loss")
    void twoSubtasksConcurrentFirstWrite() throws Exception {
        String path = tempDir.resolve("concurrent_append").toString();

        LanceSink s1 = new LanceSink(options(path), rowType());
        LanceSink s2 = new LanceSink(options(path), rowType());
        s1.open(new Configuration());
        s2.open(new Configuration());

        try {
            s1.invoke(row(1L, "one"), null);
            s1.flush();
            s2.invoke(row(2L, "two"), null);
            s2.flush();
        } finally {
            s1.close();
            s2.close();
        }

        // The second first-write must not clobber the first (regression: Overwrite clobbering).
        assertThat(countRows(path)).isEqualTo(2L);
    }
}
