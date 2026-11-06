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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #87 regression tests for {@link LanceSinkWriter} orphan-file prevention.
 *
 * <p>The core behavioural change: {@code write()} must <b>not</b> materialize {@code .lance}
 * files on disk (previously it did once {@code write.batch-size} was reached). Materialization
 * is deferred to {@code flush()} / {@code prepareCommit()}, so a TM crash between
 * {@code write()} and {@code prepareCommit()} leaves no orphan files behind.
 *
 * <p>These tests exercise the real Lance JNI (fragment materialization), which is the exact
 * side effect the orphan leak depends on. They are the direct, deterministic equivalent of the
 * issue's "kill TM between write() and prepareCommit()" MiniCluster scenario: if {@code write()}
 * produces no {@code .lance} files, a crash at that point cannot orphan anything.
 */
class LanceSinkWriterOrphanFileTest {

    @TempDir
    Path tempDir;

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("id", new BigIntType(false)),
                            new RowType.RowField("content", new VarCharType(true, VarCharType.MAX_LENGTH))));

    private LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(1024)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    private GenericRowData row(long id, String content) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(content));
        return row;
    }

    private static long countLanceFiles(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return 0;
        }
        try (Stream<Path> paths = Files.walk(dir)) {
            return paths.filter(p -> p.toString().endsWith(".lance")).count();
        }
    }

    @Test
    @DisplayName("write() below memory-cap does not materialize any .lance file")
    void writeDoesNotMaterialize() throws Exception {
        Path datasetPath = tempDir.resolve("no_materialize");
        // default write.memory-cap-bytes = -1 (unbounded) → write() never materializes.
        try (LanceSinkWriter writer = new LanceSinkWriter(options(datasetPath.toString()), ROW_TYPE, 0)) {
            for (int i = 0; i < 500; i++) {
                writer.write(row(i, "content-" + i), null);
            }

            // Core assertion: no .lance file exists yet even though we wrote 500 rows
            // (well above the old 1024-row batchSize trigger is not hit, but the old
            // behaviour materialized at batchSize; the new behaviour materializes never
            // during write).
            assertThat(countLanceFiles(datasetPath)).isZero();
        }
    }

    @Test
    @DisplayName("flush() materializes the buffered rows")
    void flushMaterializes() throws Exception {
        Path datasetPath = tempDir.resolve("flush_materialize");
        try (LanceSinkWriter writer = new LanceSinkWriter(options(datasetPath.toString()), ROW_TYPE, 0)) {
            for (int i = 0; i < 100; i++) {
                writer.write(row(i, "content-" + i), null);
            }
            assertThat(countLanceFiles(datasetPath)).isZero();

            writer.flush(false);

            assertThat(countLanceFiles(datasetPath)).isPositive();
        }
    }

    @Test
    @DisplayName("prepareCommit() materializes the buffered rows")
    void prepareCommitMaterializes() throws Exception {
        Path datasetPath = tempDir.resolve("prepare_commit_materialize");
        try (LanceSinkWriter writer = new LanceSinkWriter(options(datasetPath.toString()), ROW_TYPE, 0)) {
            for (int i = 0; i < 100; i++) {
                writer.write(row(i, "content-" + i), null);
            }
            assertThat(countLanceFiles(datasetPath)).isZero();

            writer.prepareCommit();

            assertThat(countLanceFiles(datasetPath)).isPositive();
        }
    }

    @Test
    @DisplayName("write.memory-cap-bytes triggers early materialization as an OOM safety valve")
    void memoryCapTriggersEarlyMaterialization() throws Exception {
        Path datasetPath = tempDir.resolve("memory_cap_materialize");
        // per-row estimate = 16 overhead + 8 (BIGINT) + 32 (VARCHAR) = 56; a tiny cap forces
        // materialization on the very first write.
        LanceOptions capped =
                LanceOptions.builder()
                        .path(datasetPath.toString())
                        .writeMode(LanceOptions.WriteMode.APPEND)
                        .writeMemoryCapBytes(1L)
                        .build();

        try (LanceSinkWriter writer = new LanceSinkWriter(capped, ROW_TYPE, 0)) {
            writer.write(row(1, "x"), null);

            // The OOM safety valve has already materialized on write().
            assertThat(countLanceFiles(datasetPath)).isPositive();
        }
    }
}
