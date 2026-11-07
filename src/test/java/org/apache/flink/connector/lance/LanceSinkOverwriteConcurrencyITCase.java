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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for overwrite-mode {@link LanceSink} concurrency.
 *
 * <p>Companion to {@link LanceSinkConcurrencyITCase} (append mode). {@code write.mode=overwrite}
 * is documented as "Replace the dataset on first write": the job's output replaces the previous
 * dataset contents. With parallelism &gt; 1 every subtask is a peer of the same job, so the rows of
 * one subtask must never be dropped by the first write of another subtask.
 */
class LanceSinkOverwriteConcurrencyITCase {

    @TempDir
    Path tempDir;

    private final List<MockEnvironment> environments = new ArrayList<>();

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    @AfterEach
    void closeEnvironments() throws Exception {
        for (MockEnvironment environment : environments) {
            environment.close();
        }
        environments.clear();
    }

    private static RowType rowType() {
        return new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType(false)),
                new RowType.RowField("content", new VarCharType(true, VarCharType.MAX_LENGTH))));
    }

    private static LanceOptions options(String path, LanceOptions.WriteMode mode) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(100)
                .writeMode(mode)
                .build();
    }

    /** Opens the sink of one subtask of the given job, as the Flink runtime would. */
    private LanceSink openSubtask(String path, JobID jobId, int subtaskIndex, int parallelism)
            throws Exception {
        MockEnvironment environment = new MockEnvironmentBuilder()
                .setJobID(jobId)
                .setMaxParallelism(128)
                .setParallelism(parallelism)
                .setSubtaskIndex(subtaskIndex)
                .build();
        environments.add(environment);
        LanceSink sink = new LanceSink(options(path, LanceOptions.WriteMode.OVERWRITE), rowType());
        sink.setRuntimeContext(
                new MockStreamingRuntimeContext(false, parallelism, subtaskIndex, environment));
        sink.open(new Configuration());
        return sink;
    }

    private GenericRowData row(long id, String content) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(content));
        return row;
    }

    private void writeRows(LanceSink sink, long... ids) throws Exception {
        for (long id : ids) {
            sink.invoke(row(id, "row-" + id), null);
        }
        sink.flush();
    }

    /** Seeds the dataset with rows that a previous job wrote. */
    private void seedPreviousJob(String path, long... ids) throws Exception {
        LanceSink seed = new LanceSink(options(path, LanceOptions.WriteMode.APPEND), rowType());
        seed.open(new Configuration());
        try {
            writeRows(seed, ids);
        } finally {
            seed.close();
        }
    }

    private long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    @Test
    @DisplayName("two overwrite subtasks of the same job first-write distinct rows without data loss")
    void twoSubtasksConcurrentFirstWrite() throws Exception {
        String path = tempDir.resolve("concurrent_overwrite").toString();
        JobID job = new JobID();

        // Both subtasks open before either writes, as happens when a parallel sink starts.
        LanceSink s1 = openSubtask(path, job, 0, 2);
        LanceSink s2 = openSubtask(path, job, 1, 2);

        try {
            writeRows(s1, 1L);
            assertThat(countRows(path)).isEqualTo(1L);

            writeRows(s2, 2L);
        } finally {
            s1.close();
            s2.close();
        }

        // Subtask 2's first write must add to the dataset the job is producing, not replace
        // subtask 1's rows with its own.
        assertThat(countRows(path)).isEqualTo(2L);
    }

    @Test
    @DisplayName("a subtask that opens after a peer already wrote must not wipe the peer's rows")
    void lateOpeningSubtaskMustNotDeletePeerRows() throws Exception {
        String path = tempDir.resolve("late_open_overwrite").toString();
        JobID job = new JobID();

        LanceSink s1 = openSubtask(path, job, 0, 2);
        try {
            writeRows(s1, 1L);
            assertThat(countRows(path)).isEqualTo(1L);

            // Peer subtask (or a restarted subtask of the same job) opens after s1's first commit.
            LanceSink s2 = openSubtask(path, job, 1, 2);
            try {
                writeRows(s2, 2L);
            } finally {
                s2.close();
            }
        } finally {
            s1.close();
        }

        assertThat(countRows(path)).isEqualTo(2L);
    }

    @Test
    @DisplayName("an overwrite job still replaces the rows written by a previous job")
    void overwriteReplacesPreviousJobRows() throws Exception {
        String path = tempDir.resolve("replace_previous_job").toString();
        seedPreviousJob(path, 10L, 11L, 12L);
        assertThat(countRows(path)).isEqualTo(3L);

        JobID job = new JobID();
        LanceSink s1 = openSubtask(path, job, 0, 2);
        LanceSink s2 = openSubtask(path, job, 1, 2);
        try {
            writeRows(s2, 2L);
            writeRows(s1, 1L);
            // Later flushes of the same subtask append.
            writeRows(s1, 3L);
        } finally {
            s1.close();
            s2.close();
        }

        assertThat(countRows(path)).isEqualTo(3L);

        // Running the overwrite job again replaces the result of the first run.
        LanceSink rerun = openSubtask(path, new JobID(), 0, 1);
        try {
            writeRows(rerun, 4L);
        } finally {
            rerun.close();
        }
        assertThat(countRows(path)).isEqualTo(1L);
    }

    @Test
    @DisplayName("a bounded overwrite job that produces no rows leaves an empty dataset")
    void overwriteWithNoRowsEmptiesDataset() throws Exception {
        String path = tempDir.resolve("empty_overwrite").toString();
        seedPreviousJob(path, 10L, 11L, 12L);

        JobID job = new JobID();
        LanceSink s1 = openSubtask(path, job, 0, 2);
        LanceSink s2 = openSubtask(path, job, 1, 2);
        try {
            s1.finish();
            s2.finish();
        } finally {
            s1.close();
            s2.close();
        }

        assertThat(countRows(path)).isEqualTo(0L);
    }

    @Test
    @DisplayName("overwrite into a directory that holds no committed dataset version yet")
    void overwriteIntoDirectoryWithoutDataset() throws Exception {
        Path dir = tempDir.resolve("no_version_yet");
        // Fragment.write() of a peer subtask creates the data directory before its first commit.
        Files.createDirectories(dir.resolve("data"));
        String path = dir.toString();

        LanceSink s1 = openSubtask(path, new JobID(), 0, 1);
        try {
            writeRows(s1, 1L, 2L);
        } finally {
            s1.close();
        }

        assertThat(countRows(path)).isEqualTo(2L);
    }

    @Test
    @DisplayName("INSERT INTO an overwrite table with parallelism 2 keeps every row of the job")
    void parallelSqlOverwriteKeepsAllRows() throws Exception {
        String path = tempDir.resolve("sql_overwrite").toString();

        // A second run must replace, not add to, the first run's rows.
        assertThat(runSqlOverwrite(path)).isEqualTo(100L);
        assertThat(runSqlOverwrite(path)).isEqualTo(100L);
    }

    private long runSqlOverwrite(String path) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.executeSql("CREATE TABLE src (id BIGINT, content STRING) WITH ("
                + "'connector' = 'datagen', 'number-of-rows' = '100', "
                + "'fields.id.kind' = 'sequence', 'fields.id.start' = '1', "
                + "'fields.id.end' = '100', 'fields.content.length' = '5')");
        // The hadoop.* option only works around validateExcept() rejecting an empty prefix list.
        tEnv.executeSql(String.format("CREATE TABLE snk (id BIGINT, content STRING) WITH ("
                + "'connector' = 'lance', 'path' = '%s', 'write.mode' = 'overwrite', "
                + "'hadoop.test.unused' = 'x')", path));
        tEnv.executeSql("INSERT INTO snk SELECT id, content FROM src").await();
        return countRows(path);
    }
}
