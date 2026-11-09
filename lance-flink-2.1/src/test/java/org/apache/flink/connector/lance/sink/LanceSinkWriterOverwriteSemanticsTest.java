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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the {@code write.mode=overwrite} replace-once semantics of the Flink 2.1
 * Sink V2 path.
 *
 * <p>After the fix {@link LanceSinkWriter} no longer runs a {@code Dataset.drop} pre-cleanup at
 * construction time. The "replace the dataset once per job" decision moved to the serial
 * {@link LanceCommitter}, which tags every version it commits with the {@code flink.job-id}
 * transaction property.
 *
 * <p>Covered scenarios:
 *
 * <ul>
 *   <li>a late-starting subtask 0 no longer deletes the {@code .lance} files a peer subtask has
 *       already materialized;
 *   <li>a parallel overwrite job keeps every subtask's rows;
 *   <li>a second run of an overwrite job replaces the first run's result;
 *   <li>later checkpoints of the same job append instead of overwriting again;
 *   <li>a failure while reading the latest version's properties is retried instead of being
 *       misread as "this job has not replaced the dataset yet".
 * </ul>
 */
class LanceSinkWriterOverwriteSemanticsTest {

    @TempDir Path tempDir;

    private static final RowType ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("id", new BigIntType(false)),
                            new RowType.RowField(
                                    "content", new VarCharType(true, VarCharType.MAX_LENGTH))));

    private LanceOptions options(String path, LanceOptions.WriteMode mode) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(1024)
                .writeMode(mode)
                .build();
    }

    private LanceOptions overwriteOptions(String path) {
        return options(path, LanceOptions.WriteMode.OVERWRITE);
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

    private static long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    private LanceCommittable writeCommittable(LanceOptions opts, int subtaskId, long... ids)
            throws Exception {
        try (LanceSinkWriter writer = new LanceSinkWriter(opts, ROW_TYPE, subtaskId)) {
            for (long id : ids) {
                writer.write(row(id, "row-" + id), null);
            }
            return writer.prepareCommit().iterator().next();
        }
    }

    /**
     * Seeds an existing dataset that already has a manifest via APPEND mode.
     *
     * <p>This is the precondition for the race: the removed {@code Dataset.drop} only ran when
     * {@code Dataset.open(path)} succeeded, i.e. only for an already-committed dataset.
     */
    private void seedDataset(String path, long... ids) throws Exception {
        LanceCommittable c =
                writeCommittable(options(path, LanceOptions.WriteMode.APPEND), 0, ids);
        try (LanceCommitter committer = new LanceCommitter()) {
            committer.commit(Collections.singletonList(new FakeCommitRequest(c)));
        }
    }

    @Test
    @DisplayName("A late-starting subtask 0 no longer drops peer subtasks' materialized files")
    void subtaskZeroLateStartMustNotDropPeerFragments() throws Exception {
        Path path = tempDir.resolve("race_peer_fragments");
        seedDataset(path.toString(), 100L);
        long seedFileCount = countLanceFiles(path);
        assertThat(seedFileCount).isPositive();

        // Subtask 1 starts first and materializes a fragment (real Lance JNI write).
        try (LanceSinkWriter peer =
                new LanceSinkWriter(overwriteOptions(path.toString()), ROW_TYPE, 1)) {
            peer.write(row(1L, "one"), null);
            peer.prepareCommit();
        }
        long filesBefore = countLanceFiles(path);
        assertThat(filesBefore - seedFileCount).isPositive();

        // Subtask 0 starts later: after the fix it has no drop side effect at all.
        try (LanceSinkWriter subtask0 =
                new LanceSinkWriter(overwriteOptions(path.toString()), ROW_TYPE, 0)) {
            // Constructing the writer must not touch the dataset directory.
        }

        assertThat(countLanceFiles(path)).isEqualTo(filesBefore);
    }

    @Test
    @DisplayName("A parallel overwrite job keeps every subtask's rows")
    void overwriteParallelKeepsAllRows() throws Exception {
        Path path = tempDir.resolve("overwrite_parallel");
        seedDataset(path.toString(), 100L, 101L, 102L);
        assertThat(countRows(path.toString())).isEqualTo(3L);

        // Subtask 1 materializes before subtask 0 exists, mimicking skewed start-up order.
        LanceCommittable c1 = writeCommittable(overwriteOptions(path.toString()), 1, 2L, 3L);
        LanceCommittable c0 = writeCommittable(overwriteOptions(path.toString()), 0, 1L);

        FakeCommitRequest r1 = new FakeCommitRequest(c1);
        FakeCommitRequest r0 = new FakeCommitRequest(c0);
        try (LanceCommitter committer = new LanceCommitter(new JobID().toString())) {
            committer.commit(Arrays.asList(r1, r0));
        }

        assertThat(r0.signalFailedCalls.get()).isZero();
        assertThat(r1.signalFailedCalls.get()).isZero();
        // The three seeded rows are replaced by the job's three rows (2 + 1).
        assertThat(countRows(path.toString())).isEqualTo(3L);
    }

    @Test
    @DisplayName("A second run of an overwrite job replaces the first run's result")
    void overwriteSecondRunReplacesFirst() throws Exception {
        Path path = tempDir.resolve("overwrite_rerun");

        try (LanceCommitter committer = new LanceCommitter(new JobID().toString())) {
            committer.commit(
                    Collections.singletonList(
                            new FakeCommitRequest(
                                    writeCommittable(
                                            overwriteOptions(path.toString()), 0, 1L, 2L))));
        }
        assertThat(countRows(path.toString())).isEqualTo(2L);

        // A new submission gets a new job id, so it replaces the previous result again.
        try (LanceCommitter committer = new LanceCommitter(new JobID().toString())) {
            committer.commit(
                    Collections.singletonList(
                            new FakeCommitRequest(
                                    writeCommittable(overwriteOptions(path.toString()), 0, 3L))));
        }
        assertThat(countRows(path.toString())).isEqualTo(1L);
    }

    @Test
    @DisplayName("Later checkpoints of the same overwrite job append instead of overwriting again")
    void laterCheckpointsOfSameJobAppend() throws Exception {
        Path path = tempDir.resolve("overwrite_multiple_checkpoints");
        String jobId = new JobID().toString();

        // Checkpoint 1: replaces (creates) the dataset with 1 row.
        try (LanceCommitter committer = new LanceCommitter(jobId)) {
            committer.commit(
                    Collections.singletonList(
                            new FakeCommitRequest(
                                    writeCommittable(overwriteOptions(path.toString()), 0, 1L))));
        }
        assertThat(countRows(path.toString())).isEqualTo(1L);

        // Checkpoints 2 and 3 must append; if the tags written by an append were dropped, the
        // third commit would see an untagged latest version and overwrite rows 1 and 2.
        for (long id : new long[] {2L, 3L}) {
            try (LanceCommitter committer = new LanceCommitter(jobId)) {
                committer.commit(
                        Collections.singletonList(
                                new FakeCommitRequest(
                                        writeCommittable(
                                                overwriteOptions(path.toString()), 0, id))));
            }
        }

        assertThat(countRows(path.toString())).isEqualTo(3L);
    }

    @Test
    @DisplayName("A failure reading the latest version is retried, never treated as a replace")
    void unreadableLatestVersionIsRetried() throws Exception {
        Path path = tempDir.resolve("overwrite_read_failure");
        seedDataset(path.toString(), 1L, 2L, 3L);

        FakeCommitRequest req =
                new FakeCommitRequest(
                        writeCommittable(overwriteOptions(path.toString()), 0, 4L));

        try (FailingReadCommitter committer = new FailingReadCommitter(new JobID().toString())) {
            committer.commit(Collections.singletonList(req));
        }

        assertThat(req.signalFailedCalls.get()).isZero();
        assertThat(req.retryLaterCalls.get()).isEqualTo(1);
        // The seeded data is untouched because the destructive Overwrite never ran.
        assertThat(countRows(path.toString())).isEqualTo(3L);
    }

    /** Committer whose "read the latest version properties" step fails with an I/O error. */
    private static final class FailingReadCommitter extends LanceCommitter {
        FailingReadCommitter(String jobId) {
            super(jobId);
        }

        @Override
        protected boolean datasetExists(String path) {
            return true;
        }

        @Override
        protected boolean latestVersionCommittedByThisJob(String path) throws IOException {
            throw new IOException("simulated transient object-store failure");
        }

        @Override
        protected void executeTransaction(
                String path, org.lance.operation.Operation op, java.util.Map<String, String> props)
                throws IOException {
            throw new AssertionError("Overwrite must not be executed when the tag cannot be read");
        }
    }

    /** Minimal CommitRequest used to observe the commit outcome. */
    private static final class FakeCommitRequest
            implements Committer.CommitRequest<LanceCommittable> {

        private final LanceCommittable committable;
        final AtomicInteger retryLaterCalls = new AtomicInteger();
        final AtomicInteger signalFailedCalls = new AtomicInteger();

        FakeCommitRequest(LanceCommittable committable) {
            this.committable = committable;
        }

        @Override
        public LanceCommittable getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void retryLater() {
            retryLaterCalls.incrementAndGet();
        }

        @Override
        public void updateAndRetryLater(LanceCommittable committable) {
            retryLaterCalls.incrementAndGet();
        }

        @Override
        public void signalAlreadyCommitted() {
            // no-op
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            signalFailedCalls.incrementAndGet();
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            signalFailedCalls.incrementAndGet();
        }
    }
}
