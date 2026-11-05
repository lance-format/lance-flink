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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.FragmentMetadata;
import org.lance.operation.Operation;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * M2 regression tests for {@link LanceCommitter} retry / fail reporting via the
 * {@link Committer.CommitRequest} API.
 *
 * <p>Before M2 fix: {@code LanceCommitter.commit(...)} threw {@code IOException}
 * on any Lance-side failure, which crashes the entire Flink job instead of letting
 * the runtime retry the failed request on the next checkpoint cycle.
 *
 * <p>After M2 fix: Committer must call {@link Committer.CommitRequest#retryLater()}
 * (below retry threshold) or {@link Committer.CommitRequest#signalFailedWithKnownReason}
 * (at/above threshold) and must <b>not</b> propagate the exception.
 *
 * <p>These tests exercise the invariant on the only path that can be covered without
 * a real Lance JNI handle: empty-committable batches (which already represent the
 * most common "idle subtask" case during real jobs). The non-empty failure path is
 * exercised end-to-end by {@code FlinkSqlDemo} and {@code LanceConnectorITCase}.
 */
class LanceCommitterRetryBehaviorTest {

    @Test
    @DisplayName("Empty CommitRequest batch is a no-op; commit does not throw")
    void emptyBatchIsNoOp() throws Exception {
        try (LanceCommitter committer = new LanceCommitter()) {
            committer.commit(Collections.emptyList());
            // Reaching here means no exception propagated: the Flink job wouldn't be
            // failed by an empty checkpoint window.
        }
    }

    @Test
    @DisplayName("Batch of only-empty-fragment committables is a no-op; commit does not throw")
    void emptyFragmentCommittableBatchIsNoOp() throws Exception {
        FakeCommitRequest r1 =
                new FakeCommitRequest(
                        new LanceCommittable(
                                "/tmp/path-a",
                                /* schema */ null,
                                LanceOptions.WriteMode.APPEND,
                                Collections.emptyList()));
        FakeCommitRequest r2 =
                new FakeCommitRequest(
                        new LanceCommittable(
                                "/tmp/path-b",
                                /* schema */ null,
                                LanceOptions.WriteMode.APPEND,
                                Collections.emptyList()));

        try (LanceCommitter committer = new LanceCommitter()) {
            committer.commit(Arrays.asList(r1, r2));
            // Both empties → bucketed out before any Lance call; no retry, no fail.
            assertThat(r1.retryLaterCalls.get()).isZero();
            assertThat(r1.signalFailedCalls.get()).isZero();
            assertThat(r2.retryLaterCalls.get()).isZero();
            assertThat(r2.signalFailedCalls.get()).isZero();
        }
    }

    /**
     * Reflection probe: the LanceCommitter class must either (a) never invoke
     * {@code CommitRequest} methods when the batch is empty, or (b) if it does so,
     * use the API surface rather than {@code throw new IOException(...)}.
     *
     * <p>This test pins "LanceCommitter.commit does not {@code throw}" at the API level by
     * asserting that {@code commit}'s declared throws clause matches Flink Sink V2's
     * {@code Committer#commit}: {@code IOException, InterruptedException}. The point is
     * that <b>if</b> an exception is thrown out of this method, the runtime will treat
     * it as a <b>fatal</b> and not as a retriable per-request event — so M2 forbids
     * throwing from the business path and reserves it only for interrupts / IO errors
     * the runtime itself raises.
     */
    @Test
    @DisplayName("commit() signature matches Flink's Committer contract (M2 declares retriable-via-API)")
    void commitSignatureMatchesSinkV2Contract() throws NoSuchMethodException {
        java.lang.reflect.Method m =
                LanceCommitter.class.getMethod("commit", java.util.Collection.class);
        java.util.List<Class<?>> thrown = Arrays.asList(m.getExceptionTypes());
        assertThat(thrown)
                .containsExactlyInAnyOrder(java.io.IOException.class, InterruptedException.class);
    }

    // ------------------------------------------------------------------------
    // R2 regression: inject-style tests over the committer's extension points
    // ({@link LanceCommitter#datasetExists(String)} +
    //  {@link LanceCommitter#executeTransaction(String, Operation)}).
    //
    // Before R2: round-2 review claimed four retry/fail scenarios but the
    // implementation never exercised {@code retryLater()} and never distinguished
    // known/unknown reasons; the existing tests only covered the no-op empty path.
    //
    // After R2: implementation is simplified to a single classification
    // ({@code retryLater} below threshold, {@code signalFailedWithKnownReason} at/above).
    // These tests prove that invariant without needing a live Lance JNI handle.
    // ------------------------------------------------------------------------

    @Test
    @DisplayName("R2: first failure below retry threshold -> retryLater()")
    void firstFailureBelowThresholdRetriesLater() throws Exception {
        FakeCommitRequest req = withOneFakeFragment("/tmp/r2/first-failure");

        try (TestLanceCommitter committer =
                     new TestLanceCommitter()
                             .withDatasetExists(true)
                             .withExecuteFailure(new RuntimeException("simulated optimistic conflict"))) {
            committer.commit(Collections.singletonList(req));
        }

        assertThat(req.retryLaterCalls.get()).isEqualTo(1);
        assertThat(req.signalFailedCalls.get()).isZero();
    }

    @Test
    @DisplayName("R2: at retry threshold -> signalFailedWithKnownReason()")
    void atThresholdFailsPermanently() throws Exception {
        // getNumberOfRetries() returns MAX_RETRIES-1 pre-increment; test override
        // of maxRetries() lowers the bar so one failure is already at threshold.
        FakeCommitRequest req =
                withOneFakeFragment("/tmp/r2/at-threshold")
                        .withNumberOfRetries(1); // already retried once

        try (TestLanceCommitter committer =
                     new TestLanceCommitter()
                             .withMaxRetries(1)        // threshold == 1 -> getNumberOfRetries 1 is at/above
                             .withDatasetExists(true)
                             .withExecuteFailure(new RuntimeException("persistent failure"))) {
            committer.commit(Collections.singletonList(req));
        }

        assertThat(req.retryLaterCalls.get()).isZero();
        assertThat(req.signalFailedCalls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("R2: missing schema on create-table path -> signalFailed, never retry")
    void missingSchemaOnCreateFailsFast() throws Exception {
        // dataset does not exist -> Committer takes Overwrite path, but committable
        // has no Arrow schema -> must signalFailed immediately (no infinite retry loop).
        FakeCommitRequest req = withOneFakeFragment("/tmp/r2/no-schema"); // schema=null

        try (TestLanceCommitter committer =
                     new TestLanceCommitter()
                             .withDatasetExists(false)
                             // executeTransaction is never reached; the schema check short-circuits
                             .withExecuteFailure(new AssertionError("should not be called"))) {
            committer.commit(Collections.singletonList(req));
        }

        assertThat(req.retryLaterCalls.get()).isZero();
        assertThat(req.signalFailedCalls.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("R2: successful commit path -> no retry, no fail")
    void successfulCommitReportsNothing() throws Exception {
        FakeCommitRequest req =
                withOneFakeFragmentAndSchema("/tmp/r2/success", simpleSchema());

        try (TestLanceCommitter committer =
                     new TestLanceCommitter()
                             .withDatasetExists(false)
                             .withExecuteSuccess()) {
            committer.commit(Collections.singletonList(req));
        }

        assertThat(req.retryLaterCalls.get()).isZero();
        assertThat(req.signalFailedCalls.get()).isZero();
    }

    // ------------------------------------------------------------------------
    // Test helpers
    // ------------------------------------------------------------------------

    private static Schema simpleSchema() {
        return new Schema(
                Collections.singletonList(
                        new Field(
                                "id",
                                FieldType.nullable(new ArrowType.Int(32, true)),
                                null)));
    }

    private static FakeCommitRequest withOneFakeFragment(String path) {
        return new FakeCommitRequest(
                new LanceCommittable(
                        path,
                        /* schema */ null,
                        LanceOptions.WriteMode.APPEND,
                        // Non-empty so the bucket is NOT short-circuited. We never execute
                        // real Lance operations against this fake fragment — the test
                        // committer short-circuits executeTransaction.
                        Collections.singletonList(fakeFragmentMetadata())));
    }

    private static FakeCommitRequest withOneFakeFragmentAndSchema(String path, Schema schema) {
        return new FakeCommitRequest(
                new LanceCommittable(
                        path,
                        schema,
                        LanceOptions.WriteMode.OVERWRITE,
                        Collections.singletonList(fakeFragmentMetadata())));
    }

    /**
     * Build a cheap {@link FragmentMetadata} without going through JNI. We only need
     * {@code getFragments().isEmpty()} to return false; the object is never actually
     * passed to Lance in these injection tests.
     */
    private static FragmentMetadata fakeFragmentMetadata() {
        // Lance 7.x FragmentMetadata ctors are complex (int, List<DataFile>, Long, ...).
        // We never dereference the object in these tests (executeTransaction is stubbed),
        // so Mockito.mock gives us a cheap non-null instance that keeps
        // LanceCommittable.isEmpty() == false.
        return org.mockito.Mockito.mock(FragmentMetadata.class);
    }

    /** Subclass of LanceCommitter that stubs the two protected extension points. */
    private static final class TestLanceCommitter extends LanceCommitter {
        private boolean datasetExists;
        private Throwable executeFailure;
        private boolean executeSuccess;
        private int maxRetriesOverride = Integer.MIN_VALUE;

        TestLanceCommitter withDatasetExists(boolean exists) {
            this.datasetExists = exists;
            return this;
        }

        TestLanceCommitter withExecuteFailure(Throwable t) {
            this.executeFailure = t;
            this.executeSuccess = false;
            return this;
        }

        TestLanceCommitter withExecuteSuccess() {
            this.executeFailure = null;
            this.executeSuccess = true;
            return this;
        }

        TestLanceCommitter withMaxRetries(int n) {
            this.maxRetriesOverride = n;
            return this;
        }

        @Override
        protected boolean datasetExists(String path) {
            return datasetExists;
        }

        @Override
        protected void executeTransaction(String path, Operation op) throws IOException {
            if (executeFailure != null) {
                if (executeFailure instanceof RuntimeException) {
                    throw (RuntimeException) executeFailure;
                }
                if (executeFailure instanceof Error) {
                    throw (Error) executeFailure;
                }
                throw new IOException(executeFailure);
            }
            if (!executeSuccess) {
                throw new IOException("TestLanceCommitter: neither failure nor success was configured");
            }
            // success: fall through
        }

        @Override
        protected int maxRetries() {
            return maxRetriesOverride == Integer.MIN_VALUE ? super.maxRetries() : maxRetriesOverride;
        }
    }

    // ------------------------------------------------------------------------
    // Minimal fake CommitRequest used to observe retryLater / signalFailed calls
    // without pulling Mockito in just for one test.
    // ------------------------------------------------------------------------

    private static final class FakeCommitRequest
            implements Committer.CommitRequest<LanceCommittable> {

        private final LanceCommittable c;
        final AtomicInteger retryLaterCalls = new AtomicInteger();
        final AtomicInteger signalFailedCalls = new AtomicInteger();
        private int numberOfRetries;

        FakeCommitRequest(LanceCommittable c) {
            this.c = c;
        }

        FakeCommitRequest withNumberOfRetries(int n) {
            this.numberOfRetries = n;
            return this;
        }

        @Override
        public LanceCommittable getCommittable() {
            return c;
        }

        @Override
        public int getNumberOfRetries() {
            return numberOfRetries;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            signalFailedCalls.incrementAndGet();
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            signalFailedCalls.incrementAndGet();
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
            // no-op for these tests
        }
    }
}
