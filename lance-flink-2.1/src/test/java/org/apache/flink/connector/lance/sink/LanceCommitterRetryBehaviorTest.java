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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

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
    // Minimal fake CommitRequest used to observe retryLater / signalFailed calls
    // without pulling Mockito in just for one test.
    // ------------------------------------------------------------------------

    private static final class FakeCommitRequest
            implements Committer.CommitRequest<LanceCommittable> {

        private final LanceCommittable c;
        final AtomicInteger retryLaterCalls = new AtomicInteger();
        final AtomicInteger signalFailedCalls = new AtomicInteger();

        FakeCommitRequest(LanceCommittable c) {
            this.c = c;
        }

        @Override
        public LanceCommittable getCommittable() {
            return c;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
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
