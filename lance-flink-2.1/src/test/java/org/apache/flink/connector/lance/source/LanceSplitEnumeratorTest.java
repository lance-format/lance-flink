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

package org.apache.flink.connector.lance.source;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.lance.config.LanceOptions;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceSplitEnumerator tests using Flink's {@link MockSplitEnumeratorContext}.
 *
 * <p>B2 (maintainer review PR#86): enumerator snapshot/restore roundtrip and
 * split-assignment correctness were previously uncovered. These tests exercise
 * only the restore path (which does not open a real Lance dataset) because
 * discoverSplits in {@code start()} requires a live dataset and belongs to the
 * end-to-end tests in {@code FlinkSqlDemo}.
 */
class LanceSplitEnumeratorTest {

    private static LanceOptions dummyOptions() {
        return LanceOptions.builder().path("/tmp/does-not-matter-for-restored-path").build();
    }

    @Test
    @DisplayName("Restored enumerator exposes the pre-recorded splits and does not re-discover")
    void restoreUsesPreRecordedSplits() throws Exception {
        List<LanceSplit> restored =
                Arrays.asList(LanceSplit.fragment(0L), LanceSplit.fragment(1L), LanceSplit.fragment(2L));

        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), restored)) {

            // Restoring: start() should NOT touch the dataset path at all, since
            // remainingSplits is already populated.
            enumerator.start();

            // Pretend a reader comes online and starts asking for splits.
            ctx.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");

            // Three fragment splits should have been dispatched to subtask 0 in order.
            assertThat(ctx.getSplitsAssignmentSequence()).hasSize(3);
            assertThat(ctx.getSplitsAssignmentSequence().get(0).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(0L);
            assertThat(ctx.getSplitsAssignmentSequence().get(1).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(1L);
            assertThat(ctx.getSplitsAssignmentSequence().get(2).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(2L);

            // Next request: no splits remain → enumerator signals no-more.
            enumerator.handleSplitRequest(0, "localhost");
            // Signalled: internal flag flips, no new assignment appended.
            assertThat(ctx.getSplitsAssignmentSequence()).hasSize(3);
        }
    }

    @Test
    @DisplayName("snapshotState captures remaining splits for failover")
    void snapshotStateCapturesRemaining() throws Exception {
        List<LanceSplit> restored =
                Arrays.asList(LanceSplit.fragment(0L), LanceSplit.fragment(1L));

        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), restored)) {

            enumerator.start();

            // Drain one split, keep one remaining.
            ctx.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.handleSplitRequest(0, "localhost");

            LanceEnumState snapshot = enumerator.snapshotState(42L);
            assertThat(snapshot.getRemainingSplits()).hasSize(1);
            assertThat(snapshot.getRemainingSplits().get(0).getFragmentId()).isEqualTo(1L);
        }
    }

    @Test
    @DisplayName("addSplitsBack returns splits to the pool for failover re-assignment")
    void addSplitsBackPrioritisesReturnedSplits() throws Exception {
        List<LanceSplit> restored =
                Arrays.asList(LanceSplit.fragment(10L), LanceSplit.fragment(20L));

        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), restored)) {

            enumerator.start();

            // A failed reader returns its in-flight split; it should be re-assigned
            // ahead of the un-dispatched one (returned-first ordering).
            enumerator.addSplitsBack(
                    java.util.Collections.singletonList(LanceSplit.fragment(99L)), 0);

            ctx.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.handleSplitRequest(0, "localhost");

            assertThat(ctx.getSplitsAssignmentSequence()).hasSize(1);
            assertThat(ctx.getSplitsAssignmentSequence().get(0).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(99L);
        }
    }

    @Test
    @DisplayName("Reader not yet registered → request is dropped silently")
    void unknownReaderRequestIsIgnored() throws Exception {
        List<LanceSplit> restored = java.util.Collections.singletonList(LanceSplit.fragment(0L));

        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), restored)) {

            enumerator.start();

            // Reader 0 never registered — the request should be a no-op.
            enumerator.handleSplitRequest(0, "localhost");
            assertThat(ctx.getSplitsAssignmentSequence()).isEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // #89 regression: cold-start Async discovery
    //
    // start() must schedule discoverSplits() via callAsync (not block the
    // coordinator main thread). handleSplitRequest arriving before discovery
    // completes must park in pendingSplitRequests, not emit signalNoMoreSplits
    // (round-1 minor m2). When discovery completes, pending requests must be
    // replayed in FIFO order.
    // ------------------------------------------------------------------------

    @Test
    @DisplayName("#89: cold start schedules discovery via callAsync, not inline")
    void coldStartSchedulesDiscoveryAsync() throws Exception {
        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), /* restoredSplits */ null)) {

            // start() must NOT touch the dataset synchronously; it only schedules
            // the discovery callable. If it blocked, we'd never reach the next line
            // for an unreachable path.
            enumerator.start();

            // One callable queued on the async executor, exactly.
            assertThat(ctx.getOneTimeCallables()).hasSize(1);
        }
    }

    @Test
    @DisplayName("#89: handleSplitRequest before discovery completes parks in pendingSplitRequests")
    void pendingSplitRequestIsParkedAndReplayed() throws Exception {
        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             LanceSplitEnumerator enumerator =
                     new LanceSplitEnumerator(ctx, dummyOptions(), /* restoredSplits */ null)) {

            enumerator.start();
            // callAsync scheduled; discovery has NOT yet run.

            // Reader registers and requests a split before discovery completes.
            ctx.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.handleSplitRequest(0, "localhost");

            // No assignment yet (discovery still pending); the request is parked.
            // Critically: no signalNoMoreSplits emitted either — the reader is NOT fenced.
            assertThat(ctx.getSplitsAssignmentSequence()).isEmpty();
            assertThat(ctx.hasNoMoreSplits(0)).isFalse();

            // Drive the async callable. discoverSplits will throw because the
            // dummy path points to a non-existent dataset; the enumerator must
            // propagate the exception to the coordinator (test verifies this via
            // the try/catch-and-assert pattern below).
            assertThatThrownBy(ctx::runNextOneTimeCallable)
                    .isInstanceOf(Throwable.class)
                    .hasMessageContaining("Failed to discover Lance splits");

            // After failure: parked request was cleared, nothing hangs.
            // (An alternate test below covers the success path with a fake context.)
        }
    }

    @Test
    @DisplayName("#89: successful async discovery replays pending requests and dispatches splits")
    void successfulDiscoveryReplaysPending() throws Throwable {
        // We can't open a real Lance dataset in this unit test, so we use a
        // TestLanceSplitEnumerator that stubs out discoverSplits().
        List<LanceSplit> fakeSplits =
                Arrays.asList(LanceSplit.fragment(7L), LanceSplit.fragment(8L));

        try (MockSplitEnumeratorContext<LanceSplit> ctx = new MockSplitEnumeratorContext<>(1);
             TestLanceSplitEnumerator enumerator =
                     new TestLanceSplitEnumerator(ctx, dummyOptions(), fakeSplits)) {

            enumerator.start();

            // Two readers race in requesting splits before discovery finishes.
            ctx.registerReader(new ReaderInfo(0, "localhost"));
            enumerator.handleSplitRequest(0, "localhost");
            enumerator.handleSplitRequest(0, "localhost");

            // Still no assignment: discovery hasn't completed.
            assertThat(ctx.getSplitsAssignmentSequence()).isEmpty();

            // Drive the discovery callable.
            ctx.runNextOneTimeCallable();

            // Both parked requests replayed; both fake splits dispatched in order.
            assertThat(ctx.getSplitsAssignmentSequence()).hasSize(2);
            assertThat(ctx.getSplitsAssignmentSequence().get(0).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(7L);
            assertThat(ctx.getSplitsAssignmentSequence().get(1).assignment().get(0).get(0).getFragmentId())
                    .isEqualTo(8L);
        }
    }

    /** Test-only subclass that bypasses the real Lance JNI in discoverSplits(). */
    private static final class TestLanceSplitEnumerator extends LanceSplitEnumerator {
        private final List<LanceSplit> fakeSplits;

        TestLanceSplitEnumerator(
                MockSplitEnumeratorContext<LanceSplit> ctx,
                LanceOptions options,
                List<LanceSplit> fakeSplits) {
            super(ctx, options, /* restoredSplits */ null);
            this.fakeSplits = fakeSplits;
        }

        @Override
        protected List<LanceSplit> discoverSplits() {
            return fakeSplits;
        }
    }
}
