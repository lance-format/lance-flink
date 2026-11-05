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
}
