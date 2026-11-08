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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.util.LanceOpener;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.lance.Dataset;
import org.lance.Fragment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Lance {@link SplitEnumerator} implementation.
 *
 * <p>The split strategy mirrors the two paths of the old {@code LanceSource.run()}:
 * <ul>
 *   <li>When a filter or limit is set: produce one {@link LanceSplit.Kind#WHOLE_DATASET} split
 *       handled by a single reader, guaranteeing no duplicated rows (the old implementation
 *       achieved the same effect via "subtask 0 only").
 *   <li>Otherwise: split by the number of Lance Dataset fragments, one split per fragment,
 *       distributed round-robin to requesting readers; overall parallelism ≤ concurrent readers.
 * </ul>
 *
 * <p>This enumerator opens the dataset only once on the JobManager side to read the fragment
 * list, then closes it as soon as splitting is done.
 *
 * <p><b>Async discovery (#89)</b>: {@link #start()} no longer opens the dataset synchronously on
 * the coordinator main thread; instead it schedules {@link #discoverSplits()} onto the coordinator
 * executor's async thread pool via {@link SplitEnumeratorContext#callAsync(java.util.concurrent.Callable,
 * java.util.function.BiConsumer)}. A {@code handleSplitRequest} arriving before discovery completes
 * is buffered in {@code pendingSplitRequests} and replayed in the discovery callback, preventing
 * readers from being permanently fenced by an early {@code signalNoMoreSplits} (round-1 minor m2).
 */
public class LanceSplitEnumerator implements SplitEnumerator<LanceSplit, LanceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSplitEnumerator.class);

    private final SplitEnumeratorContext<LanceSplit> context;
    private final LanceOptions options;
    private final Deque<LanceSplit> remainingSplits;

    /**
     * Whether discovery has completed (successfully or not). Together with
     * {@link #pendingSplitRequests} it decides the behavior of
     * {@link #handleSplitRequest(int, String)}.<br>
     * - restored case: constructor sees non-empty remainingSplits → start() directly marks
     *   discoveryDone=true;<br>
     * - cold-start case: start() triggers callAsync and the callback marks discoveryDone=true
     *   and replays pending requests.
     *
     * <p>All access happens on the coordinator main thread (callAsync's BiConsumer also runs on
     * the main thread), so no volatile is needed.
     */
    private boolean discoveryDone;

    /**
     * Queue of split requests that arrive before discovery completes. Each element corresponds
     * to one reader request. After discovery completes they are replayed all at once so that
     * readers are not prematurely fenced by signalNoMoreSplits.
     */
    private final Deque<Integer> pendingSplitRequests = new ArrayDeque<>();

    public LanceSplitEnumerator(
            SplitEnumeratorContext<LanceSplit> context,
            LanceOptions options,
            @Nullable Collection<LanceSplit> restoredSplits) {
        this.context = context;
        this.options = options;
        this.remainingSplits = new ArrayDeque<>();
        if (restoredSplits != null) {
            this.remainingSplits.addAll(restoredSplits);
        }
    }

    @Override
    public void start() {
        if (!remainingSplits.isEmpty()) {
            // restored enumerator — splits are already in checkpoint state, skip discovery.
            LOG.info("Lance split enumerator restored with {} pending splits", remainingSplits.size());
            discoveryDone = true;
            return;
        }

        // cold start — schedule discovery onto the coordinator's async executor to avoid
        // blocking the main thread. The callback runs on the coordinator main thread
        // (guaranteed by Flink), so it is safe to read/write the plain fields
        // discoveryDone / remainingSplits / pendingSplitRequests.
        LOG.info("Lance split enumerator scheduling async discovery for dataset {}", options.getPath());
        context.callAsync(this::discoverSplits, this::onDiscoveryComplete);
    }

    /** callAsync's callback — runs on the coordinator main thread. */
    private void onDiscoveryComplete(List<LanceSplit> splits, Throwable err) {
        discoveryDone = true;
        if (err != null) {
            // Re-throw to the coordinator so the runtime fails / restarts the whole Source.
            // Clear the pending queue immediately to avoid leaving stale data across restart loops.
            pendingSplitRequests.clear();
            throw new RuntimeException(
                    "Failed to discover Lance splits for " + options.getPath(), err);
        }
        LOG.info(
                "Lance split enumerator discovered {} split(s) for dataset {}",
                splits.size(),
                options.getPath());
        remainingSplits.addAll(splits);

        // Replay the split requests that queued up during discovery, attempting allocation in
        // arrival order; handleSplitRequest itself signals NoMoreSplits when the queue is empty,
        // so that logic doesn't need to be duplicated here.
        while (!pendingSplitRequests.isEmpty()) {
            int subtaskId = pendingSplitRequests.pollFirst();
            handleSplitRequest(subtaskId, null);
        }
    }

    /**
     * Opens the dataset once to compute the split plan; filter / limit scenarios directly
     * produce a single WHOLE_DATASET split.
     *
     * <p>protected so unit tests can inject a fake split result (same design as R2);
     * do not call it from subclasses in production code.
     */
    protected List<LanceSplit> discoverSplits() {
        String filter = options.getReadFilter();
        boolean hasFilter = filter != null && !filter.isEmpty();
        boolean hasLimit = options.getReadLimit() != null;
        if (hasFilter || hasLimit) {
            return Collections.singletonList(LanceSplit.wholeDataset());
        }

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset dataset = LanceOpener.open(options.getPath(), allocator, options)) {
            List<Fragment> fragments = dataset.getFragments();
            List<LanceSplit> splits = new ArrayList<>(fragments.size());
            for (Fragment fragment : fragments) {
                splits.add(LanceSplit.fragment(fragment.getId()));
            }
            return splits;
        } catch (Exception e) {
            throw new RuntimeException("Failed to discover Lance splits: " + options.getPath(), e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader not registered yet, ignore; it will request again once registered
            return;
        }
        if (!discoveryDone) {
            // discovery still in progress — buffer for now, replay in the callback. Do not go
            // down the signalNoMoreSplits path, to avoid prematurely fencing the reader
            // (round-1 minor m2).
            LOG.debug("Discovery not complete, enqueueing split request from reader {}", subtaskId);
            pendingSplitRequests.addLast(subtaskId);
            return;
        }
        LanceSplit next = remainingSplits.pollFirst();
        if (next != null) {
            LOG.debug("Assigning split {} to reader {}", next, subtaskId);
            context.assignSplit(next, subtaskId);
        } else {
            LOG.debug("No more splits for reader {}, signalling no-more", subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<LanceSplit> splits, int subtaskId) {
        LOG.info("Reader {} failed, {} split(s) returned to pool", subtaskId, splits.size());
        // return to the head of the queue for priority re-assignment
        for (int i = splits.size() - 1; i >= 0; i--) {
            remainingSplits.addFirst(splits.get(i));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // this implementation uses a "reader pull" model (lazy assignment) and does not
        // actively hand out splits here
    }

    @Override
    public LanceEnumState snapshotState(long checkpointId) {
        return new LanceEnumState(remainingSplits);
    }

    @Override
    public void close() {
        // no persistent resources
    }
}
