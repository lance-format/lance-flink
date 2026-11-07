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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.connector.lance.util.LanceOpener;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Lance {@link SourceReader} implementation.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Manages the current {@link LanceSplit} and its underlying {@link LanceScanner} / {@link ArrowReader};
 *   <li>Closes the scanner when a split finishes and switches to the next split in the queue;
 *   <li>Returns {@link InputStatus#END_OF_INPUT} once all splits are consumed and the enumerator has signalled no more splits;
 *   <li>Supports limit push-down: stops immediately once the limit is reached.
 * </ul>
 *
 * <p>Lazy initialization: the constructor does not open the Lance dataset; it is opened on the
 * first {@link #pollNext(ReaderOutput)} that receives a split, avoiding pointless Dataset handles
 * when parallelism exceeds the number of splits.
 *
 * <p>Availability protocol (M1 fix): follows the FLIP-27
 * {@link SourceReader#isAvailable()} contract — when {@link #pollNext} returns
 * {@link InputStatus#NOTHING_AVAILABLE}, {@link #isAvailable()} must return a
 * **not-yet-completed** future, and the runtime re-schedules the reader only after that future
 * completes. This implementation uses a volatile {@link CompletableFuture} field:
 * {@link #resetAvailability()} creates a new incomplete future before {@code pollNext} decides to
 * return {@code NOTHING_AVAILABLE}; {@link #addSplits(List)} and {@link #notifyNoMoreSplits()}
 * complete the current future to wake the runtime when new events arrive.
 */
public class LanceSourceReader implements SourceReader<RowData, LanceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSourceReader.class);

    /** Sentinel: a completed future reused for the initial "data available" state, avoiding a new future per pollNext. */
    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    private final SourceReaderContext context;
    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;
    private final Long readLimit;

    private final Deque<LanceSplit> pendingSplits = new ArrayDeque<>();

    /**
     * The current availability future. Reads/writes happen on the runtime task thread, but
     * {@link #addSplits}/{@link #notifyNoMoreSplits} may be invoked by the enumerator callback
     * thread, so it is declared volatile for visibility.
     */
    private volatile CompletableFuture<Void> availabilityFuture = COMPLETED;

    // Lazy initialization: the Lance dataset is only opened when the first split arrives
    private BufferAllocator allocator;
    private Dataset dataset;
    private RowDataConverter converter;

    // Cursor for the current split
    private LanceSplit currentSplit;
    private LanceScanner currentScanner;
    private ArrowReader currentReader;
    private Iterator<RowData> currentBatchIter = Collections.emptyIterator();

    private long emittedCount;
    private boolean noMoreSplits;

    public LanceSourceReader(
            SourceReaderContext context, LanceOptions options, RowType rowType) {
        this.context = context;
        this.options = options;
        this.rowType = rowType;

        List<String> columns = options.getReadColumns();
        this.selectedColumns =
                columns != null && !columns.isEmpty() ? columns.toArray(new String[0]) : null;
        this.readLimit = options.getReadLimit();
    }

    @Override
    public void start() {
        // Actively request the first split on startup
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (isLimitReached()) {
            closeCurrentSplit();
            return InputStatus.END_OF_INPUT;
        }

        // 1) Consume leftover rows in the current batch first
        if (currentBatchIter.hasNext()) {
            RowData row = currentBatchIter.next();
            output.collect(row);
            emittedCount++;
            return isLimitReached()
                    ? InputStatus.END_OF_INPUT
                    : (currentBatchIter.hasNext() ? InputStatus.MORE_AVAILABLE : advanceBatch());
        }

        // 2) Current batch exhausted: try to pull the next batch
        if (currentReader != null) {
            return advanceBatch();
        }

        // 3) No current scanner: try to switch to the next split
        LanceSplit next = pendingSplits.pollFirst();
        if (next != null) {
            openSplit(next);
            return advanceBatch();
        }

        // 4) No split available
        if (noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        }
        // Request a split from the enumerator; reset the availability future before declaring
        // NOTHING_AVAILABLE so isAvailable() returns an incomplete future and the runtime does
        // not immediately re-schedule this reader (avoids a CPU busy loop). It is completed later
        // by the addSplits/notifyNoMoreSplits callbacks.
        resetAvailability();
        context.sendSplitRequest();
        return InputStatus.NOTHING_AVAILABLE;
    }

    /**
     * Loads the next Arrow batch from the current reader and returns the corresponding InputStatus.
     */
    private InputStatus advanceBatch() throws Exception {
        if (currentReader == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (!currentReader.loadNextBatch()) {
            // Current split fully read
            closeCurrentSplit();
            // Immediately try to switch to the next split (if already queued)
            LanceSplit next = pendingSplits.pollFirst();
            if (next != null) {
                openSplit(next);
                return advanceBatch();
            }
            if (noMoreSplits) {
                return InputStatus.END_OF_INPUT;
            }
            resetAvailability();
            context.sendSplitRequest();
            return InputStatus.NOTHING_AVAILABLE;
        }
        VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
        currentBatchIter = converter.toRowDataList(root).iterator();
        return InputStatus.MORE_AVAILABLE;
    }

    private void openSplit(LanceSplit split) throws Exception {
        this.currentSplit = split;
        ensureDatasetOpen();

        ScanOptions.Builder sb = new ScanOptions.Builder();
        sb.batchSize(options.getReadBatchSize());
        if (selectedColumns != null && selectedColumns.length > 0) {
            sb.columns(Arrays.asList(selectedColumns));
        }

        switch (split.getKind()) {
            case WHOLE_DATASET: {
                // filter / limit go through a dataset-level scan
                String filter = options.getReadFilter();
                if (filter != null && !filter.isEmpty()) {
                    sb.filter(filter);
                }
                this.currentScanner = dataset.newScan(sb.build());
                break;
            }
            case FRAGMENT: {
                Fragment fragment = findFragment(split.getFragmentId());
                this.currentScanner = fragment.newScan(sb.build());
                break;
            }
            default:
                throw new IllegalStateException("Unknown split kind: " + split.getKind());
        }
        this.currentReader = currentScanner.scanBatches();
        this.currentBatchIter = Collections.emptyIterator();
        LOG.debug("Opened split {} on reader {}", split, context.getIndexOfSubtask());
    }

    private Fragment findFragment(long fragmentId) {
        for (Fragment f : dataset.getFragments()) {
            if (f.getId() == fragmentId) {
                return f;
            }
        }
        throw new IllegalStateException(
                "Fragment " + fragmentId + " not found in dataset " + options.getPath());
    }

    private void ensureDatasetOpen() {
        if (dataset != null) {
            return;
        }
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.dataset = LanceOpener.open(options.getPath(), allocator, options);

        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        LOG.info(
                "LanceSourceReader[{}] opened dataset {}", context.getIndexOfSubtask(), options.getPath());
    }

    private void closeCurrentSplit() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception e) {
                LOG.warn("Error closing ArrowReader for split {}", currentSplit, e);
            }
            currentReader = null;
        }
        if (currentScanner != null) {
            try {
                currentScanner.close();
            } catch (Exception e) {
                LOG.warn("Error closing LanceScanner for split {}", currentSplit, e);
            }
            currentScanner = null;
        }
        currentBatchIter = Collections.emptyIterator();
        currentSplit = null;
    }

    private boolean isLimitReached() {
        return readLimit != null && emittedCount >= readLimit;
    }

    /**
     * Creates a new incomplete availability future (if the current one is already completed).
     * It is completed later by {@link #addSplits}/{@link #notifyNoMoreSplits} to wake the runtime.
     */
    private void resetAvailability() {
        CompletableFuture<Void> current = this.availabilityFuture;
        if (current == COMPLETED || current.isDone()) {
            this.availabilityFuture = new CompletableFuture<>();
        }
    }

    /** Wakes the currently blocked availability future, if any. */
    private void completeAvailability() {
        CompletableFuture<Void> current = this.availabilityFuture;
        if (current != null && !current.isDone()) {
            current.complete(null);
        }
    }

    @Override
    public List<LanceSplit> snapshotState(long checkpointId) {
        // The current split plus all pending splits count as unfinished and are handed back to the
        // enumerator for reassignment on failover.
        List<LanceSplit> state = new ArrayList<>(pendingSplits.size() + 1);
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        state.addAll(pendingSplits);
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // Return the current future directly: data available (COMPLETED) → runtime immediately
        // calls pollNext; awaiting split assignment (incomplete future) → runtime suspends this
        // reader until completion.
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<LanceSplit> splits) {
        LOG.debug("Reader {} received {} split(s)", context.getIndexOfSubtask(), splits.size());
        pendingSplits.addAll(splits);
        // New data arrived, wake the runtime to schedule pollNext immediately
        completeAvailability();
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("Reader {} notified: no more splits", context.getIndexOfSubtask());
        this.noMoreSplits = true;
        // Enumerator declared no more splits, wake the runtime so pollNext returns END_OF_INPUT
        completeAvailability();
    }

    @Override
    public void close() throws Exception {
        LOG.info(
                "Closing LanceSourceReader[{}], emitted {} rows",
                context.getIndexOfSubtask(),
                emittedCount);
        closeCurrentSplit();
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("Error closing Lance dataset", e);
            }
            dataset = null;
        }
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing BufferAllocator", e);
            }
            allocator = null;
        }
    }
}
