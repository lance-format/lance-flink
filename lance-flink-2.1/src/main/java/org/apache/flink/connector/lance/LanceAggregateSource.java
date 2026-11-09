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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.lance.aggregate.AggregateExecutor;
import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.connector.lance.source.LanceEnumState;
import org.apache.flink.connector.lance.source.LanceEnumStateSerializer;
import org.apache.flink.connector.lance.source.LanceSplit;
import org.apache.flink.connector.lance.source.LanceSplitSerializer;
import org.apache.flink.connector.lance.util.LanceOpener;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
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

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Lance aggregate-pushdown source (Flink 2.x version).
 *
 * <p>Keeps semantics consistent with the old implementation: full scan -> accumulate -> emit the
 * aggregate result once. Aggregation is global and cannot be accumulated in parallel
 * (AggregateExecutor is not thread-safe and results must be merged), so a single
 * {@link LanceSplit.Kind#WHOLE_DATASET} split is emitted and handled only by the parallelism-0
 * reader; other readers receive noMoreSplits and terminate immediately — equivalent to the old
 * {@code subtaskIndex != 0 skip}.
 */
public class LanceAggregateSource implements Source<RowData, LanceSplit, LanceEnumState> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceAggregateSource.class);

    private final LanceOptions options;
    private final RowType sourceRowType;
    private final AggregateInfo aggregateInfo;

    public LanceAggregateSource(
            LanceOptions options, RowType sourceRowType, AggregateInfo aggregateInfo) {
        this.options = options;
        this.sourceRowType = sourceRowType;
        this.aggregateInfo = aggregateInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, LanceSplit> createReader(SourceReaderContext readerContext) {
        return new AggregateReader(readerContext, options, sourceRowType, aggregateInfo);
    }

    @Override
    public SplitEnumerator<LanceSplit, LanceEnumState> createEnumerator(
            SplitEnumeratorContext<LanceSplit> enumContext) {
        return new SingleWholeDatasetEnumerator(enumContext, null);
    }

    @Override
    public SplitEnumerator<LanceSplit, LanceEnumState> restoreEnumerator(
            SplitEnumeratorContext<LanceSplit> enumContext, LanceEnumState checkpoint) {
        return new SingleWholeDatasetEnumerator(enumContext, checkpoint.getRemainingSplits());
    }

    @Override
    public SimpleVersionedSerializer<LanceSplit> getSplitSerializer() {
        return LanceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<LanceEnumState> getEnumeratorCheckpointSerializer() {
        return LanceEnumStateSerializer.INSTANCE;
    }

    public AggregateInfo getAggregateInfo() {
        return aggregateInfo;
    }

    public LanceOptions getOptions() {
        return options;
    }

    public RowType getSourceRowType() {
        return sourceRowType;
    }

    public RowType getResultRowType() {
        // Keep consistent with the old implementation: statically construct an executor before the
        // reader actually initializes to derive the result RowType.
        RowType srt = sourceRowType;
        if (srt == null) {
            // Deriving requires the dataset; callers should pre-specify sourceRowType.
            return null;
        }
        AggregateExecutor exec = new AggregateExecutor(aggregateInfo, srt);
        return exec.buildResultRowType();
    }

    // ========================================================================
    // Enumerator: dispatch a single WHOLE_DATASET split to reader 0; other readers immediately
    // receive noMoreSplits.
    // ========================================================================
    private static class SingleWholeDatasetEnumerator
            implements SplitEnumerator<LanceSplit, LanceEnumState> {

        private final SplitEnumeratorContext<LanceSplit> context;
        private boolean dispatched;

        SingleWholeDatasetEnumerator(
                SplitEnumeratorContext<LanceSplit> context,
                @Nullable Collection<LanceSplit> restored) {
            this.context = context;
            this.dispatched = restored != null && restored.isEmpty();
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            if (!context.registeredReaders().containsKey(subtaskId)) {
                return;
            }
            if (subtaskId == 0 && !dispatched) {
                context.assignSplit(LanceSplit.wholeDataset(), subtaskId);
                dispatched = true;
            } else {
                context.signalNoMoreSplits(subtaskId);
            }
        }

        @Override
        public void addSplitsBack(List<LanceSplit> splits, int subtaskId) {
            if (!splits.isEmpty()) {
                dispatched = false;
            }
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public LanceEnumState snapshotState(long checkpointId) {
            return new LanceEnumState(
                    dispatched ? Collections.emptyList() : Collections.singletonList(LanceSplit.wholeDataset()));
        }

        @Override
        public void close() {}
    }

    // ========================================================================
    // Reader: after receiving a split, full scan + accumulate, emit the aggregate result on EOF.
    // ========================================================================
    private static class AggregateReader implements SourceReader<RowData, LanceSplit> {

        private final SourceReaderContext context;
        private final LanceOptions options;
        private final RowType sourceRowType;
        private final AggregateInfo aggregateInfo;
        private final String[] selectedColumns;
        private final Deque<LanceSplit> pending = new ArrayDeque<>();

        private BufferAllocator allocator;
        private Dataset dataset;
        private RowDataConverter converter;
        private AggregateExecutor executor;

        private boolean noMoreSplits;
        private boolean aggregateDone;
        private Iterator<RowData> resultIter = Collections.emptyIterator();

        AggregateReader(
                SourceReaderContext context,
                LanceOptions options,
                RowType sourceRowType,
                AggregateInfo aggregateInfo) {
            this.context = context;
            this.options = options;
            this.sourceRowType = sourceRowType;
            this.aggregateInfo = aggregateInfo;

            List<String> requiredColumns = aggregateInfo.getRequiredColumns();
            this.selectedColumns =
                    requiredColumns.isEmpty() ? null : requiredColumns.toArray(new String[0]);
        }

        @Override
        public void start() {
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
            if (resultIter.hasNext()) {
                output.collect(resultIter.next());
                return resultIter.hasNext() ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
            }
            if (aggregateDone) {
                return InputStatus.END_OF_INPUT;
            }
            LanceSplit split = pending.pollFirst();
            if (split != null) {
                runAggregate();
                aggregateDone = true;
                resultIter = executor.getResults().iterator();
                return resultIter.hasNext() ? InputStatus.MORE_AVAILABLE : InputStatus.END_OF_INPUT;
            }
            if (noMoreSplits) {
                // Readers other than reader 0 take this path: no split assigned, EOF directly
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        private void runAggregate() throws Exception {
            ensureDatasetOpen();

            String filter = options.getReadFilter();
            boolean hasFilter = filter != null && !filter.isEmpty();

            if (hasFilter) {
                scanDatasetAndAccumulate(filter);
            } else {
                for (Fragment f : dataset.getFragments()) {
                    scanFragmentAndAccumulate(f);
                }
            }
            LOG.info("LanceAggregateSource: aggregation completed, result rows={}",
                    executor.getResults().size());
        }

        private void scanDatasetAndAccumulate(String filter) throws Exception {
            ScanOptions.Builder sb = new ScanOptions.Builder();
            sb.batchSize(options.getReadBatchSize());
            if (selectedColumns != null && selectedColumns.length > 0) {
                sb.columns(Arrays.asList(selectedColumns));
            }
            sb.filter(filter);
            try (LanceScanner scanner = dataset.newScan(sb.build());
                 ArrowReader reader = scanner.scanBatches()) {
                drain(reader);
            }
        }

        private void scanFragmentAndAccumulate(Fragment fragment) throws Exception {
            ScanOptions.Builder sb = new ScanOptions.Builder();
            sb.batchSize(options.getReadBatchSize());
            if (selectedColumns != null && selectedColumns.length > 0) {
                sb.columns(Arrays.asList(selectedColumns));
            }
            try (LanceScanner scanner = fragment.newScan(sb.build());
                 ArrowReader reader = scanner.scanBatches()) {
                drain(reader);
            }
        }

        private void drain(ArrowReader reader) throws Exception {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                for (RowData row : converter.toRowDataList(root)) {
                    executor.accumulate(row);
                }
            }
        }

        private void ensureDatasetOpen() {
            if (dataset != null) return;
            this.allocator = new RootAllocator(Long.MAX_VALUE);
            this.dataset = LanceOpener.open(options.getPath(), allocator, options);
            RowType rt = sourceRowType;
            if (rt == null) {
                Schema sc = dataset.getSchema();
                rt = LanceTypeConverter.toFlinkRowType(sc);
            }
            // The scanner returns only the selected columns; both the converter and the aggregate
            // executor must index into that narrower layout, not the full table schema.
            RowType readRowType = projectedRowType(rt);
            this.converter = new RowDataConverter(readRowType);
            this.executor = new AggregateExecutor(aggregateInfo, readRowType);
            this.executor.init();
        }

        /** Builds the {@link RowType} describing the columns actually read from Lance. */
        private RowType projectedRowType(RowType fullRowType) {
            if (selectedColumns == null || selectedColumns.length == 0) {
                return fullRowType;
            }
            List<RowType.RowField> fields = new ArrayList<>();
            for (String column : selectedColumns) {
                int index = fullRowType.getFieldNames().indexOf(column);
                if (index >= 0) {
                    fields.add(fullRowType.getFields().get(index));
                }
            }
            return fields.isEmpty() ? fullRowType : new RowType(fields);
        }

        @Override
        public List<LanceSplit> snapshotState(long checkpointId) {
            // Aggregation is global; on mid-run failover, put the current split back to re-run it
            List<LanceSplit> state = new ArrayList<>(pending);
            if (!aggregateDone && !pending.isEmpty()) {
                return state;
            }
            return state;
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void addSplits(List<LanceSplit> splits) {
            pending.addAll(splits);
        }

        @Override
        public void notifyNoMoreSplits() {
            noMoreSplits = true;
        }

        @Override
        public void close() throws Exception {
            if (executor != null) {
                executor.reset();
                executor = null;
            }
            if (dataset != null) {
                try { dataset.close(); } catch (Exception e) {
                    LOG.warn("Error closing Lance dataset", e);
                }
                dataset = null;
            }
            if (allocator != null) {
                try { allocator.close(); } catch (Exception e) {
                    LOG.warn("Error closing BufferAllocator", e);
                }
                allocator = null;
            }
        }
    }
}
