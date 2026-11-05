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
 * Lance {@link SourceReader} 实现。
 *
 * <p>职责：
 * <ul>
 *   <li>管理当前正在读取的 {@link LanceSplit} 及其底层 {@link LanceScanner} / {@link ArrowReader}；
 *   <li>一个 split 结束后自动关闭 scanner 并切换到队列中的下一个 split；
 *   <li>所有 split 都读完且 enumerator 已 signalNoMoreSplits 时返回 {@link InputStatus#END_OF_INPUT}；
 *   <li>supports limit push-down：命中 limit 立即结束。
 * </ul>
 *
 * <p>懒加载策略：构造函数不打开 Lance dataset，首次 {@link #pollNext(ReaderOutput)} 收到
 * split 时才打开，避免并行度大于 split 数时产生无谓的 Dataset 句柄。
 *
 * <p>Availability 协议（M1 修复）：遵守 FLIP-27 的
 * {@link SourceReader#isAvailable()} 契约——当 {@link #pollNext} 返回
 * {@link InputStatus#NOTHING_AVAILABLE} 时，{@link #isAvailable()} 必须返回一个
 * **尚未 complete** 的 future，runtime 看到该 future 完成后才会重新调度 reader。
 * 本实现用 volatile {@link CompletableFuture} 字段：{@link #resetAvailability()} 在
 * {@code pollNext} 决定返回 {@code NOTHING_AVAILABLE} 之前新建一个 incomplete future；
 * {@link #addSplits(List)} 和 {@link #notifyNoMoreSplits()} 收到新事件时 complete 当前
 * future 唤醒 runtime。
 */
public class LanceSourceReader implements SourceReader<RowData, LanceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSourceReader.class);

    /** 哨兵：已完成的 future，供"有数据可用"的初态复用，避免每次 pollNext 都 new。 */
    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    private final SourceReaderContext context;
    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;
    private final Long readLimit;

    private final Deque<LanceSplit> pendingSplits = new ArrayDeque<>();

    /**
     * 当前的 availability future。读写都在 runtime 的 task thread 上发生，但
     * {@link #addSplits}/{@link #notifyNoMoreSplits} 可能被 enumerator 回调线程调用，
     * 故声明为 volatile 以保证可见性。
     */
    private volatile CompletableFuture<Void> availabilityFuture = COMPLETED;

    // 延迟初始化：首个 split 到达时才真正打开 Lance dataset
    private BufferAllocator allocator;
    private Dataset dataset;
    private RowDataConverter converter;

    // 当前 split 的游标
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
        // 启动时主动请求第一个 split
        context.sendSplitRequest();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (isLimitReached()) {
            closeCurrentSplit();
            return InputStatus.END_OF_INPUT;
        }

        // 1) 若当前 batch 还有残留行，优先消费
        if (currentBatchIter.hasNext()) {
            RowData row = currentBatchIter.next();
            output.collect(row);
            emittedCount++;
            return isLimitReached()
                    ? InputStatus.END_OF_INPUT
                    : (currentBatchIter.hasNext() ? InputStatus.MORE_AVAILABLE : advanceBatch());
        }

        // 2) 当前 batch 空：尝试拉下一个 batch
        if (currentReader != null) {
            return advanceBatch();
        }

        // 3) 当前没有 scanner：尝试切换到下一个 split
        LanceSplit next = pendingSplits.pollFirst();
        if (next != null) {
            openSplit(next);
            return advanceBatch();
        }

        // 4) 没有 split 可用
        if (noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        }
        // 发请求等待 enumerator 分配；在声明 NOTHING_AVAILABLE 之前先重置 availability future，
        // 这样 isAvailable() 返回未完成的 future，runtime 不会立即重新调度本 reader
        // （避免 CPU busy loop）。等 addSplits/notifyNoMoreSplits 回调时再 complete。
        resetAvailability();
        context.sendSplitRequest();
        return InputStatus.NOTHING_AVAILABLE;
    }

    /**
     * 从当前 reader 加载下一个 Arrow batch，返回对应的 InputStatus。
     */
    private InputStatus advanceBatch() throws Exception {
        if (currentReader == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }
        if (!currentReader.loadNextBatch()) {
            // 当前 split 读完
            closeCurrentSplit();
            // 立刻尝试切换到下一个 split（如果队列里已经有）
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
                // filter / limit 走 dataset 级 scan
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
     * 新建一个未完成的 availability future（如果当前 future 已完成）。
     * 后续 {@link #addSplits}/{@link #notifyNoMoreSplits} 会 complete 它，唤醒 runtime。
     */
    private void resetAvailability() {
        CompletableFuture<Void> current = this.availabilityFuture;
        if (current == COMPLETED || current.isDone()) {
            this.availabilityFuture = new CompletableFuture<>();
        }
    }

    /** 唤醒当前阻塞的 availability future（如果有）。 */
    private void completeAvailability() {
        CompletableFuture<Void> current = this.availabilityFuture;
        if (current != null && !current.isDone()) {
            current.complete(null);
        }
    }

    @Override
    public List<LanceSplit> snapshotState(long checkpointId) {
        // 当前 split + 待处理 split 都算未完成，交回 enumerator 以便 failover 时重分配
        List<LanceSplit> state = new ArrayList<>(pendingSplits.size() + 1);
        if (currentSplit != null) {
            state.add(currentSplit);
        }
        state.addAll(pendingSplits);
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // 直接返回当前 future：有数据可用（COMPLETED）→ runtime 立即调 pollNext；
        // 等待 split 分配中（未完成的 future）→ runtime 挂起本 reader 直到 complete。
        return availabilityFuture;
    }

    @Override
    public void addSplits(List<LanceSplit> splits) {
        LOG.debug("Reader {} received {} split(s)", context.getIndexOfSubtask(), splits.size());
        pendingSplits.addAll(splits);
        // 新数据到达，立刻唤醒 runtime 调度 pollNext
        completeAvailability();
    }

    @Override
    public void notifyNoMoreSplits() {
        LOG.debug("Reader {} notified: no more splits", context.getIndexOfSubtask());
        this.noMoreSplits = true;
        // enumerator 声明不再有新 split，唤醒 runtime 让 pollNext 走 END_OF_INPUT
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
