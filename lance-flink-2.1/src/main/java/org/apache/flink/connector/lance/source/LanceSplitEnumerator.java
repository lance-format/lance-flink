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
 * Lance {@link SplitEnumerator} 实现。
 *
 * <p>切分策略对齐旧 {@code LanceSource.run()} 的两条路径：
 * <ul>
 *   <li>设置了 filter 或 limit：产生 1 个 {@link LanceSplit.Kind#WHOLE_DATASET} split，
 *       由单个 reader 处理，保证不重复数据（旧实现靠「仅 subtask 0 执行」实现等价效果）。
 *   <li>否则：按 Lance Dataset 的 Fragment 数量切分，每个 Fragment 一个 split，
 *       由 enumerator 轮询下发给请求的 reader，整体并行度 ≤ 并发 reader 数。
 * </ul>
 *
 * <p>此 enumerator 仅在 JobManager 侧运行一次打开 dataset 用于读 fragments 列表，
 * 切分完成即关闭。
 */
public class LanceSplitEnumerator implements SplitEnumerator<LanceSplit, LanceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSplitEnumerator.class);

    private final SplitEnumeratorContext<LanceSplit> context;
    private final LanceOptions options;
    private final Deque<LanceSplit> remainingSplits;

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
            LOG.info("Lance split enumerator restored with {} pending splits", remainingSplits.size());
            return;
        }

        List<LanceSplit> splits = discoverSplits();
        LOG.info(
                "Lance split enumerator discovered {} split(s) for dataset {}",
                splits.size(),
                options.getPath());
        remainingSplits.addAll(splits);
    }

    /**
     * 打开一次 dataset 以计算切分计划；filter / limit 场景直接产出 WHOLE_DATASET 单 split。
     */
    private List<LanceSplit> discoverSplits() {
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
            // reader 尚未上线，忽略；上线后会主动再次请求
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
        // 回收到队首，优先重分配
        for (int i = splits.size() - 1; i >= 0; i--) {
            remainingSplits.addFirst(splits.get(i));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // 本实现采用「reader 拉取」模型（lazy assignment），不在此处主动下发
    }

    @Override
    public LanceEnumState snapshotState(long checkpointId) {
        return new LanceEnumState(remainingSplits);
    }

    @Override
    public void close() {
        // 无常驻资源
    }
}
