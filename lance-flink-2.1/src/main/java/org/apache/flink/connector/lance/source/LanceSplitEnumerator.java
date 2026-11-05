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
 *
 * <p><b>Async discovery (#89)</b>：{@link #start()} 不再在 coordinator 主线程上同步打开
 * dataset；而是通过 {@link SplitEnumeratorContext#callAsync(java.util.concurrent.Callable,
 * java.util.function.BiConsumer)} 把 {@link #discoverSplits()} 调度到 coordinator executor
 * 的异步线程池上。discovery 完成前到达的 {@code handleSplitRequest} 会被暂存到
 * {@code pendingSplitRequests}，在 discovery 回调里一次性回放，避免 reader 过早收到
 * {@code signalNoMoreSplits} 被永久 fencing（round-1 minor m2）。
 */
public class LanceSplitEnumerator implements SplitEnumerator<LanceSplit, LanceEnumState> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSplitEnumerator.class);

    private final SplitEnumeratorContext<LanceSplit> context;
    private final LanceOptions options;
    private final Deque<LanceSplit> remainingSplits;

    /**
     * discovery 是否已经完成（成功或失败）。配合 {@link #pendingSplitRequests} 一起决定
     * {@link #handleSplitRequest(int, String)} 的行为。<br>
     * - restored 场景：构造函数里 remainingSplits 非空 → start() 直接 mark discoveryDone=true；<br>
     * - cold-start 场景：start() 触发 callAsync，回调里 mark discoveryDone=true 并回放 pending。
     *
     * <p>所有访问都在 coordinator 主线程（callAsync 的 BiConsumer 也在主线程执行），
     * 因此无需 volatile。
     */
    private boolean discoveryDone;

    /**
     * discovery 完成前到达的 split request 暂存队列。每个元素对应一次 reader 的请求。
     * discovery 完成后一次性全部回放，保证 reader 不会被过早 signalNoMoreSplits 封掉。
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
            // restored enumerator — splits 已在 checkpoint state 里，跳过 discovery。
            LOG.info("Lance split enumerator restored with {} pending splits", remainingSplits.size());
            discoveryDone = true;
            return;
        }

        // cold start — 把 discovery 调度到 coordinator 的 async executor，避免阻塞主线程。
        // 回调在 coordinator 主线程特性上运行（Flink 保证），所以里面安全读写
        // discoveryDone / remainingSplits / pendingSplitRequests 这几个普通字段。
        LOG.info("Lance split enumerator scheduling async discovery for dataset {}", options.getPath());
        context.callAsync(this::discoverSplits, this::onDiscoveryComplete);
    }

    /** callAsync 的回调——运行在 coordinator 主线程。 */
    private void onDiscoveryComplete(List<LanceSplit> splits, Throwable err) {
        discoveryDone = true;
        if (err != null) {
            // 把异常向上抛给 coordinator，run time 会让整个 Source 失败 / 重启。
            // 立刻清理 pending 队列，避免在重启循环里遗留脂脟数据。
            pendingSplitRequests.clear();
            throw new RuntimeException(
                    "Failed to discover Lance splits for " + options.getPath(), err);
        }
        LOG.info(
                "Lance split enumerator discovered {} split(s) for dataset {}",
                splits.size(),
                options.getPath());
        remainingSplits.addAll(splits);

        // 回放在 discovery 进行中排队的 split requests。按到达顺序依次尝试分配；
        // handleSplitRequest 自己会在队空时 signalNoMoreSplits，逻辑不必在这里重写。
        while (!pendingSplitRequests.isEmpty()) {
            int subtaskId = pendingSplitRequests.pollFirst();
            handleSplitRequest(subtaskId, null);
        }
    }

    /**
     * 打开一次 dataset 以计算切分计划；filter / limit 场景直接产出 WHOLE_DATASET 单 split。
     *
     * <p>protected 是为了让单元测试可以注入假的切分结果（R2 一致的设计），
     * 不要在生产代码里从子类调用它。
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
            // reader 尚未上线，忽略；上线后会主动再次请求
            return;
        }
        if (!discoveryDone) {
            // discovery 仍在进行中——先暂存，等回调回放。不走 signalNoMoreSplits 路径，
            // 避免 reader 被过早 fencing（round-1 minor m2）。
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
