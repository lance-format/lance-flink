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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lance {@link Committer}：把 {@link LanceCommittable} 中累积的 Fragment 元数据
 * 以一次事务提交到 Lance dataset。
 *
 * <p>B1 修复后，Committer 承担 **提交决策** 但不承担 **目录物理清理**：
 * <ul>
 *   <li>目录物理清理（OVERWRITE 模式下的 drop）由 Writer 侧 subtask 0 在
 *       {@code Fragment.write()} 之前完成——必须早于任何 fragment 落盘，否则
 *       commit 时 fragments 清单会指向被 drop 删掉的 .lance 文件；
 *   <li>Committer 的职责是基于 {@link Dataset#open(String)} 探测目标 dataset
 *       当前是否存在 manifest：不存在 → {@link Overwrite} + schema 建表并提交全部
 *       fragments；已存在 → 全部走 {@link Append}。
 * </ul>
 *
 * <p>决策流程（按 datasetPath 独立判定）：
 * <ol>
 *   <li>遍历本批 CommitRequest，按 datasetPath 分桶聚合 fragments 与 schema；
 *   <li>对每个 bucket：探测 dataset 是否已 "可 open"，首次 commit 走 Overwrite，
 *       后续 commit 走 Append。</li>
 * </ol>
 *
 * <p>失败处理（R2 简化后的实际语义）：
 * <ul>
 *   <li>Lance 底层 commit 抛出任何异常 → 按 {@link #MAX_RETRIES} 阈值分流：
 *       <code>req.getNumberOfRetries() &lt; MAX_RETRIES</code> → {@link CommitRequest#retryLater()}；
 *       否则 {@link CommitRequest#signalFailedWithKnownReason(Throwable)}。</li>
 *   <li>bucket 的 schema 缺失（需建表却没有任何 committable 携带 schema）
 *       → 直接 {@link CommitRequest#signalFailedWithKnownReason(Throwable)}，不重试
 *       （这是作业配置错误，重试也解决不了）。</li>
 * </ul>
 *
 * <p>本实现存意不区分 known / unknown reason：走 object_store 的 Lance JNI 既会把
 * 底层 S3 / HDFS / local-FS 的异常包装成 {@link RuntimeException}，也会把乐观并发冲突包
 * 装成同类型，在 Java 端无法稳定区分；统一走 known reason 签名更安全。
 */
public class LanceCommitter implements Committer<LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCommitter.class);

    /**
     * 单条 CommitRequest 的最大重试次数，超过后转为 signalFailed。
     * Lance 乐观并发写冲突若反复失败，极可能是上游数据或 schema 不兼容，无限 retry
     * 只会让 Flink job 卡住。
     *
     * <p>protected 是为了在单元测试里调小这个值、暴露边界行为。
     */
    protected int maxRetries() {
        return MAX_RETRIES;
    }

    private static final int MAX_RETRIES = 5;

    private final BufferAllocator allocator;

    public LanceCommitter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void commit(Collection<CommitRequest<LanceCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }

        // 1) 按 datasetPath 分桶（支持极少见的"一次 commit 批次跨多表"情况）
        Map<String, PathBucket> buckets = new LinkedHashMap<>();
        for (CommitRequest<LanceCommittable> req : requests) {
            LanceCommittable c = req.getCommittable();
            if (c.isEmpty()) {
                continue;
            }
            buckets.computeIfAbsent(c.getDatasetPath(), p -> new PathBucket(p, c))
                    .add(req, c);
        }

        // 2) 按桶独立提交；某桶失败不影响其它桶
        for (PathBucket bucket : buckets.values()) {
            commitBucket(bucket);
        }
    }

    /** 对单个 datasetPath 的所有 CommitRequest 聚合提交，并通过 CommitRequest API 回报结果。 */
    private void commitBucket(PathBucket bucket) {
        String path = bucket.datasetPath;
        try {
            boolean datasetExists = datasetExists(path);

            if (!datasetExists) {
                // 首次建表：Overwrite + schema + 全部 fragments
                if (bucket.schema == null) {
                    IOException err =
                            new IOException(
                                    "Cannot create Lance dataset at "
                                            + path
                                            + ": no Arrow schema in any committable in this batch");
                    signalFail(bucket, err);
                    return;
                }
                Operation op =
                        Overwrite.builder()
                                .fragments(bucket.allFragments())
                                .schema(bucket.schema)
                                .build();
                executeTransaction(path, op);
                LOG.info(
                        "Lance commit: CREATE+OVERWRITE path={}, fragments={}",
                        path,
                        bucket.allFragments().size());
            } else {
                // 已存在：全部 Append
                Operation op = Append.builder().fragments(bucket.allFragments()).build();
                executeTransaction(path, op);
                LOG.info(
                        "Lance commit: APPEND path={}, fragments={}",
                        path,
                        bucket.allFragments().size());
            }
            // 整桶 commit 成功：所有 request 隐式成功，runtime 不再回放它们
        } catch (Throwable t) {
            // 乐观并发冲突 / Lance 侧的其它可恢复错误：让 runtime 下个 checkpoint 周期再试
            LOG.warn("Lance commit failed on {}, deciding retry vs fail per request", path, t);
            retryOrFail(bucket, t);
        }
    }

    /**
     * 判定目标 dataset 是否已存在；不存在用 Lance 的异常来识别。
     *
     * <p>protected 是为了让单元测试可以注入停饱行为（R2 测试点）。
     */
    protected boolean datasetExists(String path) {
        try (Dataset ignored = Dataset.open(path, allocator)) {
            return true;
        } catch (Throwable t) {
            // Lance Java API 在 "manifest 不存在" / "路径不存在" 等情况下均抛 RuntimeException；
            // 这里统一视为"不存在"。真正的 IO 异常会在下一步的 commit 阶段再暴露并走重试路径。
            LOG.debug("Dataset.open({}) failed, treating as non-existent: {}", path, t.toString());
            return false;
        }
    }

    /**
     * 执行一次 Lance 事务。
     *
     * <p>protected 是为了让单元测试可以注入模拟失败（R2 测试点）——而实际
     * 冲突很难在不起 Lance JNI handle 的情况下造出来。
     */
    protected void executeTransaction(String path, Operation op) throws IOException {
        CommitBuilder builder = new CommitBuilder(path, allocator);
        try (Transaction txn = new Transaction.Builder().operation(op).build();
             Dataset ignored = builder.execute(txn)) {
            // Dataset.execute 的返回值是本次提交后的新 Dataset 句柄；立刻关闭即可
        } catch (Exception e) {
            throw new IOException("Lance commit failed on " + path, e);
        }
    }

    /**
     * 把 bucket 内所有 request 按重试次数阈值分流为 retryLater / signalFailed。
     * 并非依赖异常类型——因为走 object_store 的 Lance JNI 会把连接失败 /
     * 权限失败 / 乐观并发冲突均包装成 {@link RuntimeException}，在 Java 端难以区分。
     */
    private void retryOrFail(PathBucket bucket, Throwable err) {
        for (CommitRequest<LanceCommittable> req : bucket.requests) {
            if (req.getNumberOfRetries() < maxRetries()) {
                req.retryLater();
            } else {
                req.signalFailedWithKnownReason(err);
            }
        }
    }

    private void signalFail(PathBucket bucket, Throwable err) {
        for (CommitRequest<LanceCommittable> req : bucket.requests) {
            req.signalFailedWithKnownReason(err);
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing BufferAllocator in LanceCommitter", e);
            }
        }
    }

    /** 单条 datasetPath 的 commit 聚合桶。 */
    private static final class PathBucket {
        final String datasetPath;
        final Schema schema; // 首个带 schema 的 committable 的 schema，用于潜在建表
        final List<CommitRequest<LanceCommittable>> requests = new ArrayList<>();
        final List<FragmentMetadata> fragments = new ArrayList<>();

        PathBucket(String path, LanceCommittable first) {
            this.datasetPath = path;
            this.schema = first.getArrowSchema();
        }

        void add(CommitRequest<LanceCommittable> req, LanceCommittable c) {
            requests.add(req);
            fragments.addAll(c.getFragments());
        }

        List<FragmentMetadata> allFragments() {
            return fragments;
        }
    }
}
