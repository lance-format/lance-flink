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
import org.apache.flink.connector.lance.ArrowArrayStreams;
import org.apache.flink.connector.lance.PrimaryKeyPersistence;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.merge.MergeInsertParams;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lance 的 keyed upsert {@link Committer}：把 {@link LanceUpsertCommittable} 中携带的
 * 序列化行数据以 {@code Dataset.mergeInsert} 提交到 Lance dataset。
 *
 * <p><b>为什么串行（方案 A）</b>：{@code mergeInsert} 的底层是 hash-join + fragment rewrite
 * + OCC retry，不同主键可能落在同一 fragment，并发 commit 会触发乐观并发冲突。串行 committer
 * 把冲突概率降到 0，{@code mergeInsert} 内置的 {@code conflict_retries} 仅作安全网。
 * 单点吞吐瓶颈留给 #81 的「按主键 hash 分区 committer」演进路径。
 *
 * <p><b>提交决策（按 datasetPath 独立判定）</b>：
 * <ol>
 *   <li>dataset 不存在 → 用 {@code upsertBatch} 物化 fragment 走 {@link Overwrite} 建表，
 *       并 {@link PrimaryKeyPersistence#persist} 主键；{@code deleteBatch} 忽略（空表无可删）；</li>
 *   <li>dataset 已存在 → 对 {@code upsertBatch} 调 {@code mergeInsert(UpdateAll + InsertAll)}，
 *       对 {@code deleteBatch} 调 {@code mergeInsert(Delete + DoNothing)}。</li>
 * </ol>
 *
 * <p><b>幂等性</b>：{@code mergeInsert} 的 upsert 与「删不存在的 key」都是幂等的，因此
 * checkpoint 重放 committable 不会产生副作用，Exactly-Once 成立。
 *
 * <p><b>失败处理</b>：Lance 底层异常按 {@link #maxRetries()} 阈值分流为 retryLater / failed，
 * 与 append 路径的 {@link LanceCommitter} 一致（object_store JNI 无法稳定区分异常类型）。
 */
public class LanceUpsertCommitter implements Committer<LanceUpsertCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertCommitter.class);

    private static final int MAX_RETRIES = 5;

    private final BufferAllocator allocator;

    public LanceUpsertCommitter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    protected int maxRetries() {
        return MAX_RETRIES;
    }

    @Override
    public void commit(Collection<CommitRequest<LanceUpsertCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }
        // 串行逐条处理：每条 committable 独立 commit，避免多 batch 的 Arrow IPC 字节合并。
        for (CommitRequest<LanceUpsertCommittable> req : requests) {
            LanceUpsertCommittable c = req.getCommittable();
            if (c.isEmpty()) {
                continue;
            }
            commitOne(req, c);
        }
    }

    private void commitOne(CommitRequest<LanceUpsertCommittable> req, LanceUpsertCommittable c) {
        String path = c.getDatasetPath();
        try {
            boolean existed = datasetExists(path);
            if (!existed) {
                createDataset(path, c);
            } else {
                applyMergeInsert(path, upsertParams(c.getPrimaryKeys()), c.getUpsertBatch());
                applyMergeInsert(path, deleteParams(c.getPrimaryKeys()), c.getDeleteBatch());
            }
            // commit 成功：request 隐式成功，runtime 不再回放
        } catch (Throwable t) {
            LOG.warn("Lance upsert commit failed on {}, retrying or failing request", path, t);
            if (req.getNumberOfRetries() < maxRetries()) {
                req.retryLater();
            } else {
                req.signalFailedWithKnownReason(t);
            }
        }
    }

    /**
     * 首次建表：用 upsertBatch 物化 fragment 走 Overwrite，并持久化主键。
     */
    private void createDataset(String path, LanceUpsertCommittable c) throws IOException {
        Schema schema = c.getArrowSchema();
        if (schema == null) {
            throw new IOException(
                    "Cannot create Lance dataset at " + path + ": no Arrow schema in committable");
        }

        List<FragmentMetadata> fragments = materializeFragments(path, c.getUpsertBatch());
        Operation op = Overwrite.builder().fragments(fragments).schema(schema).build();

        CommitBuilder builder = new CommitBuilder(path, allocator);
        try (Transaction txn = new Transaction.Builder().operation(op).build();
             Dataset ds = builder.execute(txn)) {
            // 建表成功后持久化主键（独立 UpdateConfig operation，弱一致，A-1 方案）
            PrimaryKeyPersistence.persist(ds, c.getPrimaryKeys());
        }
        LOG.info(
                "Lance upsert commit: CREATE dataset path={}, fragments={}",
                path,
                fragments.size());
    }

    /**
     * 把 upsertBatch（Arrow IPC bytes）反序列化并物化为 Lance fragment；空批返回空列表。
     */
    private List<FragmentMetadata> materializeFragments(String path, byte[] batch)
            throws IOException {
        if (batch == null || batch.length == 0) {
            return Collections.emptyList();
        }
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(batch), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return Fragment.write()
                    .datasetUri(path)
                    .allocator(allocator)
                    .data(root)
                    .execute();
        }
    }

    /**
     * 反序列化 batch 并执行一次 mergeInsert。
     */
    private void applyMergeInsert(String path, MergeInsertParams params, byte[] batch)
            throws IOException {
        if (batch == null || batch.length == 0) {
            return;
        }
        try (Dataset ds = Dataset.open(path, allocator);
             ArrowStreamReader reader =
                     new ArrowStreamReader(new ByteArrayInputStream(batch), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ArrowArrayStreams.mergeInsert(ds, params, allocator, root);
        }
    }

    private static MergeInsertParams upsertParams(List<String> primaryKeys) {
        return new MergeInsertParams(primaryKeys)
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
    }

    private static MergeInsertParams deleteParams(List<String> primaryKeys) {
        return new MergeInsertParams(primaryKeys)
                .withMatchedDelete()
                .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    }

    protected boolean datasetExists(String path) {
        try (Dataset ignored = Dataset.open(path, allocator)) {
            return true;
        } catch (Throwable t) {
            LOG.debug(
                    "Dataset.open({}) failed, treating as non-existent: {}",
                    path,
                    t.toString());
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing BufferAllocator in LanceUpsertCommitter", e);
            }
        }
    }
}
