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
import java.util.List;

/**
 * Lance {@link Committer}：把 {@link LanceCommittable} 中累积的 Fragment 元数据
 * 以一次事务提交到 Lance dataset，保证 Exactly-Once。
 *
 * <p>合并策略：Flink 一次 checkpoint 窗口内可能有多个 subtask 产生 committable，
 * runtime 以 {@code Collection<CommitRequest<CommT>>} 的形式一次性交给本 committer。
 * 我们按「相同 datasetPath 且非 createDataset」合并为一次 Append 事务，
 * 对 createDataset=true 的首次写入单独按 Overwrite 提交；这样在多并行度写同一张表时
 * 可显著减少 Lance 版本号的递增次数和并发冲突概率。
 *
 * <p>空 committable（没有 fragment）直接忽略——某个 subtask 本窗口没数据属于正常情况。
 */
public class LanceCommitter implements Committer<LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCommitter.class);

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

        // 1) 提取非空 committables；按 (datasetPath, createDataset) 分组
        //    为简单起见，同一次 commit 调用内一般只对应一张表，这里不做多表合并，
        //    只做「create 单独一次 + append 合并一次」的两阶段提交。
        String datasetPath = null;
        Schema overwriteSchema = null;
        List<FragmentMetadata> createFragments = new ArrayList<>();
        List<FragmentMetadata> appendFragments = new ArrayList<>();

        for (CommitRequest<LanceCommittable> req : requests) {
            LanceCommittable c = req.getCommittable();
            if (c.isEmpty()) {
                continue;
            }
            if (datasetPath == null) {
                datasetPath = c.getDatasetPath();
            } else if (!datasetPath.equals(c.getDatasetPath())) {
                // 多张表的 committables 混在一起：理论上不应发生，除非上游 topology 把多个
                // sink 合并到同一 committer operator。遇到就退化为「来者不拒、分别提交」。
                LOG.warn(
                        "Multiple dataset paths observed in a single commit batch: {} vs {}. "
                                + "Will commit per-request instead of merging.",
                        datasetPath,
                        c.getDatasetPath());
                commitSingle(c);
                continue;
            }
            if (c.isCreateDataset()) {
                createFragments.addAll(c.getFragments());
                if (overwriteSchema == null) {
                    overwriteSchema = c.getArrowSchema();
                }
            } else {
                appendFragments.addAll(c.getFragments());
            }
        }

        if (datasetPath == null) {
            // 全是空 committable
            return;
        }

        // 2) 先处理 create（把所有被标记 createDataset 的 fragments 一次 Overwrite 掉）
        if (!createFragments.isEmpty()) {
            if (overwriteSchema == null) {
                throw new IOException(
                        "createDataset=true but no Arrow schema in committable for " + datasetPath);
            }
            executeOverwrite(datasetPath, overwriteSchema, createFragments);
        }

        // 3) 再处理 append
        if (!appendFragments.isEmpty()) {
            executeAppend(datasetPath, appendFragments);
        }
    }

    /** 单条 committable 走独立事务的退化分支。 */
    private void commitSingle(LanceCommittable c) throws IOException {
        if (c.isCreateDataset()) {
            executeOverwrite(c.getDatasetPath(), c.getArrowSchema(), c.getFragments());
        } else {
            executeAppend(c.getDatasetPath(), c.getFragments());
        }
    }

    private void executeOverwrite(
            String path, Schema schema, List<FragmentMetadata> fragments) throws IOException {
        LOG.info("Lance commit: OVERWRITE path={}, fragments={}", path, fragments.size());
        Operation op = Overwrite.builder().fragments(fragments).schema(schema).build();
        executeTransaction(path, op);
    }

    private void executeAppend(String path, List<FragmentMetadata> fragments) throws IOException {
        LOG.info("Lance commit: APPEND path={}, fragments={}", path, fragments.size());
        Operation op = Append.builder().fragments(fragments).build();
        executeTransaction(path, op);
    }

    private void executeTransaction(String path, Operation op) throws IOException {
        CommitBuilder builder = new CommitBuilder(path, allocator);
        try (Transaction txn = new Transaction.Builder().operation(op).build();
             Dataset ignored = builder.execute(txn)) {
            // Dataset.execute 的返回值是本次提交后的新 Dataset 句柄；立刻关闭即可。
        } catch (Exception e) {
            throw new IOException("Lance commit failed on " + path, e);
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
}
