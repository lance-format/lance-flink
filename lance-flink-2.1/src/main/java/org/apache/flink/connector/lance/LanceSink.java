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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceCommittable;
import org.apache.flink.connector.lance.sink.LanceCommittableSerializer;
import org.apache.flink.connector.lance.sink.LanceCommitter;
import org.apache.flink.connector.lance.sink.LanceSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Lance 数据汇（Flink 2.x 版本）。
 *
 * <p>Flink 2.1 已经移除了 {@code TwoPhaseCommittingSink} 的组合接口，改为通过
 * 更正交的 {@link Sink} + {@link SupportsCommitter} 两个 mixin 来声明 2PC Sink。
 * 本类同时实现两个接口：
 * <ul>
 *   <li>{@link Sink#createWriter(WriterInitContext)} 返回
 *       {@link org.apache.flink.api.connector.sink2.CommittingSinkWriter}（{@link LanceSinkWriter}），
 *       负责预提交——把 RowData 攒批写成 Lance Fragment 数据文件；
 *   <li>{@link SupportsCommitter#createCommitter(CommitterInitContext)} 返回
 *       {@link LanceCommitter}，负责把 fragments 以单次 Lance Transaction 提交。
 *   <li>{@link SupportsCommitter#getCommittableSerializer()} 返回 committable 的网络传输/状态序列化器。
 * </ul>
 *
 * <p>Exactly-Once 的来源：Flink 的 checkpoint barrier 触发 writer {@code flush(false)} → 产生
 * committable，runtime 将 committable 写入 checkpoint；checkpoint 成功之后 runtime 把 committable
 * 发给 committer，由 committer 调用 Lance 的 {@code CommitBuilder.execute(Transaction)} 真正提交。
 * 任何环节失败 runtime 会回放 committable 重试，直到 commit 成功——得益于 Lance 的乐观并发控制，
 * 该协议天然 Exactly-Once。
 *
 * <p>构造函数与 Builder 签名与旧 1.x {@code RichSinkFunction} 版本保持一致，
 * 使上层共享源码（如 {@code LanceDynamicTableSink}）无需感知版本差异。
 */
public class LanceSink implements Sink<RowData>, SupportsCommitter<LanceCommittable> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;

    public LanceSink(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    // ------------------------------------------------------------------------
    // Sink V2 核心方法
    // ------------------------------------------------------------------------

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        int subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
        return new LanceSinkWriter(options, rowType, subtaskId);
    }

    @Override
    public Committer<LanceCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new LanceCommitter();
    }

    @Override
    public SimpleVersionedSerializer<LanceCommittable> getCommittableSerializer() {
        return LanceCommittableSerializer.INSTANCE;
    }

    // ------------------------------------------------------------------------
    // 向后兼容的只读访问器（保持与旧实现签名一致，供上层 LanceDynamicTableSink 等复用）
    // ------------------------------------------------------------------------

    public RowType getRowType() {
        return rowType;
    }

    public LanceOptions getOptions() {
        return options;
    }

    // ------------------------------------------------------------------------
    // Builder（与旧实现保持 100% 向后兼容）
    // ------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    /** 与旧实现签名一致的 Builder。 */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private LanceOptions.WriteMode writeMode = LanceOptions.WriteMode.APPEND;
        private int maxRowsPerFile = 1000000;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder writeMode(LanceOptions.WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder maxRowsPerFile(int maxRowsPerFile) {
            this.maxRowsPerFile = maxRowsPerFile;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSink build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }
            if (rowType == null) {
                throw new IllegalArgumentException("RowType cannot be null");
            }
            LanceOptions options =
                    LanceOptions.builder()
                            .path(path)
                            .writeBatchSize(batchSize)
                            .writeMode(writeMode)
                            .writeMaxRowsPerFile(maxRowsPerFile)
                            .build();
            return new LanceSink(options, rowType);
        }
    }
}
