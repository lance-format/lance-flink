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
import org.apache.flink.connector.lance.sink.LanceUpsertCommittable;
import org.apache.flink.connector.lance.sink.LanceUpsertCommittableSerializer;
import org.apache.flink.connector.lance.sink.LanceUpsertCommitter;
import org.apache.flink.connector.lance.sink.LanceUpsertWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;

/**
 * Lance 主键 upsert 数据汇（Flink 2.x Sink V2 版本）。
 *
 * <p>对应 issue #90 的方案 A：{@link Sink} + {@link SupportsCommitter} 组合，writer 无状态
 * 缓冲按 {@code RowKind} 分类的行数据，串行 {@link LanceUpsertCommitter} 统一执行
 * {@code Dataset.mergeInsert} 完成 {@code +I/+U/-D} 语义。
 *
 * <p>与 append 路径 {@link LanceSink} 的结构完全平行，差异在于：
 * <ul>
 *   <li>writer 产出 {@link LanceUpsertCommittable}（携带序列化行数据），而非 fragment 元数据；</li>
 *   <li>committer 串行调用 {@code mergeInsert}，而非 {@code Append}/{@code Overwrite}。</li>
 * </ul>
 */
public class LanceUpsertSink implements Sink<RowData>, SupportsCommitter<LanceUpsertCommittable> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;
    private final List<String> primaryKeys;

    public LanceUpsertSink(LanceOptions options, RowType rowType, List<String> primaryKeys) {
        this.options = options;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        int subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
        return new LanceUpsertWriter(options, rowType, primaryKeys, subtaskId);
    }

    @Override
    public Committer<LanceUpsertCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new LanceUpsertCommitter();
    }

    @Override
    public SimpleVersionedSerializer<LanceUpsertCommittable> getCommittableSerializer() {
        return LanceUpsertCommittableSerializer.INSTANCE;
    }

    public RowType getRowType() {
        return rowType;
    }

    public LanceOptions getOptions() {
        return options;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
}
