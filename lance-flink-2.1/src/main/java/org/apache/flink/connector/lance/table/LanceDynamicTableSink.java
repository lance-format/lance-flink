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

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.LanceSink;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.List;

/**
 * Flink 2.x 下的 {@link LanceDynamicTableSink} 实现。
 *
 * <p>当前只支持 append-only 场景（无主键）。上游 PR#76 带入的 primary-key upsert
 * 分支因依赖 Flink 1.x 的 {@code DataStreamSinkProvider.consumeDataStream} +
 * {@code DataStream.addSink(SinkFunction)}（在 Flink 2.x 全部被移除），
 * 暂不在 Flink 2.1 下可用；配置了 primary key 的建表语句会在 runtime 直接报错。
 *
 * <p>Follow-up：把 {@code LanceUpsertSink} 迁移到 Sink V2 +
 * {@code SupportsCommitter}（keyed 版本），然后恢复对 upsert 分支的支持。
 */
public class LanceDynamicTableSink implements DynamicTableSink {

    private final LanceOptions options;
    private final DataType physicalDataType;
    private final List<String> primaryKeys;

    public LanceDynamicTableSink(LanceOptions options, DataType physicalDataType) {
        this(options, physicalDataType, Collections.emptyList(), new int[0]);
    }

    public LanceDynamicTableSink(
            LanceOptions options,
            DataType physicalDataType,
            List<String> primaryKeys,
            int[] primaryKeyIndices) {
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.primaryKeys = primaryKeys == null ? Collections.emptyList() : primaryKeys;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (primaryKeys.isEmpty()) {
            return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
        }
        // 上游 PR#76 的 upsert 分支在 Flink 2.1 下尚未实现 Sink V2 版本；
        // 返回声明仅为了让 planner 可以继续规划作业；真正写入时会在
        // getSinkRuntimeProvider 阶段抛错。
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (!primaryKeys.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Lance primary-key upsert sink is not yet available on Flink 2.x. "
                            + "The feature exists on the Flink 1.20 build; see follow-up issue. "
                            + "Workaround: drop the PRIMARY KEY constraint for append-only writes.");
        }
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        LanceSink lanceSink = new LanceSink(options, rowType);
        return LanceSinkProviderBridge.toProvider(lanceSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new LanceDynamicTableSink(options, physicalDataType, primaryKeys, new int[0]);
    }

    @Override
    public String asSummaryString() {
        return "Lance Table Sink (Flink 2.x, append-only)";
    }

    public LanceOptions getOptions() {
        return options;
    }

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
}
