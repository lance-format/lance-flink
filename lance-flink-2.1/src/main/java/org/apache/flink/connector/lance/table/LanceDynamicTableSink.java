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
import org.apache.flink.connector.lance.LanceUpsertSink;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.List;

/**
 * Flink 2.x 下的 {@link LanceDynamicTableSink} 实现。
 *
 * <p>无主键 → append-only（{@link LanceSink}）；有主键 → 主键 upsert
 * （{@link LanceUpsertSink}，Sink V2 + {@code SupportsCommitter}，issue #90 方案 A）。
 * 两条路径共用同一 {@code LanceDynamicTableSink}，由 {@code primaryKeys} 是否为空切换。
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
        // 主键 upsert 支持 +I/+U/-D（-U 即 UPDATE_BEFORE 由 writer 侧丢弃）。
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        if (!primaryKeys.isEmpty()) {
            LanceUpsertSink upsertSink = new LanceUpsertSink(options, rowType, primaryKeys);
            return SinkV2Provider.of(upsertSink);
        }
        LanceSink lanceSink = new LanceSink(options, rowType);
        return LanceSinkProviderBridge.toProvider(lanceSink);
    }

    @Override
    public DynamicTableSink copy() {
        return new LanceDynamicTableSink(options, physicalDataType, primaryKeys, new int[0]);
    }

    @Override
    public String asSummaryString() {
        // 刻意与根 src 版本（Flink 1.x）保持一致："Lance Table Sink"。
        // 若未来需要在 EXPLAIN 输出里区分版本，应在根 src 和 2.1 版同步修改。
        return "Lance Table Sink";
    }

    public LanceOptions getOptions() {
        return options;
    }

    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
}
