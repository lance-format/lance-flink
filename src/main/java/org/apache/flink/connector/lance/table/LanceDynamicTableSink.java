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
import org.apache.flink.connector.lance.PrimaryKeySelector;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.List;

/**
 * Lance dynamic table sink.
 * 
 * <p>Implements DynamicTableSink interface, supports writing Flink data to Lance dataset.
 */
public class LanceDynamicTableSink implements DynamicTableSink {

    private final LanceOptions options;
    private final DataType physicalDataType;
    private final List<String> primaryKeys;
    private final int[] primaryKeyIndices;
    private final LogicalType[] primaryKeyTypes;

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
        this.primaryKeyIndices = primaryKeyIndices == null ? new int[0] : primaryKeyIndices;
        this.primaryKeyTypes = resolvePrimaryKeyTypes(physicalDataType, this.primaryKeyIndices);
    }

    private static LogicalType[] resolvePrimaryKeyTypes(DataType physicalDataType, int[] keyIndices) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        LogicalType[] types = new LogicalType[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            types[i] = rowType.getTypeAt(keyIndices[i]);
        }
        return types;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (primaryKeys.isEmpty()) {
            // No primary key: insert-only (append).
            return ChangelogMode.newBuilder()
                    .addContainedKind(RowKind.INSERT)
                    .build();
        }
        // With a primary key: support upsert (+I/+U) and delete (-D).
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        if (primaryKeys.isEmpty()) {
            // Append-only path: keep the existing LanceSink.
            LanceSink lanceSink = new LanceSink(options, rowType);
            return SinkFunctionProvider.of(lanceSink);
        }

        // Keyed upsert path: keyBy the primary key so all events for a key reach one subtask.
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(
                    ProviderContext providerContext, DataStream<RowData> dataStream) {
                DataStream<RowData> keyed =
                        dataStream.keyBy(new PrimaryKeySelector(primaryKeyIndices, primaryKeyTypes));
                LanceUpsertSink upsertSink =
                        new LanceUpsertSink(options, rowType, primaryKeys, primaryKeyIndices);
                return keyed.addSink(upsertSink);
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new LanceDynamicTableSink(options, physicalDataType, primaryKeys, primaryKeyIndices);
    }

    @Override
    public String asSummaryString() {
        return "Lance Table Sink";
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get physical data type
     */
    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
}
