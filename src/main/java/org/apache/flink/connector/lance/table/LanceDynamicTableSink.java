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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Lance dynamic table sink.
 * 
 * <p>Implements DynamicTableSink interface, supports writing Flink data to Lance dataset.
 *
 * <p>With a primary key declared, the sink also serves {@code UPDATE} through {@link
 * SupportsRowLevelUpdate}. The statement is answered by the same keyed upsert path used for
 * streaming writes, because Lance's {@code mergeInsert} already expresses match-by-key-then-update.
 */
public class LanceDynamicTableSink implements DynamicTableSink, SupportsRowLevelUpdate {

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
                DataStream<RowData> keyed = dataStream
                        .keyBy(new PrimaryKeySelector(primaryKeyIndices, primaryKeyTypes));
                LanceUpsertSink upsertSink =
                        new LanceUpsertSink(options, rowType, primaryKeys, primaryKeyIndices);
                DataStreamSink<RowData> sink = keyed.addSink(upsertSink).name("LanceUpsertSink");
                // Assign a stable UID so state mapping survives job upgrades / savepoints.
                providerContext.generateUid("lance-upsert-sink").ifPresent(sink::uid);
                return sink;
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new LanceDynamicTableSink(options, physicalDataType, primaryKeys, primaryKeyIndices);
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(
            List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
        if (primaryKeys.isEmpty()) {
            // Without a key there is nothing for mergeInsert to match on, so an update could only
            // be served by rewriting the table. Fail during planning rather than at runtime.
            throw new UnsupportedOperationException(
                    "UPDATE requires a PRIMARY KEY NOT ENFORCED on the Lance table. "
                            + "The table is append-only without one; "
                            + "declare a primary key to enable row-level updates.");
        }

        return new RowLevelUpdateInfo() {
            @Override
            public Optional<List<Column>> requiredColumns() {
                // Empty means "every column, in table order". The sink writes the full row through
                // mergeInsert(withMatchedUpdateAll), so a projection limited to the SET list plus
                // the key would null out every column the statement did not mention.
                return Optional.empty();
            }

            @Override
            public RowLevelUpdateMode getRowLevelUpdateMode() {
                // UPDATED_ROWS delivers only the matched rows, each tagged UPDATE_AFTER, which is
                // what the keyed upsert path already consumes. ALL_ROWS would stream back rows the
                // statement did not touch and rewrite them for no gain.
                return RowLevelUpdateMode.UPDATED_ROWS;
            }
        };
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
