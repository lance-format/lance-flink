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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;
import org.lance.operation.Overwrite;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keyed sink for Lance tables declared with a primary key.
 *
 * <p>Unlike {@link LanceSink} (append-only), this sink supports the CDC changelog kinds
 * {@code +I}/{@code +U}/{-D}. It relies on {@code DataStream#keyBy} upstream to guarantee that all
 * events for a given primary key arrive at the same subtask in order; it then collapses the
 * buffered events per key to a single final action and applies it at checkpoint boundaries:
 *
 * <ul>
 *   <li>{@code INSERT}/{@code UPDATE_AFTER} &rarr; native upsert via
 *       {@link Dataset#mergeInsert} ({@code WhenMatched.UpdateAll} +
 *       {@code WhenNotMatched.InsertAll}).</li>
 *   <li>{@code DELETE} &rarr; {@link Dataset#delete} with an OR-of-AND predicate.</li>
 *   <li>{@code UPDATE_BEFORE} &rarr; dropped (upsert has no need for the old value).</li>
 * </ul>
 */
public class LanceUpsertSink extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertSink.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final List<String> primaryKeys;
    private final int[] keyIndices;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient Schema arrowSchema;
    private transient Map<RowData, RowData> buffer;
    private transient long totalWrittenRows;

    public LanceUpsertSink(LanceOptions options, RowType rowType, List<String> primaryKeys, int[] keyIndices) {
        this.options = options;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.keyIndices = keyIndices;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        LOG.info("Opening Lance Upsert Sink: {}", options.getPath());

        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.buffer = new LinkedHashMap<>();
        this.totalWrittenRows = 0;
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);

        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }

        Path path = Paths.get(datasetPath);
        boolean datasetExists = Files.exists(path);

        if (datasetExists && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            LOG.info("Overwrite mode, deleting existing dataset: {}", datasetPath);
            deleteDirectory(path);
            datasetExists = false;
        }

        if (datasetExists) {
            this.dataset = Dataset.open(datasetPath, allocator);
        }

        LOG.info("Lance Upsert Sink opened, primary keys: {}", primaryKeys);
    }

    @Override
    public void invoke(RowData value, Context context) {
        RowKind kind = value.getRowKind();
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                buffer.put(extractKey(value), value);
                break;
            case DELETE:
                buffer.put(extractKey(value), value);
                break;
            case UPDATE_BEFORE:
                // upsert has no use for the old value
                break;
            default:
                LOG.warn("Ignoring unsupported RowKind: {}", kind);
        }
    }

    /**
     * Flush the collapsed per-key buffer to Lance.
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        List<RowData> upserts = new ArrayList<>();
        List<RowData> deletes = new ArrayList<>();
        for (RowData row : buffer.values()) {
            if (row.getRowKind() == RowKind.DELETE) {
                deletes.add(row);
            } else {
                upserts.add(row);
            }
        }

        if (dataset == null) {
            // First write: create the dataset from upserts only (deletes have no target yet).
            if (upserts.isEmpty()) {
                buffer.clear();
                return;
            }
            createDataset(upserts);
            PrimaryKeyPersistence.persist(dataset, primaryKeys);
            totalWrittenRows += upserts.size();
        } else {
            if (!upserts.isEmpty()) {
                mergeInsertRows(upserts);
            }
            if (!deletes.isEmpty()) {
                deleteRows(deletes);
            }
            totalWrittenRows += upserts.size() + deletes.size();
        }

        buffer.clear();
    }

    /**
     * Create the dataset on first write (equivalent to {@code INSERT} into an empty target).
     */
    private void createDataset(List<RowData> rows) throws IOException {
        String datasetPath = options.getPath();

        // A peer subtask may have created the dataset since this sink opened (multi-subtask first
        // write). Falling back to merge-insert avoids clobbering its data with Overwrite.
        if (Files.exists(Paths.get(datasetPath))) {
            this.dataset = Dataset.open(datasetPath, allocator);
            mergeInsertRows(rows);
            return;
        }

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rows, root);

            WriteParams writeParams = new WriteParams.Builder()
                    .withMaxRowsPerFile(options.getWriteMaxRowsPerFile())
                    .build();

            List<FragmentMetadata> fragments = Fragment.write()
                    .datasetUri(datasetPath)
                    .allocator(allocator)
                    .data(root)
                    .writeParams(writeParams)
                    .execute();

            Overwrite operation = Overwrite.builder().fragments(fragments).schema(arrowSchema).build();
            CommitBuilder builder = new CommitBuilder(datasetPath, allocator)
                    .writeParams(Collections.emptyMap());
            try (Transaction txn = new Transaction.Builder().operation(operation).build()) {
                dataset = builder.execute(txn);
            }
        } catch (Exception e) {
            throw new IOException("Failed to create Lance dataset: " + datasetPath, e);
        }
    }

    /**
     * Apply native upsert via {@code mergeInsert}.
     */
    private void mergeInsertRows(List<RowData> rows) throws IOException {
        MergeInsertParams params = new MergeInsertParams(primaryKeys)
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rows, root);
            ArrowArrayStreams.mergeInsert(dataset, params, allocator, root);
        } catch (Exception e) {
            throw new IOException("Failed to merge-insert Lance rows", e);
        }
    }

    /**
     * Delete rows by an OR-of-AND predicate over the primary-key columns.
     */
    private void deleteRows(List<RowData> rows) {
        String predicate = buildDeletePredicate(rows);
        LOG.debug("Deleting rows with predicate: {}", predicate);
        dataset.delete(predicate);
    }

    /**
     * Build a SQL predicate of the form {@code (k1 = v1 AND k2 = v2) OR (...)}.
     */
    private String buildDeletePredicate(List<RowData> rows) {
        List<String> ors = new ArrayList<>();
        for (RowData row : rows) {
            List<String> ands = new ArrayList<>();
            for (int keyIndex : keyIndices) {
                String column = rowType.getFieldNames().get(keyIndex);
                LogicalType type = rowType.getTypeAt(keyIndex);
                Object value = RowDataFieldAccessor.readField(row, keyIndex, type);
                ands.add(column + " = " + formatSqlValue(value, type));
            }
            ors.add("(" + String.join(" AND ", ands) + ")");
        }
        return String.join(" OR ", ors);
    }

    /**
     * Format a primary-key value as a Lance SQL literal.
     */
    private String formatSqlValue(Object value, LogicalType type) {
        if (value == null) {
            return "NULL";
        }
        if (type instanceof TinyIntType || type instanceof SmallIntType
                || type instanceof IntType || type instanceof BigIntType
                || type instanceof FloatType || type instanceof DoubleType
                || type instanceof BooleanType) {
            return value.toString();
        }
        if (type instanceof VarCharType) {
            StringData stringData = (StringData) value;
            return "'" + stringData.toString().replace("'", "''") + "'";
        }
        throw new UnsupportedOperationException(
                "Unsupported primary-key type for delete predicate: " + type.getClass().getSimpleName());
    }

    /**
     * Project the primary-key columns into a key that honors equals/hashCode.
     */
    private RowData extractKey(RowData value) {
        GenericRowData key = new GenericRowData(keyIndices.length);
        for (int i = 0; i < keyIndices.length; i++) {
            key.setField(i,
                    RowDataFieldAccessor.readField(value, keyIndices[i], rowType.getTypeAt(keyIndices[i])));
        }
        return key;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("Snapshot state, checkpointId: {}", context.getCheckpointId());
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        LOG.debug("Initialize state, isRestored: {}", context.isRestored());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance Upsert Sink");
        try {
            flush();
        } catch (Exception e) {
            LOG.warn("Failed to flush data on close", e);
        }
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("Failed to close dataset", e);
            }
            dataset = null;
        }
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close allocator", e);
            }
            allocator = null;
        }
        LOG.info("Lance Upsert Sink closed, total written {} rows", totalWrittenRows);
        super.close();
    }

    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    public RowType getRowType() {
        return rowType;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public long getTotalWrittenRows() {
        return totalWrittenRows;
    }
}
