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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.lance.CommitBuilder;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Operation;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Lance Sink implementation.
 * 
 * <p>Writes Flink RowData to Lance dataset, supports batch writing and Checkpoint.
 * 
 * <p>Usage example:
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .writeBatchSize(1024)
 *     .writeMode(WriteMode.APPEND)
 *     .build();
 * 
 * LanceSink sink = new LanceSink(options, rowType);
 * dataStream.addSink(sink);
 * }</pre>
 */
public class LanceSink extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceSink.class);

    /**
     * Transaction property recording the Flink job that committed a version. In overwrite mode it
     * lets a subtask tell rows written by a peer subtask of the same job (keep them) from the
     * dataset that existed before the job started (replace it).
     */
    static final String JOB_ID_PROPERTY = "flink.job-id";

    private final LanceOptions options;
    private final RowType rowType;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient Schema arrowSchema;
    private transient List<RowData> buffer;
    private transient long totalWrittenRows;
    private transient boolean datasetExists;
    private transient boolean isFirstWrite;
    private transient String jobId;

    /**
     * Create LanceSink
     *
     * @param options Lance configuration options
     * @param rowType Flink RowType
     */
    public LanceSink(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("Opening Lance Sink: {}", options.getPath());
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.buffer = new ArrayList<>(options.getWriteBatchSize());
        this.totalWrittenRows = 0;
        this.isFirstWrite = true;
        
        // Initialize converter and Schema
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        
        // Check if dataset exists
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }
        
        Path path = Paths.get(datasetPath);
        this.datasetExists = Files.exists(path);

        // Overwrite replaces the dataset once per job, not once per subtask: the existing dataset
        // is replaced by the job's first commit (see flush()), so it must not be deleted here,
        // where a parallel or restarted subtask would wipe rows a peer subtask already committed.
        if (options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            this.jobId = getRuntimeContext().getJobId().toString();
        }
        
        LOG.info("Lance Sink opened, Schema: {}", rowType);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        buffer.add(value);
        
        // When buffer reaches batch size, execute write
        if (buffer.size() >= options.getWriteBatchSize()) {
            flush();
        }
    }

    /**
     * Flush buffer, write data to Lance dataset
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        LOG.debug("Flushing buffer, row count: {}", buffer.size());
        
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            // Convert RowData to VectorSchemaRoot
            converter.toVectorSchemaRoot(buffer, root);
            
            String datasetPath = options.getPath();

            // A peer subtask may have created the dataset since this sink opened (multi-subtask
            // first write). Re-check existence BEFORE Fragment.write below, which itself creates
            // the dataset data directory and would otherwise make Files.exists unreliable.
            if (!datasetExists && Files.exists(Paths.get(datasetPath))) {
                datasetExists = true;
            }

            // In overwrite mode only the job's first commit replaces the dataset; a subtask whose
            // peer (or whose own pre-failover attempt) already committed for this job appends.
            OptionalLong versionToReplace = datasetExists && isOverwritePending()
                    ? versionToReplace(datasetPath)
                    : OptionalLong.empty();

            // Build write parameters
            WriteParams writeParams = new WriteParams.Builder()
                    .withMaxRowsPerFile(options.getWriteMaxRowsPerFile())
                    .build();
            
            // Create Fragment
            List<FragmentMetadata> fragments = Fragment.write()
                    .datasetUri(datasetPath)
                    .allocator(allocator)
                    .data(root)
                    .writeParams(writeParams)
                    .execute();
            
            if (!datasetExists) {
                // Create new dataset (using Overwrite operation)
                commit(Overwrite.builder().fragments(fragments).schema(arrowSchema).build(), null);
                LOG.info("Created new dataset: {}", datasetPath);
            } else if (versionToReplace.isPresent()) {
                replace(fragments, versionToReplace.getAsLong());
            } else {
                commit(Append.builder().fragments(fragments).build(), null);
            }
            datasetExists = true;
            isFirstWrite = false;

            totalWrittenRows += buffer.size();
            LOG.debug("Written {} rows, total: {} rows", buffer.size(), totalWrittenRows);
            
            buffer.clear();
        } catch (Exception e) {
            throw new IOException("Failed to write Lance dataset", e);
        }
    }

    private boolean isOverwritePending() {
        return isFirstWrite && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE;
    }

    /**
     * Replace the dataset version {@code readVersion} with {@code fragments}. If a peer subtask of
     * this job replaced it concurrently, Lance rejects this commit as preempted, and the fragments
     * are appended to the peer's result instead.
     */
    private void replace(List<FragmentMetadata> fragments, long readVersion) {
        String datasetPath = options.getPath();
        try {
            commit(Overwrite.builder().fragments(fragments).schema(arrowSchema).build(), readVersion);
            LOG.info("Overwrote existing dataset: {}", datasetPath);
        } catch (RuntimeException e) {
            if (versionToReplace(datasetPath).isPresent()) {
                throw e;
            }
            LOG.info("Dataset {} was already overwritten by this job, appending instead", datasetPath);
            if (!fragments.isEmpty()) {
                commit(Append.builder().fragments(fragments).build(), null);
            }
        }
    }

    /**
     * Commit an operation to the dataset. In overwrite mode the commit is tagged with this job's id.
     */
    private void commit(Operation operation, Long readVersion) {
        Transaction.Builder txnBuilder = new Transaction.Builder().operation(operation);
        if (readVersion != null) {
            txnBuilder.readVersion(readVersion);
        }
        if (jobId != null) {
            txnBuilder.transactionProperties(Collections.singletonMap(JOB_ID_PROPERTY, jobId));
        }
        final CommitBuilder builder =
                new CommitBuilder(options.getPath(), allocator).writeParams(Collections.emptyMap());
        try (Transaction txn = txnBuilder.build()) {
            dataset = builder.execute(txn);
        }
    }

    @Override
    public void finish() throws Exception {
        flush();
        // A bounded overwrite job whose subtasks received no rows must still replace the existing
        // dataset (with an empty one), unless a peer subtask has already done so for this job.
        if (isOverwritePending() && Files.exists(Paths.get(options.getPath()))) {
            OptionalLong versionToReplace = versionToReplace(options.getPath());
            if (versionToReplace.isPresent()) {
                replace(Collections.emptyList(), versionToReplace.getAsLong());
            }
        }
        super.finish();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance Sink");
        // Flush remaining data
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
        
        LOG.info("Lance Sink closed, total written {} rows", totalWrittenRows);
        
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("Snapshot state, checkpointId: {}", context.getCheckpointId());
        
        // Flush all buffered data at Checkpoint
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("Initialize state, isRestored: {}", context.isRestored());
        // State initialization (if recovery needed)
    }

    /**
     * Get RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get total written row count
     */
    public long getTotalWrittenRows() {
        return totalWrittenRows;
    }

    /**
     * The dataset version that this job's overwrite has to replace, or empty if the latest version
     * was committed by this job, i.e. a peer subtask (or an earlier attempt of this one) has already
     * replaced the dataset.
     */
    private OptionalLong versionToReplace(String datasetPath) {
        Dataset latest;
        try {
            latest = Dataset.open(datasetPath, allocator);
        } catch (IllegalArgumentException e) {
            // The directory exists but holds no committed version yet (e.g. only data files).
            return OptionalLong.of(0L);
        }
        try (Dataset ds = latest) {
            Optional<Transaction> txn = ds.readTransaction();
            if (txn.isPresent()) {
                try (Transaction t = txn.get()) {
                    Map<String, String> properties = t.transactionProperties().orElse(null);
                    if (properties != null && jobId.equals(properties.get(JOB_ID_PROPERTY))) {
                        return OptionalLong.empty();
                    }
                }
            }
            return OptionalLong.of(ds.version());
        }
    }

    /**
     * Builder pattern constructor
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSink Builder
     */
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

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .writeBatchSize(batchSize)
                    .writeMode(writeMode)
                    .writeMaxRowsPerFile(maxRowsPerFile)
                    .build();

            return new LanceSink(options, rowType);
        }
    }
}
