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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.lance.Dataset;
import org.apache.flink.connector.lance.util.LanceOpener;
import org.lance.Fragment;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.util.LanceAllocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Lance data source implementation.
 * 
 * <p>Reads data from Lance dataset and converts to Flink RowData.
 * <p>Supports column pruning, predicate push-down and Limit push-down optimization.
 * 
 * <p>Usage example:
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .readBatchSize(1024)
 *     .readLimit(100L)  // Limit push-down
 *     .build();
 * 
 * LanceSource source = new LanceSource(options, rowType);
 * DataStream<RowData> stream = env.addSource(source);
 * }</pre>
 */
public class LanceSource extends RichParallelSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceSource.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;
    private final Long readLimit;  // Added: Limit push-down

    private transient volatile boolean running;
    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient long emittedCount;  // Added: emitted row count

    /**
     * Create LanceSource
     *
     * @param options Lance configuration options
     * @param rowType Flink RowType
     */
    public LanceSource(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
        
        List<String> columns = options.getReadColumns();
        this.selectedColumns = columns != null && !columns.isEmpty() 
                ? columns.toArray(new String[0]) 
                : null;
        this.readLimit = options.getReadLimit();
    }

    /**
     * Create LanceSource (auto-infer Schema)
     *
     * @param options Lance configuration options
     */
    public LanceSource(LanceOptions options) {
        this(options, null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("Opening Lance data source: {}", options.getPath());
        if (readLimit != null) {
            LOG.info("Limit push-down enabled, max read rows: {}", readLimit);
        }
        
        this.running = true;
        this.emittedCount = 0;
        this.allocator = LanceAllocators.create(
                "lance-source", options.getArrowAllocatorMaxBytes());
        
        // Open Lance dataset
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }
        
        Path path = Paths.get(datasetPath);
        try {
            // Honor read.version / read.as-of-timestamp for time-travel reads (issue #5).
            this.dataset = LanceOpener.open(path.toString(), allocator, options, parameters);
        } catch (Exception e) {
            throw new IOException("Cannot open Lance dataset: " + datasetPath, e);
        }
        
        // Initialize RowDataConverter
        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
            // Infer RowType from dataset Schema
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        
        LOG.info("Lance data source opened, Schema: {}", actualRowType);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LOG.info("Start reading Lance dataset: {}", options.getPath());
        
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        
        String filter = options.getReadFilter();
        
        // If filter condition exists, use Dataset level scan (only execute on first subtask to avoid duplicate data)
        if (filter != null && !filter.isEmpty()) {
            if (subtaskIndex == 0) {
                LOG.info("Using Dataset level scan (with filter condition)");
                readDatasetWithFilter(ctx);
            } else {
                LOG.info("Subtask {} skipped (only subtask 0 executes in filter mode)", subtaskIndex);
            }
        } else if (readLimit != null) {
            // With Limit, only execute on first subtask to avoid duplicate data
            if (subtaskIndex == 0) {
                LOG.info("Using Dataset level scan (with Limit)");
                readDatasetWithFilter(ctx);
            } else {
                LOG.info("Subtask {} skipped (only subtask 0 executes in Limit mode)", subtaskIndex);
            }
        } else {
            // Without filter condition and Limit, use Fragment level parallel scan
            List<Fragment> fragments = dataset.getFragments();
            LOG.info("Dataset has {} Fragments, current subtask {}/{}", 
                    fragments.size(), subtaskIndex, numSubtasks);
            
            // Assign Fragments by subtask
            for (int i = 0; i < fragments.size() && running && !isLimitReached(); i++) {
                // Simple round-robin assignment strategy
                if (i % numSubtasks != subtaskIndex) {
                    continue;
                }
                
                Fragment fragment = fragments.get(i);
                readFragment(ctx, fragment);
            }
        }
        
        LOG.info("Lance data source read completed, total emitted {} rows", emittedCount);
    }

    /**
     * Use Dataset level scan (supports filter conditions and Limit)
     */
    private void readDatasetWithFilter(SourceContext<RowData> ctx) throws Exception {
        // Build scan options
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        
        // Set batch size
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        // Set column filter
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        // Set data filter condition
        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            LOG.info("Applying filter condition: {}", filter);
            scanOptionsBuilder.filter(filter);
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // Use Dataset level scan
        try (LanceScanner scanner = dataset.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running && !isLimitReached()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    
                    // Convert to RowData and output
                    List<RowData> rows = converter.toRowDataList(root);
                    synchronized (ctx.getCheckpointLock()) {
                        for (RowData row : rows) {
                            if (isLimitReached()) {
                                break;
                            }
                            ctx.collect(row);
                            emittedCount++;
                        }
                    }
                }
            }
        }
        
        if (isLimitReached()) {
            LOG.info("Reached Limit ({}), stop reading", readLimit);
        }
    }

    /**
     * Read single Fragment (without filter condition, but supports Limit)
     */
    private void readFragment(SourceContext<RowData> ctx, Fragment fragment) throws Exception {
        LOG.debug("Reading Fragment: {}", fragment.getId());
        
        // Build scan options
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        
        // Set batch size
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        // Set column filter
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        // Note: Fragment level scan does not use filter, filter is only supported at Dataset level
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // Create Scanner and read data
        try (LanceScanner scanner = fragment.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running && !isLimitReached()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    
                    // Convert to RowData and output
                    List<RowData> rows = converter.toRowDataList(root);
                    synchronized (ctx.getCheckpointLock()) {
                        for (RowData row : rows) {
                            if (isLimitReached()) {
                                break;
                            }
                            ctx.collect(row);
                            emittedCount++;
                        }
                    }
                }
            }
        }
    }

    /**
     * Check if Limit has been reached
     */
    private boolean isLimitReached() {
        return readLimit != null && emittedCount >= readLimit;
    }

    @Override
    public void cancel() {
        LOG.info("Cancel Lance data source");
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance data source");
        
        this.running = false;
        
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("Error closing Lance dataset", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing memory allocator", e);
            }
            allocator = null;
        }
        
        super.close();
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
     * Get selected columns
     */
    public String[] getSelectedColumns() {
        return selectedColumns;
    }

    /**
     * Builder pattern constructor
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSource Builder
     */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private List<String> columns;
        private String filter;
        private Long limit;  // Added
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder limit(Long limit) {
            this.limit = limit;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSource build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .readBatchSize(batchSize)
                    .readColumns(columns)
                    .readFilter(filter)
                    .readLimit(limit)
                    .build();

            return new LanceSource(options, rowType);
        }
    }
}
