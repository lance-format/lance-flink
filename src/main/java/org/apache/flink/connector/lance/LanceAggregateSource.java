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
import org.apache.flink.connector.lance.aggregate.AggregateExecutor;
import org.apache.flink.connector.lance.aggregate.AggregateInfo;
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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Lance data source with aggregate push-down support.
 *
 * <p>Executes aggregate computation at the data source, supporting COUNT, SUM, AVG, MIN, MAX and other aggregate functions.
 *
 * <p>Usage example:
 * <pre>{@code
 * AggregateInfo aggInfo = AggregateInfo.builder()
 *     .addCountStar("cnt")
 *     .addSum("amount", "total_amount")
 *     .groupBy("category")
 *     .build();
 *
 * LanceAggregateSource source = new LanceAggregateSource(options, rowType, aggInfo);
 * }</pre>
 */
public class LanceAggregateSource extends RichParallelSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceAggregateSource.class);

    private final LanceOptions options;
    private final RowType sourceRowType;
    private final AggregateInfo aggregateInfo;
    private final String[] selectedColumns;

    private transient volatile boolean running;
    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient AggregateExecutor aggregateExecutor;

    /**
     * Create LanceAggregateSource
     *
     * @param options Lance configuration options
     * @param sourceRowType RowType of source table
     * @param aggregateInfo Aggregate information
     */
    public LanceAggregateSource(LanceOptions options, RowType sourceRowType, AggregateInfo aggregateInfo) {
        this.options = options;
        this.sourceRowType = sourceRowType;
        this.aggregateInfo = aggregateInfo;

        // Calculate columns to read
        List<String> requiredColumns = aggregateInfo.getRequiredColumns();
        this.selectedColumns = requiredColumns.isEmpty() ? null : requiredColumns.toArray(new String[0]);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        LOG.info("Opening Lance aggregate data source: {}", options.getPath());
        LOG.info("Aggregate info: {}", aggregateInfo);

        this.running = true;
        this.allocator = LanceAllocators.create(
                "lance-aggregate-source", options.getArrowAllocatorMaxBytes());

        // Open Lance dataset
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }

        try {
            // Honor read.version / read.as-of-timestamp for time-travel reads (issue #5).
            this.dataset = LanceOpener.open(datasetPath, allocator, options);
        } catch (Exception e) {
            throw new IOException("Failed to open Lance dataset: " + datasetPath, e);
        }

        // Initialize RowDataConverter (using source table Schema)
        RowType actualRowType = this.sourceRowType;
        if (actualRowType == null) {
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);

        // Initialize aggregate executor
        this.aggregateExecutor = new AggregateExecutor(aggregateInfo, actualRowType);
        this.aggregateExecutor.init();

        LOG.info("Lance aggregate data source opened");
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LOG.info("Starting aggregate read from Lance dataset: {}", options.getPath());

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

        // Aggregate operation only executes on subtask 0 to avoid duplicate aggregation
        if (subtaskIndex != 0) {
            LOG.info("Subtask {} skipped (only subtask 0 executes in aggregate mode)", subtaskIndex);
            return;
        }

        String filter = options.getReadFilter();

        // Read all data and perform aggregation
        if (filter != null && !filter.isEmpty()) {
            readAndAggregateWithFilter(ctx);
        } else {
            readAndAggregateAll(ctx);
        }

        LOG.info("Lance aggregate data source read completed");
    }

    /**
     * Aggregate read with filter condition
     */
    private void readAndAggregateWithFilter(SourceContext<RowData> ctx) throws Exception {
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());

        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }

        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            LOG.info("Applying filter condition: {}", filter);
            scanOptionsBuilder.filter(filter);
        }

        ScanOptions scanOptions = scanOptionsBuilder.build();

        // Phase 1: Read data and accumulate aggregation
        try (LanceScanner scanner = dataset.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    List<RowData> rows = converter.toRowDataList(root);

                    for (RowData row : rows) {
                        aggregateExecutor.accumulate(row);
                    }
                }
            }
        }

        // Phase 2: Output aggregate results
        outputAggregateResults(ctx);
    }

    /**
     * Read all data and aggregate (without filter condition)
     */
    private void readAndAggregateAll(SourceContext<RowData> ctx) throws Exception {
        List<Fragment> fragments = dataset.getFragments();
        LOG.info("Dataset has {} Fragments", fragments.size());

        // Phase 1: Read all Fragments and accumulate aggregation
        for (Fragment fragment : fragments) {
            if (!running) {
                break;
            }
            readAndAggregateFragment(fragment);
        }

        // Phase 2: Output aggregate results
        outputAggregateResults(ctx);
    }

    /**
     * Read single Fragment and accumulate aggregation
     */
    private void readAndAggregateFragment(Fragment fragment) throws Exception {
        LOG.debug("Reading Fragment: {}", fragment.getId());

        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());

        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }

        ScanOptions scanOptions = scanOptionsBuilder.build();

        try (LanceScanner scanner = fragment.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    List<RowData> rows = converter.toRowDataList(root);

                    for (RowData row : rows) {
                        aggregateExecutor.accumulate(row);
                    }
                }
            }
        }
    }

    /**
     * Output aggregate results
     */
    private void outputAggregateResults(SourceContext<RowData> ctx) {
        List<RowData> results = aggregateExecutor.getResults();
        LOG.info("Aggregation completed, {} result rows", results.size());

        synchronized (ctx.getCheckpointLock()) {
            for (RowData result : results) {
                ctx.collect(result);
            }
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling Lance aggregate data source");
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance aggregate data source");

        this.running = false;

        if (aggregateExecutor != null) {
            aggregateExecutor.reset();
        }

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
     * Get aggregate information
     */
    public AggregateInfo getAggregateInfo() {
        return aggregateInfo;
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get source table RowType
     */
    public RowType getSourceRowType() {
        return sourceRowType;
    }

    /**
     * Get aggregate result RowType
     */
    public RowType getResultRowType() {
        if (aggregateExecutor != null) {
            return aggregateExecutor.buildResultRowType();
        }
        return null;
    }
}
