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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.core.io.InputSplitAssigner;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Lance InputFormat implementation.
 * 
 * <p>Reads data from Lance dataset using InputFormat interface, supports parallel reading with splits.
 */
public class LanceInputFormat extends RichInputFormat<RowData, LanceSplit> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceInputFormat.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient LanceScanner currentScanner;
    private transient ArrowReader currentReader;
    private transient Iterator<RowData> currentBatchIterator;
    private transient boolean reachedEnd;

    /**
     * Create LanceInputFormat
     *
     * @param options Lance configuration options
     * @param rowType Flink RowType
     */
    public LanceInputFormat(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
        
        List<String> columns = options.getReadColumns();
        this.selectedColumns = columns != null && !columns.isEmpty() 
                ? columns.toArray(new String[0]) 
                : null;
    }

    @Override
    public void configure(Configuration parameters) {
        // Configuration already done in constructor
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        // Return basic statistics
        return cachedStatistics;
    }

    @Override
    public LanceSplit[] createInputSplits(int minNumSplits) throws IOException {
        LOG.info("Creating input splits, minimum split count: {}", minNumSplits);
        
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IOException("Dataset path cannot be empty");
        }

        BufferAllocator tempAllocator = LanceAllocators.create(
                "lance-input-format-probe", options.getArrowAllocatorMaxBytes());
        try {
            // Honor read.version / read.as-of-timestamp for time-travel reads (issue #5).
            Dataset tempDataset = LanceOpener.open(datasetPath, tempAllocator, options);
            try {
                List<Fragment> fragments = tempDataset.getFragments();
                LanceSplit[] splits = new LanceSplit[fragments.size()];
                
                for (int i = 0; i < fragments.size(); i++) {
                    Fragment fragment = fragments.get(i);
                    long rowCount = fragment.countRows();
                    splits[i] = new LanceSplit(i, fragment.getId(), datasetPath, rowCount);
                }
                
                LOG.info("Created {} input splits", splits.length);
                return splits;
            } finally {
                tempDataset.close();
            }
        } finally {
            tempAllocator.close();
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(LanceSplit[] inputSplits) {
        return new LanceSplitAssigner(inputSplits);
    }

    @Override
    public void open(LanceSplit split) throws IOException {
        LOG.info("Opening split: {}", split);
        
        this.allocator = LanceAllocators.create(
                "lance-input-format", options.getArrowAllocatorMaxBytes());
        this.reachedEnd = false;
        
        // Open dataset
        String datasetPath = split.getDatasetPath();
        try {
            // Honor read.version / read.as-of-timestamp for time-travel reads (issue #5).
            this.dataset = LanceOpener.open(datasetPath, allocator, options);
        } catch (Exception e) {
            throw new IOException("Cannot open dataset: " + datasetPath, e);
        }
        
        // Initialize converter
        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        
        // Get specified Fragment
        List<Fragment> fragments = dataset.getFragments();
        Fragment targetFragment = null;
        for (Fragment fragment : fragments) {
            if (fragment.getId() == split.getFragmentId()) {
                targetFragment = fragment;
                break;
            }
        }
        
        if (targetFragment == null) {
            throw new IOException("Cannot find Fragment: " + split.getFragmentId());
        }
        
        // Build scan options
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            scanOptionsBuilder.filter(filter);
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // Create Scanner
        try {
            this.currentScanner = targetFragment.newScan(scanOptions);
            this.currentReader = currentScanner.scanBatches();
        } catch (Exception e) {
            throw new IOException("Failed to create Scanner", e);
        }
        
        // Load first batch of data
        loadNextBatch();
    }

    /**
     * Load next batch of data
     */
    private void loadNextBatch() throws IOException {
        try {
            if (currentReader.loadNextBatch()) {
                VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
                List<RowData> rows = converter.toRowDataList(root);
                this.currentBatchIterator = rows.iterator();
            } else {
                this.reachedEnd = true;
                this.currentBatchIterator = null;
            }
        } catch (Exception e) {
            throw new IOException("Failed to load data batch", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEnd;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (reachedEnd) {
            return null;
        }
        
        // Current batch still has data
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            return currentBatchIterator.next();
        }
        
        // Load next batch
        loadNextBatch();
        
        if (reachedEnd) {
            return null;
        }
        
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            return currentBatchIterator.next();
        }
        
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing LanceInputFormat");
        
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Reader", e);
            }
            currentReader = null;
        }
        
        if (currentScanner != null) {
            try {
                currentScanner.close();
            } catch (Exception e) {
                LOG.warn("Failed to close Scanner", e);
            }
            currentScanner = null;
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
     * Lance split assigner
     */
    private static class LanceSplitAssigner implements InputSplitAssigner {
        private final List<LanceSplit> remainingSplits;

        public LanceSplitAssigner(LanceSplit[] splits) {
            this.remainingSplits = new ArrayList<>();
            for (LanceSplit split : splits) {
                remainingSplits.add(split);
            }
        }

        @Override
        public synchronized LanceSplit getNextInputSplit(String host, int taskId) {
            if (remainingSplits.isEmpty()) {
                return null;
            }
            return remainingSplits.remove(remainingSplits.size() - 1);
        }

        @Override
        public void returnInputSplit(List<org.apache.flink.core.io.InputSplit> splits, int taskId) {
            for (org.apache.flink.core.io.InputSplit split : splits) {
                if (split instanceof LanceSplit) {
                    synchronized (this) {
                        remainingSplits.add((LanceSplit) split);
                    }
                }
            }
        }
    }
}
