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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.ipc.LanceScanner;
import org.lance.ipc.Query;
import org.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.util.LanceAllocators;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Lance vector search implementation.
 * 
 * <p>Supports KNN search with L2, Cosine, and Dot distance metrics.
 * 
 * <p>Usage example:
 * <pre>{@code
 * LanceVectorSearch search = LanceVectorSearch.builder()
 *     .datasetPath("/path/to/dataset")
 *     .columnName("embedding")
 *     .metricType(MetricType.L2)
 *     .nprobes(20)
 *     .build();
 * 
 * List<SearchResult> results = search.search(queryVector, 10);
 * }</pre>
 */
public class LanceVectorSearch implements Closeable, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceVectorSearch.class);

    private final String datasetPath;
    private final String columnName;
    private final MetricType metricType;
    private final int nprobes;
    private final int ef;
    private final Integer refineFactor;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowType rowType;
    private transient RowDataConverter converter;

    private LanceVectorSearch(Builder builder) {
        this.datasetPath = builder.datasetPath;
        this.columnName = builder.columnName;
        this.metricType = builder.metricType;
        this.nprobes = builder.nprobes;
        this.ef = builder.ef;
        this.refineFactor = builder.refineFactor;
    }

    /**
     * Open dataset connection
     */
    public void open() throws IOException {
        LOG.info("Opening vector search, dataset: {}", datasetPath);
        
        this.allocator = LanceAllocators.create("lance-vector-search");
        
        try {
            this.dataset = Dataset.open(datasetPath, allocator);
            
            // Get Schema and create converter
            Schema arrowSchema = dataset.getSchema();
            this.rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
            this.converter = new RowDataConverter(rowType);
            
        } catch (Exception e) {
            throw new IOException("Cannot open dataset: " + datasetPath, e);
        }
    }

    /**
     * Execute vector search
     *
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     * @return List of search results
     */
    public List<SearchResult> search(float[] queryVector, int k) throws IOException {
        return search(queryVector, k, null);
    }

    /**
     * Execute vector search (with filter condition)
     *
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     * @param filter Filter condition (SQL WHERE syntax)
     * @return List of search results
     */
    public List<SearchResult> search(float[] queryVector, int k, String filter) throws IOException {
        if (dataset == null) {
            open();
        }
        
        LOG.debug("Executing vector search, k={}, vector dimension={}", k, queryVector.length);
        
        // Validate query vector
        validateQueryVector(queryVector);
        
        List<SearchResult> results = new ArrayList<>();
        
        try {
            // Build vector query
            Query.Builder queryBuilder = new Query.Builder()
                    .setColumn(columnName)
                    .setKey(queryVector)
                    .setK(k)
                    .setNprobes(nprobes)
                    .setDistanceType(toDistanceType(metricType))
                    .setUseIndex(true);
            
            if (ef > 0) {
                queryBuilder.setEf(ef);
            }
            
            if (refineFactor != null && refineFactor > 0) {
                queryBuilder.setRefineFactor(refineFactor);
            }
            
            Query query = queryBuilder.build();
            
            // Build scan options
            ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder()
                    .nearest(query)
                    .withRowId(true);
            
            if (filter != null && !filter.isEmpty()) {
                scanOptionsBuilder.filter(filter);
            }
            
            ScanOptions scanOptions = scanOptionsBuilder.build();
            
            // Execute search
            try (LanceScanner scanner = dataset.newScan(scanOptions)) {
                try (ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        
                        // Convert to RowData
                        List<RowData> rows = converter.toRowDataList(root);
                        
                        // Try to get distance score (if _distance column exists)
                        Float8Vector distanceVector = null;
                        try {
                            distanceVector = (Float8Vector) root.getVector("_distance");
                        } catch (Exception e) {
                            // _distance column may not exist
                        }
                        
                        for (int i = 0; i < rows.size(); i++) {
                            double distance = 0.0;
                            if (distanceVector != null && !distanceVector.isNull(i)) {
                                distance = distanceVector.get(i);
                            }
                            results.add(new SearchResult(rows.get(i), distance));
                        }
                    }
                }
            }
            
            LOG.debug("Search completed, returned {} results", results.size());
            return results;
            
        } catch (Exception e) {
            throw new IOException("Vector search failed", e);
        }
    }

    /**
     * Execute vector search (return RowData list)
     *
     * @param queryVector Query vector
     * @param k Number of nearest neighbors to return
     * @return RowData list
     */
    public List<RowData> searchRowData(float[] queryVector, int k) throws IOException {
        List<SearchResult> results = search(queryVector, k);
        List<RowData> rowDataList = new ArrayList<>(results.size());
        
        for (SearchResult result : results) {
            // Append distance score to RowData
            GenericRowData rowWithDistance = new GenericRowData(rowType.getFieldCount() + 1);
            RowData originalRow = result.getRowData();
            
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                rowWithDistance.setField(i, getFieldValue(originalRow, i));
            }
            rowWithDistance.setField(rowType.getFieldCount(), result.getDistance());
            
            rowDataList.add(rowWithDistance);
        }
        
        return rowDataList;
    }

    /**
     * Get field value from RowData
     */
    private Object getFieldValue(RowData rowData, int index) {
        if (rowData.isNullAt(index)) {
            return null;
        }
        
        // Simplified handling, should get based on field type in practice
        if (rowData instanceof GenericRowData) {
            return ((GenericRowData) rowData).getField(index);
        }
        
        return null;
    }

    /**
     * Validate query vector
     */
    private void validateQueryVector(float[] queryVector) throws IOException {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("Query vector cannot be empty");
        }
        
        // Check for NaN or Infinity values
        for (float value : queryVector) {
            if (Float.isNaN(value) || Float.isInfinite(value)) {
                throw new IllegalArgumentException("Query vector contains invalid values (NaN or Infinity)");
            }
        }
    }

    /**
     * Convert distance metric type
     */
    private DistanceType toDistanceType(MetricType metricType) {
        switch (metricType) {
            case L2:
                return DistanceType.L2;
            case COSINE:
                return DistanceType.Cosine;
            case DOT:
                return DistanceType.Dot;
            default:
                return DistanceType.L2;
        }
    }

    @Override
    public void close() throws IOException {
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
     * Create builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create vector searcher from LanceOptions
     */
    public static LanceVectorSearch fromOptions(LanceOptions options) {
        return builder()
                .datasetPath(options.getPath())
                .columnName(options.getVectorColumn())
                .metricType(options.getVectorMetric())
                .nprobes(options.getVectorNprobes())
                .ef(options.getVectorEf())
                .refineFactor(options.getVectorRefineFactor())
                .build();
    }

    /**
     * Builder
     */
    public static class Builder {
        private String datasetPath;
        private String columnName;
        private MetricType metricType = MetricType.L2;
        private int nprobes = 20;
        private int ef = 100;
        private Integer refineFactor;

        public Builder datasetPath(String datasetPath) {
            this.datasetPath = datasetPath;
            return this;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder metricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder nprobes(int nprobes) {
            this.nprobes = nprobes;
            return this;
        }

        public Builder ef(int ef) {
            this.ef = ef;
            return this;
        }

        public Builder refineFactor(Integer refineFactor) {
            this.refineFactor = refineFactor;
            return this;
        }

        public LanceVectorSearch build() {
            validate();
            return new LanceVectorSearch(this);
        }

        private void validate() {
            if (datasetPath == null || datasetPath.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }
            if (columnName == null || columnName.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty");
            }
            if (nprobes <= 0) {
                throw new IllegalArgumentException("nprobes must be greater than 0");
            }
        }
    }

    /**
     * Search result
     */
    public static class SearchResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final RowData rowData;
        private final double distance;

        public SearchResult(RowData rowData, double distance) {
            this.rowData = rowData;
            this.distance = distance;
        }

        public RowData getRowData() {
            return rowData;
        }

        public double getDistance() {
            return distance;
        }

        /**
         * Get similarity score (inverse or negative of distance, depending on distance type)
         */
        public double getSimilarity() {
            if (distance == 0) {
                return 1.0;
            }
            // For L2 distance, use 1 / (1 + distance) as similarity
            return 1.0 / (1.0 + distance);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SearchResult that = (SearchResult) o;
            return Double.compare(that.distance, distance) == 0 &&
                    Objects.equals(rowData, that.rowData);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowData, distance);
        }

        @Override
        public String toString() {
            return "SearchResult{" +
                    "rowData=" + rowData +
                    ", distance=" + distance +
                    '}';
        }
    }
}
