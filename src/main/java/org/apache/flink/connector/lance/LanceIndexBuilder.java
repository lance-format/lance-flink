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

import org.lance.Dataset;
import org.lance.index.DistanceType;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.vector.HnswBuildParams;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.PQBuildParams;
import org.lance.index.vector.VectorIndexParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.util.LanceAllocators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Optional;

/**
 * Lance vector index builder.
 * 
 * <p>Supports building IVF_PQ, IVF_HNSW_PQ, and IVF_FLAT vector indices.
 * 
 * <p>Usage example:
 * <pre>{@code
 * LanceIndexBuilder builder = LanceIndexBuilder.builder()
 *     .datasetPath("/path/to/dataset")
 *     .columnName("embedding")
 *     .indexType(LanceOptions.IndexType.IVF_PQ)
 *     .numPartitions(256)
 *     .numSubVectors(16)
 *     .build();
 * 
 * IndexBuildResult result = builder.buildIndex();
 * }</pre>
 */
public class LanceIndexBuilder implements Closeable, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceIndexBuilder.class);

    private final String datasetPath;
    private final String columnName;
    private final LanceOptions.IndexType indexType;
    private final LanceOptions.MetricType metricType;
    private final int numPartitions;
    private final Integer numSubVectors;
    private final int numBits;
    private final int maxLevel;
    private final int m;
    private final int efConstruction;
    private final boolean replace;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;

    private LanceIndexBuilder(Builder builder) {
        this.datasetPath = builder.datasetPath;
        this.columnName = builder.columnName;
        this.indexType = builder.indexType;
        this.metricType = builder.metricType;
        this.numPartitions = builder.numPartitions;
        this.numSubVectors = builder.numSubVectors;
        this.numBits = builder.numBits;
        this.maxLevel = builder.maxLevel;
        this.m = builder.m;
        this.efConstruction = builder.efConstruction;
        this.replace = builder.replace;
    }

    /**
     * Build vector index
     *
     * @return Index build result
     */
    public IndexBuildResult buildIndex() throws IOException {
        LOG.info("Starting to build vector index, type: {}, column: {}, dataset: {}", 
                indexType, columnName, datasetPath);
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Initialize resources
            this.allocator = LanceAllocators.create("lance-index-builder");
            this.dataset = Dataset.open(datasetPath, allocator);
            
            // Validate column exists
            validateColumn();
            
            // Get distance metric type
            DistanceType distanceType = toDistanceType(metricType);
            
            // Build IVF parameters
            IvfBuildParams ivfParams = new IvfBuildParams.Builder()
                    .setNumPartitions(numPartitions)
                    .build();
            
            // Build index based on index type
            IndexType lanceIndexType;
            IndexParams indexParams;
            
            switch (indexType) {
                case IVF_PQ:
                    lanceIndexType = IndexType.IVF_PQ;
                    PQBuildParams pqParams = new PQBuildParams.Builder()
                            .setNumSubVectors(numSubVectors != null ? numSubVectors : 16)
                            .setNumBits(numBits)
                            .build();
                    VectorIndexParams ivfPqParams = VectorIndexParams.withIvfPqParams(
                            distanceType, ivfParams, pqParams);
                    indexParams = IndexParams.builder()
                            .setVectorIndexParams(ivfPqParams)
                            .build();
                    break;
                    
                case IVF_HNSW:
                    lanceIndexType = IndexType.IVF_HNSW_PQ;
                    HnswBuildParams hnswParams = new HnswBuildParams.Builder()
                            .setMaxLevel((short) maxLevel)
                            .setM(m)
                            .setEfConstruction(efConstruction)
                            .build();
                    PQBuildParams hnswPqParams = new PQBuildParams.Builder()
                            .setNumSubVectors(numSubVectors != null ? numSubVectors : 16)
                            .setNumBits(numBits)
                            .build();
                    VectorIndexParams ivfHnswParams = VectorIndexParams.withIvfHnswPqParams(
                            distanceType, ivfParams, hnswParams, hnswPqParams);
                    indexParams = IndexParams.builder()
                            .setVectorIndexParams(ivfHnswParams)
                            .build();
                    break;
                    
                case IVF_FLAT:
                    lanceIndexType = IndexType.IVF_FLAT;
                    VectorIndexParams ivfFlatParams = VectorIndexParams.ivfFlat(numPartitions, distanceType);
                    indexParams = IndexParams.builder()
                            .setVectorIndexParams(ivfFlatParams)
                            .build();
                    break;
                    
                default:
                    throw new IllegalArgumentException("Unsupported index type: " + indexType);
            }
            
            // Create index
            dataset.createIndex(
                    Collections.singletonList(columnName),
                    lanceIndexType,
                    Optional.empty(),  // Index name, use default
                    indexParams,
                    replace
            );
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            LOG.info("Vector index build completed, duration: {} ms", duration);
            
            return new IndexBuildResult(
                    true,
                    indexType,
                    columnName,
                    datasetPath,
                    duration,
                    null
            );
        } catch (Exception e) {
            LOG.error("Failed to build vector index", e);
            return new IndexBuildResult(
                    false,
                    indexType,
                    columnName,
                    datasetPath,
                    System.currentTimeMillis() - startTime,
                    e.getMessage()
            );
        }
    }

    /**
     * Validate vector column exists
     */
    private void validateColumn() throws IOException {
        // Check if column exists in Schema
        boolean columnExists = dataset.getSchema().getFields().stream()
                .anyMatch(field -> field.getName().equals(columnName));
        
        if (!columnExists) {
            throw new IOException("Vector column does not exist: " + columnName);
        }
    }

    /**
     * Convert distance metric type
     */
    private DistanceType toDistanceType(LanceOptions.MetricType metricType) {
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
     * Create builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create index builder from LanceOptions
     */
    public static LanceIndexBuilder fromOptions(LanceOptions options) {
        return builder()
                .datasetPath(options.getPath())
                .columnName(options.getIndexColumn())
                .indexType(options.getIndexType())
                .metricType(options.getVectorMetric())
                .numPartitions(options.getIndexNumPartitions())
                .numSubVectors(options.getIndexNumSubVectors())
                .numBits(options.getIndexNumBits())
                .maxLevel(options.getIndexMaxLevel())
                .m(options.getIndexM())
                .efConstruction(options.getIndexEfConstruction())
                .build();
    }

    /**
     * Builder
     */
    public static class Builder {
        private String datasetPath;
        private String columnName;
        private LanceOptions.IndexType indexType = LanceOptions.IndexType.IVF_PQ;
        private LanceOptions.MetricType metricType = LanceOptions.MetricType.L2;
        private int numPartitions = 256;
        private Integer numSubVectors;
        private int numBits = 8;
        private int maxLevel = 7;
        private int m = 16;
        private int efConstruction = 100;
        private boolean replace = false;

        public Builder datasetPath(String datasetPath) {
            this.datasetPath = datasetPath;
            return this;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder indexType(LanceOptions.IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder metricType(LanceOptions.MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder numPartitions(int numPartitions) {
            this.numPartitions = numPartitions;
            return this;
        }

        public Builder numSubVectors(Integer numSubVectors) {
            this.numSubVectors = numSubVectors;
            return this;
        }

        public Builder numBits(int numBits) {
            this.numBits = numBits;
            return this;
        }

        public Builder maxLevel(int maxLevel) {
            this.maxLevel = maxLevel;
            return this;
        }

        public Builder m(int m) {
            this.m = m;
            return this;
        }

        public Builder efConstruction(int efConstruction) {
            this.efConstruction = efConstruction;
            return this;
        }

        public Builder replace(boolean replace) {
            this.replace = replace;
            return this;
        }

        public LanceIndexBuilder build() {
            validate();
            return new LanceIndexBuilder(this);
        }

        private void validate() {
            if (datasetPath == null || datasetPath.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }
            if (columnName == null || columnName.isEmpty()) {
                throw new IllegalArgumentException("Column name cannot be empty");
            }
            if (numPartitions <= 0) {
                throw new IllegalArgumentException("Number of partitions must be greater than 0");
            }
            if (numSubVectors != null && numSubVectors <= 0) {
                throw new IllegalArgumentException("Number of sub-vectors must be greater than 0");
            }
            if (numBits <= 0 || numBits > 16) {
                throw new IllegalArgumentException("Quantization bits must be between 1 and 16");
            }
        }
    }

    /**
     * Index build result
     */
    public static class IndexBuildResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean success;
        private final LanceOptions.IndexType indexType;
        private final String columnName;
        private final String datasetPath;
        private final long durationMillis;
        private final String errorMessage;

        public IndexBuildResult(boolean success, LanceOptions.IndexType indexType, String columnName,
                                String datasetPath, long durationMillis, String errorMessage) {
            this.success = success;
            this.indexType = indexType;
            this.columnName = columnName;
            this.datasetPath = datasetPath;
            this.durationMillis = durationMillis;
            this.errorMessage = errorMessage;
        }

        public boolean isSuccess() {
            return success;
        }

        public LanceOptions.IndexType getIndexType() {
            return indexType;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getDatasetPath() {
            return datasetPath;
        }

        public long getDurationMillis() {
            return durationMillis;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public String toString() {
            return "IndexBuildResult{" +
                    "success=" + success +
                    ", indexType=" + indexType +
                    ", columnName='" + columnName + '\'' +
                    ", datasetPath='" + datasetPath + '\'' +
                    ", durationMillis=" + durationMillis +
                    ", errorMessage='" + errorMessage + '\'' +
                    '}';
        }
    }
}
