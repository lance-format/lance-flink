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

package org.apache.flink.connector.lance.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Lance connector configuration options.
 * 
 * <p>Defines all configuration items for Source, Sink, vector index and vector search.
 */
public class LanceOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    // ==================== Common Configuration ====================

    /**
     * Lance dataset path
     */
    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Path to Lance dataset (required)");

    // ==================== Source Configuration ====================

    /**
     * Read batch size
     */
    public static final ConfigOption<Integer> READ_BATCH_SIZE = ConfigOptions
            .key("read.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("Batch size for reading, default 1024");

    /**
     * Read row limit (Limit push-down)
     */
    public static final ConfigOption<Long> READ_LIMIT = ConfigOptions
            .key("read.limit")
            .longType()
            .noDefaultValue()
            .withDescription("Maximum number of rows to read (for Limit push-down)");

    /**
     * List of columns to read (comma separated)
     */
    public static final ConfigOption<String> READ_COLUMNS = ConfigOptions
            .key("read.columns")
            .stringType()
            .noDefaultValue()
            .withDescription("List of columns to read, comma separated. Empty reads all columns");

    /**
     * Data filter condition
     */
    public static final ConfigOption<String> READ_FILTER = ConfigOptions
            .key("read.filter")
            .stringType()
            .noDefaultValue()
            .withDescription("Data filter condition, using SQL WHERE clause syntax");

    /**
     * Time travel: read a specific historical version of the Lance dataset by version number.
     * Mutually exclusive with {@link #READ_AS_OF_TIMESTAMP} (version takes precedence if both set).
     * See issue #5.
     */
    public static final ConfigOption<Long> READ_VERSION = ConfigOptions
            .key("read.version")
            .longType()
            .noDefaultValue()
            .withDescription("Time travel: read the given Lance dataset version. If unset, reads the latest version.");

    /**
     * Time travel: read the dataset as of the given ISO-8601 timestamp (e.g. {@code 2026-07-01T00:00:00Z}).
     * The connector resolves this to the newest version whose creation time is &le; the given timestamp.
     * Ignored if {@link #READ_VERSION} is also set. See issue #5.
     */
    public static final ConfigOption<String> READ_AS_OF_TIMESTAMP = ConfigOptions
            .key("read.as-of-timestamp")
            .stringType()
            .noDefaultValue()
            .withDescription("Time travel: read the dataset as of the given ISO-8601 timestamp. Derives the version by scanning listVersions(). Ignored when read.version is set.");

    // ==================== Sink Configuration ====================

    /**
     * Write batch size
     */
    public static final ConfigOption<Integer> WRITE_BATCH_SIZE = ConfigOptions
            .key("write.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("Batch size for writing, default 1024");

    /**
     * Write mode: append or overwrite
     */
    public static final ConfigOption<String> WRITE_MODE = ConfigOptions
            .key("write.mode")
            .stringType()
            .defaultValue("append")
            .withDescription("Write mode: append or overwrite, default append");

    /**
     * Maximum rows per file
     */
    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE = ConfigOptions
            .key("write.max-rows-per-file")
            .intType()
            .defaultValue(1000000)
            .withDescription("Maximum rows per data file, default 1000000");

    // ==================== Vector Index Configuration ====================

    /**
     * Index type: IVF_PQ, IVF_HNSW, IVF_FLAT
     */
    public static final ConfigOption<String> INDEX_TYPE = ConfigOptions
            .key("index.type")
            .stringType()
            .defaultValue("IVF_PQ")
            .withDescription("Vector index type: IVF_PQ, IVF_HNSW, IVF_FLAT, default IVF_PQ");

    /**
     * Index column name
     */
    public static final ConfigOption<String> INDEX_COLUMN = ConfigOptions
            .key("index.column")
            .stringType()
            .noDefaultValue()
            .withDescription("Vector column name for indexing (required)");

    /**
     * IVF partition count
     */
    public static final ConfigOption<Integer> INDEX_NUM_PARTITIONS = ConfigOptions
            .key("index.num-partitions")
            .intType()
            .defaultValue(256)
            .withDescription("Number of IVF index partitions, default 256");

    /**
     * PQ sub-vector count
     */
    public static final ConfigOption<Integer> INDEX_NUM_SUB_VECTORS = ConfigOptions
            .key("index.num-sub-vectors")
            .intType()
            .noDefaultValue()
            .withDescription("Number of PQ index sub-vectors, default auto-calculated");

    /**
     * PQ quantization bits
     */
    public static final ConfigOption<Integer> INDEX_NUM_BITS = ConfigOptions
            .key("index.num-bits")
            .intType()
            .defaultValue(8)
            .withDescription("PQ quantization bits, default 8");

    /**
     * HNSW max level
     */
    public static final ConfigOption<Integer> INDEX_MAX_LEVEL = ConfigOptions
            .key("index.max-level")
            .intType()
            .defaultValue(7)
            .withDescription("HNSW index max level, default 7");

    /**
     * HNSW connections per level M
     */
    public static final ConfigOption<Integer> INDEX_M = ConfigOptions
            .key("index.m")
            .intType()
            .defaultValue(16)
            .withDescription("HNSW connections per level M, default 16");

    /**
     * HNSW construction search width
     */
    public static final ConfigOption<Integer> INDEX_EF_CONSTRUCTION = ConfigOptions
            .key("index.ef-construction")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW construction search width ef_construction, default 100");

    // ==================== Vector Search Configuration ====================

    /**
     * Vector search column name
     */
    public static final ConfigOption<String> VECTOR_COLUMN = ConfigOptions
            .key("vector.column")
            .stringType()
            .noDefaultValue()
            .withDescription("Vector search column name (required)");

    /**
     * Distance metric type: L2, Cosine, Dot
     */
    public static final ConfigOption<String> VECTOR_METRIC = ConfigOptions
            .key("vector.metric")
            .stringType()
            .defaultValue("L2")
            .withDescription("Vector distance metric type: L2 (Euclidean), Cosine, Dot, default L2");

    /**
     * IVF search probe count
     */
    public static final ConfigOption<Integer> VECTOR_NPROBES = ConfigOptions
            .key("vector.nprobes")
            .intType()
            .defaultValue(20)
            .withDescription("Number of IVF index search probes, default 20");

    /**
     * HNSW search width
     */
    public static final ConfigOption<Integer> VECTOR_EF = ConfigOptions
            .key("vector.ef")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW search width ef, default 100");

    /**
     * Refine factor
     */
    public static final ConfigOption<Integer> VECTOR_REFINE_FACTOR = ConfigOptions
            .key("vector.refine-factor")
            .intType()
            .noDefaultValue()
            .withDescription("Vector search refine factor for improving recall");

    // ==================== Catalog Configuration ====================

    /**
     * Default database name
     */
    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key("default-database")
            .stringType()
            .defaultValue("default")
            .withDescription("Catalog default database name, default 'default'");

    /**
     * Warehouse path
     */
    public static final ConfigOption<String> WAREHOUSE = ConfigOptions
            .key("warehouse")
            .stringType()
            .noDefaultValue()
            .withDescription("Lance data warehouse path (required)");

    // ==================== Memory Configuration ====================

    /**
     * Upper bound in bytes for each Arrow allocator created by the connector.
     *
     * <p>Opt-in: when unset the allocator is unbounded, which is the historical behaviour. The
     * bound applies per allocator instance (one per sink, source, catalog or index builder), so it
     * caps a single component's blast radius rather than the connector's total footprint.
     */
    public static final ConfigOption<Long> ARROW_ALLOCATOR_MAX_BYTES = ConfigOptions
            .key("arrow.allocator-max-bytes")
            .longType()
            .defaultValue(Long.MAX_VALUE)
            .withDescription(
                    "Maximum bytes each Arrow allocator may reserve. Defaults to unlimited.");

    // ==================== Write Mode Enum ====================

    /**
     * Write mode enum
     */
    public enum WriteMode {
        APPEND("append"),
        OVERWRITE("overwrite");

        private final String value;

        WriteMode(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static WriteMode fromValue(String value) {
            for (WriteMode mode : values()) {
                if (mode.value.equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unsupported write mode: " + value + ", supported modes: append, overwrite");
        }
    }

    // ==================== Index Type Enum ====================

    /**
     * Index type enum
     */
    public enum IndexType {
        IVF_PQ("IVF_PQ"),
        IVF_HNSW("IVF_HNSW"),
        IVF_FLAT("IVF_FLAT");

        private final String value;

        IndexType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static IndexType fromValue(String value) {
            for (IndexType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unsupported index type: " + value + ", supported types: IVF_PQ, IVF_HNSW, IVF_FLAT");
        }
    }

    // ==================== Metric Type Enum ====================

    /**
     * Distance metric type enum
     */
    public enum MetricType {
        L2("L2"),
        COSINE("Cosine"),
        DOT("Dot");

        private final String value;

        MetricType(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static MetricType fromValue(String value) {
            for (MetricType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unsupported metric type: " + value + ", supported types: L2, Cosine, Dot");
        }
    }

    // ==================== Configuration Class ====================

    private final String path;
    private final int readBatchSize;
    private final Long readLimit;
    private final List<String> readColumns;
    private final String readFilter;
    private final Long readVersion;
    private final String readAsOfTimestamp;
    private final int writeBatchSize;
    private final WriteMode writeMode;
    private final int writeMaxRowsPerFile;
    private final IndexType indexType;
    private final String indexColumn;
    private final int indexNumPartitions;
    private final Integer indexNumSubVectors;
    private final int indexNumBits;
    private final int indexMaxLevel;
    private final int indexM;
    private final int indexEfConstruction;
    private final String vectorColumn;
    private final MetricType vectorMetric;
    private final int vectorNprobes;
    private final int vectorEf;
    private final Integer vectorRefineFactor;
    private final String defaultDatabase;
    private final String warehouse;
    private final Map<String, String> hadoopConfig;
    private final long arrowAllocatorMaxBytes;

    private LanceOptions(Builder builder) {
        this.path = builder.path;
        this.readBatchSize = builder.readBatchSize;
        this.readLimit = builder.readLimit;
        this.readColumns = builder.readColumns;
        this.readFilter = builder.readFilter;
        this.readVersion = builder.readVersion;
        this.readAsOfTimestamp = builder.readAsOfTimestamp;
        this.writeBatchSize = builder.writeBatchSize;
        this.writeMode = builder.writeMode;
        this.writeMaxRowsPerFile = builder.writeMaxRowsPerFile;
        this.indexType = builder.indexType;
        this.indexColumn = builder.indexColumn;
        this.indexNumPartitions = builder.indexNumPartitions;
        this.indexNumSubVectors = builder.indexNumSubVectors;
        this.indexNumBits = builder.indexNumBits;
        this.indexMaxLevel = builder.indexMaxLevel;
        this.indexM = builder.indexM;
        this.indexEfConstruction = builder.indexEfConstruction;
        this.vectorColumn = builder.vectorColumn;
        this.vectorMetric = builder.vectorMetric;
        this.vectorNprobes = builder.vectorNprobes;
        this.vectorEf = builder.vectorEf;
        this.vectorRefineFactor = builder.vectorRefineFactor;
        this.defaultDatabase = builder.defaultDatabase;
        this.warehouse = builder.warehouse;
        this.hadoopConfig = builder.hadoopConfig == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(builder.hadoopConfig));
        this.arrowAllocatorMaxBytes = builder.arrowAllocatorMaxBytes;
    }

    // ==================== Getter Methods ====================

    public String getPath() {
        return path;
    }

    public int getReadBatchSize() {
        return readBatchSize;
    }

    public Long getReadLimit() {
        return readLimit;
    }

    public List<String> getReadColumns() {
        return readColumns;
    }

    public String getReadFilter() {
        return readFilter;
    }

    public Long getReadVersion() {
        return readVersion;
    }

    public String getReadAsOfTimestamp() {
        return readAsOfTimestamp;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public int getWriteMaxRowsPerFile() {
        return writeMaxRowsPerFile;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public int getIndexNumPartitions() {
        return indexNumPartitions;
    }

    public Integer getIndexNumSubVectors() {
        return indexNumSubVectors;
    }

    public int getIndexNumBits() {
        return indexNumBits;
    }

    public int getIndexMaxLevel() {
        return indexMaxLevel;
    }

    public int getIndexM() {
        return indexM;
    }

    public int getIndexEfConstruction() {
        return indexEfConstruction;
    }

    public String getVectorColumn() {
        return vectorColumn;
    }

    public MetricType getVectorMetric() {
        return vectorMetric;
    }

    public int getVectorNprobes() {
        return vectorNprobes;
    }

    public int getVectorEf() {
        return vectorEf;
    }

    public Integer getVectorRefineFactor() {
        return vectorRefineFactor;
    }

    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    public String getWarehouse() {
        return warehouse;
    }

    /**
     * Extra Hadoop configuration key/value pairs (e.g. {@code tbdsfs.meta}) that should be
     * injected into the Hadoop {@link org.apache.hadoop.conf.Configuration} used to resolve
     * Hadoop-family dataset paths (tbdsfs/hdfs). Populated from SQL {@code WITH} options
     * prefixed with {@code hadoop.}.
     */
    public Map<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    /**
     * Upper bound in bytes for the Arrow allocator created by the component consuming these
     * options. {@link Long#MAX_VALUE} means unbounded.
     */
    public long getArrowAllocatorMaxBytes() {
        return arrowAllocatorMaxBytes;
    }

    // ==================== Builder ====================

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create LanceOptions from Flink Configuration
     */
    public static LanceOptions fromConfiguration(Configuration config) {
        Builder builder = builder();

        // Common configuration
        if (config.contains(PATH)) {
            builder.path(config.get(PATH));
        }

        // Source configuration
        builder.readBatchSize(config.get(READ_BATCH_SIZE));
        if (config.contains(READ_LIMIT)) {
            builder.readLimit(config.get(READ_LIMIT));
        }
        if (config.contains(READ_COLUMNS)) {
            String columnsStr = config.get(READ_COLUMNS);
            if (columnsStr != null && !columnsStr.isEmpty()) {
                builder.readColumns(Arrays.asList(columnsStr.split(",")));
            }
        }
        if (config.contains(READ_FILTER)) {
            builder.readFilter(config.get(READ_FILTER));
        }
        if (config.contains(READ_VERSION)) {
            builder.readVersion(config.get(READ_VERSION));
        }
        if (config.contains(READ_AS_OF_TIMESTAMP)) {
            builder.readAsOfTimestamp(config.get(READ_AS_OF_TIMESTAMP));
        }

        // Sink configuration
        builder.writeBatchSize(config.get(WRITE_BATCH_SIZE));
        builder.writeMode(WriteMode.fromValue(config.get(WRITE_MODE)));
        builder.writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE));

        // Index configuration
        builder.indexType(IndexType.fromValue(config.get(INDEX_TYPE)));
        if (config.contains(INDEX_COLUMN)) {
            builder.indexColumn(config.get(INDEX_COLUMN));
        }
        builder.indexNumPartitions(config.get(INDEX_NUM_PARTITIONS));
        if (config.contains(INDEX_NUM_SUB_VECTORS)) {
            builder.indexNumSubVectors(config.get(INDEX_NUM_SUB_VECTORS));
        }
        builder.indexNumBits(config.get(INDEX_NUM_BITS));
        builder.indexMaxLevel(config.get(INDEX_MAX_LEVEL));
        builder.indexM(config.get(INDEX_M));
        builder.indexEfConstruction(config.get(INDEX_EF_CONSTRUCTION));

        // Vector search configuration
        if (config.contains(VECTOR_COLUMN)) {
            builder.vectorColumn(config.get(VECTOR_COLUMN));
        }
        builder.vectorMetric(MetricType.fromValue(config.get(VECTOR_METRIC)));
        builder.vectorNprobes(config.get(VECTOR_NPROBES));
        builder.vectorEf(config.get(VECTOR_EF));
        if (config.contains(VECTOR_REFINE_FACTOR)) {
            builder.vectorRefineFactor(config.get(VECTOR_REFINE_FACTOR));
        }

        // Catalog configuration
        builder.defaultDatabase(config.get(DEFAULT_DATABASE));
        if (config.contains(WAREHOUSE)) {
            builder.warehouse(config.get(WAREHOUSE));
        }

        // Memory configuration
        builder.arrowAllocatorMaxBytes(config.get(ARROW_ALLOCATOR_MAX_BYTES));

        return builder.build();
    }

    /**
     * Configuration builder
     */
    public static class Builder {
        private String path;
        private int readBatchSize = 1024;
        private Long readLimit;
        private List<String> readColumns = Collections.emptyList();
        private String readFilter;
        private Long readVersion;
        private String readAsOfTimestamp;
        private int writeBatchSize = 1024;
        private WriteMode writeMode = WriteMode.APPEND;
        private int writeMaxRowsPerFile = 1000000;
        private IndexType indexType = IndexType.IVF_PQ;
        private String indexColumn;
        private int indexNumPartitions = 256;
        private Integer indexNumSubVectors;
        private int indexNumBits = 8;
        private int indexMaxLevel = 7;
        private int indexM = 16;
        private int indexEfConstruction = 100;
        private String vectorColumn;
        private MetricType vectorMetric = MetricType.L2;
        private int vectorNprobes = 20;
        private int vectorEf = 100;
        private Integer vectorRefineFactor;
        private String defaultDatabase = "default";
        private String warehouse;
        private Map<String, String> hadoopConfig;
        private long arrowAllocatorMaxBytes = Long.MAX_VALUE;

        /**
         * Bound the Arrow allocator to {@code maxBytes}. Non-positive values are normalised to
         * unbounded so a misconfigured value degrades to the historical behaviour.
         */
        public Builder arrowAllocatorMaxBytes(long maxBytes) {
            this.arrowAllocatorMaxBytes = maxBytes > 0 ? maxBytes : Long.MAX_VALUE;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder readBatchSize(int readBatchSize) {
            this.readBatchSize = readBatchSize;
            return this;
        }

        public Builder readLimit(Long readLimit) {
            this.readLimit = readLimit;
            return this;
        }

        public Builder readColumns(List<String> readColumns) {
            this.readColumns = readColumns != null ? readColumns : Collections.emptyList();
            return this;
        }

        public Builder readFilter(String readFilter) {
            this.readFilter = readFilter;
            return this;
        }

        public Builder readVersion(Long readVersion) {
            this.readVersion = readVersion;
            return this;
        }

        public Builder readAsOfTimestamp(String readAsOfTimestamp) {
            this.readAsOfTimestamp = readAsOfTimestamp;
            return this;
        }

        public Builder writeBatchSize(int writeBatchSize) {
            this.writeBatchSize = writeBatchSize;
            return this;
        }

        public Builder writeMode(WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder writeMaxRowsPerFile(int writeMaxRowsPerFile) {
            this.writeMaxRowsPerFile = writeMaxRowsPerFile;
            return this;
        }

        public Builder indexType(IndexType indexType) {
            this.indexType = indexType;
            return this;
        }

        public Builder indexColumn(String indexColumn) {
            this.indexColumn = indexColumn;
            return this;
        }

        public Builder indexNumPartitions(int indexNumPartitions) {
            this.indexNumPartitions = indexNumPartitions;
            return this;
        }

        public Builder indexNumSubVectors(Integer indexNumSubVectors) {
            this.indexNumSubVectors = indexNumSubVectors;
            return this;
        }

        public Builder indexNumBits(int indexNumBits) {
            this.indexNumBits = indexNumBits;
            return this;
        }

        public Builder indexMaxLevel(int indexMaxLevel) {
            this.indexMaxLevel = indexMaxLevel;
            return this;
        }

        public Builder indexM(int indexM) {
            this.indexM = indexM;
            return this;
        }

        public Builder indexEfConstruction(int indexEfConstruction) {
            this.indexEfConstruction = indexEfConstruction;
            return this;
        }

        public Builder vectorColumn(String vectorColumn) {
            this.vectorColumn = vectorColumn;
            return this;
        }

        public Builder vectorMetric(MetricType vectorMetric) {
            this.vectorMetric = vectorMetric;
            return this;
        }

        public Builder vectorNprobes(int vectorNprobes) {
            this.vectorNprobes = vectorNprobes;
            return this;
        }

        public Builder vectorEf(int vectorEf) {
            this.vectorEf = vectorEf;
            return this;
        }

        public Builder vectorRefineFactor(Integer vectorRefineFactor) {
            this.vectorRefineFactor = vectorRefineFactor;
            return this;
        }

        public Builder defaultDatabase(String defaultDatabase) {
            this.defaultDatabase = defaultDatabase;
            return this;
        }

        public Builder warehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        public Builder hadoopConfig(Map<String, String> hadoopConfig) {
            this.hadoopConfig = hadoopConfig;
            return this;
        }

        /**
         * Build LanceOptions instance with validation
         */
        public LanceOptions build() {
            validate();
            return new LanceOptions(this);
        }

        /**
         * Validate configuration
         */
        private void validate() {
            // Validate read batch size
            if (readBatchSize <= 0) {
                throw new IllegalArgumentException("read.batch-size must be greater than 0, current value: " + readBatchSize);
            }

            // Validate Limit (if set)
            if (readLimit != null && readLimit < 0) {
                throw new IllegalArgumentException("read.limit must be greater than or equal to 0, current value: " + readLimit);
            }

            // Validate write batch size
            if (writeBatchSize <= 0) {
                throw new IllegalArgumentException("write.batch-size must be greater than 0, current value: " + writeBatchSize);
            }

            // Validate max rows per file
            if (writeMaxRowsPerFile <= 0) {
                throw new IllegalArgumentException("write.max-rows-per-file must be greater than 0, current value: " + writeMaxRowsPerFile);
            }

            // Validate index partition count
            if (indexNumPartitions <= 0) {
                throw new IllegalArgumentException("index.num-partitions must be greater than 0, current value: " + indexNumPartitions);
            }

            // Validate PQ sub-vector count
            if (indexNumSubVectors != null && indexNumSubVectors <= 0) {
                throw new IllegalArgumentException("index.num-sub-vectors must be greater than 0, current value: " + indexNumSubVectors);
            }

            // Validate PQ quantization bits
            if (indexNumBits <= 0 || indexNumBits > 16) {
                throw new IllegalArgumentException("index.num-bits must be between 1 and 16, current value: " + indexNumBits);
            }

            // Validate HNSW parameters
            if (indexMaxLevel <= 0) {
                throw new IllegalArgumentException("index.max-level must be greater than 0, current value: " + indexMaxLevel);
            }

            if (indexM <= 0) {
                throw new IllegalArgumentException("index.m must be greater than 0, current value: " + indexM);
            }

            if (indexEfConstruction <= 0) {
                throw new IllegalArgumentException("index.ef-construction must be greater than 0, current value: " + indexEfConstruction);
            }

            // Validate vector search parameters
            if (vectorNprobes <= 0) {
                throw new IllegalArgumentException("vector.nprobes must be greater than 0, current value: " + vectorNprobes);
            }

            if (vectorEf <= 0) {
                throw new IllegalArgumentException("vector.ef must be greater than 0, current value: " + vectorEf);
            }

            if (vectorRefineFactor != null && vectorRefineFactor <= 0) {
                throw new IllegalArgumentException("vector.refine-factor must be greater than 0, current value: " + vectorRefineFactor);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LanceOptions that = (LanceOptions) o;
        return readBatchSize == that.readBatchSize &&
                Objects.equals(readLimit, that.readLimit) &&
                writeBatchSize == that.writeBatchSize &&
                writeMaxRowsPerFile == that.writeMaxRowsPerFile &&
                indexNumPartitions == that.indexNumPartitions &&
                indexNumBits == that.indexNumBits &&
                indexMaxLevel == that.indexMaxLevel &&
                indexM == that.indexM &&
                indexEfConstruction == that.indexEfConstruction &&
                vectorNprobes == that.vectorNprobes &&
                vectorEf == that.vectorEf &&
                Objects.equals(path, that.path) &&
                Objects.equals(readColumns, that.readColumns) &&
                Objects.equals(readFilter, that.readFilter) &&
                Objects.equals(readVersion, that.readVersion) &&
                Objects.equals(readAsOfTimestamp, that.readAsOfTimestamp) &&
                writeMode == that.writeMode &&
                indexType == that.indexType &&
                Objects.equals(indexColumn, that.indexColumn) &&
                Objects.equals(indexNumSubVectors, that.indexNumSubVectors) &&
                Objects.equals(vectorColumn, that.vectorColumn) &&
                vectorMetric == that.vectorMetric &&
                Objects.equals(vectorRefineFactor, that.vectorRefineFactor) &&
                Objects.equals(defaultDatabase, that.defaultDatabase) &&
                Objects.equals(warehouse, that.warehouse);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, readBatchSize, readLimit, readColumns, readFilter, writeBatchSize, writeMode,
                writeMaxRowsPerFile, indexType, indexColumn, indexNumPartitions, indexNumSubVectors,
                indexNumBits, indexMaxLevel, indexM, indexEfConstruction, vectorColumn, vectorMetric,
                vectorNprobes, vectorEf, vectorRefineFactor, defaultDatabase, warehouse,
                readVersion, readAsOfTimestamp);
    }

    @Override
    public String toString() {
        return "LanceOptions{" +
                "path='" + path + '\'' +
                ", readBatchSize=" + readBatchSize +
                ", readLimit=" + readLimit +
                ", readColumns=" + readColumns +
                ", readFilter='" + readFilter + '\'' +
                ", readVersion=" + readVersion +
                ", readAsOfTimestamp='" + readAsOfTimestamp + '\'' +
                ", writeBatchSize=" + writeBatchSize +
                ", writeMode=" + writeMode +
                ", writeMaxRowsPerFile=" + writeMaxRowsPerFile +
                ", indexType=" + indexType +
                ", indexColumn='" + indexColumn + '\'' +
                ", indexNumPartitions=" + indexNumPartitions +
                ", indexNumSubVectors=" + indexNumSubVectors +
                ", indexNumBits=" + indexNumBits +
                ", indexMaxLevel=" + indexMaxLevel +
                ", indexM=" + indexM +
                ", indexEfConstruction=" + indexEfConstruction +
                ", vectorColumn='" + vectorColumn + '\'' +
                ", vectorMetric=" + vectorMetric +
                ", vectorNprobes=" + vectorNprobes +
                ", vectorEf=" + vectorEf +
                ", vectorRefineFactor=" + vectorRefineFactor +
                ", defaultDatabase='" + defaultDatabase + '\'' +
                ", warehouse='" + warehouse + '\'' +
                '}';
    }
}
