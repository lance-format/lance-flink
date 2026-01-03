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
import java.util.List;
import java.util.Objects;

/**
 * Lance 连接器配置选项。
 * 
 * <p>定义了 Source、Sink、向量索引和向量检索的所有配置项。
 */
public class LanceOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    // ==================== 通用配置 ====================

    /**
     * Lance 数据集路径
     */
    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Lance 数据集的路径（必填）");

    // ==================== Source 配置 ====================

    /**
     * 读取批次大小
     */
    public static final ConfigOption<Integer> READ_BATCH_SIZE = ConfigOptions
            .key("read.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("每次读取的批次大小，默认 1024");

    /**
     * 读取行数限制（Limit 下推）
     */
    public static final ConfigOption<Long> READ_LIMIT = ConfigOptions
            .key("read.limit")
            .longType()
            .noDefaultValue()
            .withDescription("读取的最大行数限制（用于 Limit 下推）");

    /**
     * 读取的列列表（逗号分隔）
     */
    public static final ConfigOption<String> READ_COLUMNS = ConfigOptions
            .key("read.columns")
            .stringType()
            .noDefaultValue()
            .withDescription("要读取的列列表，逗号分隔。为空则读取所有列");

    /**
     * 数据过滤条件
     */
    public static final ConfigOption<String> READ_FILTER = ConfigOptions
            .key("read.filter")
            .stringType()
            .noDefaultValue()
            .withDescription("数据过滤条件，使用 SQL WHERE 子句语法");

    // ==================== Sink 配置 ====================

    /**
     * 写入批次大小
     */
    public static final ConfigOption<Integer> WRITE_BATCH_SIZE = ConfigOptions
            .key("write.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("每次写入的批次大小，默认 1024");

    /**
     * 写入模式：append 或 overwrite
     */
    public static final ConfigOption<String> WRITE_MODE = ConfigOptions
            .key("write.mode")
            .stringType()
            .defaultValue("append")
            .withDescription("写入模式：append（追加）或 overwrite（覆盖），默认 append");

    /**
     * 每个文件的最大行数
     */
    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE = ConfigOptions
            .key("write.max-rows-per-file")
            .intType()
            .defaultValue(1000000)
            .withDescription("每个数据文件的最大行数，默认 1000000");

    // ==================== 向量索引配置 ====================

    /**
     * 索引类型：IVF_PQ、IVF_HNSW、IVF_FLAT
     */
    public static final ConfigOption<String> INDEX_TYPE = ConfigOptions
            .key("index.type")
            .stringType()
            .defaultValue("IVF_PQ")
            .withDescription("向量索引类型：IVF_PQ、IVF_HNSW、IVF_FLAT，默认 IVF_PQ");

    /**
     * 索引列名
     */
    public static final ConfigOption<String> INDEX_COLUMN = ConfigOptions
            .key("index.column")
            .stringType()
            .noDefaultValue()
            .withDescription("要建立索引的向量列名（必填）");

    /**
     * IVF 分区数量
     */
    public static final ConfigOption<Integer> INDEX_NUM_PARTITIONS = ConfigOptions
            .key("index.num-partitions")
            .intType()
            .defaultValue(256)
            .withDescription("IVF 索引的分区数量，默认 256");

    /**
     * PQ 子向量数量
     */
    public static final ConfigOption<Integer> INDEX_NUM_SUB_VECTORS = ConfigOptions
            .key("index.num-sub-vectors")
            .intType()
            .noDefaultValue()
            .withDescription("PQ 索引的子向量数量，默认自动计算");

    /**
     * PQ 量化位数
     */
    public static final ConfigOption<Integer> INDEX_NUM_BITS = ConfigOptions
            .key("index.num-bits")
            .intType()
            .defaultValue(8)
            .withDescription("PQ 量化位数，默认 8");

    /**
     * HNSW 最大层级
     */
    public static final ConfigOption<Integer> INDEX_MAX_LEVEL = ConfigOptions
            .key("index.max-level")
            .intType()
            .defaultValue(7)
            .withDescription("HNSW 索引的最大层级，默认 7");

    /**
     * HNSW 每层连接数 M
     */
    public static final ConfigOption<Integer> INDEX_M = ConfigOptions
            .key("index.m")
            .intType()
            .defaultValue(16)
            .withDescription("HNSW 每层的连接数 M，默认 16");

    /**
     * HNSW 构建时的搜索宽度
     */
    public static final ConfigOption<Integer> INDEX_EF_CONSTRUCTION = ConfigOptions
            .key("index.ef-construction")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW 构建时的搜索宽度 ef_construction，默认 100");

    // ==================== 向量检索配置 ====================

    /**
     * 向量检索列名
     */
    public static final ConfigOption<String> VECTOR_COLUMN = ConfigOptions
            .key("vector.column")
            .stringType()
            .noDefaultValue()
            .withDescription("向量检索的列名（必填）");

    /**
     * 距离度量类型：L2、Cosine、Dot
     */
    public static final ConfigOption<String> VECTOR_METRIC = ConfigOptions
            .key("vector.metric")
            .stringType()
            .defaultValue("L2")
            .withDescription("向量距离度量类型：L2（欧氏距离）、Cosine（余弦相似度）、Dot（点积），默认 L2");

    /**
     * IVF 检索探针数
     */
    public static final ConfigOption<Integer> VECTOR_NPROBES = ConfigOptions
            .key("vector.nprobes")
            .intType()
            .defaultValue(20)
            .withDescription("IVF 索引检索时的探针数量，默认 20");

    /**
     * HNSW 搜索宽度
     */
    public static final ConfigOption<Integer> VECTOR_EF = ConfigOptions
            .key("vector.ef")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW 搜索时的宽度 ef，默认 100");

    /**
     * 精细化因子
     */
    public static final ConfigOption<Integer> VECTOR_REFINE_FACTOR = ConfigOptions
            .key("vector.refine-factor")
            .intType()
            .noDefaultValue()
            .withDescription("向量检索的精细化因子，用于提高召回率");

    // ==================== Catalog 配置 ====================

    /**
     * 默认数据库名称
     */
    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key("default-database")
            .stringType()
            .defaultValue("default")
            .withDescription("Catalog 默认数据库名称，默认 default");

    /**
     * 仓库路径
     */
    public static final ConfigOption<String> WAREHOUSE = ConfigOptions
            .key("warehouse")
            .stringType()
            .noDefaultValue()
            .withDescription("Lance 数据仓库路径（必填）");

    // ==================== 写入模式枚举 ====================

    /**
     * 写入模式枚举
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
            throw new IllegalArgumentException("不支持的写入模式: " + value + "，支持的模式: append, overwrite");
        }
    }

    // ==================== 索引类型枚举 ====================

    /**
     * 索引类型枚举
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
            throw new IllegalArgumentException("不支持的索引类型: " + value + "，支持的类型: IVF_PQ, IVF_HNSW, IVF_FLAT");
        }
    }

    // ==================== 距离度量类型枚举 ====================

    /**
     * 距离度量类型枚举
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
            throw new IllegalArgumentException("不支持的距离度量类型: " + value + "，支持的类型: L2, Cosine, Dot");
        }
    }

    // ==================== 配置类 ====================

    private final String path;
    private final int readBatchSize;
    private final Long readLimit;
    private final List<String> readColumns;
    private final String readFilter;
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

    private LanceOptions(Builder builder) {
        this.path = builder.path;
        this.readBatchSize = builder.readBatchSize;
        this.readLimit = builder.readLimit;
        this.readColumns = builder.readColumns;
        this.readFilter = builder.readFilter;
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
    }

    // ==================== Getter 方法 ====================

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

    // ==================== Builder ====================

    public static Builder builder() {
        return new Builder();
    }

    /**
     * 从 Flink Configuration 创建 LanceOptions
     */
    public static LanceOptions fromConfiguration(Configuration config) {
        Builder builder = builder();

        // 通用配置
        if (config.contains(PATH)) {
            builder.path(config.get(PATH));
        }

        // Source 配置
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

        // Sink 配置
        builder.writeBatchSize(config.get(WRITE_BATCH_SIZE));
        builder.writeMode(WriteMode.fromValue(config.get(WRITE_MODE)));
        builder.writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE));

        // 索引配置
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

        // 向量检索配置
        if (config.contains(VECTOR_COLUMN)) {
            builder.vectorColumn(config.get(VECTOR_COLUMN));
        }
        builder.vectorMetric(MetricType.fromValue(config.get(VECTOR_METRIC)));
        builder.vectorNprobes(config.get(VECTOR_NPROBES));
        builder.vectorEf(config.get(VECTOR_EF));
        if (config.contains(VECTOR_REFINE_FACTOR)) {
            builder.vectorRefineFactor(config.get(VECTOR_REFINE_FACTOR));
        }

        // Catalog 配置
        builder.defaultDatabase(config.get(DEFAULT_DATABASE));
        if (config.contains(WAREHOUSE)) {
            builder.warehouse(config.get(WAREHOUSE));
        }

        return builder.build();
    }

    /**
     * 配置构建器
     */
    public static class Builder {
        private String path;
        private int readBatchSize = 1024;
        private Long readLimit;
        private List<String> readColumns = Collections.emptyList();
        private String readFilter;
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

        /**
         * 构建 LanceOptions 实例并进行校验
         */
        public LanceOptions build() {
            validate();
            return new LanceOptions(this);
        }

        /**
         * 校验配置
         */
        private void validate() {
            // 校验读取批次大小
            if (readBatchSize <= 0) {
                throw new IllegalArgumentException("read.batch-size 必须大于 0，当前值: " + readBatchSize);
            }

            // 校验 Limit（如果设置了）
            if (readLimit != null && readLimit < 0) {
                throw new IllegalArgumentException("read.limit 必须大于等于 0，当前值: " + readLimit);
            }

            // 校验写入批次大小
            if (writeBatchSize <= 0) {
                throw new IllegalArgumentException("write.batch-size 必须大于 0，当前值: " + writeBatchSize);
            }

            // 校验每个文件最大行数
            if (writeMaxRowsPerFile <= 0) {
                throw new IllegalArgumentException("write.max-rows-per-file 必须大于 0，当前值: " + writeMaxRowsPerFile);
            }

            // 校验索引分区数
            if (indexNumPartitions <= 0) {
                throw new IllegalArgumentException("index.num-partitions 必须大于 0，当前值: " + indexNumPartitions);
            }

            // 校验 PQ 子向量数
            if (indexNumSubVectors != null && indexNumSubVectors <= 0) {
                throw new IllegalArgumentException("index.num-sub-vectors 必须大于 0，当前值: " + indexNumSubVectors);
            }

            // 校验 PQ 量化位数
            if (indexNumBits <= 0 || indexNumBits > 16) {
                throw new IllegalArgumentException("index.num-bits 必须在 1-16 之间，当前值: " + indexNumBits);
            }

            // 校验 HNSW 参数
            if (indexMaxLevel <= 0) {
                throw new IllegalArgumentException("index.max-level 必须大于 0，当前值: " + indexMaxLevel);
            }

            if (indexM <= 0) {
                throw new IllegalArgumentException("index.m 必须大于 0，当前值: " + indexM);
            }

            if (indexEfConstruction <= 0) {
                throw new IllegalArgumentException("index.ef-construction 必须大于 0，当前值: " + indexEfConstruction);
            }

            // 校验向量检索参数
            if (vectorNprobes <= 0) {
                throw new IllegalArgumentException("vector.nprobes 必须大于 0，当前值: " + vectorNprobes);
            }

            if (vectorEf <= 0) {
                throw new IllegalArgumentException("vector.ef 必须大于 0，当前值: " + vectorEf);
            }

            if (vectorRefineFactor != null && vectorRefineFactor <= 0) {
                throw new IllegalArgumentException("vector.refine-factor 必须大于 0，当前值: " + vectorRefineFactor);
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
                vectorNprobes, vectorEf, vectorRefineFactor, defaultDatabase, warehouse);
    }

    @Override
    public String toString() {
        return "LanceOptions{" +
                "path='" + path + '\'' +
                ", readBatchSize=" + readBatchSize +
                ", readLimit=" + readLimit +
                ", readColumns=" + readColumns +
                ", readFilter='" + readFilter + '\'' +
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
