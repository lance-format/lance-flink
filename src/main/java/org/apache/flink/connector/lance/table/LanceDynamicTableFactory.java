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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lance dynamic table factory.
 * 
 * <p>Implements Flink Table API DynamicTableSourceFactory and DynamicTableSinkFactory interfaces,
 * supports creating Lance tables via SQL DDL.
 * 
 * <p>Usage example:
 * <pre>{@code
 * CREATE TABLE lance_table (
 *     id BIGINT,
 *     content STRING,
 *     embedding ARRAY<FLOAT>
 * ) WITH (
 *     'connector' = 'lance',
 *     'path' = '/path/to/dataset'
 * );
 * }</pre>
 */
public class LanceDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "lance";

    // ==================== Configuration Options Definition ====================

    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Lance dataset path");

    public static final ConfigOption<Integer> READ_BATCH_SIZE = ConfigOptions
            .key("read.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("Read batch size");

    public static final ConfigOption<String> READ_COLUMNS = ConfigOptions
            .key("read.columns")
            .stringType()
            .noDefaultValue()
            .withDescription("Columns to read, comma separated");

    public static final ConfigOption<String> READ_FILTER = ConfigOptions
            .key("read.filter")
            .stringType()
            .noDefaultValue()
            .withDescription("Data filter condition");

    public static final ConfigOption<Long> READ_VERSION = ConfigOptions
            .key("read.version")
            .longType()
            .noDefaultValue()
            .withDescription("Time travel: read the given Lance dataset version (issue #5)");

    public static final ConfigOption<String> READ_AS_OF_TIMESTAMP = ConfigOptions
            .key("read.as-of-timestamp")
            .stringType()
            .noDefaultValue()
            .withDescription("Time travel: read the dataset as of the given ISO-8601 timestamp (issue #5). Ignored when read.version is set.");

    public static final ConfigOption<Integer> WRITE_BATCH_SIZE = ConfigOptions
            .key("write.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("Write batch size");

    public static final ConfigOption<String> WRITE_MODE = ConfigOptions
            .key("write.mode")
            .stringType()
            .defaultValue("append")
            .withDescription("Write mode: append or overwrite");

    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE = ConfigOptions
            .key("write.max-rows-per-file")
            .intType()
            .defaultValue(1000000)
            .withDescription("Maximum rows per file");

    public static final ConfigOption<String> INDEX_TYPE = ConfigOptions
            .key("index.type")
            .stringType()
            .defaultValue("IVF_PQ")
            .withDescription("Vector index type");

    public static final ConfigOption<String> INDEX_COLUMN = ConfigOptions
            .key("index.column")
            .stringType()
            .noDefaultValue()
            .withDescription("Index column name");

    public static final ConfigOption<Integer> INDEX_NUM_PARTITIONS = ConfigOptions
            .key("index.num-partitions")
            .intType()
            .defaultValue(256)
            .withDescription("IVF partition count");

    public static final ConfigOption<Integer> INDEX_NUM_SUB_VECTORS = ConfigOptions
            .key("index.num-sub-vectors")
            .intType()
            .noDefaultValue()
            .withDescription("PQ sub-vector count");

    public static final ConfigOption<Integer> INDEX_NUM_BITS = ConfigOptions
            .key("index.num-bits")
            .intType()
            .defaultValue(8)
            .withDescription("PQ quantization bits (1-16)");

    public static final ConfigOption<Integer> INDEX_MAX_LEVEL = ConfigOptions
            .key("index.max-level")
            .intType()
            .defaultValue(7)
            .withDescription("HNSW index max level");

    public static final ConfigOption<Integer> INDEX_M = ConfigOptions
            .key("index.m")
            .intType()
            .defaultValue(16)
            .withDescription("HNSW connections per level");

    public static final ConfigOption<Integer> INDEX_EF_CONSTRUCTION = ConfigOptions
            .key("index.ef-construction")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW construction search width");

    public static final ConfigOption<String> VECTOR_COLUMN = ConfigOptions
            .key("vector.column")
            .stringType()
            .noDefaultValue()
            .withDescription("Vector column name");

    public static final ConfigOption<String> VECTOR_METRIC = ConfigOptions
            .key("vector.metric")
            .stringType()
            .defaultValue("L2")
            .withDescription("Distance metric type: L2, Cosine, Dot");

    public static final ConfigOption<Integer> VECTOR_NPROBES = ConfigOptions
            .key("vector.nprobes")
            .intType()
            .defaultValue(20)
            .withDescription("IVF search probe count");

    public static final ConfigOption<Integer> VECTOR_EF = ConfigOptions
            .key("vector.ef")
            .intType()
            .defaultValue(100)
            .withDescription("HNSW search width");

    public static final ConfigOption<Integer> VECTOR_REFINE_FACTOR = ConfigOptions
            .key("vector.refine-factor")
            .intType()
            .noDefaultValue()
            .withDescription("Re-rank factor for recall");

    public static final ConfigOption<Long> READ_LIMIT = ConfigOptions
            .key("read.limit")
            .longType()
            .noDefaultValue()
            .withDescription("Maximum number of rows to read");

    public static final ConfigOption<Long> WRITE_MEMORY_CAP_BYTES = ConfigOptions
            .key("write.memory-cap-bytes")
            .longType()
            .defaultValue(-1L)
            .withDescription(
                    "Maximum buffered bytes before an early fragment materialization; "
                            + "-1 means unbounded");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(READ_BATCH_SIZE);
        options.add(READ_LIMIT);
        options.add(READ_COLUMNS);
        options.add(READ_FILTER);
        options.add(READ_VERSION);
        options.add(READ_AS_OF_TIMESTAMP);
        options.add(WRITE_BATCH_SIZE);
        options.add(WRITE_MEMORY_CAP_BYTES);
        options.add(WRITE_MODE);
        options.add(WRITE_MAX_ROWS_PER_FILE);
        options.add(INDEX_TYPE);
        options.add(INDEX_COLUMN);
        options.add(INDEX_NUM_PARTITIONS);
        options.add(INDEX_NUM_SUB_VECTORS);
        options.add(INDEX_NUM_BITS);
        options.add(INDEX_MAX_LEVEL);
        options.add(INDEX_M);
        options.add(INDEX_EF_CONSTRUCTION);
        options.add(VECTOR_COLUMN);
        options.add(VECTOR_METRIC);
        options.add(VECTOR_NPROBES);
        options.add(VECTOR_EF);
        options.add(VECTOR_REFINE_FACTOR);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        validateOptionsSkippingHadoopPrefixes(helper, tableOptions);

        ReadableConfig config = helper.getOptions();
        LanceOptions options = buildLanceOptions(config, tableOptions);

        return new LanceDynamicTableSource(
                options,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
        );
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        validateOptionsSkippingHadoopPrefixes(helper, tableOptions);

        ReadableConfig config = helper.getOptions();
        LanceOptions options = buildLanceOptions(config, tableOptions);

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        List<String> primaryKeys = extractPrimaryKeys(schema);
        int[] primaryKeyIndices = resolvePrimaryKeyIndices(schema, primaryKeys);

        return new LanceDynamicTableSink(
                options,
                schema.toPhysicalRowDataType(),
                primaryKeys,
                primaryKeyIndices
        );
    }

    /**
     * Extract the {@code PRIMARY KEY ... NOT ENFORCED} column names from the resolved schema.
     */
    private List<String> extractPrimaryKeys(ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .orElse(Collections.emptyList());
    }

    /**
     * Resolve primary-key column names to physical column indices.
     */
    private int[] resolvePrimaryKeyIndices(ResolvedSchema schema, List<String> primaryKeys) {
        if (primaryKeys.isEmpty()) {
            return new int[0];
        }
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();
        int[] indices = new int[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            int idx = fieldNames.indexOf(primaryKeys.get(i));
            Preconditions.checkArgument(
                    idx >= 0, "Primary key column '%s' not found in schema", primaryKeys.get(i));
            indices[i] = idx;
        }
        return indices;
    }

    /**
     * Extracts option keys prefixed with {@code hadoop.}, for {@code validateExcept} to skip
     * validation.
     */
    private static String[] extractHadoopOptionKeys(Map<String, String> tableOptions) {
        if (tableOptions == null) {
            return new String[0];
        }
        return tableOptions.keySet().stream()
                .filter(key -> key != null && key.startsWith("hadoop."))
                .toArray(String[]::new);
    }

    /**
     * {@link FactoryUtil.FactoryHelper#validateExcept(String...)} tightened its argument validation
     * in Flink 2.1: {@code Preconditions.checkArgument(prefixesToSkip.length > 0,
     * "Prefixes to skip can not be empty.")}. When the WITH clause has no {@code hadoop.}-prefixed
     * options, {@link #extractHadoopOptionKeys(Map)} returns an empty array, and passing it directly
     * to {@code validateExcept} throws {@code IllegalArgumentException} under Flink 2.1. This method
     * adds a guard: use {@code validateExcept(prefixes)} only when prefixes exist, otherwise fall
     * back to {@code validate()}. It is fully compatible with Flink 1.20 since 1.x accepts empty
     * varargs.
     */
    private static void validateOptionsSkippingHadoopPrefixes(
            FactoryUtil.TableFactoryHelper helper, Map<String, String> tableOptions) {
        String[] prefixes = extractHadoopOptionKeys(tableOptions);
        if (prefixes.length == 0) {
            helper.validate();
        } else {
            helper.validateExcept(prefixes);
        }
    }

    /**
     * Build LanceOptions from configuration
     */
    private LanceOptions buildLanceOptions(ReadableConfig config, Map<String, String> tableOptions) {
        LanceOptions.Builder builder = LanceOptions.builder();

        // Common configuration
        builder.path(config.get(PATH));

        // Source configuration
        builder.readBatchSize(config.get(READ_BATCH_SIZE));
        config.getOptional(READ_LIMIT).ifPresent(builder::readLimit);
        config.getOptional(READ_COLUMNS).ifPresent(columns -> {
            if (!columns.isEmpty()) {
                builder.readColumns(java.util.Arrays.asList(columns.split(",")));
            }
        });
        config.getOptional(READ_FILTER).ifPresent(builder::readFilter);
        config.getOptional(READ_VERSION).ifPresent(builder::readVersion);
        config.getOptional(READ_AS_OF_TIMESTAMP).ifPresent(builder::readAsOfTimestamp);

        // Sink configuration
        builder.writeBatchSize(config.get(WRITE_BATCH_SIZE));
        builder.writeMemoryCapBytes(config.get(WRITE_MEMORY_CAP_BYTES));
        builder.writeMode(LanceOptions.WriteMode.fromValue(config.get(WRITE_MODE)));
        builder.writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE));

        // Index configuration
        builder.indexType(LanceOptions.IndexType.fromValue(config.get(INDEX_TYPE)));
        config.getOptional(INDEX_COLUMN).ifPresent(builder::indexColumn);
        builder.indexNumPartitions(config.get(INDEX_NUM_PARTITIONS));
        config.getOptional(INDEX_NUM_SUB_VECTORS).ifPresent(builder::indexNumSubVectors);
        builder.indexNumBits(config.get(INDEX_NUM_BITS));
        builder.indexMaxLevel(config.get(INDEX_MAX_LEVEL));
        builder.indexM(config.get(INDEX_M));
        builder.indexEfConstruction(config.get(INDEX_EF_CONSTRUCTION));

        // Vector search configuration
        config.getOptional(VECTOR_COLUMN).ifPresent(builder::vectorColumn);
        builder.vectorMetric(LanceOptions.MetricType.fromValue(config.get(VECTOR_METRIC)));
        builder.vectorNprobes(config.get(VECTOR_NPROBES));
        builder.vectorEf(config.get(VECTOR_EF));
        config.getOptional(VECTOR_REFINE_FACTOR).ifPresent(builder::vectorRefineFactor);

        // Hadoop-family FileSystem configuration (e.g. hadoop.tbdsfs.meta), used for tbdsfs/hdfs
        // path resolution
        Map<String, String> hadoopConfig = extractHadoopConfig(tableOptions);
        if (!hadoopConfig.isEmpty()) {
            builder.hadoopConfig(hadoopConfig);
        }

        return builder.build();
    }

    /**
     * Extracts {@code hadoop.}-prefixed configs from the raw WITH params of the table DDL, stripping
     * the prefix and using the result as Hadoop Configuration key/value pairs (for example
     * {@code 'hadoop.tbdsfs.meta' = 'zk://...'} → {@code tbdsfs.meta = zk://...}). Used to work
     * around missing core-site.xml on some cluster nodes.
     */
    private Map<String, String> extractHadoopConfig(Map<String, String> tableOptions) {
        Map<String, String> result = new HashMap<>();
        if (tableOptions == null) {
            return result;
        }
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            String key = entry.getKey();
            if (key != null && key.startsWith("hadoop.") && entry.getValue() != null) {
                result.put(key.substring("hadoop.".length()), entry.getValue());
            }
        }
        return result;
    }
}
