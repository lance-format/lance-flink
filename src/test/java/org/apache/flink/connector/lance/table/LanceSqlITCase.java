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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Lance SQL integration tests.
 */
class LanceSqlITCase {

    @TempDir
    Path tempDir;

    private String datasetPath;
    private String warehousePath;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_sql_dataset").toString();
        warehousePath = tempDir.resolve("test_warehouse").toString();
    }

    @Test
    @DisplayName("Test LanceDynamicTableFactory identifier")
    void testFactoryIdentifier() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("lance");
    }

    @Test
    @DisplayName("Test LanceDynamicTableFactory required options")
    void testRequiredOptions() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        Set<String> requiredOptionKeys = new HashSet<>();
        factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

        assertThat(requiredOptionKeys).contains("path");
    }

    @Test
    @DisplayName("Test LanceDynamicTableFactory optional options")
    void testOptionalOptions() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        Set<String> optionalOptionKeys = new HashSet<>();
        factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

        assertThat(optionalOptionKeys).contains(
                "read.batch-size",
                "read.limit",
                "read.columns",
                "read.filter",
                "read.version",
                "read.as-of-timestamp",
                "write.batch-size",
                "write.memory-cap-bytes",
                "write.mode",
                "write.max-rows-per-file",
                "index.type",
                "index.column",
                "index.num-partitions",
                "index.num-sub-vectors",
                "index.num-bits",
                "index.max-level",
                "index.m",
                "index.ef-construction",
                "vector.column",
                "vector.metric",
                "vector.nprobes",
                "vector.ef",
                "vector.refine-factor"
        );
    }

    @Test
    @DisplayName("Every documented tuning option survives DDL translation")
    void testDocumentedTuningOptionsAreAcceptedByTheFactory() {
        // Regression guard: these keys exist in LanceOptions but were missing from the
        // factory's optionalOptions(), so a WITH clause using them failed at translation
        // with "Unsupported options found for 'lance'".
        String[] documentedKeys = {
            "read.limit",
            "write.memory-cap-bytes",
            "index.num-bits",
            "index.max-level",
            "index.m",
            "index.ef-construction",
            "vector.ef",
            "vector.refine-factor"
        };

        for (String key : documentedKeys) {
            TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
            env.executeSql(
                    "CREATE TABLE probe (id BIGINT, embedding ARRAY<FLOAT>) WITH ("
                            + "'connector' = 'lance', "
                            + "'path' = '"
                            + tempDir.resolve("probe-" + key.replace('.', '-'))
                            + "', "
                            + "'"
                            + key
                            + "' = '8')");

            // Translation runs the factory's option validation.
            assertThatCode(() -> env.explainSql("SELECT id FROM probe"))
                    .as("option '%s' must be accepted by the lance table factory", key)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("Test LanceDynamicTableSource creation")
    void testDynamicTableSourceCreation() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(512)
                .build();

        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        RowType rowType = new RowType(fields);

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
        );

        LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);

        assertThat(source.getOptions()).isEqualTo(options);
        assertThat(source.getPhysicalDataType()).isEqualTo(dataType);
        assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");
    }

    @Test
    @DisplayName("Test LanceDynamicTableSink creation")
    void testDynamicTableSinkCreation() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .writeBatchSize(256)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
        );

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

        assertThat(sink.getOptions()).isEqualTo(options);
        assertThat(sink.getPhysicalDataType()).isEqualTo(dataType);
        assertThat(sink.asSummaryString()).isEqualTo("Lance Table Sink");
    }

    @Test
    @DisplayName("Test LanceDynamicTableSource copy")
    void testDynamicTableSourceCopy() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT())
        );

        LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);
        LanceDynamicTableSource copiedSource = (LanceDynamicTableSource) source.copy();

        assertThat(copiedSource).isNotSameAs(source);
        assertThat(copiedSource.getOptions()).isEqualTo(source.getOptions());
    }

    @Test
    @DisplayName("Test LanceDynamicTableSink copy")
    void testDynamicTableSinkCopy() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT())
        );

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
        LanceDynamicTableSink copiedSink = (LanceDynamicTableSink) sink.copy();

        assertThat(copiedSink).isNotSameAs(sink);
        assertThat(copiedSink.getOptions()).isEqualTo(sink.getOptions());
    }

    @Test
    @DisplayName("Test LanceCatalogFactory identifier")
    void testCatalogFactoryIdentifier() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("lance");
    }

    @Test
    @DisplayName("Test LanceCatalogFactory required options")
    void testCatalogRequiredOptions() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        Set<String> requiredOptionKeys = new HashSet<>();
        factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

        assertThat(requiredOptionKeys).contains("warehouse");
    }

    @Test
    @DisplayName("Test LanceCatalogFactory optional options")
    void testCatalogOptionalOptions() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        Set<String> optionalOptionKeys = new HashSet<>();
        factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

        assertThat(optionalOptionKeys).contains("default-database");
    }

    @Test
    @DisplayName("Test LanceCatalog creation and basic operations")
    void testLanceCatalogBasicOperations() throws Exception {
        LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);
        
        try {
            catalog.open();
            
            // Verify default database exists
            assertThat(catalog.databaseExists("default")).isTrue();
            
            // List databases
            List<String> databases = catalog.listDatabases();
            assertThat(databases).contains("default");
            
            // Create new database
            catalog.createDatabase("test_db", null, false);
            assertThat(catalog.databaseExists("test_db")).isTrue();
            
            // List tables (empty)
            List<String> tables = catalog.listTables("test_db");
            assertThat(tables).isEmpty();
            
            // Drop database
            catalog.dropDatabase("test_db", false, true);
            assertThat(catalog.databaseExists("test_db")).isFalse();
            
        } finally {
            catalog.close();
        }
    }

    @Test
    @DisplayName("Test LanceCatalog warehouse path")
    void testLanceCatalogWarehouse() throws Exception {
        LanceCatalog catalog = new LanceCatalog("test", "default", warehousePath);
        
        try {
            catalog.open();
            assertThat(catalog.getWarehouse()).isEqualTo(warehousePath);
        } finally {
            catalog.close();
        }
    }

    @Test
    @DisplayName("Test configuration options definition")
    void testConfigOptions() {
        assertThat(LanceDynamicTableFactory.PATH.key()).isEqualTo("path");
        assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.key()).isEqualTo("read.batch-size");
        assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.defaultValue()).isEqualTo(1024);
        assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.key()).isEqualTo("write.batch-size");
        assertThat(LanceDynamicTableFactory.WRITE_MODE.key()).isEqualTo("write.mode");
        assertThat(LanceDynamicTableFactory.WRITE_MODE.defaultValue()).isEqualTo("append");
        assertThat(LanceDynamicTableFactory.INDEX_TYPE.key()).isEqualTo("index.type");
        assertThat(LanceDynamicTableFactory.INDEX_TYPE.defaultValue()).isEqualTo("IVF_PQ");
        assertThat(LanceDynamicTableFactory.VECTOR_METRIC.key()).isEqualTo("vector.metric");
        assertThat(LanceDynamicTableFactory.VECTOR_METRIC.defaultValue()).isEqualTo("L2");
    }

    @Test
    @DisplayName("Test Catalog configuration options definition")
    void testCatalogConfigOptions() {
        assertThat(LanceCatalogFactory.WAREHOUSE.key()).isEqualTo("warehouse");
        assertThat(LanceCatalogFactory.DEFAULT_DATABASE.key()).isEqualTo("default-database");
        assertThat(LanceCatalogFactory.DEFAULT_DATABASE.defaultValue()).isEqualTo("default");
    }

    @Test
    @DisplayName("Test S3 Catalog configuration options definition")
    void testS3CatalogConfigOptions() {
        // S3 configuration options
        assertThat(LanceCatalogFactory.S3_ACCESS_KEY.key()).isEqualTo("s3-access-key");
        assertThat(LanceCatalogFactory.S3_SECRET_KEY.key()).isEqualTo("s3-secret-key");
        assertThat(LanceCatalogFactory.S3_REGION.key()).isEqualTo("s3-region");
        assertThat(LanceCatalogFactory.S3_ENDPOINT.key()).isEqualTo("s3-endpoint");
        assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.key()).isEqualTo("s3-virtual-hosted-style");
        assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.key()).isEqualTo("s3-allow-http");
        
        // Default values
        assertThat(LanceCatalogFactory.S3_VIRTUAL_HOSTED_STYLE.defaultValue()).isTrue();
        assertThat(LanceCatalogFactory.S3_ALLOW_HTTP.defaultValue()).isFalse();
    }

    @Test
    @DisplayName("Test LanceCatalog S3 remote storage detection")
    void testLanceCatalogRemoteStorageDetection() {
        // S3 path should be identified as remote storage
        LanceCatalog s3Catalog = new LanceCatalog("test", "default", "s3://bucket/path");
        assertThat(s3Catalog.isRemoteStorage()).isTrue();
        
        // S3A path
        LanceCatalog s3aCatalog = new LanceCatalog("test", "default", "s3a://bucket/path");
        assertThat(s3aCatalog.isRemoteStorage()).isTrue();
        
        // GCS path
        LanceCatalog gcsCatalog = new LanceCatalog("test", "default", "gs://bucket/path");
        assertThat(gcsCatalog.isRemoteStorage()).isTrue();
        
        // Azure path
        LanceCatalog azCatalog = new LanceCatalog("test", "default", "az://container/path");
        assertThat(azCatalog.isRemoteStorage()).isTrue();
        
        // Local path should be identified as local storage
        LanceCatalog localCatalog = new LanceCatalog("test", "default", warehousePath);
        assertThat(localCatalog.isRemoteStorage()).isFalse();
    }

    @Test
    @DisplayName("Test LanceCatalog construction with storage options")
    void testLanceCatalogWithStorageOptions() {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("aws_access_key_id", "test-key");
        storageOptions.put("aws_secret_access_key", "test-secret");
        storageOptions.put("aws_region", "us-east-1");
        
        LanceCatalog catalog = new LanceCatalog(
                "test_catalog", 
                "default", 
                "s3://bucket/warehouse",
                storageOptions
        );
        
        assertThat(catalog.getStorageOptions()).containsEntry("aws_access_key_id", "test-key");
        assertThat(catalog.getStorageOptions()).containsEntry("aws_secret_access_key", "test-secret");
        assertThat(catalog.getStorageOptions()).containsEntry("aws_region", "us-east-1");
    }

    @Test
    @DisplayName("Test vector search UDF configuration")
    void testVectorSearchFunctionConfiguration() {
        LanceVectorSearchFunction function = new LanceVectorSearchFunction();
        assertThat(function).isNotNull();
    }

    // ------------------------------------------------------------------
    // Regression tests for issue #5: SQL push-down must not drop
    // read.version / read.as-of-timestamp when the planner rebuilds
    // LanceOptions inside LanceDynamicTableSource#buildRuntimeOptions.
    // ------------------------------------------------------------------

    @Test
    @DisplayName("Regression #5: read.version survives projection push-down")
    void testReadVersionSurvivesPushDown() {
        LanceOptions withVersion = LanceOptions.builder()
                .path(datasetPath)
                .readVersion(7L)
                .build();
        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()));

        LanceDynamicTableSource source = new LanceDynamicTableSource(withVersion, dataType);
        // Simulate the planner applying a projection push-down.
source.applyProjection(new int[][]{ new int[]{0} }, null);

        RowType rowType = (RowType) dataType.getLogicalType();
        LanceOptions runtime = source.buildRuntimeOptions(rowType);

        assertThat(runtime.getReadVersion())
                .as("read.version must be carried into runtime options after projection push-down")
                .isEqualTo(7L);
    }

    @Test
    @DisplayName("Regression #5: read.as-of-timestamp survives projection push-down")
    void testReadAsOfTimestampSurvivesPushDown() {
        String ts = "2026-07-28T00:00:00Z";
        LanceOptions withTs = LanceOptions.builder()
                .path(datasetPath)
                .readAsOfTimestamp(ts)
                .build();
        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()));

        LanceDynamicTableSource source = new LanceDynamicTableSource(withTs, dataType);
source.applyProjection(new int[][]{ new int[]{1} }, null);

        RowType rowType = (RowType) dataType.getLogicalType();
        LanceOptions runtime = source.buildRuntimeOptions(rowType);

        assertThat(runtime.getReadAsOfTimestamp())
                .as("read.as-of-timestamp must be carried into runtime options after push-down")
                .isEqualTo(ts);
    }
}
