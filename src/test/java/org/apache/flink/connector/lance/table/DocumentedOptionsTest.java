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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Guards the documentation against drift: every option shown in a {@code WITH (...)} clause under
 * {@code docs/src} must be accepted by {@link LanceDynamicTableFactory} and must land in the
 * {@link LanceOptions} handed to the table source.
 *
 * <p>This test exists because a set of tuning options ({@code read.limit},
 * {@code write.memory-cap-bytes}, {@code index.num-bits}, {@code index.max-level}, {@code index.m},
 * {@code index.ef-construction}, {@code vector.ef}, {@code vector.refine-factor}) were documented
 * and present in {@link LanceOptions} but missing from the factory's {@code optionalOptions()}, so
 * a DDL using them failed at translation with
 * {@code ValidationException: Unsupported options found for 'lance'}.
 */
class DocumentedOptionsTest {

    @TempDir Path tempDir;

    /** Option keys documented under docs/src (config.md, create-table.md, performance.md, ...). */
    private static final List<String> DOCUMENTED_KEYS =
            Arrays.asList(
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
                    "vector.refine-factor");

    @Test
    @DisplayName("Every documented option key is registered on the factory")
    void everyDocumentedKeyIsRegistered() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        List<String> registered = new java.util.ArrayList<>();
        factory.optionalOptions().forEach(option -> registered.add(option.key()));
        factory.requiredOptions().forEach(option -> registered.add(option.key()));

        assertThat(registered).containsAll(DOCUMENTED_KEYS);
    }

    @Test
    @DisplayName(
            "A DDL using every documented option reaches translation without a validation error")
    void allDocumentedOptionsAreAcceptedTogether() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.executeSql(
                "CREATE TABLE doc_options ("
                        + "id BIGINT, embedding ARRAY<FLOAT>, kind STRING) WITH ("
                        + "'connector' = 'lance', "
                        + "'path' = '"
                        + tempDir.resolve("doc-options")
                        + "', "
                        + "'read.batch-size' = '512', "
                        + "'read.limit' = '100', "
                        + "'read.columns' = 'id,embedding', "
                        + "'read.filter' = 'id > 0', "
                        + "'read.version' = '2', "
                        + "'write.batch-size' = '2048', "
                        + "'write.memory-cap-bytes' = '67108864', "
                        + "'write.mode' = 'append', "
                        + "'write.max-rows-per-file' = '100000', "
                        + "'index.type' = 'IVF_PQ', "
                        + "'index.column' = 'embedding', "
                        + "'index.num-partitions' = '256', "
                        + "'index.num-sub-vectors' = '16', "
                        + "'index.num-bits' = '8', "
                        + "'vector.column' = 'embedding', "
                        + "'vector.metric' = 'COSINE', "
                        + "'vector.nprobes' = '20', "
                        + "'vector.ef' = '50', "
                        + "'vector.refine-factor' = '2')");

        // Explaining triggers the factory's option validation, which is where the missing keys
        // used to fail with "Unsupported options found for 'lance'".
        assertThatCode(() -> env.explainSql("SELECT id FROM doc_options"))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("write.memory-cap-bytes and read.limit reach LanceOptions")
    void boundedWriteOptionsReachLanceOptions() {
        Map<String, String> options = baseOptions();
        options.put("read.limit", "100");
        options.put("write.memory-cap-bytes", "4096");

        LanceOptions lanceOptions = createSourceOptions(options);

        assertThat(lanceOptions.getReadLimit()).isEqualTo(100L);
        assertThat(lanceOptions.getWriteMemoryCapBytes()).isEqualTo(4096L);
    }

    @Test
    @DisplayName("PQ and HNSW index tuning options reach LanceOptions")
    void indexTuningOptionsReachLanceOptions() {
        Map<String, String> options = baseOptions();
        options.put("index.type", "IVF_HNSW");
        options.put("index.column", "embedding");
        options.put("index.num-partitions", "128");
        options.put("index.num-sub-vectors", "8");
        options.put("index.num-bits", "6");
        options.put("index.max-level", "9");
        options.put("index.m", "32");
        options.put("index.ef-construction", "200");
        options.put("vector.column", "embedding");
        options.put("vector.ef", "150");
        options.put("vector.refine-factor", "3");

        LanceOptions lanceOptions = createSourceOptions(options);

        assertThat(lanceOptions.getIndexType()).isEqualTo(LanceOptions.IndexType.IVF_HNSW);
        assertThat(lanceOptions.getIndexNumPartitions()).isEqualTo(128);
        assertThat(lanceOptions.getIndexNumSubVectors()).isEqualTo(8);
        assertThat(lanceOptions.getIndexNumBits()).isEqualTo(6);
        assertThat(lanceOptions.getIndexMaxLevel()).isEqualTo(9);
        assertThat(lanceOptions.getIndexM()).isEqualTo(32);
        assertThat(lanceOptions.getIndexEfConstruction()).isEqualTo(200);
        assertThat(lanceOptions.getVectorEf()).isEqualTo(150);
        assertThat(lanceOptions.getVectorRefineFactor()).isEqualTo(3);
    }

    private Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "lance");
        options.put("path", tempDir.resolve("doc-options").toString());
        return options;
    }

    /** Runs the real factory over the given WITH options and returns the source's options. */
    private static LanceOptions createSourceOptions(Map<String, String> options) {
        ResolvedSchema resolvedSchema =
                ResolvedSchema.physical(
                        Arrays.asList("id", "embedding"),
                        Arrays.asList(DataTypes.BIGINT(), DataTypes.ARRAY(DataTypes.FLOAT())));

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .options(options)
                        .build();

        DynamicTableFactory.Context context =
                new DynamicTableFactory.Context() {
                    @Override
                    public ResolvedCatalogTable getCatalogTable() {
                        return new ResolvedCatalogTable(catalogTable, resolvedSchema);
                    }

                    @Override
                    public org.apache.flink.configuration.ReadableConfig getConfiguration() {
                        return new Configuration();
                    }

                    @Override
                    public ObjectIdentifier getObjectIdentifier() {
                        return ObjectIdentifier.of(
                                "default_catalog", "default_database", "doc_options");
                    }

                    @Override
                    public boolean isTemporary() {
                        return false;
                    }

                    @Override
                    public ClassLoader getClassLoader() {
                        return DocumentedOptionsTest.class.getClassLoader();
                    }
                };

        DynamicTableSource source =
                new LanceDynamicTableFactory().createDynamicTableSource(context);
        return ((LanceDynamicTableSource) source).getOptions();
    }
}
