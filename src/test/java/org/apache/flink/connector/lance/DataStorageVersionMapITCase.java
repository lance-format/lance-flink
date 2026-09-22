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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the effect of the Lance file format version on what can actually be written.
 *
 * <p>This is the evidence behind {@code write.data-storage-version}: a MAP column is accepted into
 * the schema at creation on any version, but writing rows only works on 2.2+. Without the option
 * there is no way to create a dataset that can hold a map, so adding MAP to the type converters
 * alone would produce a table that fails on first write.
 *
 * <p>The version is fixed when the dataset is created, which is why the connector applies the
 * option in {@code LanceCatalog#createTable} as well as on the sinks.
 */
class DataStorageVersionMapITCase {

    @TempDir Path tempDir;

    private static Schema mapSchema(RootAllocator allocator) {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        try (MapVector probe = MapVector.empty("attrs", allocator, false)) {
            UnionMapWriter w = probe.getWriter();
            w.allocate();
            w.setPosition(0);
            w.startMap();
            w.startEntry();
            w.key().varChar().writeVarChar("k");
            w.value().integer().writeInt(1);
            w.endEntry();
            w.endMap();
            w.setValueCount(1);
            probe.setValueCount(1);
            return new Schema(Arrays.asList(idField, probe.getField()));
        }
    }

    /** Writes three rows -- populated map, empty map, NULL map -- through the sink's merge path. */
    private static void writeRows(Dataset ds, RootAllocator allocator) throws Exception {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(ds.getSchema(), allocator)) {
            BigIntVector ids = (BigIntVector) root.getVector("id");
            MapVector attrs = (MapVector) root.getVector("attrs");
            ids.allocateNew(3);
            UnionMapWriter writer = attrs.getWriter();
            writer.allocate();

            ids.setSafe(0, 100L);
            writer.setPosition(0);
            writer.startMap();
            writer.startEntry();
            writer.key().varChar().writeVarChar("a");
            writer.value().integer().writeInt(1);
            writer.endEntry();
            writer.startEntry();
            writer.key().varChar().writeVarChar("b");
            writer.value().integer().writeInt(2);
            writer.endEntry();
            writer.endMap();

            ids.setSafe(1, 200L);
            writer.setPosition(1);
            writer.startMap();
            writer.endMap();

            ids.setSafe(2, 300L);
            writer.setPosition(2);
            writer.writeNull();

            writer.setValueCount(3);
            ids.setValueCount(3);
            attrs.setValueCount(3);
            root.setRowCount(3);

            MergeInsertParams params =
                    new MergeInsertParams(Collections.singletonList("id"))
                            .withMatchedUpdateAll()
                            .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
            ArrowArrayStreamsTestAccess.mergeInsert(ds, params, allocator, root);
        }
    }

    @Test
    @DisplayName("On the SDK default the map schema is accepted but the first write is rejected")
    void defaultVersionAcceptsSchemaButRejectsWrite() {
        String path = tempDir.resolve("map_default.lance").toString();

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = mapSchema(allocator);

            // CREATE TABLE succeeds, which is exactly why this gap is easy to miss: a test that
            // only exercises DDL sees nothing wrong.
            Dataset.create(allocator, path, schema, new WriteParams.Builder().build()).close();

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                assertThat(ds.getSchema().findField("attrs").getType())
                        .isInstanceOf(ArrowType.Map.class);

                assertThatThrownBy(() -> writeRows(ds, allocator))
                        .hasMessageContaining("Map data type is only supported in Lance file "
                                + "format 2.2+");
            }
        } catch (Exception e) {
            throw new AssertionError("unexpected failure outside the asserted write", e);
        }
    }

    @Test
    @DisplayName("With 2.2 the map column round-trips, including empty and NULL maps")
    void version22RoundTripsMapColumn() throws Exception {
        String path = tempDir.resolve("map_22.lance").toString();

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Schema schema = mapSchema(allocator);
            Dataset.create(
                            allocator,
                            path,
                            schema,
                            new WriteParams.Builder().withDataStorageVersion("2.2").build())
                    .close();

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                writeRows(ds, allocator);
            }

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                assertThat(ds.countRows()).isEqualTo(3L);

                // mergeInsert does not preserve input order, so rows are matched by id
                // rather than by position.
                java.util.Map<Long, String> byId = new java.util.HashMap<>();
                java.util.Set<Long> nulls = new java.util.HashSet<>();
                try (org.apache.arrow.vector.ipc.ArrowReader reader = ds.newScan().scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot out = reader.getVectorSchemaRoot();
                        BigIntVector ids = (BigIntVector) out.getVector("id");
                        MapVector attrs = (MapVector) out.getVector("attrs");
                        for (int i = 0; i < out.getRowCount(); i++) {
                            long id = ids.get(i);
                            if (attrs.isNull(i)) {
                                nulls.add(id);
                            } else {
                                byId.put(id, String.valueOf(attrs.getObject(i)));
                            }
                        }
                    }
                }

                assertThat(byId.get(100L))
                        .as("a populated map must survive the round-trip")
                        .contains("\"key\":\"a\"")
                        .contains("\"key\":\"b\"");

                // An empty map must stay distinct from a NULL map.
                assertThat(byId).containsKey(200L);
                assertThat(byId.get(200L)).isEqualTo("[]");
                assertThat(nulls).doesNotContain(200L);

                // The NULL row exercises setNull's MapVector branch, which has to precede the
                // ListVector branch because MapVector extends ListVector.
                assertThat(nulls).contains(300L);
            }
        }
    }

    @Test
    @DisplayName("CREATE TABLE stamps the configured format version onto the dataset")
    void createTableAppliesConfiguredVersion() throws Exception {
        // Covers LanceCatalog#createTable specifically. The version is fixed at creation, so if
        // the catalog ignored the option the table could never hold a map no matter what the sink
        // later requests. Disabling the catalog's withDataStorageVersion call leaves every other
        // suite green, which is why this case exists.
        org.apache.flink.connector.lance.table.LanceCatalog catalog =
                new org.apache.flink.connector.lance.table.LanceCatalog(
                        "lance", "default", tempDir.toString());
        catalog.open();
        try {
            catalog.createDatabase("db", null, false);

            java.util.Map<String, String> options = new java.util.HashMap<>();
            options.put("write.data-storage-version", "2.2");
            catalog.createTable(
                    new org.apache.flink.table.catalog.ObjectPath("db", "v22"),
                    org.apache.flink.table.catalog.CatalogTable.of(
                            org.apache.flink.table.api.Schema.newBuilder()
                                    .column("id", org.apache.flink.table.api.DataTypes.BIGINT())
                                    .build(),
                            "",
                            Collections.emptyList(),
                            options),
                    false);

            // And a table without the option, to prove the difference comes from the option
            // rather than from the SDK having changed its default.
            catalog.createTable(
                    new org.apache.flink.table.catalog.ObjectPath("db", "vdefault"),
                    org.apache.flink.table.catalog.CatalogTable.of(
                            org.apache.flink.table.api.Schema.newBuilder()
                                    .column("id", org.apache.flink.table.api.DataTypes.BIGINT())
                                    .build(),
                            "",
                            Collections.emptyList(),
                            Collections.emptyMap()),
                    false);

            try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                String configured =
                        formatVersionOf(allocator, tempDir.resolve("db").resolve("v22"));
                String fallback =
                        formatVersionOf(allocator, tempDir.resolve("db").resolve("vdefault"));

                assertThat(configured)
                        .as("createTable must pass write.data-storage-version to Lance")
                        .isEqualTo("2.2");
                assertThat(fallback)
                        .as("an unset option must leave the SDK default in place")
                        .isNotEqualTo(configured);
            }
        } finally {
            catalog.close();
        }
    }

    private static String formatVersionOf(RootAllocator allocator, Path datasetPath) {
        try (Dataset ds =
                Dataset.open(allocator, datasetPath.toString(), new ReadOptions.Builder().build())) {
            return ds.getLanceFileFormatVersion();
        }
    }

    @Test
    @DisplayName("CREATE TABLE with a MAP column on a pre-2.2 version fails before the table exists")
    void mapColumnOnOldVersionIsRejectedAtDdlTime() throws Exception {
        // Without this check the schema is accepted, the table is materialized, and the user only
        // finds out on the first write via a Rust-level encoder message that names neither the
        // table nor the option to set.
        org.apache.flink.connector.lance.table.LanceCatalog catalog =
                new org.apache.flink.connector.lance.table.LanceCatalog(
                        "lance", "default", tempDir.toString());
        catalog.open();
        try {
            catalog.createDatabase("db", null, false);

            java.util.Map<String, String> options = new java.util.HashMap<>();
            options.put("write.data-storage-version", "2.1");

            org.apache.flink.table.catalog.ObjectPath tablePath =
                    new org.apache.flink.table.catalog.ObjectPath("db", "bad_map");
            org.apache.flink.table.catalog.CatalogTable table =
                    org.apache.flink.table.catalog.CatalogTable.of(
                            org.apache.flink.table.api.Schema.newBuilder()
                                    .column("id", org.apache.flink.table.api.DataTypes.INT())
                                    .column(
                                            "attrs",
                                            org.apache.flink.table.api.DataTypes.MAP(
                                                    org.apache.flink.table.api.DataTypes.STRING()
                                                            .notNull(),
                                                    org.apache.flink.table.api.DataTypes.INT()))
                                    .build(),
                            "",
                            Collections.emptyList(),
                            options);

            org.assertj.core.api.Assertions.assertThatThrownBy(
                            () -> catalog.createTable(tablePath, table, false))
                    .hasMessageContaining("attrs")
                    .hasMessageContaining("2.2");

            // The failure must come before materialization, otherwise a half-created table is left
            // behind for the next CREATE to trip over.
            assertThat(catalog.tableExists(tablePath))
                    .as("the table must not be left behind after a rejected CREATE")
                    .isFalse();
        } finally {
            catalog.close();
        }
    }

    @Test
    @DisplayName("CREATE TABLE with a MAP column on 2.2 is accepted")
    void mapColumnOn22IsAccepted() throws Exception {
        org.apache.flink.connector.lance.table.LanceCatalog catalog =
                new org.apache.flink.connector.lance.table.LanceCatalog(
                        "lance", "default", tempDir.toString());
        catalog.open();
        try {
            catalog.createDatabase("db", null, false);

            java.util.Map<String, String> options = new java.util.HashMap<>();
            options.put("write.data-storage-version", "2.2");

            org.apache.flink.table.catalog.ObjectPath tablePath =
                    new org.apache.flink.table.catalog.ObjectPath("db", "good_map");
            catalog.createTable(
                    tablePath,
                    org.apache.flink.table.catalog.CatalogTable.of(
                            org.apache.flink.table.api.Schema.newBuilder()
                                    .column("id", org.apache.flink.table.api.DataTypes.INT())
                                    .column(
                                            "attrs",
                                            org.apache.flink.table.api.DataTypes.MAP(
                                                    org.apache.flink.table.api.DataTypes.STRING()
                                                            .notNull(),
                                                    org.apache.flink.table.api.DataTypes.INT()))
                                    .build(),
                            "",
                            Collections.emptyList(),
                            options),
                    false);

            assertThat(catalog.tableExists(tablePath)).isTrue();
        } finally {
            catalog.close();
        }
    }
}
