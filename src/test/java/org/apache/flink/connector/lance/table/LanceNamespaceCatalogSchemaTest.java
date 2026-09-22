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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.NamespaceExistsRequest;

import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the Arrow IPC schema stream that {@link LanceNamespaceCatalog} sends with
 * {@code createTable}.
 *
 * <p>Most cases call the package-private serialization helpers directly, so they need no
 * namespace at all. The one case that goes through {@code createTable} uses a stub namespace,
 * since it asserts how a schema failure is reported before the namespace is ever called.
 * {@code LanceNamespaceCatalogITCase} covers createTable against a real directory namespace.
 */
@DisplayName("Lance Namespace Catalog Schema Serialization Tests")
class LanceNamespaceCatalogSchemaTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    @DisplayName("Schema is serialized as a readable Arrow IPC stream")
    void testSchemaRoundTrip() throws Exception {
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("score", DataTypes.DOUBLE())
                        .build());

        byte[] ipc = LanceNamespaceCatalog.toArrowIpcSchema(table, allocator);

        assertThat(ipc).isNotEmpty();

        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            org.apache.arrow.vector.types.pojo.Schema arrowSchema = reader.getVectorSchemaRoot()
                    .getSchema();
            assertThat(arrowSchema.getFields())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "name", "score");

            // The stream carries one batch of zero rows: the namespace rejects a
            // schema-only stream, and an empty table must not gain phantom rows.
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isZero();
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    @Test
    @DisplayName("Computed and metadata columns are excluded from the stored schema")
    void testOnlyPhysicalColumnsAreSerialized() throws Exception {
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .columnByExpression("id_doubled", "id * 2")
                        .build());

        byte[] ipc = LanceNamespaceCatalog.toArrowIpcSchema(table, allocator);

        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.getVectorSchemaRoot().getSchema().getFields())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id");
        }
    }

    @Test
    @DisplayName("A schema with no physical columns is rejected with a clear message")
    void testNoPhysicalColumnsRejected() {
        CatalogTable table = tableWith(Schema.newBuilder().build());

        assertThatThrownBy(() -> LanceNamespaceCatalog.toArrowIpcSchema(table, allocator))
                .isInstanceOf(org.apache.flink.table.catalog.exceptions.CatalogException.class)
                .hasMessageContaining("without physical columns");
    }

    @Test
    @DisplayName("A vector column is serialized")
    void testVectorColumn() throws Exception {
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .build());

        byte[] ipc = LanceNamespaceCatalog.toArrowIpcSchema(table, allocator);

        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.getVectorSchemaRoot().getSchema().getFields())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "embedding");
        }
    }

    @Test
    @DisplayName("A column declared with a type string is rejected by name")
    void testUnresolvedDataTypeRejected() {
        // Schema.column(String, String) yields an UnresolvedDataType, which cannot be
        // resolved without a catalog's type factory.
        CatalogTable table = tableWith(Schema.newBuilder().column("id", "BIGINT").build());

        assertThatThrownBy(() -> LanceNamespaceCatalog.toArrowIpcSchema(table, allocator))
                .isInstanceOf(org.apache.flink.table.catalog.exceptions.CatalogException.class)
                .hasMessageContaining("'id'")
                .hasMessageContaining("unresolved data type");
    }

    @Test
    @DisplayName("An unsupported column type is rejected as a schema problem")
    void testUnsupportedTypeRejected() {
        // MAP has no Arrow mapping yet. DECIMAL used to stand in here, so this case had to move
        // when DECIMAL gained one; the assertion is about how an unmappable type is surfaced, not
        // about MAP specifically.
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("attrs", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .build());

        assertThatThrownBy(() -> LanceNamespaceCatalog.toArrowIpcSchema(table, allocator))
                .isInstanceOf(org.apache.flink.table.catalog.exceptions.CatalogException.class)
                .hasMessageContaining("Cannot create a Lance table with this schema")
                .hasMessageContaining("MapType");
    }

    @Test
    @DisplayName("A resolved table uses its resolved physical schema")
    void testResolvedTableUsesPhysicalSchema() {
        // The declared schema is deliberately different from the resolved one: only the resolved
        // path can return "id", and only the unresolved path would reject the type string. So
        // this fails if the resolved branch stops being taken.
        CatalogTable declared = tableWith(Schema.newBuilder().column("wrong", "BIGINT").build());
        ResolvedSchema resolved = ResolvedSchema.of(
                Column.physical("id", DataTypes.BIGINT()),
                Column.metadata("ts", DataTypes.TIMESTAMP_LTZ(3), "timestamp", false));

        RowType rowType =
                LanceNamespaceCatalog.resolveRowType(new ResolvedCatalogTable(declared, resolved));

        assertThat(rowType.getFieldNames()).containsExactly("id");
    }

    @Test
    @DisplayName("createTable wraps a non-CatalogException schema failure")
    void testCreateTableWrapsSchemaFailure() {
        // Duplicate names survive Schema.Builder and fail inside RowType with a bare
        // ValidationException. createTable has to surface that as a CatalogException so it
        // keeps the contract its signature declares, with the original cause attached.
        CatalogTable table = tableWith(
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("a", DataTypes.STRING())
                        .build());

        LanceNamespaceCatalog catalog = new LanceNamespaceCatalog(
                "test", "default", new NoopNamespace(), allocator, Collections.emptyMap());

        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        new ObjectPath("default", "dup"), table, false))
                .isInstanceOf(org.apache.flink.table.catalog.exceptions.CatalogException.class)
                .hasMessageContaining("Cannot build the Arrow schema")
                .hasCauseInstanceOf(ValidationException.class);
    }

    private static CatalogTable tableWith(Schema schema) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "lance");
        options.put("path", "/tmp/does-not-need-to-exist");
        return CatalogTable.of(schema, "", Collections.emptyList(), options);
    }

    /**
     * Namespace stub for the one test that goes through createTable without reaching the
     * namespace. Every other method on the
     * interface has a default implementation, so only these two need bodies; {@code
     * namespaceExists} returning normally is what makes {@code databaseExists} true.
     */
    private static final class NoopNamespace implements LanceNamespace {
        @Override
        public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {}

        @Override
        public String namespaceId() {
            return "test";
        }

        @Override
        public void namespaceExists(NamespaceExistsRequest request) {}
    }
}
