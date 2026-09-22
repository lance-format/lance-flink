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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for the directory-backed {@link LanceCatalog} table lifecycle.
 *
 * <p>Locks in that {@code createTable} materializes an empty Lance dataset immediately (rather
 * than deferring to first write), so the schema and primary-key constraint survive a catalog
 * round-trip before any data is written — matching the community Spark/Trino behavior.
 */
class LanceCatalogTableITCase {

    @TempDir
    Path tempDir;

    private LanceCatalog catalog;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    @BeforeEach
    void setUp() {
        String warehouse = tempDir.resolve("warehouse").toString();
        catalog = new LanceCatalog("test", "default", warehouse);
        catalog.open();
    }

    @AfterEach
    void tearDown() {
        catalog.close();
    }

    @Test
    @DisplayName("createTable materializes an empty dataset immediately")
    void createTableMaterializesEmptyDatasetImmediately() throws Exception {
        catalog.createDatabase("db", null, false);
        ObjectPath tablePath = new ObjectPath("db", "t");

        catalog.createTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        // The empty dataset must be materialized on disk right away, not on first write.
        assertThat(catalog.tableExists(tablePath)).isTrue();
        assertThat(catalog.listTables("db")).contains("t");
    }

    @Test
    @DisplayName("createTable persists and restores the primary key")
    void createTablePersistsAndRestoresPrimaryKey() throws Exception {
        catalog.createDatabase("db", null, false);
        ObjectPath tablePath = new ObjectPath("db", "t");

        catalog.createTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        // The primary key must survive a round-trip via the dataset config.
        CatalogTable loaded = (CatalogTable) catalog.getTable(tablePath);
        Schema unresolved = loaded.getUnresolvedSchema();
        assertThat(unresolved.getPrimaryKey()).isPresent();
        assertThat(unresolved.getPrimaryKey().get().getColumnNames()).containsExactly("id");
    }

    @Test
    @DisplayName("createTable without a primary key restores no constraint")
    void createTableWithoutPrimaryKeyRestoresNoConstraint() throws Exception {
        catalog.createDatabase("db", null, false);
        ObjectPath tablePath = new ObjectPath("db", "t");

        catalog.createTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        CatalogTable loaded = (CatalogTable) catalog.getTable(tablePath);
        assertThat(loaded.getUnresolvedSchema().getPrimaryKey()).isEmpty();
    }

    @Test
    @DisplayName("alterTable renames a column")
    void alterTableRenamesColumn() throws Exception {
        catalog.createDatabase("db", null, false);
        ObjectPath tablePath = new ObjectPath("db", "t");

        catalog.createTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        catalog.alterTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("full_name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        CatalogTable loaded = (CatalogTable) catalog.getTable(tablePath);
        List<String> columns = loaded.getUnresolvedSchema().getColumns().stream()
                .map(c -> ((Schema.UnresolvedPhysicalColumn) c).getName())
                .collect(Collectors.toList());
        assertThat(columns).containsExactlyInAnyOrder("id", "full_name");
    }

    @Test
    @DisplayName("alterTable rejects a data type change")
    void alterTableRejectsTypeChange() throws Exception {
        catalog.createDatabase("db", null, false);
        ObjectPath tablePath = new ObjectPath("db", "t");

        catalog.createTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false);

        assertThatThrownBy(() -> catalog.alterTable(tablePath, CatalogTable.of(
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .build(),
                "",
                Collections.emptyList(),
                Collections.emptyMap()), false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("data type change");
    }
}
