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

import org.apache.flink.connector.lance.catalog.namespace.LanceNamespaceConfig;

import org.lance.namespace.LanceNamespace;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link LanceNamespaceCatalog} using real {@code DirectoryNamespace}
 * backend — no mocks.
 *
 * <p>Exercises Flink SQL operations through the catalog programmatic API rather than
 * Flink SQL DDL to avoid Flink test classloader complications with Arrow memory-netty SPI.
 */
class LanceNamespaceCatalogITCase {

    @TempDir
    static Path warehouseDir;

    private LanceNamespaceCatalog catalog;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        // Force Arrow to use Netty allocation manager via system property.
        // In some classloader configurations (e.g. Flink test infrastructure),
        // the SPI-based DefaultAllocationManager discovery fails because
        // arrow-memory-netty is not visible to the ServiceLoader.
        // Setting this property forces Arrow to use the Netty allocator directly.
        System.setProperty(
                "arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    @BeforeEach
    void setUp() {
        Map<String, String> namespaceProps = new HashMap<>();
        namespaceProps.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        namespaceProps.put(LanceNamespaceConfig.KEY_ROOT, warehouseDir.toAbsolutePath().toString());

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        catalogOptions.put(LanceNamespaceConfig.KEY_ROOT, warehouseDir.toAbsolutePath().toString());

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        LanceNamespace namespace =
                LanceNamespace.connect("dir", namespaceProps, allocator);

        catalog = new LanceNamespaceCatalog(
                "test_lance", "default", namespace, allocator, catalogOptions);
        catalog.open();
    }

    @AfterEach
    void tearDown() {
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception ignored) {
            }
        }
        try {
            Files.walk(warehouseDir)
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (Exception ignored) {
        }
    }

    @Test
    void testListDatabases() {
        assertThat(catalog.listDatabases()).contains("default");
    }

    @Test
    void testCreateAndDropDatabase() throws Exception {
        catalog.createDatabase("test_db", null, false);
        assertThat(catalog.databaseExists("test_db")).isTrue();
        assertThat(catalog.listDatabases()).contains("test_db");

        catalog.dropDatabase("test_db", false, false);
        assertThat(catalog.databaseExists("test_db")).isFalse();
    }

    @Test
    void testCreateTable() throws Exception {
        catalog.createDatabase("test_db", null, false);

        // Create a simple table via namespace
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("path", warehouseDir.resolve("my_table").toAbsolutePath().toString());

        org.apache.flink.table.catalog.ObjectPath tablePath =
                new org.apache.flink.table.catalog.ObjectPath("test_db", "my_table");

        catalog.createTable(tablePath,
                org.apache.flink.table.catalog.CatalogTable.newBuilder()
                        .schema(org.apache.flink.table.api.Schema.newBuilder()
                                .column("id", org.apache.flink.table.api.DataTypes.INT())
                                .column("name", org.apache.flink.table.api.DataTypes.STRING())
                                .build())
                        .comment("")
                        .partitionKeys(java.util.Collections.emptyList())
                        .options(tableOptions)
                        .build(),
                false);

        assertThat(catalog.tableExists(tablePath)).isTrue();
        assertThat(catalog.listTables("test_db")).contains("my_table");
    }
}