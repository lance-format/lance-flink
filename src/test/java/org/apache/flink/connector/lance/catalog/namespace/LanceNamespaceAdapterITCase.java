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

package org.apache.flink.connector.lance.catalog.namespace;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Namespace Adapter Integration Test.
 * 
 * This test class covers all CRUD operations of LanceNamespaceAdapter for Table API,
 * including complete lifecycle management of table creation, query, update and deletion.
 * 
 * Test scope:
 * - Namespace management: create, list, check, delete
 * - Table management: create, query, check, delete
 * - Metadata operations: get namespace and table metadata
 * - Error handling: duplicate creation, non-existing resources and other exception scenarios
 */
@DisplayName("Lance Namespace Adapter Integration Test")
class LanceNamespaceAdapterITCase {
    
    @TempDir
    Path tempDir;
    
    private LanceNamespaceAdapter adapter;
    private String warehousePath;
    
    /**
     * Setup before test.
     */
    @BeforeEach
    void setUp() {
        warehousePath = tempDir.resolve("warehouse").toString();
        
        // Create configuration
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        // Create adapter instance
        adapter = LanceNamespaceAdapter.create(properties);
        adapter.init();
    }
    
    /**
     * Cleanup after test.
     */
    @AfterEach
    void tearDown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }
    
    // ==================== Namespace Management Tests ====================
    
    /**
     * Test namespace creation (Create).
     */
    @Test
    @DisplayName("Test creating namespace")
    void testCreateNamespace() {
        // Prepare
        String namespaceName = "test_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("description", "Test database");
        
        // Execute
        adapter.createNamespace(properties, namespaceName);
        
        // Verify
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // Verify metadata
        Map<String, String> metadata = adapter.getNamespaceMetadata(namespaceName);
        assertThat(metadata).isNotNull();
    }
    
    /**
     * Test creating nested namespace.
     */
    @Test
    @DisplayName("Test creating nested namespace")
    void testCreateNestedNamespace() {
        // Prepare
        String parentNamespace = "parent_db";
        String childNamespace = "child_db";
        
        // Execute
        adapter.createNamespace(new HashMap<>(), parentNamespace);
        adapter.createNamespace(new HashMap<>(), parentNamespace, childNamespace);
        
        // Verify
        assertThat(adapter.namespaceExists(parentNamespace)).isTrue();
        assertThat(adapter.namespaceExists(parentNamespace, childNamespace)).isTrue();
    }
    
    /**
     * Test listing namespaces (Read).
     */
    @Test
    @DisplayName("Test listing all top-level namespaces")
    void testListNamespaces() {
        // Prepare
        adapter.createNamespace(new HashMap<>(), "db1");
        adapter.createNamespace(new HashMap<>(), "db2");
        adapter.createNamespace(new HashMap<>(), "db3");
        
        // Execute
        List<String> namespaces = adapter.listNamespaces();
        
        // Verify
        assertThat(namespaces).isNotNull();
        assertThat(namespaces).contains("db1", "db2", "db3");
        assertThat(namespaces.size()).isGreaterThanOrEqualTo(3);
    }
    
    /**
     * Test listing child namespaces.
     */
    @Test
    @DisplayName("Test listing child namespaces")
    void testListChildNamespaces() {
        // Prepare
        String parent = "my_warehouse";
        adapter.createNamespace(new HashMap<>(), parent);
        adapter.createNamespace(new HashMap<>(), parent, "schema1");
        adapter.createNamespace(new HashMap<>(), parent, "schema2");
        
        // Execute
        List<String> childNamespaces = adapter.listNamespaces(parent);
        
        // Verify
        assertThat(childNamespaces).isNotNull();
        assertThat(childNamespaces).contains("schema1", "schema2");
    }
    
    /**
     * Test checking namespace existence (Read).
     */
    @Test
    @DisplayName("Test checking namespace existence")
    void testNamespaceExists() {
        // Prepare
        String namespaceName = "existing_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // Execute and verify
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        assertThat(adapter.namespaceExists("non_existing_db")).isFalse();
    }
    
    /**
     * Test dropping namespace (Delete).
     */
    @Test
    @DisplayName("Test dropping namespace")
    void testDropNamespace() {
        // Prepare
        String namespaceName = "temp_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // Execute
        adapter.dropNamespace(false, namespaceName);
        
        // Verify
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * Test dropping namespace (cascade delete).
     */
    @Test
    @DisplayName("Test cascade dropping namespace and its contents")
    void testDropNamespaceCascade() {
        // Prepare
        String namespaceName = "cascade_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // Execute
        adapter.dropNamespace(true, namespaceName);
        
        // Verify
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * Test getting namespace metadata (Read).
     */
    @Test
    @DisplayName("Test getting namespace metadata")
    void testGetNamespaceMetadata() {
        // Prepare
        String namespaceName = "metadata_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("owner", "admin");
        properties.put("environment", "test");
        
        adapter.createNamespace(properties, namespaceName);
        
        // Execute
        Map<String, String> metadata = adapter.getNamespaceMetadata(namespaceName);
        
        // Verify
        assertThat(metadata).isNotNull();
        assertThat(metadata).containsKeys("owner", "environment");
    }
    
    // ==================== Table Management Tests ====================
    
    /**
     * Test creating table (Create).
     */
    @Test
    @DisplayName("Test creating table in namespace")
    void testCreateTable() {
        // Prepare
        String namespaceName = "my_db";
        String tableName = "my_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        
        // Execute
        adapter.createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        // Verify
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
    }
    
    /**
     * Test creating multiple tables.
     */
    @Test
    @DisplayName("Test creating multiple tables in same namespace")
    void testCreateMultipleTables() {
        // Prepare
        String namespaceName = "test_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"users", "products", "orders", "analytics"};
        
        // Execute
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // Verify
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables).contains(tableNames);
    }
    
    /**
     * Test listing tables (Read).
     */
    @Test
    @DisplayName("Test listing all tables in namespace")
    void testListTables() {
        // Prepare
        String namespaceName = "query_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        adapter.createEmptyTable(
            warehousePath + "/" + namespaceName + "/table1",
            new HashMap<>(),
            namespaceName, "table1"
        );
        adapter.createEmptyTable(
            warehousePath + "/" + namespaceName + "/table2",
            new HashMap<>(),
            namespaceName, "table2"
        );
        
        // Execute
        List<String> tables = adapter.listTables(namespaceName);
        
        // Verify
        assertThat(tables).isNotNull();
        assertThat(tables).contains("table1", "table2");
        assertThat(tables.size()).isGreaterThanOrEqualTo(2);
    }
    
    /**
     * Test checking table existence (Read).
     */
    @Test
    @DisplayName("Test checking table existence")
    void testTableExists() {
        // Prepare
        String namespaceName = "check_db";
        String tableName = "check_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        // Execute and verify
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        assertThat(adapter.tableExists(namespaceName, "non_existing_table")).isFalse();
    }
    
    /**
     * Test getting table metadata (Read).
     */
    @Test
    @DisplayName("Test getting table metadata")
    void testGetTableMetadata() {
        // Prepare
        String namespaceName = "metadata_db";
        String tableName = "metadata_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        tableProperties.put("index", "ivf");
        
        adapter.createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        // Execute
        AbstractLanceNamespaceAdapter.TableMetadata metadata = 
            adapter.getTableMetadata(namespaceName, tableName);
        
        // Verify
        assertThat(metadata).isNotNull();
        assertThat(metadata.getLocation()).isNotNull();
        assertThat(metadata.getStorageOptions()).isNotNull();
    }
    
    /**
     * Test dropping table (Delete).
     */
    @Test
    @DisplayName("Test dropping table")
    void testDropTable() {
        // Prepare
        String namespaceName = "drop_db";
        String tableName = "drop_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        
        // Execute
        adapter.dropTable(namespaceName, tableName);
        
        // Verify
        assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
    }
    
    /**
     * Test dropping multiple tables.
     */
    @Test
    @DisplayName("Test dropping multiple tables in namespace")
    void testDropMultipleTables() {
        // Prepare
        String namespaceName = "cleanup_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"temp1", "temp2", "temp3"};
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // Verify creation successful
        for (String tableName : tableNames) {
            assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        }
        
        // Execute - drop all tables
        for (String tableName : tableNames) {
            adapter.dropTable(namespaceName, tableName);
        }
        
        // Verify - all tables dropped
        for (String tableName : tableNames) {
            assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
        }
    }
    
    // ==================== Comprehensive Scenario Tests ====================
    
    /**
     * Test complete CRUD lifecycle.
     */
    @Test
    @DisplayName("Test complete table CRUD lifecycle")
    void testCompleteTableCrudLifecycle() {
        // 1. Create - create namespace
        String namespaceName = "complete_db";
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put("owner", "admin");
        adapter.createNamespace(dbProps, namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // 2. Create - create table
        String tableName = "complete_table";
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("format", "lance");
        adapter.createEmptyTable(tableLocation, tableProps, namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        
        // 3. Read - list tables
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).contains(tableName);
        
        // 4. Read - get table metadata
        AbstractLanceNamespaceAdapter.TableMetadata metadata = 
            adapter.getTableMetadata(namespaceName, tableName);
        assertThat(metadata.getLocation()).contains(tableName);
        
        // 5. Delete - drop table
        adapter.dropTable(namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
        
        // 6. Delete - drop namespace
        adapter.dropNamespace(true, namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * Test multiple namespace independence.
     */
    @Test
    @DisplayName("Test independence of multiple namespaces")
    void testMultipleNamespaceIndependence() {
        // Prepare - create multiple independent namespaces
        String db1 = "database1";
        String db2 = "database2";
        String tableName = "test_table";
        
        adapter.createNamespace(new HashMap<>(), db1);
        adapter.createNamespace(new HashMap<>(), db2);
        
        // Create table in db1
        adapter.createEmptyTable(
            warehousePath + "/" + db1 + "/" + tableName,
            new HashMap<>(),
            db1, tableName
        );
        
        // Create table with same name in db2
        adapter.createEmptyTable(
            warehousePath + "/" + db2 + "/" + tableName,
            new HashMap<>(),
            db2, tableName
        );
        
        // Verify - both tables exist independently
        assertThat(adapter.tableExists(db1, tableName)).isTrue();
        assertThat(adapter.tableExists(db2, tableName)).isTrue();
        
        // Verify - dropping table in db1 doesn't affect db2
        adapter.dropTable(db1, tableName);
        assertThat(adapter.tableExists(db1, tableName)).isFalse();
        assertThat(adapter.tableExists(db2, tableName)).isTrue();
    }
    
    /**
     * Test special character support in table and namespace names.
     */
    @Test
    @DisplayName("Test naming with underscores and numbers")
    void testSpecialCharacterNaming() {
        // Prepare
        String namespaceName = "test_db_123";
        String tableName = "data_table_v2_001";
        
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        // Verify
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
    }
    
    /**
     * Test table quantity and performance.
     */
    @Test
    @DisplayName("Test creating many tables in single namespace")
    void testCreateManyTables() {
        // Prepare
        String namespaceName = "scale_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        int tableCount = 50;
        
        // Execute - create multiple tables
        for (int i = 0; i < tableCount; i++) {
            String tableName = "table_" + String.format("%03d", i);
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // Verify
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables.size()).isGreaterThanOrEqualTo(tableCount);
    }
    
    /**
     * Test exception scenario - creating existing namespace.
     */
    @Test
    @DisplayName("Test creating existing namespace throws exception")
    void testCreateExistingNamespaceThrowsException() {
        // Prepare
        String namespaceName = "existing_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // Execute and verify - expect exception
        assertThatThrownBy(() -> 
            adapter.createNamespace(new HashMap<>(), namespaceName)
        ).isNotNull();
    }
    
    /**
     * Test exception scenario - dropping non-existing namespace.
     */
    @Test
    @DisplayName("Test dropping non-existing namespace throws exception")
    void testDropNonExistingNamespaceThrowsException() {
        // Execute and verify - expect exception
        assertThatThrownBy(() -> 
            adapter.dropNamespace(false, "non_existing_db")
        ).isNotNull();
    }
    
    /**
     * Test exception scenario - dropping non-existing table.
     */
    @Test
    @DisplayName("Test dropping non-existing table throws exception")
    void testDropNonExistingTableThrowsException() {
        // Prepare
        String namespaceName = "error_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // Execute and verify - expect exception
        assertThatThrownBy(() -> 
            adapter.dropTable(namespaceName, "non_existing_table")
        ).isNotNull();
    }
    
    /**
     * Test resource cleanup and closing.
     */
    @Test
    @DisplayName("Test adapter closes correctly and releases resources")
    void testAdapterCloseAndResourceCleanup() throws Exception {
        // Prepare
        String namespaceName = "cleanup_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // Verify operation works
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // Execute - close adapter
        adapter.close();
        
        // Verify - resources released correctly when creating new adapter
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        LanceNamespaceAdapter newAdapter = LanceNamespaceAdapter.create(properties);
        newAdapter.init();
        
        try {
            // Verify previous operations preserved
            assertThat(newAdapter.namespaceExists(namespaceName)).isTrue();
        } finally {
            newAdapter.close();
        }
    }
}
