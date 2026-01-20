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

import java.util.List;
import java.util.Map;

/**
 * Abstract adapter interface for Lance Namespace operations.
 * 
 * This interface defines the contract for implementing namespace-based catalog operations,
 * allowing for different backend implementations (directory-based, REST-based, etc.).
 */
public interface AbstractLanceNamespaceAdapter extends AutoCloseable {
    
    /**
     * Initialize the adapter.
     */
    void init();
    
    /**
     * List all namespaces at root level.
     */
    List<String> listNamespaces();
    
    /**
     * List namespaces under a parent namespace.
     */
    List<String> listNamespaces(String... parentNamespace);
    
    /**
     * Check if a namespace exists.
     */
    boolean namespaceExists(String... namespaceId);
    
    /**
     * Create a namespace.
     */
    void createNamespace(Map<String, String> properties, String... namespaceId);
    
    /**
     * Drop a namespace.
     */
    void dropNamespace(boolean cascade, String... namespaceId);
    
    /**
     * Get namespace metadata.
     */
    Map<String, String> getNamespaceMetadata(String... namespaceId);
    
    /**
     * List tables in a namespace.
     */
    List<String> listTables(String... namespaceId);
    
    /**
     * Check if a table exists.
     */
    boolean tableExists(String... tableId);
    
    /**
     * Create an empty table.
     */
    void createEmptyTable(String location, Map<String, String> properties, String... tableId);
    
    /**
     * Drop a table.
     */
    void dropTable(String... tableId);
    
    /**
     * Get table metadata.
     */
    TableMetadata getTableMetadata(String... tableId);
    
    /**
     * Table metadata holder.
     */
    class TableMetadata {
        private final String location;
        private final Map<String, String> storageOptions;
        
        public TableMetadata(String location, Map<String, String> storageOptions) {
            this.location = location;
            this.storageOptions = storageOptions;
        }
        
        public String getLocation() {
            return location;
        }
        
        public Map<String, String> getStorageOptions() {
            return storageOptions;
        }
    }
}
