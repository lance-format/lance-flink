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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Lance Namespace Adapter Implementation.
 * 
 * Directly calls Lance Namespace SDK APIs to implement database and table management.
 * Supports both local file system and REST backend implementations.
 */
public class LanceNamespaceAdapter implements AbstractLanceNamespaceAdapter {
    
    private static final Logger LOG = LoggerFactory.getLogger(LanceNamespaceAdapter.class);
    
    private final BufferAllocator allocator;
    private final LanceNamespaceConfig config;
    private LanceNamespace namespace;
    
    public LanceNamespaceAdapter(BufferAllocator allocator, LanceNamespaceConfig config) {
        this.allocator = Objects.requireNonNull(allocator, "BufferAllocator cannot be null");
        this.config = Objects.requireNonNull(config, "LanceNamespaceConfig cannot be null");
    }
    
    /**
     * Factory method to create Adapter instance.
     */
    public static LanceNamespaceAdapter create(Map<String, String> properties) {
        LanceNamespaceConfig config = LanceNamespaceConfig.from(properties);
        BufferAllocator allocator = new RootAllocator();
        return new LanceNamespaceAdapter(allocator, config);
    }
    
    /**
     * Initialize Lance Namespace connection.
     * Directly calls LanceNamespace.connect() method.
     */
    @Override
    public void init() {
        try {
            if (config.isDirectoryNamespace() && config.getRoot().isPresent()) {
                // Call: LanceNamespace.connect("file", root_path, allocator)
                LOG.info("Initializing local file system namespace: {}", config.getRoot().get());
                namespace = LanceNamespace.connect("file", config.getRoot().get(), allocator);
            } else if (config.isRestNamespace() && config.getUri().isPresent()) {
                // Call: LanceNamespace.connect("rest", uri, allocator)
                LOG.info("Initializing REST namespace: {}", config.getUri().get());
                namespace = LanceNamespace.connect("rest", config.getUri().get(), allocator);
            } else {
                throw new IllegalArgumentException("Invalid namespace configuration");
            }
            
            LOG.info("Lance Namespace connection successful");
        } catch (Exception e) {
            LOG.error("Failed to initialize Lance Namespace", e);
            throw new RuntimeException("Failed to initialize Lance Namespace", e);
        }
    }
    
    /**
     * List all top-level namespaces.
     */
    @Override
    public List<String> listNamespaces() {
        return listNamespaces(new String[0]);
    }
    
    /**
     * List child namespaces under a parent namespace.
     * Directly calls: LanceNamespace.listNamespaces(ListNamespacesRequest)
     */
    @Override
    public List<String> listNamespaces(String... parentNamespace) {
        try {
            ListNamespacesRequest request = new ListNamespacesRequest();
            if (parentNamespace.length > 0) {
                request.setId(Arrays.asList(parentNamespace));
            }
            
            ListNamespacesResponse response = namespace.listNamespaces(request);
            
            if (response.getNamespaces() != null) {
                Set<String> namespaceSet = response.getNamespaces();
                return new ArrayList<>(namespaceSet);
            }
            return new ArrayList<>();
            
        } catch (Exception e) {
            LOG.warn("Failed to list namespaces", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Check if namespace exists.
     * Directly calls: LanceNamespace.namespaceExists(NamespaceExistsRequest)
     */
    @Override
    public boolean namespaceExists(String... namespaceId) {
        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(Arrays.asList(namespaceId));
            
            namespace.namespaceExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Namespace does not exist: {}", Arrays.toString(namespaceId));
            return false;
        }
    }
    
    /**
     * Create namespace.
     * Directly calls: LanceNamespace.createNamespace(CreateNamespaceRequest)
     */
    @Override
    public void createNamespace(Map<String, String> properties, String... namespaceId) {
        try {
            CreateNamespaceRequest request = new CreateNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            
            if (properties != null && !properties.isEmpty()) {
                request.setProperties(properties);
            }
            
            namespace.createNamespace(request);
            
            LOG.info("Namespace created successfully: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to create namespace", e);
            throw new RuntimeException("Failed to create namespace", e);
        }
    }
    
    /**
     * Drop namespace.
     * Directly calls: LanceNamespace.dropNamespace(DropNamespaceRequest)
     */
    @Override
    public void dropNamespace(boolean cascade, String... namespaceId) {
        try {
            DropNamespaceRequest request = new DropNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            request.setCascade(cascade);
            
            namespace.dropNamespace(request);
            
            LOG.info("Namespace dropped successfully: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to drop namespace", e);
            throw new RuntimeException("Failed to drop namespace", e);
        }
    }
    
    /**
     * Get namespace metadata.
     * Directly calls: LanceNamespace.describeNamespace(DescribeNamespaceRequest)
     */
    @Override
    public Map<String, String> getNamespaceMetadata(String... namespaceId) {
        try {
            DescribeNamespaceRequest request = new DescribeNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            
            DescribeNamespaceResponse response = namespace.describeNamespace(request);
            
            if (response.getProperties() != null) {
                return response.getProperties();
            }
            return new HashMap<>();
        } catch (Exception e) {
            LOG.warn("Failed to get namespace metadata", e);
            return new HashMap<>();
        }
    }
    
    /**
     * List all tables in namespace.
     * Directly calls: LanceNamespace.listTables(ListTablesRequest)
     */
    @Override
    public List<String> listTables(String... namespaceId) {
        try {
            ListTablesRequest request = new ListTablesRequest();
            request.setId(Arrays.asList(namespaceId));
            
            ListTablesResponse response = namespace.listTables(request);
            
            if (response.getTables() != null) {
                Set<String> tableSet = response.getTables();
                return new ArrayList<>(tableSet);
            }
            return new ArrayList<>();
        } catch (Exception e) {
            LOG.warn("Failed to list tables", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Check if table exists.
     * Directly calls: LanceNamespace.tableExists(TableExistsRequest)
     */
    @Override
    public boolean tableExists(String... tableId) {
        try {
            TableExistsRequest request = new TableExistsRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.tableExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Table does not exist: {}", Arrays.toString(tableId));
            return false;
        }
    }
    
    /**
     * Create empty table.
     * Directly calls: LanceNamespace.createEmptyTable(CreateEmptyTableRequest)
     */
    @Override
    public void createEmptyTable(String location, Map<String, String> properties, String... tableId) {
        try {
            CreateEmptyTableRequest request = new CreateEmptyTableRequest();
            request.setId(Arrays.asList(tableId));
            
            // Set table location information
            if (location != null) {
                request.setPath(location);
            }
            
            namespace.createEmptyTable(request);
            
            LOG.info("Table created successfully: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to create table", e);
            throw new RuntimeException("Failed to create table", e);
        }
    }
    
    /**
     * Drop table.
     * Directly calls: LanceNamespace.dropTable(DropTableRequest)
     */
    @Override
    public void dropTable(String... tableId) {
        try {
            DropTableRequest request = new DropTableRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.dropTable(request);
            
            LOG.info("Table dropped successfully: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to drop table", e);
            throw new RuntimeException("Failed to drop table", e);
        }
    }
    
    /**
     * Get table metadata.
     * Directly calls: LanceNamespace.describeTable(DescribeTableRequest)
     */
    @Override
    public TableMetadata getTableMetadata(String... tableId) {
        try {
            DescribeTableRequest request = new DescribeTableRequest();
            request.setId(Arrays.asList(tableId));
            
            DescribeTableResponse response = namespace.describeTable(request);
            
            String location = "/path/to/table";
            Map<String, String> options = new HashMap<>();
            
            // Call API to get table path
            if (response.getTable_path() != null) {
                location = response.getTable_path();
            }
            
            // Call API to get properties
            if (response.getProperties() != null) {
                options = response.getProperties();
            }
            
            return new TableMetadata(location, options);
        } catch (Exception e) {
            LOG.warn("Failed to get table metadata", e);
            return new TableMetadata("/path/to/table", new HashMap<>());
        }
    }
    

    
    @Override
    public void close() throws Exception {
        try {
            if (namespace != null) {
                namespace.close();
            }
        } catch (Exception e) {
            LOG.warn("Error during namespace cleanup", e);
        }
        
        if (allocator != null) {
            allocator.close();
        }
    }
}
