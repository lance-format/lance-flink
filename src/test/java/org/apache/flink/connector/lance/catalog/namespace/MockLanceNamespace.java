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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mock implementation of Lance Namespace for testing purposes.
 * 
 * This mock allows tests to run without requiring the actual Lance Namespace
 * library to be available. It simulates the basic behavior of the real API.
 */
public class MockLanceNamespace {
    
    /**
     * Storage for namespaces and their properties.
     * Map structure: namespace_path -> properties
     */
    private final Map<String, Map<String, String>> namespaces = new HashMap<>();
    
    /**
     * Storage for tables and their properties.
     * Map structure: table_path -> properties
     */
    private final Map<String, Map<String, String>> tables = new HashMap<>();
    
    /**
     * Check if namespace exists locally.
     */
    private boolean hasNamespace(String id) {
        return namespaces.containsKey(id);
    }
    
    /**
     * Check if table exists locally.
     */
    private boolean hasTable(String id) {
        return tables.containsKey(id);
    }
    
    /**
     * Create a mock namespace instance.
     */
    public static MockLanceNamespace connect(String impl, String location, Object allocator) {
        return new MockLanceNamespace();
    }
    
    /**
     * Mock: Create a namespace.
     */
    public void createNamespace(Object request) {
        String id = extractId(request);
        if (hasNamespace(id)) {
            throw new RuntimeException("Namespace already exists: " + id);
        }
        Map<String, String> props = extractProperties(request);
        namespaces.put(id, props != null ? new HashMap<>(props) : new HashMap<>());
    }
    
    /**
     * Mock: List namespaces.
     */
    public Object listNamespaces(Object request) {
        Set<String> result = new HashSet<>(namespaces.keySet());
        return createResponse(result);
    }
    
    /**
     * Mock: Check if namespace exists.
     */
    public Object namespaceExists(Object request) {
        String id = extractId(request);
        if (!hasNamespace(id)) {
            throw new RuntimeException("Namespace does not exist: " + id);
        }
        return null;
    }
    
    /**
     * Mock: Drop a namespace.
     */
    public void dropNamespace(Object request) {
        String id = extractId(request);
        if (!hasNamespace(id)) {
            throw new RuntimeException("Namespace does not exist: " + id);
        }
        namespaces.remove(id);
    }
    
    /**
     * Mock: Describe a namespace (get metadata).
     */
    public Object describeNamespace(Object request) {
        String id = extractId(request);
        if (!hasNamespace(id)) {
            throw new RuntimeException("Namespace does not exist: " + id);
        }
        Map<String, String> props = namespaces.get(id);
        return createDescribeResponse(props);
    }
    
    /**
     * Mock: Create an empty table.
     */
    public void createEmptyTable(Object request) {
        String id = extractId(request);
        if (hasTable(id)) {
            throw new RuntimeException("Table already exists: " + id);
        }
        Map<String, String> props = extractProperties(request);
        tables.put(id, props != null ? new HashMap<>(props) : new HashMap<>());
    }
    
    /**
     * Mock: List tables.
     */
    public Object listTables(Object request) {
        Set<String> result = new HashSet<>(tables.keySet());
        return createResponse(result);
    }
    
    /**
     * Mock: Check if table exists.
     */
    public Object tableExists(Object request) {
        String id = extractId(request);
        if (!hasTable(id)) {
            throw new RuntimeException("Table does not exist: " + id);
        }
        return null;
    }
    
    /**
     * Mock: Drop a table.
     */
    public void dropTable(Object request) {
        String id = extractId(request);
        if (!hasTable(id)) {
            throw new RuntimeException("Table does not exist: " + id);
        }
        tables.remove(id);
    }
    
    /**
     * Mock: Describe a table (get metadata).
     */
    public Object describeTable(Object request) {
        String id = extractId(request);
        if (!hasTable(id)) {
            throw new RuntimeException("Table does not exist: " + id);
        }
        Map<String, String> props = tables.get(id);
        return createTableDescribeResponse(props);
    }
    
    /**
     * Close the connection.
     */
    public void close() throws Exception {
        // Cleanup
        namespaces.clear();
        tables.clear();
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Extract ID from request object.
     */
    private String extractId(Object request) {
        if (request == null) {
            return "";
        }
        try {
            Object id = request.getClass()
                    .getMethod("getId")
                    .invoke(request);
            if (id instanceof java.util.List) {
                java.util.List<?> list = (java.util.List<?>) id;
                return list.isEmpty() ? "" : list.get(0).toString();
            }
            return id != null ? id.toString() : "";
        } catch (Exception e) {
            return "";
        }
    }
    
    /**
     * Extract properties from request object.
     */
    private Map<String, String> extractProperties(Object request) {
        if (request == null) {
            return null;
        }
        try {
            Object props = request.getClass()
                    .getMethod("getProperties")
                    .invoke(request);
            if (props instanceof Map) {
                return (Map<String, String>) props;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }
    
    /**
     * Create a response object with namespaces.
     */
    private Object createResponse(Set<String> namespaces) {
        try {
            Class<?> responseClass = Class.forName(
                    "org.lance.namespace.model.ListNamespacesResponse");
            Object response = responseClass.getDeclaredConstructor().newInstance();
            
            // Try to set the namespaces using reflection
            try {
                responseClass.getMethod("setNamespaces", Set.class)
                        .invoke(response, namespaces);
            } catch (Exception e) {
                // If setNamespaces fails, try setting as field
                java.lang.reflect.Field field = responseClass.getDeclaredField("namespaces");
                field.setAccessible(true);
                field.set(response, namespaces);
            }
            return response;
        } catch (Exception e) {
            // Fallback: return a simple object with getNamespaces method
            return new Object() {
                public Set<String> getNamespaces() {
                    return namespaces;
                }
            };
        }
    }
    
    /**
     * Create a describe response object.
     */
    private Object createDescribeResponse(Map<String, String> properties) {
        try {
            Class<?> responseClass = Class.forName(
                    "org.lance.namespace.model.DescribeNamespaceResponse");
            Object response = responseClass.getDeclaredConstructor().newInstance();
            
            try {
                responseClass.getMethod("setProperties", Map.class)
                        .invoke(response, properties);
            } catch (Exception e) {
                // Fallback to field
                java.lang.reflect.Field field = responseClass.getDeclaredField("properties");
                field.setAccessible(true);
                field.set(response, properties);
            }
            return response;
        } catch (Exception e) {
            // Fallback
            return new Object() {
                public Map<String, String> getProperties() {
                    return properties;
                }
            };
        }
    }
    
    /**
     * Create a table describe response object.
     */
    private Object createTableDescribeResponse(Map<String, String> properties) {
        try {
            Class<?> responseClass = Class.forName(
                    "org.lance.namespace.model.DescribeTableResponse");
            Object response = responseClass.getDeclaredConstructor().newInstance();
            
            try {
                responseClass.getMethod("setProperties", Map.class)
                        .invoke(response, properties);
            } catch (Exception e) {
                // Fallback to field
                java.lang.reflect.Field field = responseClass.getDeclaredField("properties");
                field.setAccessible(true);
                field.set(response, properties);
            }
            
            // Try to set table_path
            try {
                responseClass.getMethod("setTable_path", String.class)
                        .invoke(response, "/path/to/table");
            } catch (Exception ignored) {
                // Optional field
            }
            
            return response;
        } catch (Exception e) {
            // Fallback
            return new Object() {
                public Map<String, String> getProperties() {
                    return properties;
                }
                
                public String getTable_path() {
                    return "/path/to/table";
                }
            };
        }
    }
}
