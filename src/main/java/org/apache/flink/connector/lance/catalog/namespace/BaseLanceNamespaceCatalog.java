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

import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * Base class for Lance Catalog implementation integrated with Lance Namespace.
 * 
 * This class provides the foundation for catalog operations using a namespace adapter,
 * supporting multi-level namespace hierarchies and flexible backend implementations.
 */
public abstract class BaseLanceNamespaceCatalog extends AbstractCatalog {
    
    protected AbstractLanceNamespaceAdapter adapter;
    protected LanceNamespaceConfig config;
    
    public BaseLanceNamespaceCatalog(String catalogName, 
                                    AbstractLanceNamespaceAdapter adapter,
                                    LanceNamespaceConfig config) {
        super(catalogName, "default");
        this.adapter = adapter;
        this.config = config;
    }
    
    /**
     * Abstract method to be implemented by subclasses to create CatalogTable from metadata.
     */
    protected abstract CatalogTable createCatalogTable(
            String databaseName,
            String tableName,
            AbstractLanceNamespaceAdapter.TableMetadata metadata) throws CatalogException;
    
    /**
     * Transform database name to namespace path array.
     */
    protected String[] transformDatabaseNameToNamespace(String databaseName) {
        java.util.Optional<String[]> parentPrefix = config.getParentArray();
        java.util.Optional<String> extraLevel = config.getExtraLevel();
        
        if (parentPrefix.isPresent()) {
            String[] parent = parentPrefix.get();
            String[] result = new String[parent.length + 1];
            System.arraycopy(parent, 0, result, 0, parent.length);
            result[parent.length] = databaseName;
            return result;
        } else if (extraLevel.isPresent()) {
            return new String[]{extraLevel.get(), databaseName};
        } else {
            return new String[]{databaseName};
        }
    }
    
    /**
     * Transform table name to full table ID (namespace path + table name).
     */
    protected String[] transformTableNameToId(String databaseName, String tableName) {
        String[] dbPath = transformDatabaseNameToNamespace(databaseName);
        String[] result = new String[dbPath.length + 1];
        System.arraycopy(dbPath, 0, result, 0, dbPath.length);
        result[dbPath.length] = tableName;
        return result;
    }
}
