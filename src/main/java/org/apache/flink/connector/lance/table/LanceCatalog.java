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

import org.apache.flink.connector.lance.PrimaryKeyPersistence;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.schema.ColumnAlteration;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Lance Catalog implementation.
 * 
 * <p>Implements Flink Catalog interface, supports managing Lance datasets as Flink tables.
 * Supports local file system and S3 protocol object storage.
 * 
 * <p>Usage example (local path):
 * <pre>{@code
 * CREATE CATALOG lance_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = '/path/to/warehouse',
 *     'default-database' = 'default'
 * );
 * }</pre>
 * 
 * <p>Usage example (S3 path):
 * <pre>{@code
 * CREATE CATALOG lance_s3_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = 's3://bucket-name/warehouse',
 *     'default-database' = 'default',
 *     's3-access-key' = 'your-access-key',
 *     's3-secret-key' = 'your-secret-key',
 *     's3-region' = 'us-east-1'
 * );
 * }</pre>
 */
public class LanceCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    /** Option keys that are connector/runtime configuration rather than user TBLPROPERTIES. */
    private static final Set<String> RESERVED_OPTION_KEYS = Collections.unmodifiableSet(
            new HashSet<>(java.util.Arrays.asList(
                    "connector", "path",
                    "s3-access-key", "s3-secret-key", "s3-region", "s3-endpoint")));

    private final String warehouse;
    private final Map<String, String> storageOptions;
    private final boolean isRemoteStorage;
    private transient BufferAllocator allocator;
    
    // Cache known databases and tables for remote storage
    private final Set<String> knownDatabases = ConcurrentHashMap.newKeySet();
    private final Set<String> knownTables = ConcurrentHashMap.newKeySet();

    /**
     * Create LanceCatalog (local storage)
     *
     * @param name Catalog name
     * @param defaultDatabase Default database name
     * @param warehouse Warehouse path
     */
    public LanceCatalog(String name, String defaultDatabase, String warehouse) {
        this(name, defaultDatabase, warehouse, Collections.emptyMap());
    }

    /**
     * Create LanceCatalog (supports remote storage)
     *
     * @param name Catalog name
     * @param defaultDatabase Default database name
     * @param warehouse Warehouse path (local path or S3 URI)
     * @param storageOptions Storage configuration options (e.g., S3 credentials)
     */
    public LanceCatalog(String name, String defaultDatabase, String warehouse, Map<String, String> storageOptions) {
        super(name, defaultDatabase);
        this.warehouse = normalizeWarehousePath(warehouse);
        this.storageOptions = storageOptions != null ? new HashMap<>(storageOptions) : Collections.emptyMap();
        this.isRemoteStorage = isRemotePath(warehouse);
    }

    /**
     * Check if path is remote storage path
     */
    private boolean isRemotePath(String path) {
        if (path == null) {
            return false;
        }
        String lowerPath = path.toLowerCase();
        return lowerPath.startsWith("s3://") || 
               lowerPath.startsWith("s3a://") || 
               lowerPath.startsWith("gs://") || 
               lowerPath.startsWith("az://") ||
               lowerPath.startsWith("https://") ||
               lowerPath.startsWith("http://");
    }

    /**
     * Normalize warehouse path
     */
    private String normalizeWarehousePath(String path) {
        if (path == null) {
            return null;
        }
        // Remove trailing slashes
        while (path.endsWith("/") && path.length() > 1) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("Opening Lance Catalog: {}, warehouse path: {}, remote storage: {}", getName(), warehouse, isRemoteStorage);
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        if (isRemoteStorage) {
            // Remote storage: initialize default database record
            knownDatabases.add(getDefaultDatabase());
            LOG.info("Remote storage mode enabled, storage config count: {}", storageOptions.size());
        } else {
            // Local storage: ensure warehouse directory exists
            Path warehousePath = Paths.get(warehouse);
            if (!Files.exists(warehousePath)) {
                try {
                    Files.createDirectories(warehousePath);
                } catch (IOException e) {
                    throw new CatalogException("Cannot create warehouse directory: " + warehouse, e);
                }
            }
            
            // Ensure default database exists
            Path defaultDbPath = warehousePath.resolve(getDefaultDatabase());
            if (!Files.exists(defaultDbPath)) {
                try {
                    Files.createDirectories(defaultDbPath);
                } catch (IOException e) {
                    throw new CatalogException("Cannot create default database directory: " + defaultDbPath, e);
                }
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Closing Lance Catalog: {}", getName());
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close allocator", e);
            }
            allocator = null;
        }
        
        knownDatabases.clear();
        knownTables.clear();
    }

    // ==================== Database Operations ====================

    @Override
    public List<String> listDatabases() throws CatalogException {
        if (isRemoteStorage) {
            // Remote storage: return known database list
            return new ArrayList<>(knownDatabases);
        }
        
        try {
            Path warehousePath = Paths.get(warehouse);
            if (!Files.exists(warehousePath)) {
                return Collections.emptyList();
            }
            
            return Files.list(warehousePath)
                    .filter(Files::isDirectory)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        return new CatalogDatabaseImpl(Collections.emptyMap(), "Lance Database: " + databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (isRemoteStorage) {
            // Remote storage: check known databases or try listing tables to verify
            if (knownDatabases.contains(databaseName)) {
                return true;
            }
            // Try to confirm database exists by checking for tables
            try {
                String dbPath = getDatabasePath(databaseName);
                // For remote storage, assume database always exists (actual table operations will verify)
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        
        Path dbPath = Paths.get(warehouse, databaseName);
        return Files.exists(dbPath) && Files.isDirectory(dbPath);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (isRemoteStorage) {
            // Remote storage: only record database name, actual directory created when creating table
            if (knownDatabases.contains(name)) {
                if (!ignoreIfExists) {
                    throw new DatabaseAlreadyExistException(getName(), name);
                }
                return;
            }
            knownDatabases.add(name);
            LOG.info("Registered remote database: {}", name);
            return;
        }
        
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            Files.createDirectories(dbPath);
            LOG.info("Created database: {}", name);
        } catch (IOException e) {
            throw new CatalogException("Failed to create database: " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (isRemoteStorage) {
            // Remote storage: remove database record
            if (!knownDatabases.contains(name)) {
                if (!ignoreIfNotExists) {
                    throw new DatabaseNotExistException(getName(), name);
                }
                return;
            }
            
            // Check if has tables
            List<String> tables = listTables(name);
            if (!tables.isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            // If cascade, delete all tables
            if (cascade) {
                for (String table : tables) {
                    try {
                        dropTable(new ObjectPath(name, table), true);
                    } catch (TableNotExistException e) {
                        // Ignore
                    }
                }
            }
            
            knownDatabases.remove(name);
            LOG.info("Removed remote database record: {}", name);
            return;
        }
        
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            List<String> tables = listTables(name);
            if (!tables.isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            // Delete database directory
            deleteDirectory(dbPath);
            LOG.info("Deleted database: {}", name);
        } catch (IOException e) {
            throw new CatalogException("Failed to delete database: " + name, e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        // Lance database does not support modifying properties
        LOG.warn("Lance Catalog does not support modifying database properties");
    }

    // ==================== Table Operations ====================

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        if (isRemoteStorage) {
            // Remote storage: return known table list
            String prefix = databaseName + "/";
            return knownTables.stream()
                    .filter(t -> t.startsWith(prefix))
                    .map(t -> t.substring(prefix.length()))
                    .collect(Collectors.toList());
        }
        
        try {
            Path dbPath = Paths.get(warehouse, databaseName);
            return Files.list(dbPath)
                    .filter(Files::isDirectory)
                    .filter(path -> Files.exists(path.resolve("_versions"))) // Lance dataset identifier
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Failed to list tables", e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        // Lance does not support views
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        try {
            // For remote storage, configure S3 credentials via environment variables
            if (isRemoteStorage) {
                configureStorageEnvironment();
            }
            Dataset dataset = Dataset.open(datasetPath, allocator);
            
            try {
                // Infer Flink Schema from Lance Schema
                org.apache.arrow.vector.types.pojo.Schema arrowSchema = dataset.getSchema();
                RowType rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
                
                // Build CatalogTable
                Schema.Builder schemaBuilder = Schema.newBuilder();
                for (RowType.RowField field : rowType.getFields()) {
                    DataType dataType = LanceTypeConverter.toDataType(field.getType());
                    schemaBuilder.column(field.getName(), dataType);
                }

                // Restore the primary key persisted in the dataset config.
                List<String> primaryKeys = PrimaryKeyPersistence.load(dataset);
                if (!primaryKeys.isEmpty()) {
                    schemaBuilder.primaryKey(primaryKeys);
                }

                Map<String, String> options = new HashMap<>();
                options.put("connector", LanceDynamicTableFactory.IDENTIFIER);
                options.put("path", datasetPath);
                
                // If remote storage, add storage config to table options
                if (isRemoteStorage) {
                    options.putAll(getStorageOptionsForTable());
                }
                
                return CatalogTable.of(
                        schemaBuilder.build(),
                        "Lance Table: " + tablePath.getFullName(),
                        Collections.emptyList(),
                        options
                );
            } finally {
                dataset.close();
            }
        } catch (Exception e) {
            throw new CatalogException("Failed to get table info: " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            return false;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        if (isRemoteStorage) {
            // Remote storage: check known tables or try opening dataset
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            if (knownTables.contains(tableKey)) {
                return true;
            }
            
            // Try to open dataset to verify existence
            try {
                configureStorageEnvironment();
                Dataset dataset = Dataset.open(datasetPath, allocator);
                dataset.close();
                knownTables.add(tableKey);
                return true;
            } catch (Exception e) {
                LOG.debug("Table does not exist or cannot be accessed: {}", datasetPath, e);
                return false;
            }
        }
        
        Path path = Paths.get(datasetPath);
        
        // Check if valid Lance dataset
        return Files.exists(path) && Files.isDirectory(path) && 
               Files.exists(path.resolve("_versions"));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        if (isRemoteStorage) {
            // Remote storage: Lance Java SDK does not directly support deleting remote datasets
            // Only remove record here, actual deletion requires cloud storage API
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            knownTables.remove(tableKey);
            LOG.warn("Remote storage mode, table record removed, but actual data needs manual deletion from storage: {}", datasetPath);
            return;
        }
        
        try {
            deleteDirectory(Paths.get(datasetPath));
            LOG.info("Deleted table: {}", tablePath);
        } catch (IOException e) {
            throw new CatalogException("Failed to delete table: " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tableExists(newTablePath)) {
            throw new TableAlreadyExistException(getName(), newTablePath);
        }
        
        if (isRemoteStorage) {
            // Remote storage: does not support renaming
            throw new CatalogException("Remote storage mode does not support renaming tables");
        }
        
        String oldPath = getDatasetPath(tablePath);
        String newPath = getDatasetPath(newTablePath);
        
        try {
            Files.move(Paths.get(oldPath), Paths.get(newPath));
            LOG.info("Renamed table: {} -> {}", tablePath, newTablePath);
        } catch (IOException e) {
            throw new CatalogException("Failed to rename table: " + tablePath, e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }

        RowType rowType = resolveRowType(table);
        org.apache.arrow.vector.types.pojo.Schema arrowSchema =
                LanceTypeConverter.toArrowSchema(rowType);
        List<String> primaryKeys = extractPrimaryKeys(table);

        String datasetPath = getDatasetPath(tablePath);
        if (isRemoteStorage) {
            configureStorageEnvironment();
        }

        // Materialize an empty dataset immediately (matching the community Spark/Trino behavior),
        // so the schema and primary-key metadata survive a catalog round-trip before any write.
        try (Dataset dataset = Dataset.create(
                allocator, datasetPath, arrowSchema, new WriteParams.Builder().build())) {
            PrimaryKeyPersistence.persist(dataset, primaryKeys);
        } catch (Exception e) {
            throw new CatalogException("Failed to create table: " + tablePath, e);
        }

        if (isRemoteStorage) {
            String tableKey = tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
            knownTables.add(tableKey);
        }

        LOG.info("Created table: {} (primary keys: {})", tablePath, primaryKeys);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }

        String datasetPath = getDatasetPath(tablePath);
        if (isRemoteStorage) {
            configureStorageEnvironment();
        }

        try (Dataset dataset = Dataset.open(datasetPath, allocator)) {
            RowType newRowType = resolveRowType(newTable);
            RowType oldRowType = LanceTypeConverter.toFlinkRowType(dataset.getSchema());

            SchemaDiff diff = SchemaDiff.compute(oldRowType, newRowType);

            // Data-type changes are rejected: Lance Java SDK 7.0.0's castTo is verified not to
            // mutate the schema (a silent no-op), so supporting it would be a correctness hazard.
            if (diff.hasTypeChanges()) {
                List<String> affected = new ArrayList<>(diff.getTypeChangedColumns());
                for (SchemaDiff.Rename rename : diff.getRenames()) {
                    if (rename.getNewType() != null) {
                        affected.add(rename.getNewName());
                    }
                }
                throw new CatalogException(
                        "ALTER COLUMN data type change is not supported yet (Lance Java SDK castTo is unreliable). Affected columns: "
                                + affected);
            }

            // Pure renames are metadata-only and reliable.
            List<ColumnAlteration> alterations = new ArrayList<>();
            for (SchemaDiff.Rename rename : diff.getRenames()) {
                alterations.add(new ColumnAlteration.Builder(rename.getOldName())
                        .rename(rename.getNewName())
                        .build());
            }
            if (!alterations.isEmpty()) {
                dataset.alterColumns(alterations);
            }

            if (!diff.getAddedColumns().isEmpty()) {
                List<Field> fields = new ArrayList<>();
                for (RowType.RowField added : diff.getAddedColumns()) {
                    fields.add(LanceTypeConverter.flinkTypeToArrowField(added.getName(), added.getType()));
                }
                dataset.addColumns(fields);
            }

            if (!diff.getDroppedColumns().isEmpty()) {
                dataset.dropColumns(diff.getDroppedColumns());
            }

            applyTableProperties(dataset, newTable);

            LOG.info("Altered table: {} (added={}, dropped={})",
                    tablePath, diff.getAddedColumns().size(), diff.getDroppedColumns().size());
        } catch (CatalogException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to alter table: " + tablePath, e);
        }
    }

    /**
     * Build a physical {@link RowType} from a table's unresolved schema, skipping computed and
     * metadata columns.
     */
    private RowType resolveRowType(CatalogBaseTable table) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (Schema.UnresolvedColumn column : table.getUnresolvedSchema().getColumns()) {
            if (!(column instanceof Schema.UnresolvedPhysicalColumn)) {
                continue;
            }
            Schema.UnresolvedPhysicalColumn physical = (Schema.UnresolvedPhysicalColumn) column;
            if (!(physical.getDataType() instanceof DataType)) {
                throw new CatalogException(
                        "Column '" + physical.getName() + "' has an unresolved data type");
            }
            DataType dataType = (DataType) physical.getDataType();
            fields.add(new RowType.RowField(physical.getName(), dataType.getLogicalType()));
        }
        if (fields.isEmpty()) {
            throw new CatalogException("Cannot create or alter a Lance table without physical columns");
        }
        return new RowType(fields);
    }

    /**
     * Extract the {@code PRIMARY KEY ... NOT ENFORCED} column names from the table's schema.
     *
     * <p>The catalog receives an unresolved schema on the SQL DDL path, whose primary key is
     * represented by {@link Schema.UnresolvedPrimaryKey}; the resolved variant
     * ({@code UniqueConstraint}) is only available once the schema has been resolved, so this
     * deliberately reads the unresolved form.
     */
    private List<String> extractPrimaryKeys(CatalogBaseTable table) {
        return table.getUnresolvedSchema()
                .getPrimaryKey()
                .map(Schema.UnresolvedPrimaryKey::getColumnNames)
                .orElse(Collections.emptyList());
    }

    /**
     * Apply {@code SET}/{@code UNSET} TBLPROPERTIES by syncing non-connector options into the
     * Lance dataset config (and removing config keys absent from the new options).
     */
    private void applyTableProperties(Dataset dataset, CatalogBaseTable newTable) {
        Map<String, String> options = newTable.getOptions();

        Map<String, String> toSet = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            if (isTblProperty(entry.getKey())) {
                toSet.put(entry.getKey(), entry.getValue());
            }
        }
        if (!toSet.isEmpty()) {
            dataset.updateConfig(toSet);
        }

        Set<String> toUnset = new HashSet<>();
        for (String key : dataset.getConfig().keySet()) {
            if (PrimaryKeyPersistence.PK_CONFIG_KEY.equals(key)) {
                continue;
            }
            if (isTblProperty(key) && !options.containsKey(key)) {
                toUnset.add(key);
            }
        }
        if (!toUnset.isEmpty()) {
            dataset.deleteConfigKeys(toUnset);
        }
    }

    private boolean isTblProperty(String key) {
        if (key == null) {
            return false;
        }
        if (RESERVED_OPTION_KEYS.contains(key)) {
            return false;
        }
        if (key.startsWith("hadoop.")) {
            return false;
        }
        return !key.startsWith("read.")
                && !key.startsWith("write.")
                && !key.startsWith("index.")
                && !key.startsWith("vector.");
    }

    // ==================== Partition Operations (Lance does not support partitions) ====================

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new CatalogException("Lance Catalog does not support partition operations");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog does not support partition operations");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog does not support partition operations");
    }

    // ==================== Function Operations (Lance does not support UDFs) ====================

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog does not support user-defined functions");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog does not support user-defined functions");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog does not support user-defined functions");
    }

    // ==================== Statistics Operations ====================

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // Not supported
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // Not supported
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // Not supported
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // Not supported
    }

    // ==================== Utility Methods ====================

    /**
     * Configure storage environment variables (for S3 and other remote storage)
     * 
     * <p>Lance configures S3 credentials via environment variables:
     * <ul>
     *   <li>AWS_ACCESS_KEY_ID - AWS access key ID</li>
     *   <li>AWS_SECRET_ACCESS_KEY - AWS secret access key</li>
     *   <li>AWS_DEFAULT_REGION - AWS region</li>
     *   <li>AWS_ENDPOINT - Custom endpoint URL (for S3-compatible storage)</li>
     * </ul>
     */
    private void configureStorageEnvironment() {
        if (!isRemoteStorage || storageOptions.isEmpty()) {
            return;
        }
        
        // Set environment variables for Lance SDK object_store configuration
        // Note: Since Java cannot directly modify environment variables, system properties are used as fallback
        // Lance's Rust backend will read these environment variables
        
        if (storageOptions.containsKey("aws_access_key_id")) {
            System.setProperty("AWS_ACCESS_KEY_ID", storageOptions.get("aws_access_key_id"));
        }
        if (storageOptions.containsKey("aws_secret_access_key")) {
            System.setProperty("AWS_SECRET_ACCESS_KEY", storageOptions.get("aws_secret_access_key"));
        }
        if (storageOptions.containsKey("aws_region")) {
            System.setProperty("AWS_DEFAULT_REGION", storageOptions.get("aws_region"));
        }
        if (storageOptions.containsKey("aws_endpoint")) {
            System.setProperty("AWS_ENDPOINT", storageOptions.get("aws_endpoint"));
        }
        if (storageOptions.containsKey("aws_virtual_hosted_style_request")) {
            System.setProperty("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", 
                    storageOptions.get("aws_virtual_hosted_style_request"));
        }
        if (storageOptions.containsKey("allow_http")) {
            System.setProperty("AWS_ALLOW_HTTP", storageOptions.get("allow_http"));
        }
        
        LOG.debug("Configured remote storage environment variables");
    }

    /**
     * Get database path
     */
    private String getDatabasePath(String databaseName) {
        if (isRemoteStorage) {
            return warehouse + "/" + databaseName;
        }
        return Paths.get(warehouse, databaseName).toString();
    }

    /**
     * Get dataset path
     */
    private String getDatasetPath(ObjectPath tablePath) {
        if (isRemoteStorage) {
            return warehouse + "/" + tablePath.getDatabaseName() + "/" + tablePath.getObjectName();
        }
        return Paths.get(warehouse, tablePath.getDatabaseName(), tablePath.getObjectName()).toString();
    }

    /**
     * Get storage options for table configuration
     */
    private Map<String, String> getStorageOptionsForTable() {
        Map<String, String> options = new HashMap<>();
        
        // Convert storage options to table config format
        if (storageOptions.containsKey("aws_access_key_id")) {
            options.put("s3-access-key", storageOptions.get("aws_access_key_id"));
        }
        if (storageOptions.containsKey("aws_secret_access_key")) {
            options.put("s3-secret-key", storageOptions.get("aws_secret_access_key"));
        }
        if (storageOptions.containsKey("aws_region")) {
            options.put("s3-region", storageOptions.get("aws_region"));
        }
        if (storageOptions.containsKey("aws_endpoint")) {
            options.put("s3-endpoint", storageOptions.get("aws_endpoint"));
        }
        
        return options;
    }

    /**
     * Recursively delete directory
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    /**
     * Get warehouse path
     */
    public String getWarehouse() {
        return warehouse;
    }

    /**
     * Get storage configuration options
     */
    public Map<String, String> getStorageOptions() {
        return Collections.unmodifiableMap(storageOptions);
    }

    /**
     * Whether is remote storage
     */
    public boolean isRemoteStorage() {
        return isRemoteStorage;
    }
}
