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

import org.apache.flink.connector.lance.catalog.namespace.LanceNamespaceConfig;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;

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
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateTableRequest;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.singletonList;

/**
 * Catalog backed by a Lance namespace.
 *
 * <p>Uses {@link LanceNamespace} directly — no intermediate adapter layer.
 * The namespace implementation ({@code dir}, {@code rest}, etc.) is
 * selected through {@link LanceNamespaceConfig} and connected via
 * {@link LanceNamespace#connect(String, Map, BufferAllocator)}.
 *
 * <p>Usage via Flink SQL:
 * <pre>{@code
 * CREATE CATALOG my_lance WITH (
 *     'type' = 'lance-namespace',
 *     'impl' = 'dir',
 *     'root' = '/tmp/lance-warehouse'
 * );
 * USE CATALOG my_lance;
 * }</pre>
 */
public class LanceNamespaceCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(LanceNamespaceCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    private final LanceNamespace namespace;
    private final BufferAllocator allocator;
    private final Map<String, String> options;
    private boolean opened;
    private boolean closed;

    public LanceNamespaceCatalog(
            String name,
            String defaultDatabase,
            LanceNamespace namespace,
            BufferAllocator allocator,
            Map<String, String> options) {
        super(name, defaultDatabase);
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.options = new HashMap<>(Objects.requireNonNull(options, "options"));
    }

    @Override
    public void open() throws CatalogException {
        if (closed) {
            throw new CatalogException("Cannot open a closed Lance namespace catalog");
        }
        if (opened) {
            return;
        }

        try {
            // LanceNamespace.connect() already initializes the namespace;
            // no need to call namespace.initialize() again.
            createDatabaseIfMissing(getDefaultDatabase());
            opened = true;
        } catch (Exception e) {
            closed = true;
            closeQuietly(e);
            throw new CatalogException("Failed to open Lance namespace catalog", e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (closed) {
            return;
        }
        closed = true;
        opened = false;

        try {
            allocator.close();
        } catch (Exception e) {
            throw new CatalogException("Failed to close Arrow allocator", e);
        }
    }

    // ========== Database Operations ==========

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        ListNamespacesRequest request = new ListNamespacesRequest();
        String pageToken = null;
        try {
            do {
                if (pageToken != null) {
                    request.setPageToken(pageToken);
                }
                ListNamespacesResponse response = namespace.listNamespaces(request);
                databases.addAll(response.getNamespaces());
                pageToken = response.getPageToken();
            } while (pageToken != null && !pageToken.isEmpty());
        } catch (RuntimeException e) {
            throw new CatalogException("Failed to list databases", e);
        }
        return databases;
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
        if (databaseExists(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        }
        throw new DatabaseNotExistException(getName(), databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) {
        try {
            namespace.namespaceExists(new NamespaceExistsRequest().id(singletonList(databaseName)));
            return true;
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                return false;
            }
            throw new CatalogException("Failed to check if database exists: " + databaseName, e);
        }
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            namespace.createNamespace(new CreateNamespaceRequest().id(singletonList(name)));
        } catch (RuntimeException e) {
            if (isAlreadyExists(e)) {
                if (!ignoreIfExists) {
                    throw new DatabaseAlreadyExistException(getName(), name);
                }
                return;
            }
            throw new CatalogException("Failed to create database: " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        try {
            namespace.dropNamespace(new DropNamespaceRequest().id(singletonList(name)));
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                if (!ignoreIfNotExists) {
                    throw new DatabaseNotExistException(getName(), name);
                }
                return;
            }
            if (isNotEmpty(e)) {
                if (!cascade) {
                    throw new DatabaseNotEmptyException(getName(), name);
                }
                throw new CatalogException(
                        "Lance namespace catalog does not support dropping non-empty databases with CASCADE");
            }
            throw new CatalogException("Failed to drop database: " + name, e);
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
        throw new CatalogException("Lance namespace catalog does not support altering databases");
    }

    // ========== Table Operations ==========

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        List<String> tables = new ArrayList<>();
        ListTablesRequest request = new ListTablesRequest().id(singletonList(databaseName));
        String pageToken = null;
        try {
            do {
                if (pageToken != null) {
                    request.setPageToken(pageToken);
                }
                ListTablesResponse response = namespace.listTables(request);
                tables.addAll(response.getTables());
                pageToken = response.getPageToken();
            } while (pageToken != null && !pageToken.isEmpty());
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                throw new DatabaseNotExistException(getName(), databaseName);
            }
            throw new CatalogException("Failed to list tables in database: " + databaseName, e);
        }
        return tables;
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            namespace.tableExists(new TableExistsRequest().id(tableId(tablePath)));
            return true;
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                return false;
            }
            throw new CatalogException("Failed to check if table exists: " + tablePath, e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        DescribeTableResponse response;
        try {
            response = namespace.describeTable(new DescribeTableRequest().id(tableId(tablePath)));
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                throw new TableNotExistException(getName(), tablePath);
            }
            throw new CatalogException("Failed to get table: " + tablePath, e);
        }

        String location = response.getLocation();
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("connector", "lance");
        tableOptions.put("path", location);

        Map<String, String> props = response.getProperties();
        if (props != null) {
            tableOptions.putAll(props);
        }

        return CatalogTable.newBuilder()
                .schema(org.apache.flink.table.api.Schema.newBuilder().build())
                .comment("")
                .partitionKeys(Collections.emptyList())
                .options(tableOptions)
                .build();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        if (ignoreIfExists && tableExists(tablePath)) {
            return;
        }

        Map<String, String> tableOptions = table.getOptions();
        String location = tableOptions.get("path");
        if (location == null || location.isEmpty()) {
            throw new CatalogException(
                    "Table option 'path' is required when creating tables in Lance namespace catalog. "
                            + "Specify the Lance dataset path, e.g. WITH ('path' = '/data/my_dataset')");
        }

        // Serialized before the try below: that catch classifies errors coming back from the
        // namespace, and wrapping a local schema failure in "Failed to create table" would bury
        // the message that says what is actually wrong with the schema. The catch here keeps the
        // method's contract of only throwing CatalogException, since serialization can also fail
        // with a ValidationException (duplicate column names) or an IllegalStateException (an
        // Arrow buffer leak detected when the child allocator closes).
        byte[] arrowIpcSchema;
        try {
            arrowIpcSchema = toArrowIpcSchema(table, allocator);
        } catch (CatalogException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new CatalogException("Cannot build the Arrow schema for table " + tablePath, e);
        }

        try {
            namespace.createTable(
                    new CreateTableRequest().id(tableId(tablePath)), arrowIpcSchema);
        } catch (RuntimeException e) {
            if (isAlreadyExists(e)) {
                if (!ignoreIfExists) {
                    throw new TableAlreadyExistException(getName(), tablePath);
                }
                return;
            }
            throw new CatalogException("Failed to create table: " + tablePath, e);
        }
    }

    /**
     * Serialize a table's schema as an Arrow IPC stream.
     *
     * <p>{@code createTable} on the namespace API takes the request plus the table schema as an
     * Arrow IPC stream; a directory-backed namespace rejects an empty body with {@code
     * InvalidInputException code=13}, and a schema-only stream with {@code InternalException
     * code=18}. So the stream carries the schema plus one batch of zero rows: enough for the
     * namespace to derive the schema, while creating an empty dataset.
     *
     * @param table the table whose schema to serialize
     * @param allocator the allocator backing the Arrow vectors
     * @return the Arrow IPC stream bytes
     */
    static byte[] toArrowIpcSchema(CatalogBaseTable table, BufferAllocator allocator) {
        Schema arrowSchema;
        try {
            arrowSchema = LanceTypeConverter.toArrowSchema(resolveRowType(table));
        } catch (LanceTypeConverter.UnsupportedTypeException e) {
            throw new CatalogException(
                    "Cannot create a Lance table with this schema: " + e.getMessage(), e);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // The child allocator attributes any Arrow buffer leak to this method instead of to
        // close() at the end of the catalog's life. Declared first so it closes last.
        try (BufferAllocator ipcAllocator =
                        allocator.newChildAllocator("lance-ipc-schema", 0, Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, ipcAllocator);
                ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            root.setRowCount(0);
            writer.start();
            writer.writeBatch();
            writer.end();
        } catch (IOException e) {
            // Defensive: ByteArrayOutputStream never throws here. Two other failures do escape
            // this clause as RuntimeException, which is why the caller wraps them: a real stream
            // or channel would surface IO errors through ArrowWriter.close(), and ipcAllocator
            // .close() throws IllegalStateException on a leaked buffer.
            throw new CatalogException("Failed to serialize table schema as an Arrow IPC stream", e);
        }
        return out.toByteArray();
    }

    /**
     * Build a {@link RowType} from a table's physical columns.
     *
     * <p>A {@link ResolvedCatalogTable} carries a resolved schema, and {@code
     * toPhysicalRowDataType()} already drops computed and metadata columns, so that path is used
     * when available. Flink hands catalogs a resolved table on the SQL DDL path; a programmatic
     * caller may pass a plain {@link CatalogBaseTable}, which is why the unresolved fallback stays.
     *
     * @param table the table to read the schema from
     * @return the row type of the table's physical columns
     */
    static RowType resolveRowType(CatalogBaseTable table) {
        if (table instanceof ResolvedCatalogTable) {
            RowType rowType =
                    (RowType)
                            ((ResolvedCatalogTable) table)
                                    .getResolvedSchema()
                                    .toPhysicalRowDataType()
                                    .getLogicalType();
            if (rowType.getFieldCount() == 0) {
                throw new CatalogException(
                        "Cannot create a Lance table without physical columns: the schema declares none");
            }
            return rowType;
        }

        List<RowType.RowField> fields = new ArrayList<>();
        for (org.apache.flink.table.api.Schema.UnresolvedColumn column :
                table.getUnresolvedSchema().getColumns()) {
            if (!(column instanceof org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn)) {
                // Computed and metadata columns are not part of the stored dataset.
                continue;
            }
            org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn physical =
                    (org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn) column;
            // getDataType() is declared as AbstractDataType: a column declared with
            // Schema.column(name, "BIGINT") yields an UnresolvedDataType, which cannot be
            // resolved without a catalog's type factory.
            if (!(physical.getDataType() instanceof DataType)) {
                throw new CatalogException(
                        "Column '"
                                + physical.getName()
                                + "' has an unresolved data type. Declare it with DataTypes.* "
                                + "or create the table through SQL DDL.");
            }
            DataType dataType = (DataType) physical.getDataType();
            fields.add(new RowType.RowField(physical.getName(), dataType.getLogicalType()));
        }
        if (fields.isEmpty()) {
            throw new CatalogException(
                    "Cannot create a Lance table without physical columns: the schema declares none");
        }
        return new RowType(fields);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            namespace.dropTable(new DropTableRequest().id(tableId(tablePath)));
        } catch (RuntimeException e) {
            if (isNotFound(e)) {
                if (!ignoreIfNotExists) {
                    throw new TableNotExistException(getName(), tablePath);
                }
                return;
            }
            throw new CatalogException("Failed to drop table: " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        throw new CatalogException("Lance namespace catalog does not support renaming tables");
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        throw new CatalogException("Lance namespace catalog does not support altering tables");
    }

    // ========== Partition Operations (unsupported) ==========

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters) {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists) {
        throw new CatalogException("Lance namespace catalog does not support partitions");
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            boolean ignoreIfNotExists) {
        throw new CatalogException("Lance namespace catalog does not support partitions");
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists) {
        throw new CatalogException("Lance namespace catalog does not support partitions");
    }

    // ========== Function Operations (unsupported) ==========

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(dbName)) {
            throw new DatabaseNotExistException(getName(), dbName);
        }
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
        throw new CatalogException(
                "Lance namespace catalog does not support user-defined functions");
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
        throw new CatalogException(
                "Lance namespace catalog does not support user-defined functions");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
        throw new CatalogException(
                "Lance namespace catalog does not support user-defined functions");
    }

    // ========== Statistics Operations (unsupported) ==========

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath,
            CatalogTableStatistics tableStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Lance namespace catalog does not support updating statistics");
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Lance namespace catalog does not support updating statistics");
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Lance namespace catalog does not support updating statistics");
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws CatalogException {
        throw new CatalogException("Lance namespace catalog does not support updating statistics");
    }

    // ========== Helpers ==========

    private void createDatabaseIfMissing(String database) {
        if (databaseExists(database)) {
            return;
        }
        try {
            namespace.createNamespace(new CreateNamespaceRequest().id(singletonList(database)));
        } catch (RuntimeException e) {
            if (!isAlreadyExists(e)) {
                throw new CatalogException("Failed to create database: " + database, e);
            }
        }
    }

    private static List<String> tableId(ObjectPath tablePath) {
        return List.of(tablePath.getDatabaseName(), tablePath.getObjectName());
    }

    private void closeQuietly(Exception originalError) {
        try {
            allocator.close();
        } catch (Exception closeError) {
            originalError.addSuppressed(closeError);
        }
    }

    // ========== Error classification ==========

    private static boolean isNotFound(RuntimeException e) {
        return messageContains(e, "not found");
    }

    private static boolean isAlreadyExists(RuntimeException e) {
        return messageContains(e, "already exists");
    }

    private static boolean isNotEmpty(RuntimeException e) {
        return messageContains(e, "not empty");
    }

    private static boolean messageContains(Throwable t, String needle) {
        String lower = needle.toLowerCase(Locale.ROOT);
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            String msg = cur.getMessage();
            if (msg != null && msg.toLowerCase(Locale.ROOT).contains(lower)) {
                return true;
            }
            if (cur.getCause() == cur) {
                break;
            }
        }
        return false;
    }
}
