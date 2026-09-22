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
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the A6 rewrite: {@code ALTER TABLE} is driven by the planner's explicit
 * {@link TableChange} list rather than by inferring intent from a schema diff.
 *
 * <p>The decisive case is {@link #dropThenAddSameNameIsNotTreatedAsRename}. Under the diff-based
 * path a {@code DROP COLUMN a} plus an {@code ADD COLUMN a} of a different type at the same
 * position is indistinguishable from a rename, and the heuristic refuses the statement outright.
 * With explicit changes the two operations are simply applied in order.
 */
class LanceCatalogTableChangeITCase {

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
        if (catalog != null) {
            catalog.close();
        }
    }

    private static CatalogTable table(Schema schema, Map<String, String> options) {
        return CatalogTable.of(schema, null, Collections.emptyList(), options);
    }

    private static Schema baseSchema() {
        return Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .build();
    }

    private ObjectPath createBaseTable(String name) throws Exception {
        ObjectPath path = new ObjectPath("default", name);
        catalog.createTable(path, table(baseSchema(), Collections.emptyMap()), false);
        return path;
    }

    private List<String> columnNames(ObjectPath path) throws Exception {
        CatalogBaseTable stored = catalog.getTable(path);
        return stored.getUnresolvedSchema().getColumns().stream()
                .map(Schema.UnresolvedColumn::getName)
                .collect(Collectors.toList());
    }

    // ------------------------------------------------------------------
    // the case the heuristic could not handle
    // ------------------------------------------------------------------

    /**
     * Acceptance criterion from the issue: a drop followed by an add of the same name with a
     * different type must actually drop, not be reinterpreted as a rename-with-type-change.
     */
    @Test
    @DisplayName("DROP then ADD of the same column name is applied literally, not as a rename")
    void dropThenAddSameNameIsNotTreatedAsRename() throws Exception {
        ObjectPath path = createBaseTable("drop_add");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.STRING())
                .build();

        List<TableChange> changes = Arrays.asList(
                TableChange.dropColumn("score"),
                TableChange.add(Column.physical("score", DataTypes.STRING())));

        catalog.alterTable(path, table(altered, Collections.emptyMap()), changes, false);

        List<String> names = columnNames(path);
        assertThat(names).containsExactly("id", "name", "score");

        CatalogBaseTable stored = catalog.getTable(path);
        Schema.UnresolvedPhysicalColumn score = stored.getUnresolvedSchema().getColumns().stream()
                .filter(c -> c.getName().equals("score"))
                .map(c -> (Schema.UnresolvedPhysicalColumn) c)
                .findFirst()
                .orElseThrow(() -> new AssertionError("score column missing"));

        assertThat(score.getDataType().toString())
                .as("the column must carry the newly added type, proving the drop really happened")
                .contains("STRING");
    }

    // ------------------------------------------------------------------
    // individual change kinds
    // ------------------------------------------------------------------

    @Test
    @DisplayName("ADD COLUMN via explicit change")
    void addColumn() throws Exception {
        ObjectPath path = createBaseTable("add_col");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .column("tag", DataTypes.STRING())
                .build();

        catalog.alterTable(path, table(altered, Collections.emptyMap()),
                Collections.singletonList(
                        TableChange.add(Column.physical("tag", DataTypes.STRING()))),
                false);

        assertThat(columnNames(path)).containsExactly("id", "name", "score", "tag");
    }

    @Test
    @DisplayName("DROP COLUMN via explicit change")
    void dropColumn() throws Exception {
        ObjectPath path = createBaseTable("drop_col");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .build();

        catalog.alterTable(path, table(altered, Collections.emptyMap()),
                Collections.singletonList(TableChange.dropColumn("score")), false);

        assertThat(columnNames(path)).containsExactly("id", "name");
    }

    /**
     * The rename arrives as an explicit {@code ModifyColumnName}, so no position matching is
     * involved and the operation cannot be confused with a drop+add.
     */
    @Test
    @DisplayName("RENAME COLUMN via explicit change does not go through SchemaDiff")
    void renameColumn() throws Exception {
        ObjectPath path = createBaseTable("rename_col");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("full_name", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .build();

        catalog.alterTable(path, table(altered, Collections.emptyMap()),
                Collections.singletonList(TableChange.modifyColumnName(
                        Column.physical("name", DataTypes.STRING()), "full_name")),
                false);

        assertThat(columnNames(path)).containsExactly("id", "full_name", "score");
    }

    @Test
    @DisplayName("ALTER COLUMN type change is rejected with a clear reason")
    void modifyColumnTypeIsRejected() throws Exception {
        ObjectPath path = createBaseTable("modify_type");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.STRING())
                .build();

        assertThatThrownBy(() -> catalog.alterTable(
                path, table(altered, Collections.emptyMap()),
                Collections.singletonList(TableChange.modifyPhysicalColumnType(
                        Column.physical("score", DataTypes.DOUBLE()), DataTypes.STRING())),
                false))
                .isInstanceOf(CatalogException.class)
                .hasMessageContaining("castTo");

        assertThat(columnNames(path))
                .as("a rejected ALTER must leave the schema untouched")
                .containsExactly("id", "name", "score");
    }

    // ------------------------------------------------------------------
    // option changes, including the cross-engine guarantee
    // ------------------------------------------------------------------

    /** Dataset path as laid out by the catalog: warehouse/database/table, with no suffix. */
    private String datasetPathOf(String tableName) {
        return tempDir.resolve("warehouse").resolve("default").resolve(tableName).toString();
    }

    private Map<String, String> datasetConfig(String tableName) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             org.lance.Dataset dataset = org.lance.Dataset.open(
                     datasetPathOf(tableName), allocator)) {
            return new HashMap<>(dataset.getConfig());
        }
    }

    /**
     * Asserts against the Lance dataset config directly rather than {@code getTable}, because the
     * catalog reconstructs a table's options from connector configuration and does not echo user
     * TBLPROPERTIES back. The dataset config is where SET/RESET actually takes effect.
     */
    @Test
    @DisplayName("SET and RESET TBLPROPERTIES via explicit changes")
    void setAndResetOptions() throws Exception {
        ObjectPath path = createBaseTable("options");

        Map<String, String> withOwner = new HashMap<>();
        withOwner.put("owner", "data-team");
        catalog.alterTable(path, table(baseSchema(), withOwner),
                Collections.singletonList(TableChange.set("owner", "data-team")), false);

        assertThat(datasetConfig("options")).containsEntry("owner", "data-team");

        catalog.alterTable(path, table(baseSchema(), Collections.emptyMap()),
                Collections.singletonList(TableChange.reset("owner")), false);

        assertThat(datasetConfig("options")).doesNotContainKey("owner");
    }

    /**
     * Acceptance criterion from the A7 issue, exercised through the explicit-change path: a
     * foreign engine's metadata must survive a Flink RESET of an unrelated key.
     */
    @Test
    @DisplayName("RESET does not delete another engine's dataset config")
    void resetLeavesForeignConfigIntact() throws Exception {
        ObjectPath path = createBaseTable("cross_engine");

        Map<String, String> options = new HashMap<>();
        options.put("owner", "data-team");
        catalog.alterTable(path, table(baseSchema(), options),
                Collections.singletonList(TableChange.set("owner", "data-team")), false);

        // Simulate a sibling engine writing its own metadata onto the same dataset.
        String datasetPath = datasetPathOf("cross_engine");
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             org.lance.Dataset dataset = org.lance.Dataset.open(datasetPath, allocator)) {
            dataset.updateConfig(Collections.singletonMap("spark.stats.approx_count", "42"));
        }

        catalog.alterTable(path, table(baseSchema(), Collections.emptyMap()),
                Collections.singletonList(TableChange.reset("owner")), false);

        Map<String, String> config = datasetConfig("cross_engine");
        assertThat(config)
                .as("a Flink RESET must never remove another engine's config")
                .containsEntry("spark.stats.approx_count", "42");
        assertThat(config)
                .as("the Flink-owned key must actually be removed")
                .doesNotContainKey("owner");
    }

    @Test
    @DisplayName("an empty change list falls back to the diff-based path")
    void emptyChangeListFallsBack() throws Exception {
        ObjectPath path = createBaseTable("fallback");

        Schema altered = Schema.newBuilder()
                .column("id", DataTypes.BIGINT().notNull())
                .column("name", DataTypes.STRING())
                .column("score", DataTypes.DOUBLE())
                .column("extra", DataTypes.INT())
                .build();

        catalog.alterTable(path, table(altered, Collections.emptyMap()),
                Collections.emptyList(), false);

        assertThat(columnNames(path)).containsExactly("id", "name", "score", "extra");
    }
}
