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

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage for {@code UPDATE}, served through {@link
 * org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate}.
 *
 * <p>Assertions read the data back rather than inspecting the returned {@code RowLevelUpdateInfo},
 * because the risk in this path is not whether the interface is wired up but whether the rewritten
 * statement preserves columns the user never mentioned.
 */
class LanceRowLevelUpdateTest {

    @TempDir Path tempDir;

    private TableEnvironment tableEnv;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    @BeforeEach
    void setUp() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        tableEnv = TableEnvironment.create(settings);
    }

    private String datasetPath(String name) {
        return tempDir.resolve(name + ".lance").toString();
    }

    /** Creates a keyed table and seeds it with three rows. */
    private void createSeededTable(String table) throws Exception {
        tableEnv.executeSql(
                "CREATE TABLE "
                        + table
                        + " ("
                        + "  id BIGINT NOT NULL,"
                        + "  name STRING,"
                        + "  score DOUBLE,"
                        + "  PRIMARY KEY (id) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'lance',"
                        + "  'path' = '"
                        + datasetPath(table)
                        + "'"
                        + ")");

        tableEnv.executeSql(
                        "INSERT INTO "
                                + table
                                + " VALUES "
                                + "(1, 'alice', 10.0), (2, 'bob', 20.0), (3, 'carol', 30.0)")
                .await();
    }

    private List<Row> query(String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        TableResult result = tableEnv.executeSql(sql);
        try (org.apache.flink.util.CloseableIterator<Row> it = result.collect()) {
            while (it.hasNext()) {
                rows.add(it.next());
            }
        }
        return rows;
    }

    private Row rowWithId(List<Row> rows, long id) {
        return rows.stream()
                .filter(r -> id == (Long) r.getField(0))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no row with id=" + id));
    }

    @Test
    @DisplayName("UPDATE changes only the rows matching WHERE")
    void updateAffectsOnlyMatchingRows() throws Exception {
        createSeededTable("t_update_basic");

        tableEnv.executeSql("UPDATE t_update_basic SET score = 99.0 WHERE id = 2").await();

        List<Row> rows = query("SELECT id, name, score FROM t_update_basic");
        assertThat(rows).as("UPDATE must not change the row count").hasSize(3);
        assertThat(rowWithId(rows, 2).getField(2)).isEqualTo(99.0);
        assertThat(rowWithId(rows, 1).getField(2)).isEqualTo(10.0);
        assertThat(rowWithId(rows, 3).getField(2)).isEqualTo(30.0);
    }

    @Test
    @DisplayName("Columns absent from SET keep their value instead of being nulled")
    void untouchedColumnsSurvive() throws Exception {
        // mergeInsert runs withMatchedUpdateAll, so it overwrites every column of a matched row.
        // If requiredColumns() narrowed the projection to the SET list plus the key, every other
        // column would arrive empty and be written as NULL.
        createSeededTable("t_update_projection");

        tableEnv.executeSql("UPDATE t_update_projection SET score = 55.0 WHERE id = 1").await();

        Row updated = rowWithId(query("SELECT id, name, score FROM t_update_projection"), 1);
        assertThat(updated.getField(1)).as("name was not in the SET list").isEqualTo("alice");
        assertThat(updated.getField(2)).isEqualTo(55.0);
    }

    @Test
    @DisplayName("UPDATE can set several columns at once")
    void multiColumnUpdate() throws Exception {
        createSeededTable("t_update_multi");

        tableEnv.executeSql(
                        "UPDATE t_update_multi SET name = 'robert', score = 21.5 WHERE id = 2")
                .await();

        Row updated = rowWithId(query("SELECT id, name, score FROM t_update_multi"), 2);
        assertThat(updated.getField(1)).isEqualTo("robert");
        assertThat(updated.getField(2)).isEqualTo(21.5);
    }

    @Test
    @DisplayName("An UPDATE matching several rows applies to all of them")
    void updateMatchingManyRows() throws Exception {
        createSeededTable("t_update_many");

        tableEnv.executeSql("UPDATE t_update_many SET score = 0.0 WHERE score >= 20.0").await();

        List<Row> rows = query("SELECT id, name, score FROM t_update_many");
        assertThat(rows).hasSize(3);
        assertThat(rowWithId(rows, 1).getField(2)).as("below the filter").isEqualTo(10.0);
        assertThat(rowWithId(rows, 2).getField(2)).isEqualTo(0.0);
        assertThat(rowWithId(rows, 3).getField(2)).isEqualTo(0.0);
    }

    @Test
    @DisplayName("An UPDATE matching nothing leaves the table unchanged")
    void updateMatchingNothingIsNoOp() throws Exception {
        createSeededTable("t_update_nomatch");

        tableEnv.executeSql("UPDATE t_update_nomatch SET score = 1.0 WHERE id = 999").await();

        List<Row> rows = query("SELECT id, name, score FROM t_update_nomatch");
        assertThat(rows).hasSize(3);
        assertThat(rowWithId(rows, 1).getField(2)).isEqualTo(10.0);
        assertThat(rowWithId(rows, 2).getField(2)).isEqualTo(20.0);
        assertThat(rowWithId(rows, 3).getField(2)).isEqualTo(30.0);
    }

    @Test
    @DisplayName("UPDATE can write NULL into a nullable column")
    void updateToNull() throws Exception {
        createSeededTable("t_update_null");

        tableEnv.executeSql("UPDATE t_update_null SET name = CAST(NULL AS STRING) WHERE id = 3")
                .await();

        Row updated = rowWithId(query("SELECT id, name, score FROM t_update_null"), 3);
        assertThat(updated.getField(1)).isNull();
        assertThat(updated.getField(2)).as("score must be untouched").isEqualTo(30.0);
    }

    @Test
    @DisplayName("UPDATE on a table without a primary key is rejected with a usable message")
    void updateWithoutPrimaryKeyIsRejected() {
        // There is nothing for mergeInsert to match on, so this has to fail during planning rather
        // than silently append or fail deep inside the sink at runtime.
        tableEnv.executeSql(
                "CREATE TABLE t_update_nokey ("
                        + "  id BIGINT NOT NULL,"
                        + "  name STRING"
                        + ") WITH ("
                        + "  'connector' = 'lance',"
                        + "  'path' = '"
                        + datasetPath("t_update_nokey")
                        + "'"
                        + ")");

        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql(
                                        "UPDATE t_update_nokey SET name = 'x' WHERE id = 1"))
                .hasStackTraceContaining("PRIMARY KEY");
    }
}
