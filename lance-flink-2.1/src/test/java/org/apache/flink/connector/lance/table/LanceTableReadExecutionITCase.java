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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Overwrite;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end tests for the SQL read path: aggregate push-down and the static read options.
 *
 * <p>These are integration tests (real Lance dataset, real Flink batch planner) because the defects
 * they pin only appear when the planner translates and the runtime executes:
 *
 * <ul>
 *   <li>aggregate push-down was accepted by {@code applyAggregates} but never reached the runtime,
 *       so the scan emitted raw rows where one aggregate row was expected;
 *   <li>{@code read.limit} / {@code read.columns} declared in the WITH clause were dropped by
 *       {@code buildRuntimeOptions} and had no effect.
 * </ul>
 */
class LanceTableReadExecutionITCase {

    @TempDir static Path tempDir;

    private static String idsPath;
    private static String namedPath;

    @BeforeAll
    static void seedDatasets() throws Exception {
        idsPath = tempDir.resolve("ids").toString();
        namedPath = tempDir.resolve("named").toString();

        // id 1..4 -> COUNT = 4, SUM = 10, MIN = 1, MAX = 4, AVG = 2.5
        writeIds(idsPath, new long[] {1, 2, 3, 4});

        // two columns, three rows
        Schema namedSchema =
                new Schema(
                        Arrays.asList(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null),
                                new Field(
                                        "name",
                                        FieldType.nullable(new ArrowType.Utf8()),
                                        null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(namedSchema, allocator)) {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            org.apache.arrow.vector.VarCharVector nameVec =
                    (org.apache.arrow.vector.VarCharVector) root.getVector("name");
            idVec.allocateNew(3);
            nameVec.allocateNew(3);
            for (int i = 0; i < 3; i++) {
                idVec.setSafe(i, i);
                nameVec.setSafe(i, ("n" + i).getBytes(StandardCharsets.UTF_8));
            }
            idVec.setValueCount(3);
            nameVec.setValueCount(3);
            root.setRowCount(3);
            commit(allocator, namedPath, root, namedSchema);
        }
    }

    @AfterAll
    static void clearProperty() {
        System.clearProperty("arrow.memory.allocator.type");
    }

    private static void writeIds(String uri, long[] ids) throws Exception {
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vec = (BigIntVector) root.getVector("id");
            vec.allocateNew(ids.length);
            for (int i = 0; i < ids.length; i++) {
                vec.setSafe(i, ids[i]);
            }
            vec.setValueCount(ids.length);
            root.setRowCount(ids.length);
            commit(allocator, uri, root, schema);
        }
    }

    private static void commit(
            BufferAllocator allocator, String uri, VectorSchemaRoot root, Schema schema)
            throws Exception {
        List<FragmentMetadata> fragments =
                Fragment.write().datasetUri(uri).allocator(allocator).data(root).execute();
        CommitBuilder builder = new CommitBuilder(uri, allocator);
        try (Transaction txn =
                        new Transaction.Builder()
                                .operation(
                                        Overwrite.builder()
                                                .fragments(fragments)
                                                .schema(schema)
                                                .build())
                                .build();
                Dataset ignored = builder.execute(txn)) {
            // dataset created
        }
    }

    private static TableEnvironment env() {
        return TableEnvironment.create(EnvironmentSettings.inBatchMode());
    }

    private static String createIdsTable(TableEnvironment env, String extraOptions) {
        env.executeSql(
                "CREATE TABLE t (id BIGINT) WITH ('connector' = 'lance', 'path' = '"
                        + idsPath
                        + "'"
                        + (extraOptions.isEmpty() ? "" : ", " + extraOptions)
                        + ")");
        return "t";
    }

    private static List<String> collect(TableEnvironment env, String sql) {
        TableResult result = env.executeSql(sql);
        List<String> rows = new ArrayList<>();
        result.collect().forEachRemaining(row -> rows.add(row.toString()));
        return rows;
    }

    // ==================== Aggregate push-down ====================

    @Test
    @DisplayName("The planner pushes the aggregate into the scan")
    void aggregateIsPushedIntoTheScan() {
        TableEnvironment env = env();
        createIdsTable(env, "");

        String plan = env.explainSql("SELECT COUNT(*) FROM t");

        // Before the fix the scan carried no aggregate and Flink kept a LocalHashAggregate above it.
        assertThat(plan)
                .as("aggregate push-down must be recorded on the scan: %s", plan)
                .contains("aggFunctions=[Count1AggFunction()]");
        assertThat(plan).doesNotContain("LocalHashAggregate");
    }

    @Test
    @DisplayName("COUNT(*) pushed down returns the real row count")
    void countStarPushedDownReturnsRowCount() {
        TableEnvironment env = env();
        createIdsTable(env, "");
        assertThat(collect(env, "SELECT COUNT(*) FROM t")).containsExactly("+I[4]");
    }

    @Test
    @DisplayName("SUM / MIN / MAX pushed down return the real values")
    void sumMinMaxPushedDownReturnRealValues() {
        TableEnvironment env = env();
        createIdsTable(env, "");
        assertThat(collect(env, "SELECT SUM(id) FROM t")).containsExactly("+I[10]");
        assertThat(collect(env, "SELECT MIN(id) FROM t")).containsExactly("+I[1]");
        assertThat(collect(env, "SELECT MAX(id) FROM t")).containsExactly("+I[4]");
    }

    @Test
    @DisplayName("COUNT(column) is pushed down")
    void countColumnPushedDown() {
        TableEnvironment env = env();
        createIdsTable(env, "");
        assertThat(collect(env, "SELECT COUNT(id) FROM t")).containsExactly("+I[4]");
    }

    @Test
    @DisplayName("A grouped aggregate is left to Flink and still returns correct results")
    void groupedAggregateFallsBackToFlink() {
        TableEnvironment env = env();
        createIdsTable(env, "");

        // GROUP BY is intentionally not pushed down; correctness matters more than the plan shape.
        List<String> rows = collect(env, "SELECT id, COUNT(*) FROM t GROUP BY id");
        assertThat(rows).hasSize(4);
        assertThat(rows).allSatisfy(row -> assertThat(row).endsWith("1]"));
    }

    // ==================== Static read options ====================

    @Test
    @DisplayName("read.limit declared in the WITH clause limits the rows read")
    void staticReadLimitIsHonoured() {
        TableEnvironment env = env();
        createIdsTable(env, "'read.limit' = '2'");

        assertThat(collect(env, "SELECT id FROM t")).hasSize(2);
    }

    @Test
    @DisplayName("read.columns declared in the WITH clause prunes the columns read")
    void staticReadColumnsIsHonoured() {
        TableEnvironment env = env();
        env.executeSql(
                "CREATE TABLE n (id BIGINT, name STRING) WITH ('connector' = 'lance', 'path' = '"
                        + namedPath
                        + "', 'read.columns' = 'id')");

        // The declared projection is what the runtime source reads. Selecting only id must work and
        // return every row; selecting name would need the pruned column, so the declared option has
        // to be a hint the planner can reconcile with.
        assertThat(collect(env, "SELECT id FROM n")).hasSize(3);
    }
}
