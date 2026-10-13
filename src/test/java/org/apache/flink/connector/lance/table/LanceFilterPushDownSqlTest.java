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
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end SQL filter push-down tests against a real Lance dataset.
 *
 * <p>The other push-down tests assert the filter string the source renders. Nothing asserted that
 * Lance's filter engine actually parses that string: {@code buildLogicalFilter} emits parenthesised
 * operands and {@code !=} rather than SQL-standard {@code <>}, and an unparseable filter only
 * surfaces when the dataset is opened at runtime. These tests run the query for real and compare
 * rows, so a rejected filter fails the build instead of production.
 *
 * <p>Runs in streaming mode on purpose. {@code getScanRuntimeProvider} returns a
 * {@link org.apache.flink.table.connector.source.DataStreamScanProvider} that calls
 * {@code addSource} with a legacy {@code RichParallelSourceFunction}, which yields a source
 * transformation marked unbounded even though the provider reports {@code isBounded() == true}, and
 * an explicit BATCH runtime mode rejects that combination. {@code LanceSource.run()} returns once
 * the fragments are drained, so the job still terminates.
 */
class LanceFilterPushDownSqlTest {

    @TempDir
    Path tempDir;

    private TableEnvironment tableEnv;

    @BeforeEach
    void setUp() throws Exception {
        tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());
        // Parallelism 1 keeps the sink deterministic: concurrent Fragment.create calls from
        // several subtasks would race, and a pushed-down filter makes only subtask 0 read.
        tableEnv.getConfig().set("parallelism.default", "1");

        tableEnv.executeSql(
                "CREATE TABLE lance_events ("
                        + "  id BIGINT,"
                        + "  status STRING,"
                        + "  label STRING"
                        + ") WITH ("
                        + "  'connector' = 'lance',"
                        + "  'path' = '" + tempDir.resolve("events") + "',"
                        + "  'write.mode' = 'overwrite'"
                        + ")");

        // Buffered rows below write.batch-size still land: LanceSink.close() calls flush().
        tableEnv.executeSql(
                "INSERT INTO lance_events VALUES"
                        + "  (1, 'active', 'alpha'),"
                        + "  (2, 'pending', 'beta'),"
                        + "  (3, 'completed', 'gamma'),"
                        + "  (4, 'archived', 'delta'),"
                        + "  (5, 'active', 'o''brien')")
                .await(60, TimeUnit.SECONDS);
    }

    @Test
    @DisplayName("String IN is pushed down and Lance accepts the rendered filter")
    void testStringInReturnsMatchingRows() {
        String sql = "SELECT id, status FROM lance_events WHERE status IN ('active', 'pending')";

        assertFilterReachesLance(sql);
        assertThat(query(sql)).containsExactly("+I[1, active]", "+I[2, pending]", "+I[5, active]");
    }

    @Test
    @DisplayName("Numeric IN is pushed down and Lance accepts the rendered filter")
    void testNumericInReturnsMatchingRows() {
        String sql = "SELECT id, status FROM lance_events WHERE id IN (1, 3)";

        assertFilterReachesLance(sql);
        assertThat(query(sql)).containsExactly("+I[1, active]", "+I[3, completed]");
    }

    @Test
    @DisplayName("NOT IN arrives as separate conjuncts and Lance accepts the rendered != operator")
    void testNotInReturnsMatchingRows() {
        // NOT IN expands to <> AND <>, and the planner splits top-level AND into separate
        // conjunctive terms, so applyFilters receives two NOT_EQUALS calls rather than one AND
        // call. That covers the "!=" spelling the source emits in place of SQL-standard "<>",
        // and buildFilterExpression joining several accepted filters with " AND ".
        String sql = "SELECT id, status FROM lance_events WHERE status NOT IN ('active', 'pending')";

        assertFilterReachesLance(sql);
        assertThat(query(sql)).containsExactly("+I[3, completed]", "+I[4, archived]");
    }

    @Test
    @DisplayName("Disjunctive terms keep their grouping when combined into one filter")
    void testNestedAndInsideOrReturnsMatchingRows() {
        // Regression test for buildFilterExpression combining accepted filters. The planner
        // rewrites this predicate into CNF - and(OR(status, status), OR(id, status)) - then hands
        // over the two conjunctive terms separately, each with a top-level OR. Joining them
        // without parentheses regroups the predicate under SQL precedence, dropping "id = 1" and
        // wrongly returning row 5.
        String sql = "SELECT id, status FROM lance_events "
                + "WHERE (status = 'active' AND id = 1) OR status = 'archived'";

        assertFilterReachesLance(sql);
        assertThat(query(sql)).containsExactly("+I[1, active]", "+I[4, archived]");
    }

    @Test
    @DisplayName("Literal with an apostrophe survives escaping into the Lance filter")
    void testLiteralWithApostropheIsEscaped() {
        // extractLiteralValue doubles the quote; Lance has to read it back as one character.
        String sql = "SELECT id, label FROM lance_events WHERE label = 'o''brien'";

        assertFilterReachesLance(sql);
        assertThat(query(sql)).containsExactly("+I[5, o'brien]");
    }

    @Test
    @DisplayName("IN reaches the source as an OR chain, with nothing left for Flink")
    void testInIsFullyPushedIntoTheScan() {
        String plan = tableEnv.explainSql("SELECT id, status FROM lance_events "
                + "WHERE status IN ('active', 'pending', 'completed')");

        // Calcite expands IN over a literal list into OR(=, =, ...) while converting SQL to
        // RelNode, so BuiltInFunctionDefinitions.IN never reaches applyFilters and the source
        // needs no IN branch. Operand order is normalised by RexSimplify, so match on the
        // shape rather than the sequence.
        assertThat(plan).contains("filter=[OR(");
        assertThat(plan).contains("=(status, _UTF-16LE'active'");
        assertThat(plan).contains("=(status, _UTF-16LE'pending'");
        assertThat(plan).contains("=(status, _UTF-16LE'completed'");
        // "where=[" would mean part of the predicate was left for Flink to evaluate.
        assertThat(plan).doesNotContain("where=[");
    }

    /**
     * Fail unless the whole predicate reached Lance. Without this the row assertions would still
     * pass if push-down regressed, because Flink would evaluate the predicate itself in a Calc
     * above the scan and return the same rows - green, but no longer testing Lance at all.
     */
    private void assertFilterReachesLance(String sql) {
        String plan = tableEnv.explainSql(sql);

        assertThat(plan)
                .as("predicate should be pushed into the scan, plan was:%n%s", plan)
                .contains("filter=[");
        // Match on "where=[" rather than the presence of a Calc: a Calc can legitimately sit above
        // the scan for projection or constant propagation even when the whole predicate was pushed.
        // Flink only renders "where=[" when it kept a predicate for itself.
        assertThat(plan)
                .as("Flink retained part of the predicate, plan was:%n%s", plan)
                .doesNotContain("where=[");
    }

    private List<String> query(String sql) {
        List<Row> collected = CollectionUtil.iteratorToList(tableEnv.executeSql(sql).collect());
        // Row order across fragments is not guaranteed, so compare on a stable ordering.
        return collected.stream().map(Row::toString).sorted().collect(Collectors.toList());
    }
}
