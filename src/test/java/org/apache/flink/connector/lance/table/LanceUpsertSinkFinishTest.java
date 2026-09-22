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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards durability at end-of-input for the keyed write path.
 *
 * <p>{@code LanceUpsertSink} treats the checkpoint as its persistence boundary and deliberately
 * does not flush from {@code close()}, since {@code close()} also runs on cancel and failover. A
 * batch job never checkpoints, so before {@code finish()} was implemented every buffered row of a
 * keyed write was discarded and the write completed reporting success while writing nothing.
 *
 * <p>The append path was unaffected because {@code LanceSink.close()} does flush, so the two sinks
 * disagreed on whether a completed bounded job was durable. These cases pin the keyed path.
 */
class LanceUpsertSinkFinishTest {

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

    private void createTable(String table, boolean withPrimaryKey) {
        tableEnv.executeSql(
                "CREATE TABLE "
                        + table
                        + " ("
                        + "  id BIGINT NOT NULL,"
                        + "  name STRING"
                        + (withPrimaryKey ? ", PRIMARY KEY (id) NOT ENFORCED" : "")
                        + ") WITH ("
                        + "  'connector' = 'lance',"
                        + "  'path' = '"
                        + tempDir.resolve(table + ".lance")
                        + "'"
                        + ")");
    }

    @Test
    @DisplayName("A keyed batch INSERT is durable once the job completes")
    void keyedBatchInsertIsDurable() throws Exception {
        // The row count here stays below write.batch-size, so nothing triggers a size-based flush
        // and durability rests entirely on end-of-input handling.
        createTable("t_finish_keyed", true);

        tableEnv.executeSql("INSERT INTO t_finish_keyed VALUES (1, 'a'), (2, 'b')").await();

        assertThat(query("SELECT id, name FROM t_finish_keyed"))
                .as("a completed batch job must not report success while writing nothing")
                .hasSize(2);
    }

    @Test
    @DisplayName("The append path is durable too, so both sinks agree")
    void appendBatchInsertIsDurable() throws Exception {
        createTable("t_finish_append", false);

        tableEnv.executeSql("INSERT INTO t_finish_append VALUES (1, 'a'), (2, 'b')").await();

        assertThat(query("SELECT id, name FROM t_finish_append")).hasSize(2);
    }

    @Test
    @DisplayName("Consecutive batch writes accumulate instead of overwriting")
    void consecutiveKeyedWritesAccumulate() throws Exception {
        createTable("t_finish_repeat", true);

        tableEnv.executeSql("INSERT INTO t_finish_repeat VALUES (1, 'a')").await();
        tableEnv.executeSql("INSERT INTO t_finish_repeat VALUES (2, 'b')").await();

        assertThat(query("SELECT id, name FROM t_finish_repeat"))
                .as("the second job must not lose the first job's row")
                .hasSize(2);
    }

    @Test
    @DisplayName("Re-inserting a key updates it rather than duplicating it")
    void reinsertingKeyUpserts() throws Exception {
        createTable("t_finish_upsert", true);

        tableEnv.executeSql("INSERT INTO t_finish_upsert VALUES (1, 'first')").await();
        tableEnv.executeSql("INSERT INTO t_finish_upsert VALUES (1, 'second')").await();

        List<Row> rows = query("SELECT id, name FROM t_finish_upsert");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(1)).isEqualTo("second");
    }
}
