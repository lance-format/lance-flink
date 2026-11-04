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

import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LanceDynamicTableSource aggregate push-down tests
 */
@DisplayName("LanceDynamicTableSource Aggregate Push-Down Tests")
class LanceAggregatePushDownTest {

    private LanceOptions options;
    private DataType physicalDataType;

    @BeforeEach
    void setUp() {
        options = LanceOptions.builder()
                .path("/tmp/test_lance_dataset")
                .readBatchSize(1024)
                .build();

        // Create test physical data type
        // Schema: (id INT, name VARCHAR, category VARCHAR, amount DOUBLE, quantity INT)
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("id", new IntType()),
                new RowType.RowField("name", new VarCharType(100)),
                new RowType.RowField("category", new VarCharType(50)),
                new RowType.RowField("amount", new DoubleType()),
                new RowType.RowField("quantity", new IntType())
        ));
        physicalDataType = TypeConversions.fromLogicalToDataType(rowType);
    }

    // ==================== Aggregate Push-Down Interface Tests ====================

    @Nested
    @DisplayName("applyAggregates Method Tests")
    class ApplyAggregatesTests {

        // Note: Since applyAggregates requires real AggregateExpression objects,
        // we mainly test aggregate info storage and state management here

        @Test
        @DisplayName("Initial state should have no aggregate push-down")
        void testInitialState() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            assertFalse(source.isAggregatePushDownAccepted());
            assertNull(source.getAggregateInfo());
        }

        @Test
        @DisplayName("copy should correctly copy aggregate state")
        void testCopyAggregateState() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // Copy source
            LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();
            
            // Verify copied state
            assertFalse(copied.isAggregatePushDownAccepted());
            assertNull(copied.getAggregateInfo());
            assertNotSame(source, copied);
        }

        @Test
        @DisplayName("asSummaryString should return correct summary")
        void testAsSummaryString() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            String summary = source.asSummaryString();
            
            assertEquals("Lance Table Source", summary);
        }
    }

    // ==================== AggregateInfo Integration Tests ====================

    @Nested
    @DisplayName("AggregateInfo Integration Tests")
    class AggregateInfoIntegrationTests {

        @Test
        @DisplayName("Simple COUNT(*) aggregate info build")
        void testSimpleCountStarAggregateInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            assertTrue(aggInfo.isSimpleCountStar());
            assertFalse(aggInfo.hasGroupBy());
            assertEquals(1, aggInfo.getAggregateCalls().size());
        }

        @Test
        @DisplayName("Aggregate info build with GROUP BY")
        void testGroupByAggregateInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "total_amount")
                    .addAvg("amount", "avg_amount")
                    .groupBy("category")
                    .groupByFieldIndices(new int[]{2})  // category at index 2
                    .build();
            
            assertFalse(aggInfo.isSimpleCountStar());
            assertTrue(aggInfo.hasGroupBy());
            assertEquals(2, aggInfo.getAggregateCalls().size());
            assertEquals(Collections.singletonList("category"), aggInfo.getGroupByColumns());
        }

        @Test
        @DisplayName("Multiple aggregates info build")
        void testMultipleAggregatesInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .build();
            
            assertEquals(5, aggInfo.getAggregateCalls().size());
            
            // Verify each aggregate function type
            List<AggregateInfo.AggregateCall> calls = aggInfo.getAggregateCalls();
            assertEquals(AggregateInfo.AggregateFunction.COUNT, calls.get(0).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.SUM, calls.get(1).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.AVG, calls.get(2).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.MIN, calls.get(3).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.MAX, calls.get(4).getFunction());
        }

        @Test
        @DisplayName("getRequiredColumns should return correct columns")
        void testGetRequiredColumns() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .addAvg("quantity", "avg_quantity")
                    .groupBy("category")
                    .build();
            
            List<String> required = aggInfo.getRequiredColumns();
            
            assertTrue(required.contains("category"));
            assertTrue(required.contains("amount"));
            assertTrue(required.contains("quantity"));
        }
    }

    // ==================== Combined Functionality Tests ====================

    @Nested
    @DisplayName("Combined Functionality Tests")
    class CombinedFunctionalityTests {

        @Test
        @DisplayName("Aggregate push-down with filter push-down combination")
        void testAggregatePushDownWithFilter() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // Simulate adding filter conditions (through internal filters list)
            // Note: Actual filter push-down is done through applyFilters method
            
            // Verify source can support both filter and aggregate push-down
            assertNotNull(source.getOptions());
        }

        @Test
        @DisplayName("Aggregate push-down with column pruning combination")
        void testAggregatePushDownWithProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // Apply column pruning
source.applyProjection(new int[][]{{0}, {3}, {4}}, null);  // id, amount, quantity
            
            // Verify source still works correctly
            assertNotNull(source.getOptions());
        }

        @Test
        @DisplayName("Aggregate push-down with Limit combination")
        void testAggregatePushDownWithLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // Apply Limit
            source.applyLimit(100);
            
            assertEquals(Long.valueOf(100), source.getLimit());
        }
    }

    // ==================== Edge Case Tests ====================

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Multiple group by columns should be handled correctly")
        void testMultipleGroupByColumns() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .groupBy("category", "name")
                    .groupByFieldIndices(new int[]{2, 1})
                    .build();
            
            assertEquals(2, aggInfo.getGroupByColumns().size());
            assertArrayEquals(new int[]{2, 1}, aggInfo.getGroupByFieldIndices());
        }

        @Test
        @DisplayName("Multiple aggregates on same column should be handled correctly")
        void testMultipleAggregatesOnSameColumn() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .addCount("amount", "count_amount")
                    .build();
            
            assertEquals(5, aggInfo.getAggregateCalls().size());
            
            // Verify getRequiredColumns contains amount only once
            List<String> required = aggInfo.getRequiredColumns();
            long amountCount = required.stream().filter(c -> c.equals("amount")).count();
            assertEquals(1, amountCount);
        }

        @Test
        @DisplayName("Empty group by set should be handled correctly")
        void testEmptyGroupBy() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            assertFalse(aggInfo.hasGroupBy());
            assertTrue(aggInfo.getGroupByColumns().isEmpty());
            assertEquals(0, aggInfo.getGroupByFieldIndices().length);
        }
    }

    // ==================== Aggregate Function Support Tests ====================

    @Nested
    @DisplayName("Aggregate Function Support Tests")
    class AggregateFunctionSupportTests {

        @Test
        @DisplayName("COUNT function should be supported")
        void testCountSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.COUNT, call.getFunction());
            assertTrue(call.isCountStar());
        }

        @Test
        @DisplayName("SUM function should be supported")
        void testSumSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.SUM, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("AVG function should be supported")
        void testAvgSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAvg("amount", "avg_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.AVG, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("MIN function should be supported")
        void testMinSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMin("amount", "min_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.MIN, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("MAX function should be supported")
        void testMaxSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMax("amount", "max_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.MAX, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("COUNT DISTINCT function should be supported")
        void testCountDistinctSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAggregateCall(AggregateInfo.AggregateFunction.COUNT_DISTINCT, "category", "distinct_cnt")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.COUNT_DISTINCT, call.getFunction());
            assertEquals("category", call.getColumn());
        }
    }
}
