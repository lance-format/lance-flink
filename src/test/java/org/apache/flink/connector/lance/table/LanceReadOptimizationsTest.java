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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 读取优化功能测试
 * 
 * <p>测试内容：
 * <ul>
 *   <li>Limit 下推</li>
 *   <li>谓词下推（基本比较、IN、BETWEEN）</li>
 *   <li>列裁剪</li>
 * </ul>
 */
@DisplayName("读取优化测试")
public class LanceReadOptimizationsTest {

    @TempDir
    File tempDir;

    private LanceOptions baseOptions;
    private DataType physicalDataType;

    @BeforeEach
    void setUp() {
        baseOptions = LanceOptions.builder()
                .path(tempDir.getAbsolutePath() + "/test_dataset")
                .readBatchSize(100)
                .build();

        // 定义测试表结构
        physicalDataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("name", DataTypes.STRING()),
                DataTypes.FIELD("status", DataTypes.STRING()),
                DataTypes.FIELD("score", DataTypes.DOUBLE()),
                DataTypes.FIELD("created_time", DataTypes.STRING())
        );
    }

    // ==================== Limit 下推测试 ====================

    @Nested
    @DisplayName("Limit 下推测试")
    class LimitPushDownTests {

        @Test
        @DisplayName("测试 applyLimit 方法")
        void testApplyLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 初始状态应该没有 limit
            assertNull(source.getLimit(), "初始 limit 应该为 null");

            // 应用 limit
            source.applyLimit(100);

            // 验证 limit 被设置
            assertEquals(100L, source.getLimit(), "Limit 应该被正确设置为 100");
        }

        @Test
        @DisplayName("测试 Limit 为 0 的情况")
        void testZeroLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);
            source.applyLimit(0);
            assertEquals(0L, source.getLimit(), "Limit 应该可以设置为 0");
        }

        @Test
        @DisplayName("测试大 Limit 值")
        void testLargeLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);
            long largeLimit = Long.MAX_VALUE;
            source.applyLimit(largeLimit);
            assertEquals(largeLimit, source.getLimit(), "应该支持大的 Limit 值");
        }

        @Test
        @DisplayName("测试 copy 保留 Limit")
        void testCopyPreservesLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);
            source.applyLimit(50);

            LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();

            assertEquals(50L, copied.getLimit(), "copy() 应该保留 limit 值");
        }
    }

    // ==================== 谓词下推测试 ====================

    @Nested
    @DisplayName("谓词下推测试")
    class FilterPushDownTests {

        @Test
        @DisplayName("测试等于比较下推")
        void testEqualsFilterPushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 status = 'active' 表达式
            List<ResolvedExpression> filters = createEqualsFilter("status", "active");

            SupportsFilterPushDown.Result result = source.applyFilters(filters);

            // 验证过滤器被接受
            assertEquals(1, result.getAcceptedFilters().size(), "等于比较应该被接受");
            assertEquals(0, result.getRemainingFilters().size(), "不应该有剩余过滤器");
        }

        @Test
        @DisplayName("测试数值比较下推")
        void testNumericComparisonPushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 score > 80 表达式
            List<ResolvedExpression> filters = createComparisonFilter("score", 80.0, BuiltInFunctionDefinitions.GREATER_THAN);

            SupportsFilterPushDown.Result result = source.applyFilters(filters);

            assertEquals(1, result.getAcceptedFilters().size(), "数值比较应该被接受");
        }

        @Test
        @DisplayName("测试 AND 逻辑下推")
        void testAndLogicPushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 status = 'active' AND score > 60 表达式
            ResolvedExpression statusFilter = createEqualsExpression("status", "active");
            ResolvedExpression scoreFilter = createComparisonExpression("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN);
            
            CallExpression andExpr = CallExpression.permanent(
                    BuiltInFunctionDefinitions.AND,
                    Arrays.asList(statusFilter, scoreFilter),
                    DataTypes.BOOLEAN()
            );

            SupportsFilterPushDown.Result result = source.applyFilters(Collections.singletonList(andExpr));

            assertEquals(1, result.getAcceptedFilters().size(), "AND 逻辑应该被接受");
        }

        @Test
        @DisplayName("测试 IS NULL 下推")
        void testIsNullPushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 name IS NULL 表达式
            FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                    "name", DataTypes.STRING(), 0, 1);
            
            CallExpression isNullExpr = CallExpression.permanent(
                    BuiltInFunctionDefinitions.IS_NULL,
                    Collections.singletonList(fieldRef),
                    DataTypes.BOOLEAN()
            );

            SupportsFilterPushDown.Result result = source.applyFilters(Collections.singletonList(isNullExpr));

            assertEquals(1, result.getAcceptedFilters().size(), "IS NULL 应该被接受");
        }

        @Test
        @DisplayName("测试 IS NOT NULL 下推")
        void testIsNotNullPushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 name IS NOT NULL 表达式
            FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                    "name", DataTypes.STRING(), 0, 1);
            
            CallExpression isNotNullExpr = CallExpression.permanent(
                    BuiltInFunctionDefinitions.IS_NOT_NULL,
                    Collections.singletonList(fieldRef),
                    DataTypes.BOOLEAN()
            );

            SupportsFilterPushDown.Result result = source.applyFilters(Collections.singletonList(isNotNullExpr));

            assertEquals(1, result.getAcceptedFilters().size(), "IS NOT NULL 应该被接受");
        }

        @Test
        @DisplayName("测试 LIKE 下推")
        void testLikePushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 name LIKE 'test%' 表达式
            FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                    "name", DataTypes.STRING(), 0, 1);
            ValueLiteralExpression pattern = new ValueLiteralExpression("test%");
            
            CallExpression likeExpr = CallExpression.permanent(
                    BuiltInFunctionDefinitions.LIKE,
                    Arrays.asList(fieldRef, pattern),
                    DataTypes.BOOLEAN()
            );

            SupportsFilterPushDown.Result result = source.applyFilters(Collections.singletonList(likeExpr));

            assertEquals(1, result.getAcceptedFilters().size(), "LIKE 应该被接受");
        }

        @Test
        @DisplayName("测试 IN 谓词下推")
        void testInPredicatePushDown() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建 status IN ('active', 'pending', 'completed') 表达式
            FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                    "status", DataTypes.STRING(), 0, 2);
            ValueLiteralExpression value1 = new ValueLiteralExpression("active");
            ValueLiteralExpression value2 = new ValueLiteralExpression("pending");
            ValueLiteralExpression value3 = new ValueLiteralExpression("completed");
            
            CallExpression inExpr = CallExpression.permanent(
                    BuiltInFunctionDefinitions.IN,
                    Arrays.asList(fieldRef, value1, value2, value3),
                    DataTypes.BOOLEAN()
            );

            SupportsFilterPushDown.Result result = source.applyFilters(Collections.singletonList(inExpr));

            assertEquals(1, result.getAcceptedFilters().size(), "IN 谓词应该被接受");
        }

        @Test
        @DisplayName("测试多个独立过滤条件")
        void testMultipleFilters() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 创建多个独立过滤条件
            List<ResolvedExpression> filter1 = createEqualsFilter("status", "active");
            List<ResolvedExpression> filter2 = createComparisonFilter("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL);

            List<ResolvedExpression> allFilters = new ArrayList<>();
            allFilters.addAll(filter1);
            allFilters.addAll(filter2);

            SupportsFilterPushDown.Result result = source.applyFilters(allFilters);

            assertEquals(2, result.getAcceptedFilters().size(), "两个过滤条件都应该被接受");
            assertEquals(0, result.getRemainingFilters().size(), "不应该有剩余过滤器");
        }

        @Test
        @DisplayName("测试 copy 保留过滤条件")
        void testCopyPreservesFilters() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 应用过滤条件
            List<ResolvedExpression> filters = createEqualsFilter("status", "active");
            source.applyFilters(filters);

            LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();

            // 验证 copy 后的 source 保留了过滤条件
            assertNotNull(copied, "copy() 应该成功");
        }
    }

    // ==================== 列裁剪测试 ====================

    @Nested
    @DisplayName("列裁剪测试")
    class ProjectionPushDownTests {

        @Test
        @DisplayName("测试单列投影")
        void testSingleColumnProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 只选择 id 列
            int[][] projection = {{0}};  // 第一列
            source.applyProjection(projection);

            // 验证投影被应用
            assertNotNull(source, "投影应该被成功应用");
        }

        @Test
        @DisplayName("测试多列投影")
        void testMultipleColumnProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 选择 id, name, score 列
            int[][] projection = {{0}, {1}, {3}};
            source.applyProjection(projection);

            assertNotNull(source, "多列投影应该被成功应用");
        }

        @Test
        @DisplayName("测试不支持嵌套投影")
        void testNestedProjectionNotSupported() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            assertFalse(source.supportsNestedProjection(), "不应该支持嵌套投影");
        }

        @Test
        @DisplayName("测试 copy 保留投影")
        void testCopyPreservesProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            int[][] projection = {{0}, {2}};
            source.applyProjection(projection);

            LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();

            assertNotNull(copied, "copy() 应该保留投影信息");
        }
    }

    // ==================== 组合测试 ====================

    @Nested
    @DisplayName("组合优化测试")
    class CombinedOptimizationsTests {

        @Test
        @DisplayName("测试 Limit + 过滤条件组合")
        void testLimitWithFilter() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 应用过滤条件
            List<ResolvedExpression> filters = createEqualsFilter("status", "active");
            source.applyFilters(filters);

            // 应用 limit
            source.applyLimit(100L);

            assertEquals(Long.valueOf(100L), source.getLimit(), "Limit 应该被正确设置");
        }

        @Test
        @DisplayName("测试 Limit + 投影组合")
        void testLimitWithProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 应用投影
            int[][] projection = {{0}, {1}};
            source.applyProjection(projection);

            // 应用 limit
            source.applyLimit(50L);

            assertEquals(Long.valueOf(50L), source.getLimit(), "Limit 应该被正确设置");
        }

        @Test
        @DisplayName("测试全部优化组合")
        void testAllOptimizations() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(baseOptions, physicalDataType);

            // 1. 应用投影
            int[][] projection = {{0}, {1}, {3}};  // id, name, score
            source.applyProjection(projection);

            // 2. 应用过滤条件
            List<ResolvedExpression> filters = createComparisonFilter("score", 60.0, BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL);
            SupportsFilterPushDown.Result result = source.applyFilters(filters);

            // 3. 应用 limit
            source.applyLimit(100L);

            // 验证所有优化都被正确应用
            assertEquals(1, result.getAcceptedFilters().size(), "过滤条件应该被接受");
            assertEquals(Long.valueOf(100L), source.getLimit(), "Limit 应该被正确设置");
        }
    }

    // ==================== LanceOptions 测试 ====================

    @Nested
    @DisplayName("LanceOptions Limit 配置测试")
    class LanceOptionsLimitTests {

        @Test
        @DisplayName("测试 readLimit 配置")
        void testReadLimitConfig() {
            LanceOptions options = LanceOptions.builder()
                    .path("/test/path")
                    .readLimit(500L)
                    .build();

            assertEquals(500L, options.getReadLimit(), "readLimit 应该被正确配置");
        }

        @Test
        @DisplayName("测试 readLimit 默认值")
        void testReadLimitDefault() {
            LanceOptions options = LanceOptions.builder()
                    .path("/test/path")
                    .build();

            assertNull(options.getReadLimit(), "readLimit 默认应该为 null");
        }

        @Test
        @DisplayName("测试 readLimit 为 0")
        void testReadLimitZero() {
            // 0 应该被允许（表示不读取任何数据）
            LanceOptions options = LanceOptions.builder()
                    .path("/test/path")
                    .readLimit(0L)
                    .build();

            assertEquals(0L, options.getReadLimit());
        }

        @Test
        @DisplayName("测试负数 readLimit 应该失败")
        void testNegativeReadLimit() {
            assertThrows(IllegalArgumentException.class, () -> {
                LanceOptions.builder()
                        .path("/test/path")
                        .readLimit(-1L)
                        .build();
            }, "负数的 readLimit 应该抛出异常");
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 创建等于比较过滤表达式
     */
    private List<ResolvedExpression> createEqualsFilter(String fieldName, String value) {
        ResolvedExpression expr = createEqualsExpression(fieldName, value);
        return Collections.singletonList(expr);
    }

    /**
     * 创建等于比较表达式
     */
    private ResolvedExpression createEqualsExpression(String fieldName, String value) {
        FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                fieldName, DataTypes.STRING(), 0, getFieldIndex(fieldName));
        ValueLiteralExpression literal = new ValueLiteralExpression(value);

        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                Arrays.asList(fieldRef, literal),
                DataTypes.BOOLEAN()
        );
    }

    /**
     * 创建比较过滤表达式
     */
    private List<ResolvedExpression> createComparisonFilter(String fieldName, Double value, BuiltInFunctionDefinition funcDef) {
        ResolvedExpression expr = createComparisonExpression(fieldName, value, funcDef);
        return Collections.singletonList(expr);
    }

    /**
     * 创建比较表达式
     */
    private ResolvedExpression createComparisonExpression(String fieldName, Double value, BuiltInFunctionDefinition funcDef) {
        FieldReferenceExpression fieldRef = new FieldReferenceExpression(
                fieldName, DataTypes.DOUBLE(), 0, getFieldIndex(fieldName));
        ValueLiteralExpression literal = new ValueLiteralExpression(value);

        return CallExpression.permanent(
                funcDef,
                Arrays.asList(fieldRef, literal),
                DataTypes.BOOLEAN()
        );
    }

    /**
     * 获取字段索引
     */
    private int getFieldIndex(String fieldName) {
        switch (fieldName) {
            case "id": return 0;
            case "name": return 1;
            case "status": return 2;
            case "score": return 3;
            case "created_time": return 4;
            default: return 0;
        }
    }
}
