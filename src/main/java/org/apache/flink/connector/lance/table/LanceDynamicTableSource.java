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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.lance.LanceInputFormat;
import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lance 动态表数据源。
 * 
 * <p>实现 ScanTableSource 接口，支持列裁剪、过滤下推和 Limit 下推。
 */
public class LanceDynamicTableSource implements ScanTableSource, 
        SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown {

    private final LanceOptions options;
    private final DataType physicalDataType;
    private int[] projectedFields;
    private List<String> filters;
    private Long limit;  // 新增：Limit 下推

    public LanceDynamicTableSource(LanceOptions options, DataType physicalDataType) {
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.projectedFields = null;
        this.filters = new ArrayList<>();
        this.limit = null;
    }

    private LanceDynamicTableSource(LanceDynamicTableSource source) {
        this.options = source.options;
        this.physicalDataType = source.physicalDataType;
        this.projectedFields = source.projectedFields;
        this.filters = new ArrayList<>(source.filters);
        this.limit = source.limit;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        
        // 如果有列裁剪，构建新的 RowType
        RowType projectedRowType = rowType;
        if (projectedFields != null) {
            List<RowType.RowField> projectedFieldList = new ArrayList<>();
            for (int fieldIndex : projectedFields) {
                projectedFieldList.add(rowType.getFields().get(fieldIndex));
            }
            projectedRowType = new RowType(projectedFieldList);
        }

        // 构建 LanceOptions（应用列裁剪和过滤条件）
        LanceOptions.Builder optionsBuilder = LanceOptions.builder()
                .path(options.getPath())
                .readBatchSize(options.getReadBatchSize())
                .readFilter(buildFilterExpression());

        // 设置 Limit（如果有）
        if (limit != null) {
            optionsBuilder.readLimit(limit);
        }

        // 设置要读取的列
        if (projectedFields != null) {
            List<String> columnNames = Arrays.stream(projectedFields)
                    .mapToObj(i -> rowType.getFieldNames().get(i))
                    .collect(Collectors.toList());
            optionsBuilder.readColumns(columnNames);
        }

        LanceOptions finalOptions = optionsBuilder.build();
        final RowType finalRowType = projectedRowType;

        // 使用 DataStreamScanProvider
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                LanceSource source = new LanceSource(finalOptions, finalRowType);
                return execEnv.addSource(source, "LanceSource");
            }

            @Override
            public boolean isBounded() {
                return true; // Lance 数据集是有界的
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new LanceDynamicTableSource(this);
    }

    @Override
    public String asSummaryString() {
        return "Lance Table Source";
    }

    // ==================== SupportsProjectionPushDown ====================

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // 仅支持顶层字段投影
        this.projectedFields = Arrays.stream(projectedFields)
                .mapToInt(arr -> arr[0])
                .toArray();
    }

    // ==================== SupportsFilterPushDown ====================

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // 将 Flink 表达式转换为 Lance 过滤条件
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            String lanceFilter = convertToLanceFilter(filter);
            if (lanceFilter != null) {
                this.filters.add(lanceFilter);
                acceptedFilters.add(filter);
            } else {
                remainingFilters.add(filter);
            }
        }

        return Result.of(acceptedFilters, remainingFilters);
    }

    /**
     * 将 Flink 表达式转换为 Lance 过滤条件
     * Lance 支持标准 SQL 过滤语法，如：column = 'value', column > 10
     */
    private String convertToLanceFilter(ResolvedExpression expression) {
        try {
            if (expression instanceof CallExpression) {
                CallExpression callExpr = (CallExpression) expression;
                return convertCallExpression(callExpr);
            }
            // 其他类型的表达式暂不支持下推
            return null;
        } catch (Exception e) {
            // 无法转换的表达式返回 null，由 Flink 在上层处理
            return null;
        }
    }

    /**
     * 转换 CallExpression 为 Lance 过滤字符串
     */
    private String convertCallExpression(CallExpression callExpr) {
        FunctionDefinition funcDef = callExpr.getFunctionDefinition();
        List<ResolvedExpression> args = callExpr.getResolvedChildren();

        // 比较运算符
        if (funcDef == BuiltInFunctionDefinitions.EQUALS) {
            return buildComparisonFilter(args, "=");
        } else if (funcDef == BuiltInFunctionDefinitions.NOT_EQUALS) {
            return buildComparisonFilter(args, "!=");
        } else if (funcDef == BuiltInFunctionDefinitions.GREATER_THAN) {
            return buildComparisonFilter(args, ">");
        } else if (funcDef == BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL) {
            return buildComparisonFilter(args, ">=");
        } else if (funcDef == BuiltInFunctionDefinitions.LESS_THAN) {
            return buildComparisonFilter(args, "<");
        } else if (funcDef == BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL) {
            return buildComparisonFilter(args, "<=");
        }
        // 逻辑运算符
        else if (funcDef == BuiltInFunctionDefinitions.AND) {
            return buildLogicalFilter(args, "AND");
        } else if (funcDef == BuiltInFunctionDefinitions.OR) {
            return buildLogicalFilter(args, "OR");
        } else if (funcDef == BuiltInFunctionDefinitions.NOT) {
            if (args.size() == 1) {
                String inner = convertToLanceFilter(args.get(0));
                if (inner != null) {
                    return "NOT (" + inner + ")";
                }
            }
        }
        // IS NULL / IS NOT NULL
        else if (funcDef == BuiltInFunctionDefinitions.IS_NULL) {
            if (args.size() == 1 && args.get(0) instanceof FieldReferenceExpression) {
                String fieldName = ((FieldReferenceExpression) args.get(0)).getName();
                return fieldName + " IS NULL";
            }
        } else if (funcDef == BuiltInFunctionDefinitions.IS_NOT_NULL) {
            if (args.size() == 1 && args.get(0) instanceof FieldReferenceExpression) {
                String fieldName = ((FieldReferenceExpression) args.get(0)).getName();
                return fieldName + " IS NOT NULL";
            }
        }
        // LIKE
        else if (funcDef == BuiltInFunctionDefinitions.LIKE) {
            return buildComparisonFilter(args, "LIKE");
        }
        // IN 谓词支持（新增）
        else if (funcDef == BuiltInFunctionDefinitions.IN) {
            return buildInFilter(args);
        }
        // BETWEEN 谓词支持 - 注意：Flink 通常将 BETWEEN 转换为 AND 连接的两个比较
        // 这里作为备用支持

        // 不支持的函数，返回 null
        return null;
    }

    /**
     * 构建 IN 过滤表达式（新增）
     */
    private String buildInFilter(List<ResolvedExpression> args) {
        if (args.isEmpty()) {
            return null;
        }
        
        ResolvedExpression field = args.get(0);
        if (!(field instanceof FieldReferenceExpression)) {
            return null;
        }
        
        String fieldName = ((FieldReferenceExpression) field).getName();
        List<String> values = new ArrayList<>();
        
        for (int i = 1; i < args.size(); i++) {
            String value = extractLiteralValue(args.get(i));
            if (value == null) {
                return null; // 如果有一个值无法提取，则不下推
            }
            values.add(value);
        }
        
        if (values.isEmpty()) {
            return null;
        }
        
        return fieldName + " IN (" + String.join(", ", values) + ")";
    }

    /**
     * 构建比较过滤表达式
     */
    private String buildComparisonFilter(List<ResolvedExpression> args, String operator) {
        if (args.size() != 2) {
            return null;
        }

        ResolvedExpression left = args.get(0);
        ResolvedExpression right = args.get(1);

        // 提取字段名和值
        String fieldName = null;
        String value = null;

        if (left instanceof FieldReferenceExpression) {
            fieldName = ((FieldReferenceExpression) left).getName();
            value = extractLiteralValue(right);
        } else if (right instanceof FieldReferenceExpression) {
            fieldName = ((FieldReferenceExpression) right).getName();
            value = extractLiteralValue(left);
            // 对于非对称运算符，需要交换操作符
            if (">".equals(operator)) operator = "<";
            else if ("<".equals(operator)) operator = ">";
            else if (">=".equals(operator)) operator = "<=";
            else if ("<=".equals(operator)) operator = ">=";
        }

        if (fieldName != null && value != null) {
            return fieldName + " " + operator + " " + value;
        }

        return null;
    }

    /**
     * 构建逻辑过滤表达式
     */
    private String buildLogicalFilter(List<ResolvedExpression> args, String operator) {
        List<String> convertedArgs = new ArrayList<>();
        for (ResolvedExpression arg : args) {
            String converted = convertToLanceFilter(arg);
            if (converted == null) {
                return null; // 如果任何一个子表达式无法转换，则整个表达式都不下推
            }
            convertedArgs.add("(" + converted + ")");
        }
        return String.join(" " + operator + " ", convertedArgs);
    }

    /**
     * 从 ValueLiteralExpression 提取字面值
     */
    private String extractLiteralValue(ResolvedExpression expr) {
        if (expr instanceof ValueLiteralExpression) {
            ValueLiteralExpression literal = (ValueLiteralExpression) expr;
            Object value = literal.getValueAs(Object.class).orElse(null);
            
            if (value == null) {
                return "NULL";
            } else if (value instanceof String) {
                // 字符串需要用单引号包裹，并转义内部的单引号
                String strValue = (String) value;
                strValue = strValue.replace("'", "''");
                return "'" + strValue + "'";
            } else if (value instanceof Number) {
                return value.toString();
            } else if (value instanceof Boolean) {
                return value.toString().toUpperCase();
            } else {
                // 其他类型尝试转为字符串
                return "'" + value.toString().replace("'", "''") + "'";
            }
        }
        return null;
    }

    /**
     * 构建过滤表达式
     */
    private String buildFilterExpression() {
        if (filters.isEmpty()) {
            return options.getReadFilter();
        }

        String combinedFilter = String.join(" AND ", filters);
        String originalFilter = options.getReadFilter();

        if (originalFilter != null && !originalFilter.isEmpty()) {
            return "(" + originalFilter + ") AND (" + combinedFilter + ")";
        }

        return combinedFilter;
    }

    /**
     * 获取配置选项
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * 获取物理数据类型
     */
    public DataType getPhysicalDataType() {
        return physicalDataType;
    }

    // ==================== SupportsLimitPushDown ====================

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    /**
     * 获取 Limit 值
     */
    public Long getLimit() {
        return limit;
    }
}
