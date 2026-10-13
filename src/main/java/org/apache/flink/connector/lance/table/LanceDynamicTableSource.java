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
import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lance dynamic table source.
 * 
 * <p>Implements ScanTableSource interface, supports column pruning and filter push-down.
 */
public class LanceDynamicTableSource implements ScanTableSource, 
        SupportsProjectionPushDown, SupportsFilterPushDown, SupportsLimitPushDown,
        SupportsAggregatePushDown {

    private final LanceOptions options;
    private final DataType physicalDataType;
    private int[] projectedFields;
    private List<String> filters;
    private Long limit;  // Limit push-down
    private AggregateInfo aggregateInfo;  // Aggregate push-down
    private boolean aggregatePushDownAccepted;  // Whether aggregate push-down is accepted

    public LanceDynamicTableSource(LanceOptions options, DataType physicalDataType) {
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.projectedFields = null;
        this.filters = new ArrayList<>();
        this.limit = null;
        this.aggregateInfo = null;
        this.aggregatePushDownAccepted = false;
    }

    private LanceDynamicTableSource(LanceDynamicTableSource source) {
        this.options = source.options;
        this.physicalDataType = source.physicalDataType;
        this.projectedFields = source.projectedFields;
        this.filters = new ArrayList<>(source.filters);
        this.limit = source.limit;
        this.aggregateInfo = source.aggregateInfo;
        this.aggregatePushDownAccepted = source.aggregatePushDownAccepted;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();

        // If column pruning applied, build new RowType
        RowType projectedRowType = rowType;
        if (projectedFields != null) {
            List<RowType.RowField> projectedFieldList = new ArrayList<>();
            for (int fieldIndex : projectedFields) {
                projectedFieldList.add(rowType.getFields().get(fieldIndex));
            }
            projectedRowType = new RowType(projectedFieldList);
        }

        LanceOptions finalOptions = buildRuntimeOptions(rowType);
        final RowType finalRowType = projectedRowType;

        // Use DataStreamScanProvider
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                LanceSource source = new LanceSource(finalOptions, finalRowType);
                return execEnv.addSource(source, "LanceSource");
            }

            @Override
            public boolean isBounded() {
                return true; // Lance dataset is bounded
            }
        };
    }

    /**
     * Build the {@link LanceOptions} that will actually be handed to the runtime source, applying
     * projection / limit / filter push-down on top of the original SQL-WITH options.
     *
     * <p>Exposed at package scope so unit tests can assert that push-down does not lose any
     * important options (e.g. time-travel {@code read.version} / {@code read.as-of-timestamp} —
     * see issue #5).
     */
    LanceOptions buildRuntimeOptions(RowType rowType) {
        LanceOptions.Builder optionsBuilder = LanceOptions.builder()
                .path(options.getPath())
                .readBatchSize(options.getReadBatchSize())
                .readFilter(buildFilterExpression());

        // 携带 hadoop.* 配置（如 tbdsfs.meta），避免投影/过滤下推重建 options 时丢失
        if (options.getHadoopConfig() != null && !options.getHadoopConfig().isEmpty()) {
            optionsBuilder.hadoopConfig(options.getHadoopConfig());
        }

        // Carry over time-travel options from the SQL WITH clause (issue #5).
        // Without this the readVersion / readAsOfTimestamp get dropped when the planner
        // rebuilds options during projection/filter push-down.
        if (options.getReadVersion() != null) {
            optionsBuilder.readVersion(options.getReadVersion());
        }
        if (options.getReadAsOfTimestamp() != null) {
            optionsBuilder.readAsOfTimestamp(options.getReadAsOfTimestamp());
        }

        // Set Limit (if any)
        if (limit != null) {
            optionsBuilder.readLimit(limit);
        }

        // Set columns to read
        if (projectedFields != null) {
            List<String> columnNames = Arrays.stream(projectedFields)
                    .mapToObj(i -> rowType.getFieldNames().get(i))
                    .collect(Collectors.toList());
            optionsBuilder.readColumns(columnNames);
        }

        return optionsBuilder.build();
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
        // Only support top-level field projection
        this.projectedFields = Arrays.stream(projectedFields)
                .mapToInt(arr -> arr[0])
                .toArray();
    }

    // ==================== SupportsFilterPushDown ====================

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // Convert Flink expressions to Lance filter conditions
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
     * Convert Flink expression to Lance filter condition.
     * Lance supports standard SQL filter syntax, e.g., column = 'value', column > 10
     */
    private String convertToLanceFilter(ResolvedExpression expression) {
        try {
            if (expression instanceof CallExpression) {
                CallExpression callExpr = (CallExpression) expression;
                return convertCallExpression(callExpr);
            }
            // Other expression types not supported for push-down
            return null;
        } catch (Exception e) {
            // Return null for unconvertible expressions, handled by Flink at upper layer
            return null;
        }
    }

    /**
     * Convert CallExpression to Lance filter string
     */
    private String convertCallExpression(CallExpression callExpr) {
        FunctionDefinition funcDef = callExpr.getFunctionDefinition();
        List<ResolvedExpression> args = callExpr.getResolvedChildren();

        // Comparison operators
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
        // Logical operators
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
        // IN (not supported yet, requires more complex handling)
        // BETWEEN (not supported yet)

        // Unsupported functions, return null
        return null;
    }

    /**
     * Build comparison filter expression
     */
    private String buildComparisonFilter(List<ResolvedExpression> args, String operator) {
        if (args.size() != 2) {
            return null;
        }

        ResolvedExpression left = args.get(0);
        ResolvedExpression right = args.get(1);

        // Extract field name and value
        String fieldName = null;
        String value = null;

        if (left instanceof FieldReferenceExpression) {
            fieldName = ((FieldReferenceExpression) left).getName();
            value = extractLiteralValue(right);
        } else if (right instanceof FieldReferenceExpression) {
            fieldName = ((FieldReferenceExpression) right).getName();
            value = extractLiteralValue(left);
            // For asymmetric operators, need to swap operator
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
     * Build logical filter expression
     */
    private String buildLogicalFilter(List<ResolvedExpression> args, String operator) {
        List<String> convertedArgs = new ArrayList<>();
        for (ResolvedExpression arg : args) {
            String converted = convertToLanceFilter(arg);
            if (converted == null) {
                return null; // If any sub-expression cannot be converted, don't push down entire expression
            }
            convertedArgs.add("(" + converted + ")");
        }
        return String.join(" " + operator + " ", convertedArgs);
    }

    /**
     * Extract literal value from ValueLiteralExpression
     */
    private String extractLiteralValue(ResolvedExpression expr) {
        if (expr instanceof ValueLiteralExpression) {
            ValueLiteralExpression literal = (ValueLiteralExpression) expr;
            Object value = literal.getValueAs(Object.class).orElse(null);
            
            if (value == null) {
                return "NULL";
            } else if (value instanceof String) {
                // Strings need single quotes and escape internal single quotes
                String strValue = (String) value;
                strValue = strValue.replace("'", "''");
                return "'" + strValue + "'";
            } else if (value instanceof Number) {
                return value.toString();
            } else if (value instanceof Boolean) {
                return value.toString().toUpperCase();
            } else {
                // Other types try to convert to string
                return "'" + value.toString().replace("'", "''") + "'";
            }
        }
        return null;
    }

    /**
     * Build filter expression
     */
    private String buildFilterExpression() {
        if (filters.isEmpty()) {
            return options.getReadFilter();
        }

        // Parenthesise each accepted filter before joining. The planner splits a conjunction into
        // separate terms, and a term can itself contain a top-level OR; since AND binds tighter
        // than OR, joining them bare regroups the predicate and silently returns wrong rows.
        String combinedFilter = filters.stream()
                .map(filter -> "(" + filter + ")")
                .collect(Collectors.joining(" AND "));
        String originalFilter = options.getReadFilter();

        if (originalFilter != null && !originalFilter.isEmpty()) {
            return "(" + originalFilter + ") AND (" + combinedFilter + ")";
        }

        return combinedFilter;
    }

    /**
     * Get configuration options
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * Get physical data type
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
     * Get Limit value
     */
    public Long getLimit() {
        return limit;
    }

    // ==================== SupportsAggregatePushDown ====================

    @Override
    public boolean applyAggregates(
            List<int[]> groupingSets,
            List<AggregateExpression> aggregateExpressions,
            DataType producedDataType) {
        
        // Currently only support simple single grouping set
        if (groupingSets.size() != 1) {
            return false;
        }

        int[] groupingSet = groupingSets.get(0);
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();

        try {
            AggregateInfo.Builder builder = AggregateInfo.builder();

            // Handle grouping columns
            List<String> groupByColumns = new ArrayList<>();
            for (int fieldIndex : groupingSet) {
                if (fieldIndex >= 0 && fieldIndex < fieldNames.size()) {
                    groupByColumns.add(fieldNames.get(fieldIndex));
                }
            }
            builder.groupBy(groupByColumns);
            builder.groupByFieldIndices(groupingSet);

            // Handle aggregate expressions
            int aggIndex = 0;
            for (AggregateExpression aggExpr : aggregateExpressions) {
                AggregateInfo.AggregateCall aggCall = convertAggregateExpression(aggExpr, fieldNames, aggIndex++);
                if (aggCall == null) {
                    // Unsupported aggregate function, reject push-down
                    return false;
                }
                builder.addAggregateCall(aggCall);
            }

            this.aggregateInfo = builder.build();
            this.aggregatePushDownAccepted = true;
            return true;

        } catch (Exception e) {
            // Conversion failed, reject push-down
            return false;
        }
    }

    /**
     * Convert Flink aggregate expression to internal aggregate call
     */
    private AggregateInfo.AggregateCall convertAggregateExpression(
            AggregateExpression aggExpr, 
            List<String> fieldNames,
            int aggIndex) {
        
        FunctionDefinition funcDef = aggExpr.getFunctionDefinition();
        List<FieldReferenceExpression> args = aggExpr.getArgs();
        String alias = "agg_" + aggIndex;

        // COUNT(*)
        if (funcDef == BuiltInFunctionDefinitions.COUNT) {
            if (args.isEmpty()) {
                // COUNT(*)
                return new AggregateInfo.AggregateCall(
                        AggregateInfo.AggregateFunction.COUNT, null, alias);
            } else {
                // COUNT(column)
                String columnName = args.get(0).getName();
                return new AggregateInfo.AggregateCall(
                        AggregateInfo.AggregateFunction.COUNT, columnName, alias);
            }
        }

        // SUM
        if (funcDef == BuiltInFunctionDefinitions.SUM || funcDef == BuiltInFunctionDefinitions.SUM0) {
            if (args.isEmpty()) {
                return null;
            }
            String columnName = args.get(0).getName();
            return new AggregateInfo.AggregateCall(
                    AggregateInfo.AggregateFunction.SUM, columnName, alias);
        }

        // AVG
        if (funcDef == BuiltInFunctionDefinitions.AVG) {
            if (args.isEmpty()) {
                return null;
            }
            String columnName = args.get(0).getName();
            return new AggregateInfo.AggregateCall(
                    AggregateInfo.AggregateFunction.AVG, columnName, alias);
        }

        // MIN
        if (funcDef == BuiltInFunctionDefinitions.MIN) {
            if (args.isEmpty()) {
                return null;
            }
            String columnName = args.get(0).getName();
            return new AggregateInfo.AggregateCall(
                    AggregateInfo.AggregateFunction.MIN, columnName, alias);
        }

        // MAX
        if (funcDef == BuiltInFunctionDefinitions.MAX) {
            if (args.isEmpty()) {
                return null;
            }
            String columnName = args.get(0).getName();
            return new AggregateInfo.AggregateCall(
                    AggregateInfo.AggregateFunction.MAX, columnName, alias);
        }

        // Unsupported aggregate function
        return null;
    }

    /**
     * Get aggregate info
     */
    public AggregateInfo getAggregateInfo() {
        return aggregateInfo;
    }

    /**
     * Whether aggregate push-down is accepted
     */
    public boolean isAggregatePushDownAccepted() {
        return aggregatePushDownAccepted;
    }
}
