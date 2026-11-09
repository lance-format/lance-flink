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

import org.apache.flink.connector.lance.LanceAggregateSource;
import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
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
import org.apache.flink.table.functions.AggregateFunctionDefinition;
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

        // Aggregate push-down was accepted: the runtime source must emit the aggregate result.
        // Without this branch the planner would still fold the aggregate into the scan, and the
        // raw rows would be returned where a single aggregate row is expected.
        if (aggregatePushDownAccepted && aggregateInfo != null) {
            final LanceOptions aggregateOptions = buildRuntimeOptions(rowType);
            final RowType sourceRowType = rowType;
            final AggregateInfo pushedInfo = aggregateInfo;
            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(
                        ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                    LanceAggregateSource source =
                            new LanceAggregateSource(aggregateOptions, sourceRowType, pushedInfo);
                    return LanceSourceStreamBridge.toStream(
                            execEnv, source, "LanceAggregateSource");
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }

        LanceOptions finalOptions = buildRuntimeOptions(rowType);
        final RowType finalRowType = projectedRowType;

        // Use DataStreamScanProvider. Implement Flink 2.x's new signature
        // produceDataStream(ProviderContext, env); Flink 1.20 still keeps the old signature as a
        // default method, so this implementation is compatible with both 1.20 and 2.1.
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                LanceSource source = new LanceSource(finalOptions, finalRowType);
                // Version bridge: Flink 1.x uses env.addSource(SourceFunction, name),
                // Flink 2.x uses env.fromSource(Source, WatermarkStrategy.noWatermarks(), name).
                // The concrete implementation is provided by the LanceSourceStreamBridge under each
                // Flink-version submodule.
                return LanceSourceStreamBridge.toStream(execEnv, source, "LanceSource");
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
     * <p>Planner push-down wins over the declared table option, but the option is no longer dropped
     * when the planner does not push anything: a static {@code read.limit} / {@code read.columns}
     * in the WITH clause used to be silently ignored because only the planner-provided values were
     * copied here.
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

        // Carry hadoop.* configs (such as tbdsfs.meta) so they are not lost when projection/filter
        // push-down rebuilds the options.
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

        // Limit: the planner-pushed value wins, otherwise fall back to the declared table option.
        if (limit != null) {
            optionsBuilder.readLimit(limit);
        } else if (options.getReadLimit() != null) {
            optionsBuilder.readLimit(options.getReadLimit());
        }

        // Columns: the planner-pushed projection wins, otherwise honour the declared option.
        if (projectedFields != null) {
            List<String> columnNames = Arrays.stream(projectedFields)
                    .mapToObj(i -> rowType.getFieldNames().get(i))
                    .collect(Collectors.toList());
            optionsBuilder.readColumns(columnNames);
        } else if (options.getReadColumns() != null && !options.getReadColumns().isEmpty()) {
            optionsBuilder.readColumns(options.getReadColumns());
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
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // Only support top-level field projection; producedDataType is passed in by the planner and
        // represents the post-projection output schema. This connector derives it later from
        // projectedFields, so it is not used directly.
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

        String combinedFilter = String.join(" AND ", filters);
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

        // Only global aggregates (no GROUP BY) are pushed down. A grouped aggregate would need the
        // group-key columns to line up with the projected scan output across the FLIP-27
        // serialization boundary; that layout is not stable yet, so grouped aggregates are left to
        // Flink. Rejecting push-down here is always safe — Flink computes the aggregate itself.
        if (groupingSets.size() != 1) {
            return false;
        }
        int[] groupingSet = groupingSets.get(0);
        if (groupingSet != null && groupingSet.length > 0) {
            return false;
        }

        RowType rowType = (RowType) physicalDataType.getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();

        try {
            AggregateInfo.Builder builder = AggregateInfo.builder();
            builder.groupBy(new ArrayList<>());
            builder.groupByFieldIndices(new int[0]);

            // Handle aggregate expressions
            int aggIndex = 0;
            for (AggregateExpression aggExpr : aggregateExpressions) {
                AggregateInfo.AggregateCall aggCall =
                        convertAggregateExpression(aggExpr, fieldNames, aggIndex++);
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
     * Convert a Flink aggregate expression to the internal aggregate call.
     *
     * <p>Flink 2.x hands {@code applyAggregates} an {@link AggregateFunctionDefinition} whose
     * aggregate function is a concrete planner class such as {@code LongSumAggFunction},
     * {@code Count1AggFunction} or {@code LongMaxAggFunction} — not one of the
     * {@code BuiltInFunctionDefinitions} constants. Matching those constants therefore never
     * succeeded, which silently disabled aggregate push-down. The mapping below is by simple class
     * name prefix so it keeps working across planner variants ({@code SumAggFunction},
     * {@code Sum0AggFunction}, {@code LongSumAggFunction}, ...).
     *
     * <p>Returns {@code null} for any shape that cannot be evaluated natively; the caller then
     * rejects push-down and Flink computes the aggregate itself.
     */
    private AggregateInfo.AggregateCall convertAggregateExpression(
            AggregateExpression aggExpr,
            List<String> fieldNames,
            int aggIndex) {

        List<FieldReferenceExpression> args = aggExpr.getArgs();
        String alias = "agg_" + aggIndex;
        String name = aggFunctionName(aggExpr.getFunctionDefinition());
        if (name == null) {
            return null;
        }

        boolean distinct = aggExpr.isDistinct();
        // COUNT(*) and COUNT(column) are the only aggregates whose result is a row count.
        if (name.startsWith("Count1") || name.startsWith("Count")) {
            // COUNT(*) carries no argument; COUNT(column) / COUNT(DISTINCT column) carry one.
            if (args.isEmpty() && !distinct) {
                return new AggregateInfo.AggregateCall(
                        AggregateInfo.AggregateFunction.COUNT, null, alias);
            }
            if (args.size() != 1) {
                return null;
            }
            String columnName = args.get(0).getName();
            AggregateInfo.AggregateFunction function =
                    distinct
                            ? AggregateInfo.AggregateFunction.COUNT_DISTINCT
                            : AggregateInfo.AggregateFunction.COUNT;
            return new AggregateInfo.AggregateCall(function, columnName, alias);
        }

        // SUM / SUM0 / AVG / MIN / MAX all require exactly one argument.
        if (args.size() != 1) {
            return null;
        }
        AggregateInfo.AggregateFunction function;
        if (name.startsWith("Sum") || name.startsWith("Sum0")) {
            function = AggregateInfo.AggregateFunction.SUM;
        } else if (name.startsWith("Avg")) {
            function = AggregateInfo.AggregateFunction.AVG;
        } else if (name.startsWith("Min")) {
            function = AggregateInfo.AggregateFunction.MIN;
        } else if (name.startsWith("Max")) {
            function = AggregateInfo.AggregateFunction.MAX;
        } else {
            return null;
        }
        // SUM(DISTINCT col) / AVG(DISTINCT col) are not implemented natively.
        if (distinct) {
            return null;
        }
        return new AggregateInfo.AggregateCall(function, args.get(0).getName(), alias);
    }

    /**
     * Extracts the aggregate function's simple name from the definition, or {@code null} when the
     * definition does not describe a user-level aggregate (e.g. a table aggregate).
     */
    private static String aggFunctionName(FunctionDefinition definition) {
        if (definition instanceof AggregateFunctionDefinition) {
            Object function = ((AggregateFunctionDefinition) definition).getAggregateFunction();
            if (function != null) {
                return function.getClass().getSimpleName();
            }
        }
        // Fall back to the definition's own name, which some planner paths set to the function
        // name instead of wrapping it in an AggregateFunctionDefinition.
        String name = definition.toString();
        return name == null || name.isEmpty() ? null : name;
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
