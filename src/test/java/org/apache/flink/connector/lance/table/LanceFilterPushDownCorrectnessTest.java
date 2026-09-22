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
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correctness tests for {@code LanceDynamicTableSource}'s filter push-down.
 *
 * <h3>Why these are separate from {@code LanceReadOptimizationsTest}</h3>
 * <p>That suite asserts which filters are <em>accepted</em>. Acceptance alone is not correctness:
 * a filter that is accepted but rendered into a wrong predicate string is strictly worse than one
 * that is rejected, because rejection leaves Flink to evaluate it correctly, while a wrong
 * predicate silently changes the result set.
 *
 * <p>Push-down is the read-side mirror of the DELETE predicate that follow-up A4 removed from
 * {@code LanceUpsertSink}. The sink no longer builds SQL strings at all; the source still does,
 * so the same three hazards A4 catalogued apply here and are pinned below.
 */
class LanceFilterPushDownCorrectnessTest {

    private static final DataType PHYSICAL_TYPE = DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("status", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.DOUBLE()),
            DataTypes.FIELD("created_date", DataTypes.DATE()),
            DataTypes.FIELD("created_ts", DataTypes.TIMESTAMP(3)),
            DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)));

    private static LanceDynamicTableSource newSource() {
        LanceOptions options = LanceOptions.builder().path("/tmp/does-not-need-to-exist").build();
        return new LanceDynamicTableSource(options, PHYSICAL_TYPE);
    }

    private static String pushedPredicate(LanceDynamicTableSource source) {
        RowType rowType = (RowType) PHYSICAL_TYPE.getLogicalType();
        return source.buildRuntimeOptions(rowType).getReadFilter();
    }

    private static ResolvedExpression eq(String field, DataType fieldType, Object literal) {
        FieldReferenceExpression ref = new FieldReferenceExpression(field, fieldType, 0, 0);
        ValueLiteralExpression value = new ValueLiteralExpression(literal);
        return CallExpression.permanent(
                BuiltInFunctionDefinitions.EQUALS,
                Arrays.asList(ref, value),
                DataTypes.BOOLEAN());
    }

    /**
     * A column name is concatenated into the predicate without quoting. This is the read-side
     * twin of the injection surface A4 removed from the sink.
     */
    @Test
    @DisplayName("column name with a space is backtick-quoted")
    void columnNameWithSpaceIsQuoted() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("user name", DataTypes.STRING(), "alice");
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source)).isEqualTo("`user name` = 'alice'");
    }

    /** A dotted name is unaddressable in Lance, so it must not be pushed down at all. */
    @Test
    @DisplayName("column name containing a dot is declined, not pushed down")
    void dottedColumnNameIsDeclined() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("a.b", DataTypes.STRING(), "x");
        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).hasSize(1);
        assertThat(pushedPredicate(source)).isNull();
    }

    /** A backtick in the name would terminate the quoted identifier; decline instead. */
    @Test
    @DisplayName("column name containing a backtick is declined")
    void backtickColumnNameIsDeclined() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("we`ird", DataTypes.STRING(), "x");
        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(pushedPredicate(source)).isNull();
    }

    /**
     * {@code Double.NaN} is a {@link Number}, so the old literal branch rendered it via
     * {@code toString()} as the bare token {@code NaN}. A4 rejected exactly this value on the
     * write side rather than emit it.
     */
    @Test
    @DisplayName("NaN is declined rather than emitted as a bare token")
    void nonFiniteDoubleIsDeclined() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("score", DataTypes.DOUBLE(), Double.NaN);
        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).hasSize(1);
        assertThat(pushedPredicate(source)).isNull();
    }

    @Test
    @DisplayName("Infinity is declined rather than emitted as a bare token")
    void infiniteDoubleIsDeclined() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("score", DataTypes.DOUBLE(), Double.POSITIVE_INFINITY);
        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(pushedPredicate(source)).isNull();
    }

    /** A finite double must still push down unchanged. */
    @Test
    @DisplayName("finite double still pushes down")
    void finiteDoubleStillPushesDown() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("score", DataTypes.DOUBLE(), 60.5);
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source)).isEqualTo("`score` = 60.5");
    }

    /**
     * DATE / TIMESTAMP previously fell through to a catch-all that quoted {@code toString()},
     * comparing a Date32 column against a Utf8 literal.
     */
    @Test
    @DisplayName("DATE literal uses Lance's typed date syntax")
    void dateLiteralUsesTypedSyntax() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter =
                eq("created_date", DataTypes.DATE(), LocalDate.of(2026, 3, 14));
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source)).isEqualTo("`created_date` = date '2026-03-14'");
    }

    @Test
    @DisplayName("TIMESTAMP literal uses typed syntax with a space separator and precision")
    void timestampLiteralUsesTypedSyntax() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq(
                "created_ts",
                DataTypes.TIMESTAMP(3),
                LocalDateTime.of(2026, 3, 14, 15, 9, 26));
        source.applyFilters(Collections.singletonList(filter));

        String predicate = pushedPredicate(source);
        assertThat(predicate).isEqualTo("`created_ts` = timestamp(3) '2026-03-14 15:09:26'");
        assertThat(predicate).doesNotContain("T15:09:26");
    }

    @Test
    @DisplayName("TIMESTAMP literal with fractional seconds keeps them")
    void timestampLiteralKeepsFractionalSeconds() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq(
                "created_ts",
                DataTypes.TIMESTAMP(3),
                LocalDateTime.of(2026, 3, 14, 15, 9, 26, 123_000_000));
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source))
                .isEqualTo("`created_ts` = timestamp(3) '2026-03-14 15:09:26.123000000'");
    }

    /** DECIMAL must keep its exact scale; float round-tripping would lose it. */
    @Test
    @DisplayName("DECIMAL literal uses typed syntax and keeps trailing scale")
    void decimalLiteralIsExact() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter =
                eq("amount", DataTypes.DECIMAL(10, 2), new BigDecimal("1234.50"));
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source))
                .isEqualTo("`amount` = decimal(10,2) '1234.50'");
    }

    /** A string literal with an embedded quote must stay escaped. This already works; pin it. */
    @Test
    @DisplayName("string literal with an embedded single quote stays escaped")
    void stringLiteralQuoteIsEscaped() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("name", DataTypes.STRING(), "O'Brien");
        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source)).isEqualTo("`name` = 'O''Brien'");
    }

    /**
     * A literal crafted to terminate the quoted section must not be able to append predicate
     * syntax of its own.
     */
    @Test
    @DisplayName("string literal cannot inject trailing predicate syntax")
    void stringLiteralCannotInjectPredicate() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression filter = eq("name", DataTypes.STRING(), "x' OR '1'='1");
        source.applyFilters(Collections.singletonList(filter));

        String predicate = pushedPredicate(source);
        assertThat(predicate).isEqualTo("`name` = 'x'' OR ''1''=''1'");
    }

    /** IS NULL takes a separate code path whose identifier was also unquoted. */
    @Test
    @DisplayName("IS NULL quotes the column name too")
    void isNullQuotesIdentifier() {
        LanceDynamicTableSource source = newSource();

        FieldReferenceExpression ref =
                new FieldReferenceExpression("user name", DataTypes.STRING(), 0, 0);
        ResolvedExpression filter = CallExpression.permanent(
                BuiltInFunctionDefinitions.IS_NULL,
                Collections.singletonList(ref),
                DataTypes.BOOLEAN());

        source.applyFilters(Collections.singletonList(filter));

        assertThat(pushedPredicate(source)).isEqualTo("`user name` IS NULL");
    }

    /**
     * Partial push-down must never silently drop a conjunct: anything not accepted has to come
     * back in the remaining list so Flink still evaluates it.
     */
    @Test
    @DisplayName("an unconvertible conjunct is returned to Flink, not dropped")
    void unconvertibleFilterIsReturnedToFlink() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression convertible = eq("status", DataTypes.STRING(), "active");
        ResolvedExpression unconvertible = eq("score", DataTypes.DOUBLE(), Double.NaN);

        List<ResolvedExpression> filters = Arrays.asList(convertible, unconvertible);
        SupportsFilterPushDown.Result result = source.applyFilters(filters);

        int total = result.getAcceptedFilters().size() + result.getRemainingFilters().size();
        assertThat(total)
                .as("every input filter must be accounted for in exactly one output list")
                .isEqualTo(filters.size());
        assertThat(result.getRemainingFilters()).containsExactly(unconvertible);
    }

    /**
     * An OR whose branch cannot be converted must not be pushed down partially: dropping one
     * side of a disjunction widens the predicate and would silently lose rows.
     */
    @Test
    @DisplayName("an OR with one unconvertible branch is not pushed down at all")
    void orWithUnconvertibleBranchIsNotPartiallyPushed() {
        LanceDynamicTableSource source = newSource();

        ResolvedExpression ok = eq("status", DataTypes.STRING(), "active");
        ResolvedExpression bad = eq("score", DataTypes.DOUBLE(), Double.NaN);
        ResolvedExpression or = CallExpression.permanent(
                BuiltInFunctionDefinitions.OR,
                Arrays.asList(ok, bad),
                DataTypes.BOOLEAN());

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(or));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(pushedPredicate(source)).isNull();
    }
}
