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

package org.apache.flink.connector.lance.converter;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers DECIMAL, TIME and TIMESTAMP_LTZ, which had no Arrow mapping at all: {@code
 * flinkTypeToArrowField} rejected them outright, so such a table could not be created even though
 * {@code RowDataFieldAccessor} already encoded these types as primary keys.
 *
 * <p>Assertions run over a full write-then-read cycle rather than over the schema mapping alone,
 * because a type can map cleanly and still lose its value in the vector round-trip.
 */
class LanceTypeConverterNewTypesTest {

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    /** Writes {@code rows} through the converter and reads them back out. */
    private List<RowData> roundTrip(RowType rowType, List<RowData> rows) {
        RowDataConverter converter = new RowDataConverter(rowType);
        try (VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
            converter.toVectorSchemaRoot(rows, root);
            return converter.toRowDataList(root);
        }
    }

    private static RowType rowTypeOf(String name, LogicalType type) {
        return new RowType(Collections.singletonList(new RowType.RowField(name, type)));
    }

    // ------------------------------------------------------------------
    // schema mapping
    // ------------------------------------------------------------------

    @Test
    @DisplayName("DECIMAL maps to Arrow Decimal preserving precision and scale")
    void decimalMapsToArrowDecimal() {
        Field field =
                LanceTypeConverter.flinkTypeToArrowField("amount", new DecimalType(true, 10, 2));

        assertThat(field.getType()).isInstanceOf(ArrowType.Decimal.class);
        ArrowType.Decimal decimal = (ArrowType.Decimal) field.getType();
        assertThat(decimal.getPrecision()).isEqualTo(10);
        assertThat(decimal.getScale()).isEqualTo(2);
    }

    @Test
    @DisplayName("TIME picks the Arrow bit width its unit requires")
    void timeUsesUnitCompatibleBitWidth() {
        // Arrow rejects Time32 paired with MICROSECOND and Time64 paired with MILLISECOND, so the
        // width has to track the precision rather than being fixed.
        ArrowType milli = LanceTypeConverter.flinkTypeToArrowField("a", new TimeType(true, 3)).getType();
        ArrowType micro = LanceTypeConverter.flinkTypeToArrowField("b", new TimeType(true, 6)).getType();

        assertThat(((ArrowType.Time) milli).getBitWidth()).isEqualTo(32);
        assertThat(((ArrowType.Time) micro).getBitWidth()).isEqualTo(64);
    }

    @Test
    @DisplayName("TIMESTAMP_LTZ is tagged with a timezone, plain TIMESTAMP is not")
    void zonedAndUnzonedTimestampsStayDistinct() {
        ArrowType zoned =
                LanceTypeConverter.flinkTypeToArrowField("a", new LocalZonedTimestampType(true, 6))
                        .getType();
        ArrowType unzoned =
                LanceTypeConverter.flinkTypeToArrowField("b", new TimestampType(true, 6)).getType();

        assertThat(((ArrowType.Timestamp) zoned).getTimezone()).isEqualTo("UTC");
        assertThat(((ArrowType.Timestamp) unzoned).getTimezone())
                .as("an unzoned TIMESTAMP must not acquire a zone")
                .isNull();
    }

    @Test
    @DisplayName("Arrow zoned timestamp reads back as TIMESTAMP_LTZ, not TIMESTAMP")
    void zonedTimestampReadsBackAsLtz() {
        // Without the timezone check on the reverse mapping both Arrow forms would collapse into
        // TIMESTAMP and the round-trip would silently change the column's type.
        Schema schema =
                new Schema(
                        Arrays.asList(
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "zoned", new LocalZonedTimestampType(true, 6)),
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "plain", new TimestampType(true, 6))));

        RowType recovered = LanceTypeConverter.toFlinkRowType(schema);

        assertThat(recovered.getTypeAt(0)).isInstanceOf(LocalZonedTimestampType.class);
        assertThat(recovered.getTypeAt(1)).isInstanceOf(TimestampType.class);
    }

    // ------------------------------------------------------------------
    // value round-trip
    // ------------------------------------------------------------------

    @Test
    @DisplayName("DECIMAL values survive the vector round-trip")
    void decimalValueRoundTrips() {
        RowType rowType = rowTypeOf("amount", new DecimalType(true, 10, 2));
        DecimalData value = DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2);

        List<RowData> out = roundTrip(rowType, Collections.singletonList(GenericRowData.of(value)));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("123.45"));
    }

    @Test
    @DisplayName("Negative DECIMAL values keep their sign")
    void negativeDecimalRoundTrips() {
        RowType rowType = rowTypeOf("amount", new DecimalType(true, 10, 2));
        DecimalData value = DecimalData.fromBigDecimal(new BigDecimal("-7.89"), 10, 2);

        List<RowData> out = roundTrip(rowType, Collections.singletonList(GenericRowData.of(value)));

        assertThat(out.get(0).getDecimal(0, 10, 2).toBigDecimal())
                .isEqualByComparingTo(new BigDecimal("-7.89"));
    }

    @Test
    @DisplayName("TIME values survive the vector round-trip")
    void timeValueRoundTrips() {
        RowType rowType = rowTypeOf("at", new TimeType(true, 3));
        int millisOfDay = 45_296_123; // 12:34:56.123

        List<RowData> out =
                roundTrip(rowType, Collections.singletonList(GenericRowData.of(millisOfDay)));

        assertThat(out.get(0).getInt(0)).isEqualTo(millisOfDay);
    }

    @Test
    @DisplayName("TIMESTAMP_LTZ values survive the vector round-trip")
    void localZonedTimestampRoundTrips() {
        RowType rowType = rowTypeOf("ts", new LocalZonedTimestampType(true, 6));
        TimestampData value = TimestampData.fromEpochMillis(1_700_000_000_123L);

        List<RowData> out = roundTrip(rowType, Collections.singletonList(GenericRowData.of(value)));

        assertThat(out.get(0).getTimestamp(0, 6).getMillisecond())
                .isEqualTo(1_700_000_000_123L);
    }

    @Test
    @DisplayName("Pre-epoch TIMESTAMP_LTZ values are not corrupted by the micro split")
    void preEpochLocalZonedTimestampRoundTrips() {
        // Splitting epoch micros with % and / truncates towards zero, which yields a negative
        // nanosecond remainder for instants before 1970; floorDiv/floorMod are used instead.
        RowType rowType = rowTypeOf("ts", new LocalZonedTimestampType(true, 6));
        TimestampData value = TimestampData.fromEpochMillis(-1_000L);

        List<RowData> out = roundTrip(rowType, Collections.singletonList(GenericRowData.of(value)));

        assertThat(out.get(0).getTimestamp(0, 6).getMillisecond()).isEqualTo(-1_000L);
    }

    @Test
    @DisplayName("NULLs in the new types are preserved rather than written as zero")
    void nullsArePreserved() {
        // setNull has no trailing else, so a vector type missing from its dispatch chain leaves the
        // slot at its default value instead of marking it null.
        RowType rowType =
                new RowType(
                        Arrays.asList(
                                new RowType.RowField("amount", new DecimalType(true, 10, 2)),
                                new RowType.RowField("at", new TimeType(true, 3)),
                                new RowType.RowField("ts", new LocalZonedTimestampType(true, 6))));

        List<RowData> out =
                roundTrip(
                        rowType,
                        Collections.singletonList(GenericRowData.of(null, null, null)));

        assertThat(out.get(0).isNullAt(0)).as("DECIMAL null").isTrue();
        assertThat(out.get(0).isNullAt(1)).as("TIME null").isTrue();
        assertThat(out.get(0).isNullAt(2)).as("TIMESTAMP_LTZ null").isTrue();
    }
}
