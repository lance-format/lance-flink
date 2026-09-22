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

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Covers the MAP mapping in {@link LanceTypeConverter}. */
class LanceTypeConverterMapTest {

    private static MapType mapOf(LogicalType key, LogicalType value) {
        return new MapType(true, key, value);
    }

    private static LogicalType notNullString() {
        return new VarCharType(false, VarCharType.MAX_LENGTH);
    }

    @Test
    @DisplayName("A MAP column becomes an Arrow map with the entries/key/value shape Arrow requires")
    void mapBecomesArrowMapWithEntriesStruct() {
        Field field =
                LanceTypeConverter.flinkTypeToArrowField(
                        "attrs", mapOf(notNullString(), new IntType(true)));

        assertThat(field.getType()).isInstanceOf(ArrowType.Map.class);
        assertThat(field.getChildren()).hasSize(1);

        Field entries = field.getChildren().get(0);
        // The names are fixed by Arrow. MapVector.DATA_VECTOR_NAME is "entries", while the
        // inherited BaseRepeatedValueVector constant is "$data$" -- using the wrong one produces a
        // schema that fails much later.
        assertThat(entries.getName()).isEqualTo("entries");
        assertThat(entries.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(entries.isNullable())
                .as("the entries struct itself must be non-nullable")
                .isFalse();

        assertThat(entries.getChildren()).hasSize(2);
        Field key = entries.getChildren().get(0);
        Field value = entries.getChildren().get(1);
        assertThat(key.getName()).isEqualTo(MapVector.KEY_NAME);
        assertThat(value.getName()).isEqualTo(MapVector.VALUE_NAME);
        assertThat(key.isNullable()).as("Arrow map keys cannot be nullable").isFalse();
        assertThat(value.isNullable()).as("map values may be null").isTrue();
    }

    @Test
    @DisplayName("A nullable MAP key is rejected up front, naming the column")
    void nullableKeyIsRejected() {
        // Arrow forbids a nullable key. Widening it silently would push the failure down into the
        // encoder, where the message no longer says which column is at fault.
        assertThatThrownBy(
                        () ->
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "attrs",
                                        mapOf(
                                                new VarCharType(true, VarCharType.MAX_LENGTH),
                                                new IntType(true))))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                .hasMessageContaining("attrs")
                .hasMessageContaining("NOT NULL");
    }

    @Test
    @DisplayName("An Arrow map converts back to MAP rather than ARRAY<ROW<key, value>>")
    void arrowMapConvertsBackToMap() {
        // An Arrow map is physically a list of entry structs, so a List-first check in
        // arrowTypeToFlinkType would silently degrade the column to ARRAY<ROW<..>> and the schema
        // would stop round-tripping.
        Field field =
                LanceTypeConverter.flinkTypeToArrowField(
                        "attrs", mapOf(notNullString(), new IntType(true)));

        LogicalType back = LanceTypeConverter.arrowTypeToFlinkType(field);

        assertThat(back).isInstanceOf(MapType.class);
        assertThat(back).isNotInstanceOf(ArrayType.class);

        MapType mapType = (MapType) back;
        assertThat(mapType.getKeyType()).isInstanceOf(VarCharType.class);
        assertThat(mapType.getValueType()).isInstanceOf(IntType.class);
        assertThat(mapType.getKeyType().isNullable())
                .as("the non-null key must survive the round-trip, or the reverse conversion "
                        + "would produce a MAP the forward conversion then refuses")
                .isFalse();
    }

    @Test
    @DisplayName("A MAP nested inside a ROW round-trips")
    void mapNestedInRowRoundTrips() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(true), mapOf(notNullString(), new IntType(true))},
                        new String[] {"id", "attrs"});

        Field field = LanceTypeConverter.flinkTypeToArrowField("payload", rowType);
        LogicalType back = LanceTypeConverter.arrowTypeToFlinkType(field);

        assertThat(back).isInstanceOf(RowType.class);
        LogicalType nested = ((RowType) back).getTypeAt(1);
        assertThat(nested).isInstanceOf(MapType.class);
    }

    @Test
    @DisplayName("toDataType exposes MAP with a NOT NULL key")
    void toDataTypeExposesMap() {
        assertThat(LanceTypeConverter.toDataType(mapOf(notNullString(), new IntType(true))).toString())
                .startsWith("MAP<")
                .contains("NOT NULL");
    }

    @Test
    @DisplayName("A value type the map path cannot move is rejected at DDL time, not on first write")
    void unsupportedValueTypeIsRejectedAtDdlTime() {
        // Arrow itself is happy with Map<Utf8, Date>, so without this check CREATE TABLE would
        // succeed and the failure would only surface inside the converter on the first write --
        // the same trap the Lance 2.2 format requirement set for MAP in the first place.
        assertThatThrownBy(
                        () ->
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "attrs",
                                        mapOf(
                                                notNullString(),
                                                new org.apache.flink.table.types.logical.DateType(
                                                        true))))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                .hasMessageContaining("attrs")
                .hasMessageContaining("MAP value");
    }

    @Test
    @DisplayName("An unsupported key type is rejected the same way")
    void unsupportedKeyTypeIsRejected() {
        assertThatThrownBy(
                        () ->
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "attrs",
                                        mapOf(
                                                new org.apache.flink.table.types.logical.DateType(
                                                        false),
                                                new IntType(true))))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                .hasMessageContaining("MAP key");
    }
}
