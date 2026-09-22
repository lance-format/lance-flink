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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * MULTISET support, which is layered on the MAP encoding.
 *
 * <p>A multiset is physically {@code MAP<element, count>} and Flink hands it around as
 * {@link MapData} at runtime, so it reuses the map read/write path rather than getting its own.
 */
class LanceTypeConverterMultisetTest {

    private static LogicalType notNullString() {
        return new VarCharType(false, VarCharType.MAX_LENGTH);
    }

    private static MultisetType multisetOf(LogicalType element) {
        return new MultisetType(true, element);
    }

    @Test
    @DisplayName("A MULTISET becomes an Arrow map whose value side is a non-null INT count")
    void multisetBecomesArrowMapWithIntCount() {
        Field field =
                LanceTypeConverter.flinkTypeToArrowField("tags", multisetOf(notNullString()));

        assertThat(field.getType()).isInstanceOf(ArrowType.Map.class);
        Field entries = field.getChildren().get(0);
        assertThat(entries.getName()).isEqualTo("entries");
        assertThat(entries.isNullable()).isFalse();

        Field key = entries.getChildren().get(0);
        Field count = entries.getChildren().get(1);
        assertThat(key.getName()).isEqualTo(MapVector.KEY_NAME);
        assertThat(key.isNullable()).isFalse();

        // The count is an occurrence tally, never user data: always present, always an int.
        assertThat(count.getName()).isEqualTo(MapVector.VALUE_NAME);
        assertThat(count.getType()).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) count.getType()).getBitWidth()).isEqualTo(32);
        assertThat(count.isNullable())
                .as("a count cannot be absent for an element that is present")
                .isFalse();
    }

    @Test
    @DisplayName("A nullable MULTISET element is rejected, naming the column")
    void nullableElementIsRejected() {
        // The element becomes the Arrow map key, and Arrow keys cannot be nullable. This matters
        // because DataTypes.MULTISET(DataTypes.STRING()) produces exactly this.
        assertThatThrownBy(
                        () ->
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "tags",
                                        multisetOf(new VarCharType(true, VarCharType.MAX_LENGTH))))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                .hasMessageContaining("tags")
                .hasMessageContaining("MULTISET element")
                .hasMessageContaining("NOT NULL");
    }

    @Test
    @DisplayName("An element type the map path cannot move is rejected at DDL time")
    void unsupportedElementTypeIsRejected() {
        assertThatThrownBy(
                        () ->
                                LanceTypeConverter.flinkTypeToArrowField(
                                        "tags",
                                        multisetOf(
                                                new org.apache.flink.table.types.logical.DateType(
                                                        false))))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                // The message must say MULTISET element, not "MAP key" -- a user who wrote a
                // MULTISET has no key in their DDL to go looking for.
                .hasMessageContaining("MULTISET element")
                .hasMessageContaining("tags");
    }

    @Test
    @DisplayName("A MULTISET reads back as MAP, because the two are physically identical")
    void multisetReadsBackAsMap() {
        // Deliberate trade-off. An Arrow map carries nothing that distinguishes MAP<T NOT NULL, INT>
        // from MULTISET<T NOT NULL>, and MAP is by far the more common declaration, so an untagged
        // map resolves to MAP. Tagging the field with metadata would work -- Lance does round-trip
        // field metadata -- but it would add a Flink-specific key to the stored schema for the sake
        // of a rarely declared type, which is the kind of cross-engine coupling being removed
        // elsewhere.
        Field field =
                LanceTypeConverter.flinkTypeToArrowField("tags", multisetOf(notNullString()));

        LogicalType back = LanceTypeConverter.arrowTypeToFlinkType(field);

        assertThat(back).isInstanceOf(MapType.class);
        assertThat(back).isNotInstanceOf(MultisetType.class);
        MapType asMap = (MapType) back;
        assertThat(asMap.getKeyType()).isInstanceOf(VarCharType.class);
        assertThat(asMap.getValueType()).isInstanceOf(IntType.class);
    }

    @Test
    @DisplayName("toDataType exposes MULTISET with a NOT NULL element")
    void toDataTypeExposesMultiset() {
        assertThat(LanceTypeConverter.toDataType(multisetOf(notNullString())).toString())
                .startsWith("MULTISET<")
                .contains("NOT NULL");
    }

    @Test
    @DisplayName("Counts survive a round-trip through the converter")
    void countsRoundTrip() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(false), multisetOf(notNullString())},
                        new String[] {"id", "tags"});

        Map<Object, Object> counts = new HashMap<>();
        counts.put(StringData.fromString("a"), 2);
        counts.put(StringData.fromString("b"), 1);

        List<RowData> out =
                roundTrip(rowType, Arrays.asList(GenericRowData.of(1, new GenericMapData(counts))));

        MapData read = out.get(0).getMap(1);
        assertThat(read.size()).isEqualTo(2);

        Map<String, Integer> asJava = new HashMap<>();
        for (int i = 0; i < read.size(); i++) {
            asJava.put(read.keyArray().getString(i).toString(), read.valueArray().getInt(i));
        }
        assertThat(asJava).containsEntry("a", 2).containsEntry("b", 1);
    }

    @Test
    @DisplayName("An empty MULTISET stays distinct from a NULL one")
    void emptyAndNullAreDistinct() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(false), multisetOf(notNullString())},
                        new String[] {"id", "tags"});

        List<RowData> out =
                roundTrip(
                        rowType,
                        Arrays.asList(
                                GenericRowData.of(1, new GenericMapData(new HashMap<>())),
                                GenericRowData.of(2, null)));

        assertThat(out.get(0).isNullAt(1)).isFalse();
        assertThat(out.get(0).getMap(1).size()).isZero();
        assertThat(out.get(1).isNullAt(1)).isTrue();
    }

    @Test
    @DisplayName("A NULL element is rejected with MULTISET wording, not MAP wording")
    void nullElementIsRejectedWithMultisetWording() {
        RowType rowType =
                RowType.of(
                        new LogicalType[] {new IntType(false), multisetOf(notNullString())},
                        new String[] {"id", "tags"});

        Map<Object, Object> counts = new HashMap<>();
        counts.put(null, 1);

        assertThatThrownBy(
                        () ->
                                roundTrip(
                                        rowType,
                                        Arrays.asList(
                                                GenericRowData.of(1, new GenericMapData(counts)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MULTISET element");
    }

    private static List<RowData> roundTrip(RowType rowType, List<RowData> input) {
        RowDataConverter converter = new RowDataConverter(rowType);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
            converter.toVectorSchemaRoot(input, root);
            return new ArrayList<>(converter.toRowDataList(root));
        }
    }
}
