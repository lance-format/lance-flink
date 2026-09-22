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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
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
 * Round-trips MAP columns through {@link RowDataConverter}.
 *
 * <p>The schema mapping alone is not enough to make MAP usable: the read and write dispatches both
 * had only a {@code ListVector} branch, and because {@code MapVector extends ListVector} a map
 * column would have fallen into it and been handled with list semantics.
 */
class RowDataConverterMapTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new IntType(false),
                        new MapType(
                                true,
                                new VarCharType(false, VarCharType.MAX_LENGTH),
                                new IntType(true))
                    },
                    new String[] {"id", "attrs"});

    private static MapData map(Object... kv) {
        Map<Object, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(StringData.fromString((String) kv[i]), kv[i + 1]);
        }
        return new GenericMapData(m);
    }

    private static List<RowData> roundTrip(List<RowData> input) {
        RowDataConverter converter = new RowDataConverter(ROW_TYPE);
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
            converter.toVectorSchemaRoot(input, root);
            return new ArrayList<>(converter.toRowDataList(root));
        }
    }

    @Test
    @DisplayName("A populated map survives the round-trip with its entries intact")
    void populatedMapRoundTrips() {
        List<RowData> out =
                roundTrip(
                        Arrays.asList(
                                GenericRowData.of(1, map("a", 10, "b", 20))));

        assertThat(out).hasSize(1);
        MapData read = out.get(0).getMap(1);
        assertThat(read.size()).isEqualTo(2);

        Map<String, Integer> asJava = new HashMap<>();
        for (int i = 0; i < read.size(); i++) {
            asJava.put(
                    read.keyArray().getString(i).toString(),
                    read.valueArray().isNullAt(i) ? null : read.valueArray().getInt(i));
        }
        assertThat(asJava).containsEntry("a", 10).containsEntry("b", 20);
    }

    @Test
    @DisplayName("An empty map stays empty and does not become NULL")
    void emptyMapStaysEmpty() {
        // Distinguishing the two matters: an entries struct left without its validity bit reads
        // back as NULL, which would look like data loss rather than a missing flag.
        List<RowData> out = roundTrip(Arrays.asList(GenericRowData.of(1, map())));

        assertThat(out.get(0).isNullAt(1)).isFalse();
        assertThat(out.get(0).getMap(1).size()).isZero();
    }

    @Test
    @DisplayName("A NULL map stays NULL")
    void nullMapStaysNull() {
        List<RowData> out = roundTrip(Arrays.asList(GenericRowData.of(1, null)));

        assertThat(out.get(0).isNullAt(1)).isTrue();
    }

    @Test
    @DisplayName("Rows keep their own maps when populated, empty and NULL are interleaved")
    void interleavedRowsKeepTheirOwnMaps() {
        // Offsets are per row, so a mistake there leaks one row's entries into the next -- which a
        // single-row test cannot see.
        List<RowData> out =
                roundTrip(
                        Arrays.asList(
                                GenericRowData.of(1, map("x", 1)),
                                GenericRowData.of(2, map()),
                                GenericRowData.of(3, null),
                                GenericRowData.of(4, map("y", 2, "z", 3))));

        assertThat(out).hasSize(4);
        assertThat(out.get(0).getMap(1).size()).isEqualTo(1);
        assertThat(out.get(0).getMap(1).keyArray().getString(0).toString()).isEqualTo("x");

        assertThat(out.get(1).isNullAt(1)).isFalse();
        assertThat(out.get(1).getMap(1).size()).isZero();

        assertThat(out.get(2).isNullAt(1)).isTrue();

        assertThat(out.get(3).getMap(1).size()).isEqualTo(2);
    }

    @Test
    @DisplayName("A NULL map value is preserved, only the key may not be NULL")
    void nullValueIsPreserved() {
        Map<Object, Object> m = new HashMap<>();
        m.put(StringData.fromString("k"), null);
        List<RowData> out =
                roundTrip(Arrays.asList(GenericRowData.of(1, new GenericMapData(m))));

        MapData read = out.get(0).getMap(1);
        assertThat(read.size()).isEqualTo(1);
        assertThat(read.valueArray().isNullAt(0)).isTrue();
    }

    @Test
    @DisplayName("A NULL key is rejected rather than written as a broken entry")
    void nullKeyIsRejected() {
        Map<Object, Object> m = new HashMap<>();
        m.put(null, 1);

        assertThatThrownBy(
                        () -> roundTrip(Arrays.asList(GenericRowData.of(1, new GenericMapData(m)))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MAP key must not be NULL");
    }
}
