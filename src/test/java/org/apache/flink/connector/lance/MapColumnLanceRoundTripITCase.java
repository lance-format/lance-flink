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

package org.apache.flink.connector.lance;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
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
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end MAP support: Flink rows through {@link RowDataConverter} into a real Lance dataset and
 * back.
 *
 * <p>The in-memory converter test cannot prove much on its own, because it reads back from the same
 * vectors it wrote. Only a real dataset exercises Lance's own encode and decode, which is where the
 * 2.2 format requirement and the entries-struct validity actually bite.
 */
class MapColumnLanceRoundTripITCase {

    @TempDir Path tempDir;

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

    @Test
    @DisplayName("MAP rows written through the converter survive a real Lance round-trip")
    void mapSurvivesLanceRoundTrip() throws Exception {
        String path = tempDir.resolve("map_rows").toString();

        List<RowData> input =
                Arrays.asList(
                        GenericRowData.of(1, map("a", 10, "b", 20)),
                        GenericRowData.of(2, map()),
                        GenericRowData.of(3, null),
                        GenericRowData.of(4, map("only", 7)));

        RowDataConverter converter = new RowDataConverter(ROW_TYPE);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // MAP data needs Lance format 2.2+; on the SDK default the write fails in the encoder.
            Dataset.create(
                            allocator,
                            path,
                            LanceTypeConverter.toArrowSchema(ROW_TYPE),
                            new WriteParams.Builder().withDataStorageVersion("2.2").build())
                    .close();

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build());
                    VectorSchemaRoot root = converter.createVectorSchemaRoot(allocator)) {
                converter.toVectorSchemaRoot(input, root);
                MergeInsertParams params =
                        new MergeInsertParams(Collections.singletonList("id"))
                                .withMatchedUpdateAll()
                                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
                ArrowArrayStreamsTestAccess.mergeInsert(ds, params, allocator, root);
            }

            // Reopen so the assertions read committed data rather than the pre-merge snapshot.
            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                assertThat(ds.countRows()).isEqualTo(4L);

                List<RowData> readBack = new ArrayList<>();
                try (org.apache.arrow.vector.ipc.ArrowReader reader = ds.newScan().scanBatches()) {
                    while (reader.loadNextBatch()) {
                        readBack.addAll(converter.toRowDataList(reader.getVectorSchemaRoot()));
                    }
                }

                // mergeInsert does not preserve input order, so rows are keyed by id.
                Map<Integer, RowData> byId = new HashMap<>();
                for (RowData row : readBack) {
                    byId.put(row.getInt(0), row);
                }
                assertThat(byId).hasSize(4);

                Map<String, Integer> first = flatten(byId.get(1).getMap(1));
                assertThat(first).containsEntry("a", 10).containsEntry("b", 20);

                // An empty map must stay distinct from a NULL map through Lance's own encoding.
                assertThat(byId.get(2).isNullAt(1)).isFalse();
                assertThat(byId.get(2).getMap(1).size()).isZero();

                assertThat(byId.get(3).isNullAt(1)).isTrue();

                assertThat(flatten(byId.get(4).getMap(1))).containsExactlyEntriesOf(
                        Collections.singletonMap("only", 7));
            }
        }
    }

    private static Map<String, Integer> flatten(MapData mapData) {
        Map<String, Integer> out = new HashMap<>();
        for (int i = 0; i < mapData.size(); i++) {
            out.put(
                    mapData.keyArray().getString(i).toString(),
                    mapData.valueArray().isNullAt(i) ? null : mapData.valueArray().getInt(i));
        }
        return out;
    }
}
