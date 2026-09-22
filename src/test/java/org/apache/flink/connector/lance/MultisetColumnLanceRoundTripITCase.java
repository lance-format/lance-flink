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
import org.apache.flink.table.types.logical.MultisetType;
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
 * End-to-end MULTISET support against a real Lance dataset.
 *
 * <p>The in-memory converter test reads back the vectors it just wrote and never passes through
 * Lance's own validation, so it cannot see a non-null constraint violation. Only a real dataset can
 * — which is how the entries-struct validity bug was caught for MAP.
 */
class MultisetColumnLanceRoundTripITCase {

    @TempDir Path tempDir;

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {
                        new IntType(false),
                        new MultisetType(true, new VarCharType(false, VarCharType.MAX_LENGTH))
                    },
                    new String[] {"id", "tags"});

    private static MapData counts(Object... kv) {
        Map<Object, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(StringData.fromString((String) kv[i]), kv[i + 1]);
        }
        return new GenericMapData(m);
    }

    @Test
    @DisplayName("MULTISET rows survive a real Lance round-trip with their counts")
    void multisetSurvivesLanceRoundTrip() throws Exception {
        String path = tempDir.resolve("multiset_rows").toString();

        List<RowData> input =
                Arrays.asList(
                        GenericRowData.of(1, counts("a", 2, "b", 1)),
                        GenericRowData.of(2, counts()),
                        GenericRowData.of(3, null),
                        GenericRowData.of(4, counts("solo", 5)));

        RowDataConverter converter = new RowDataConverter(ROW_TYPE);

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // A multiset is stored as a map, so it inherits the Lance 2.2 format requirement.
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

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                assertThat(ds.countRows()).isEqualTo(4L);

                List<RowData> readBack = new ArrayList<>();
                try (org.apache.arrow.vector.ipc.ArrowReader reader = ds.newScan().scanBatches()) {
                    while (reader.loadNextBatch()) {
                        readBack.addAll(converter.toRowDataList(reader.getVectorSchemaRoot()));
                    }
                }

                // mergeInsert does not preserve input order, so key the rows by id.
                Map<Integer, RowData> byId = new HashMap<>();
                for (RowData row : readBack) {
                    byId.put(row.getInt(0), row);
                }
                assertThat(byId).hasSize(4);

                assertThat(flatten(byId.get(1).getMap(1)))
                        .containsEntry("a", 2)
                        .containsEntry("b", 1);

                assertThat(byId.get(2).isNullAt(1)).isFalse();
                assertThat(byId.get(2).getMap(1).size()).isZero();

                assertThat(byId.get(3).isNullAt(1)).isTrue();

                assertThat(flatten(byId.get(4).getMap(1)))
                        .containsExactlyEntriesOf(Collections.singletonMap("solo", 5));
            }
        }
    }

    @Test
    @DisplayName("The persisted schema stores a MULTISET as a map with a non-null int count")
    void persistedSchemaShapeIsAMap() throws Exception {
        String path = tempDir.resolve("multiset_schema").toString();

        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Dataset.create(
                            allocator,
                            path,
                            LanceTypeConverter.toArrowSchema(ROW_TYPE),
                            new WriteParams.Builder().withDataStorageVersion("2.2").build())
                    .close();

            try (Dataset ds = Dataset.open(allocator, path, new ReadOptions.Builder().build())) {
                org.apache.arrow.vector.types.pojo.Field tags = ds.getSchema().findField("tags");
                assertThat(tags.getType())
                        .isInstanceOf(org.apache.arrow.vector.types.pojo.ArrowType.Map.class);

                org.apache.arrow.vector.types.pojo.Field entries = tags.getChildren().get(0);
                assertThat(entries.isNullable()).isFalse();

                org.apache.arrow.vector.types.pojo.Field count = entries.getChildren().get(1);
                assertThat(count.getType())
                        .isInstanceOf(org.apache.arrow.vector.types.pojo.ArrowType.Int.class);
                // Lance has to agree the count is non-null; if it were declared nullable the
                // writer would accept the schema and then reject the first row.
                assertThat(count.isNullable()).isFalse();
            }
        }
    }

    private static Map<String, Integer> flatten(MapData mapData) {
        Map<String, Integer> out = new HashMap<>();
        for (int i = 0; i < mapData.size(); i++) {
            out.put(mapData.keyArray().getString(i).toString(), mapData.valueArray().getInt(i));
        }
        return out;
    }
}
