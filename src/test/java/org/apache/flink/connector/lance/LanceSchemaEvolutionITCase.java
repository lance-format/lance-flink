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

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.WriteParams;
import org.lance.operation.Overwrite;
import org.lance.schema.ColumnAlteration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the Lance SDK {@code addColumns}/{@code dropColumns} behavior that backs
 * {@code ALTER TABLE ADD/DROP COLUMN} in {@code LanceCatalog#alterTable}.
 */
class LanceSchemaEvolutionITCase {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    private void seed(String uri) throws Exception {
        Schema schema = new Schema(Collections.singletonList(
                new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector idVec = (BigIntVector) root.getVector("id");
            root.setRowCount(1);
            idVec.setSafe(0, 1L);

            WriteParams writeParams = new WriteParams.Builder().withMaxRowsPerFile(1_000_000).build();
            List<FragmentMetadata> fragments = Fragment.write()
                    .datasetUri(uri)
                    .allocator(allocator)
                    .data(root)
                    .writeParams(writeParams)
                    .execute();

            CommitBuilder builder = new CommitBuilder(uri, allocator).writeParams(Collections.emptyMap());
            try (Transaction txn = new Transaction.Builder()
                    .operation(Overwrite.builder().fragments(fragments).schema(schema).build())
                    .build();
                 Dataset ds = builder.execute(txn)) {
                assertThat(ds.countRows()).isEqualTo(1L);
            }
        }
    }

    @Test
    @DisplayName("addColumns then dropColumns mutates the dataset schema")
    void addAndDropColumns() throws Exception {
        String uri = tempDir.resolve("schema_evo").toString();
        seed(uri);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(uri, allocator)) {
            // Add a 'name' column.
            Field nameField = LanceTypeConverter.flinkTypeToArrowField(
                    "name", new VarCharType(true, VarCharType.MAX_LENGTH));
            ds.addColumns(Collections.singletonList(nameField));

            List<String> columns = ds.getSchema().getFields().stream()
                    .map(Field::getName)
                    .collect(Collectors.toList());
            assertThat(columns).containsExactlyInAnyOrder("id", "name");

            // Drop the 'name' column.
            ds.dropColumns(Collections.singletonList("name"));
            List<String> afterDrop = ds.getSchema().getFields().stream()
                    .map(Field::getName)
                    .collect(Collectors.toList());
            assertThat(afterDrop).containsExactly("id");
        }
    }

    @Test
    @DisplayName("type change detection helper flags a type change")
    void typeChangeDetection() {
        Field old = new Field("id", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field changed = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        assertThat(old.getType().equals(changed.getType())).isFalse();
    }

    private void seedWithName(String uri) throws Exception {
        Schema schema = new Schema(Arrays.asList(
                new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector idVec = (IntVector) root.getVector("id");
            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            root.setRowCount(1);
            idVec.setSafe(0, 42);
            nameVec.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));

            WriteParams writeParams = new WriteParams.Builder().withMaxRowsPerFile(1_000_000).build();
            List<FragmentMetadata> fragments = Fragment.write()
                    .datasetUri(uri)
                    .allocator(allocator)
                    .data(root)
                    .writeParams(writeParams)
                    .execute();

            CommitBuilder builder = new CommitBuilder(uri, allocator).writeParams(Collections.emptyMap());
            try (Transaction txn = new Transaction.Builder()
                    .operation(Overwrite.builder().fragments(fragments).schema(schema).build())
                    .build();
                 Dataset ds = builder.execute(txn)) {
                assertThat(ds.countRows()).isEqualTo(1L);
            }
        }
    }

    @Test
    @DisplayName("alterColumns renames a column and preserves its data")
    void renameColumn() throws Exception {
        String uri = tempDir.resolve("rename").toString();
        seedWithName(uri);

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(uri, allocator)) {
            ds.alterColumns(Collections.singletonList(
                    new ColumnAlteration.Builder("name").rename("full_name").build()));

            List<String> columns = ds.getSchema().getFields().stream()
                    .map(Field::getName)
                    .collect(Collectors.toList());
            assertThat(columns).containsExactlyInAnyOrder("id", "full_name");
            assertThat(ds.countRows()).isEqualTo(1L);
            assertThat(ds.countRows("full_name = 'alice'")).isEqualTo(1L);
        }
    }
}
