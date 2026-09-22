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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.WriteParams;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the idempotence and conflict handling of {@link PrimaryKeyPersistence#persist}.
 *
 * <p>{@code updateConfig} is a versioned Lance transaction, not an idempotent put, so several
 * subtasks opening the same dataset used to be rejected by the conflict resolver. The tolerance
 * added for that must not swallow a real disagreement, which is what the last case pins down.
 */
class PrimaryKeyPersistenceConcurrencyTest {

    @TempDir Path tempDir;

    private static final List<String> KEYS = Arrays.asList("id", "region");

    private String newDataset(String name) {
        String path = tempDir.resolve(name + ".lance").toString();
        Schema schema =
                new Schema(
                        Collections.singletonList(
                                new Field(
                                        "id",
                                        FieldType.nullable(new ArrowType.Int(64, true)),
                                        null)));
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            Dataset.create(allocator, path, schema, new WriteParams.Builder().build()).close();
        }
        return path;
    }

    @Test
    @DisplayName("persist writes the encoded key list on first call")
    void persistWritesOnFirstCall() {
        String path = newDataset("first");
        try (Dataset dataset = Dataset.open(path)) {
            PrimaryKeyPersistence.persist(dataset, KEYS);
            assertThat(PrimaryKeyPersistence.load(dataset)).isEqualTo(KEYS);
        }
    }

    @Test
    @DisplayName("Repeating persist with the same value commits no further version")
    void repeatedPersistIsAVersionNoOp() {
        // The comparison before the write is what keeps the steady state free of transactions.
        // Without it every subtask open would append a version to the dataset history.
        String path = newDataset("repeat");
        try (Dataset dataset = Dataset.open(path)) {
            PrimaryKeyPersistence.persist(dataset, KEYS);
            long afterFirst = dataset.version();

            PrimaryKeyPersistence.persist(dataset, KEYS);

            assertThat(dataset.version())
                    .as("an unchanged value must not open a new transaction")
                    .isEqualTo(afterFirst);
        }
    }

    @Test
    @DisplayName("A second handle observing the same value also stays quiet")
    void peerHandleWithSameValueIsANoOp() {
        // Models the common case: one subtask already stored the keys, another opens the dataset
        // afterwards and finds them present.
        String path = newDataset("peer");
        try (Dataset first = Dataset.open(path)) {
            PrimaryKeyPersistence.persist(first, KEYS);
        }
        try (Dataset second = Dataset.open(path)) {
            long before = second.version();
            PrimaryKeyPersistence.persist(second, KEYS);
            assertThat(second.version()).isEqualTo(before);
            assertThat(PrimaryKeyPersistence.load(second)).isEqualTo(KEYS);
        }
    }

    @Test
    @DisplayName("A genuinely different key list is still written")
    void differingValueIsWritten() {
        String path = newDataset("differ");
        try (Dataset dataset = Dataset.open(path)) {
            PrimaryKeyPersistence.persist(dataset, Collections.singletonList("id"));
            PrimaryKeyPersistence.persist(dataset, KEYS);
            assertThat(PrimaryKeyPersistence.load(dataset))
                    .as("the conflict tolerance must not turn persist into a no-op")
                    .isEqualTo(KEYS);
        }
    }

    @Test
    @DisplayName("A comma in a column name is rejected before any write")
    void commaInColumnNameIsRejected() {
        String path = newDataset("comma");
        try (Dataset dataset = Dataset.open(path)) {
            assertThatThrownBy(
                            () ->
                                    PrimaryKeyPersistence.persist(
                                            dataset, Collections.singletonList("a,b")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("comma");
            assertThat(PrimaryKeyPersistence.load(dataset)).isEmpty();
        }
    }
}
