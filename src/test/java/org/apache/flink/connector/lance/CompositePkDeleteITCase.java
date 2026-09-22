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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regression test for composite-primary-key DELETE predicate syntax (issue #63, Phase 0 Q3).
 *
 * <p>Locks in the spike finding that Lance's filter parser <b>does not support row-value
 * tuple {@code IN}</b> for composite keys, and that the OR-of-AND shape is the only working form.
 * This is the acceptance basis for Phase B's predicate builder.
 *
 * <p>Verified against {@code org.lance:lance-core:7.0.0} (arrow 18.3.0, JDK 11):
 *
 * <ul>
 *   <li>{@code (user_id, event_ts) IN ((..), (..))} → rejected with
 *       {@code IllegalArgumentException: Expression '(user_id, event_ts)' is not supported SQL}.</li>
 *   <li>{@code (user_id = .. AND event_ts = timestamp '..') OR (..)} → accepted and deletes
 *       exactly the matching rows.</li>
 * </ul>
 */
class CompositePkDeleteITCase {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        // Force Arrow to use the Netty allocator, matching LanceTimeTravelITCase and
        // LanceNamespaceCatalogITCase to avoid classloader-related SPI issues.
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    // 2023-11-14T22:13:20Z / 22:13:21Z / 22:13:22Z in epoch microseconds.
    private static final long TS1 = 1_700_000_000_000_000L;
    private static final long TS2 = 1_700_000_001_000_000L;
    private static final long TS3 = 1_700_000_002_000_000L;

    private static Schema compositeSchema() {
        return new Schema(Arrays.asList(
                new Field("user_id", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("event_ts",
                        FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null),
                new Field("payload", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
        ));
    }

    @Test
    @DisplayName("tuple-IN is rejected; OR-of-AND deletes exactly the matching composite-PK rows")
    void compositePkDeleteMustUseOrOfAnd() throws Exception {
        String tupleInUri = tempDir.resolve("tuple_in").toString();
        seedComposite(tupleInUri);

        // tuple-IN must be rejected by the Lance filter parser.
        String tupleIn =
                "(user_id, event_ts) IN ((100, timestamp '2023-11-14 22:13:20'),"
                        + " (200, timestamp '2023-11-14 22:13:22'))";
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(tupleInUri, allocator)) {
            try {
                ds.delete(tupleIn);
                fail("tuple-IN predicate must be rejected by the Lance filter parser");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage()).contains("not supported");
            }
        }

        // OR-of-AND must succeed and remove exactly the two matched rows (TS1 and TS3),
        // leaving only the TS2 row.
        String orOfAndUri = tempDir.resolve("or_of_and").toString();
        seedComposite(orOfAndUri);
        String orOfAnd =
                "(user_id = 100 AND event_ts = timestamp '2023-11-14 22:13:20')"
                        + " OR "
                        + "(user_id = 200 AND event_ts = timestamp '2023-11-14 22:13:22')";
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(orOfAndUri, allocator)) {
            ds.delete(orOfAnd);

            assertThat(ds.countRows()).isEqualTo(1L);
            // The surviving row is (user_id=100, event_ts=TS2, payload="b").
            assertThat(ds.countRows("user_id = 100")).isEqualTo(1L);
            assertThat(ds.countRows("user_id = 200")).isZero();
            assertThat(ds.countRows("event_ts = timestamp '2023-11-14 22:13:21'")).isEqualTo(1L);
        }
    }

    private void seedComposite(String uri) throws Exception {
        Schema schema = compositeSchema();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector userVec = (BigIntVector) root.getVector("user_id");
            TimeStampMicroVector tsVec = (TimeStampMicroVector) root.getVector("event_ts");
            VarCharVector payloadVec = (VarCharVector) root.getVector("payload");
            root.setRowCount(3);
            userVec.setSafe(0, 100L);
            tsVec.setSafe(0, TS1);
            payloadVec.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
            userVec.setSafe(1, 100L);
            tsVec.setSafe(1, TS2);
            payloadVec.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
            userVec.setSafe(2, 200L);
            tsVec.setSafe(2, TS3);
            payloadVec.setSafe(2, "c".getBytes(StandardCharsets.UTF_8));

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
                assertThat(ds.countRows()).isEqualTo(3L);
            }
        }
    }
}
