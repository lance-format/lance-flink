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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Isolates the handle-staleness hazard found by {@link MatchedDeleteSpikeTest} and shows exactly
 * when it does and does not bite, so the A4 rewrite does not reintroduce it.
 *
 * <p>Context: {@code LanceUpsertSink#mergeInsertRows} discards the {@link MergeInsertResult} and
 * keeps using the {@code dataset} field captured in {@code open()}. That field is a pinned
 * snapshot. Measured consequences:
 *
 * <ul>
 *   <li><b>Writes are NOT lost.</b> Lance resolves a merge against the latest committed table
 *       state at commit time, not against the handle's pinned version, so successive merges on a
 *       stale handle still produce v1 -&gt; v2 -&gt; v3 with every row present. The sink therefore
 *       has no data-loss bug today.</li>
 *   <li><b>Reads are stale.</b> The pinned handle cannot observe its own prior write. Any logic
 *       that reads through the long-lived field after a merge -- row counts, existence checks,
 *       read-modify-write -- sees a pre-merge snapshot.</li>
 *   <li>{@code Dataset#delete} mutates in place, which is the other reason the existing
 *       {@code LanceUpsertSinkITCase} passes today.</li>
 * </ul>
 *
 * <p>Implication for A4: adopting {@code result.dataset()} is required for correct read-your-own-
 * write behaviour and is cheap, but it is a robustness fix, not an outstanding data-loss defect.
 */
class MergeInsertHandleStalenessTest {

    private static final Schema SCHEMA = new Schema(Arrays.asList(
            new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));

    private static MergeInsertParams upsertParams() {
        return new MergeInsertParams(Collections.singletonList("id"))
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
    }

    /**
     * Reusing the stale handle across two merges: this is what the sink does today. Documents the
     * observed behaviour rather than asserting a desired one, so the result is informative either
     * way.
     */
    @Test
    @DisplayName("successive merges on a STALE handle: record what survives")
    void successiveMergesOnStaleHandle(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("stale").toString();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            create(allocator, path);

            try (Dataset pinned = Dataset.open(path, allocator)) {
                long v0 = pinned.version();

                merge(pinned, allocator, 1L, "alice");
                long vAfterFirst;
                try (Dataset probe = Dataset.open(path, allocator)) {
                    vAfterFirst = probe.version();
                }

                // Second merge, still using the pinned (now stale) handle.
                merge(pinned, allocator, 2L, "bob");

                try (Dataset reopened = Dataset.open(path, allocator)) {
                    System.out.println("[stale handle] v0=" + v0
                            + " afterFirst=" + vAfterFirst
                            + " afterSecond=" + reopened.version()
                            + " rows=" + reopened.countRows()
                            + " id1=" + reopened.countRows("id = 1")
                            + " id2=" + reopened.countRows("id = 2"));

                    // Recorded expectation: both rows land. Lance resolves the write against the
                    // latest table state at commit time rather than the handle's pinned version,
                    // so the stale handle does not lose data here.
                    assertThat(reopened.countRows())
                            .as("both merges must be visible after reopen")
                            .isEqualTo(2L);
                    assertThat(reopened.countRows("id = 1")).isEqualTo(1L);
                    assertThat(reopened.countRows("id = 2")).isEqualTo(1L);
                }
            }
        }
    }

    /**
     * The correct pattern: adopt {@code result.dataset()} after each merge. This must hold
     * unconditionally, and is what the A4 rewrite should follow.
     */
    @Test
    @DisplayName("successive merges on a CHAINED handle keep every write")
    void successiveMergesOnChainedHandle(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("chained").toString();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            create(allocator, path);

            Dataset handle = Dataset.open(path, allocator);
            try {
                handle = merge(handle, allocator, 1L, "alice").dataset();
                handle = merge(handle, allocator, 2L, "bob").dataset();
                handle = merge(handle, allocator, 1L, "alice-v2").dataset();

                assertThat(handle.countRows())
                        .as("the chained handle must observe every write")
                        .isEqualTo(2L);
            } finally {
                handle.close();
            }

            try (Dataset reopened = Dataset.open(path, allocator)) {
                assertThat(reopened.countRows()).isEqualTo(2L);
                assertThat(reopened.countRows("name = 'alice-v2'"))
                        .as("the update must win over the original value")
                        .isEqualTo(1L);
                assertThat(reopened.countRows("name = 'alice'")).isZero();
            }
        }
    }

    /**
     * The read-your-own-write case, which is the one that actually matters for the A4 rewrite: the
     * sink must be able to observe its own prior flush. A stale handle cannot.
     */
    @Test
    @DisplayName("a stale handle cannot read its own prior write; a chained handle can")
    void staleHandleCannotReadOwnWrite(@TempDir Path tempDir) throws Exception {
        String path = tempDir.resolve("read_own_write").toString();
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            create(allocator, path);

            try (Dataset pinned = Dataset.open(path, allocator)) {
                MergeInsertResult result = merge(pinned, allocator, 1L, "alice");

                assertThat(pinned.countRows())
                        .as("the pinned handle is blind to its own write")
                        .isZero();
                assertThat(result.dataset().countRows())
                        .as("the returned handle sees the write immediately")
                        .isEqualTo(1L);
                assertThat(result.dataset().version())
                        .as("and points at a newer version")
                        .isGreaterThan(pinned.version());
            }
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static MergeInsertResult merge(
            Dataset target, BufferAllocator allocator, long id, String name) throws IOException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            BigIntVector ids = (BigIntVector) root.getVector("id");
            VarCharVector names = (VarCharVector) root.getVector("name");
            ids.allocateNew(1);
            names.allocateNew(1);
            ids.setSafe(0, id);
            names.setSafe(0, name.getBytes(StandardCharsets.UTF_8));
            root.setRowCount(1);
            return ArrowArrayStreamsTestAccess.mergeInsert(target, upsertParams(), allocator, root);
        }
    }

    private static void create(BufferAllocator allocator, String path) {
        try (Dataset created = Dataset.create(
                allocator, path, SCHEMA, new WriteParams.Builder().build())) {
            assertThat(created.countRows()).isZero();
        }
    }
}
