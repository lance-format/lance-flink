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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceUpsertCommittable;
import org.apache.flink.connector.lance.sink.LanceUpsertCommitter;
import org.apache.flink.connector.lance.sink.LanceUpsertWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sink V2 primary-key upsert ITCase: drives {@link LanceUpsertWriter} (buffer + serialize)
 * and {@link LanceUpsertCommitter} (serial mergeInsert) to verify {@code +I/+U/-D} semantics.
 *
 * <p>This is the Flink 2.1 equivalent rewrite of the 1.x {@code LanceUpsertSinkITCase} in the
 * root src (which used the {@code RichSinkFunction} {@code open/invoke/flush/close} lifecycle);
 * the 1.x version has been deleted (its RichSinkFunction lifecycle was removed in Flink 2.x), and
 * this class carries the upsert semantics tests in the Sink V2 form under the same package and name.
 */
class LanceUpsertSinkITCase {

    @TempDir
    Path tempDir;

    @BeforeAll
    static void ensureArrowNettyLoaded() {
        System.setProperty("arrow.memory.allocator.type", "Netty");
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            // allocator created and closed successfully
        }
    }

    private static RowType rowType() {
        return new RowType(Arrays.asList(
                new RowType.RowField("id", new BigIntType(false)),
                new RowType.RowField("name", new VarCharType(true, VarCharType.MAX_LENGTH))));
    }

    private static final List<String> PRIMARY_KEYS = Arrays.asList("id");

    private LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(100)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    private GenericRowData row(long id, String name, RowKind kind) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(name));
        row.setRowKind(kind);
        return row;
    }

    private long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    // ------------------------------------------------------------------------
    // Sink V2 lifecycle: writer.write -> writer.prepareCommit -> committer.commit
    // ------------------------------------------------------------------------

    private void write(LanceUpsertWriter writer, RowData... rows) throws Exception {
        for (RowData r : rows) {
            writer.write(r, null);
        }
    }

    private void commit(LanceUpsertCommitter committer, Collection<LanceUpsertCommittable> cs)
            throws Exception {
        List<Committer.CommitRequest<LanceUpsertCommittable>> requests = new ArrayList<>();
        for (LanceUpsertCommittable c : cs) {
            requests.add(new FakeCommitRequest(c));
        }
        committer.commit(requests);
    }

    /** Write + commit in a single window. */
    private void writeAndCommit(
            String path, int subtaskId, RowData... rows) throws Exception {
        try (LanceUpsertWriter writer =
                     new LanceUpsertWriter(options(path), rowType(), PRIMARY_KEYS, subtaskId);
             LanceUpsertCommitter committer = new LanceUpsertCommitter()) {
            write(writer, rows);
            Collection<LanceUpsertCommittable> cs = writer.prepareCommit();
            commit(committer, cs);
        }
    }

    // ------------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------------

    @Test
    @DisplayName("First write creates the dataset and persists primary keys")
    void firstWriteCreatesDatasetAndPersistsPrimaryKey() throws Exception {
        String path = tempDir.resolve("first").toString();
        writeAndCommit(path, 0, row(1L, "alice", RowKind.INSERT), row(2L, "bob", RowKind.INSERT));

        assertThat(countRows(path)).isEqualTo(2L);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(PrimaryKeyPersistence.load(ds)).containsExactly("id");
        }
    }

    @Test
    @DisplayName("+U(k) replaces the previous value")
    void updateAfterReplacesPreviousValue() throws Exception {
        String path = tempDir.resolve("update").toString();
        writeAndCommit(path, 0, row(1L, "old", RowKind.INSERT));
        writeAndCommit(path, 0, row(1L, "new", RowKind.UPDATE_AFTER));

        assertThat(countRows(path)).isEqualTo(1L);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(ds.countRows("name = 'new'")).isEqualTo(1L);
            assertThat(ds.countRows("name = 'old'")).isZero();
        }
    }

    @Test
    @DisplayName("-D(k) deletes an existing key; deleting an absent key is idempotent")
    void deleteRemovesKeyAndReplayIsIdempotent() throws Exception {
        String path = tempDir.resolve("delete").toString();
        writeAndCommit(path, 0, row(1L, "alice", RowKind.INSERT), row(2L, "bob", RowKind.INSERT));
        writeAndCommit(path, 0, row(1L, "alice", RowKind.DELETE));

        assertThat(countRows(path)).isEqualTo(1L);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            assertThat(ds.countRows("id = 1")).isZero();
            assertThat(ds.countRows("id = 2")).isEqualTo(1L);
        }

        // Replay the delete (checkpoint replay) -- deleting an absent key is a no-op
        writeAndCommit(path, 0, row(1L, "alice", RowKind.DELETE));
        assertThat(countRows(path)).isEqualTo(1L);
    }

    @Test
    @DisplayName("+I(k) then -D(k) in the same window leaves k absent")
    void insertThenDeleteSameWindowLeavesKeyAbsent() throws Exception {
        String path = tempDir.resolve("insert_then_delete").toString();
        // Seed one row first so the dataset already exists
        writeAndCommit(path, 0, row(1L, "alice", RowKind.INSERT));

        // Same window +I(1) then -D(1): upsert first, then delete, so 1 ends up absent
        try (LanceUpsertWriter writer =
                     new LanceUpsertWriter(options(path), rowType(), PRIMARY_KEYS, 0);
             LanceUpsertCommitter committer = new LanceUpsertCommitter()) {
            write(writer, row(1L, "alice", RowKind.INSERT), row(1L, "alice", RowKind.DELETE));
            Collection<LanceUpsertCommittable> cs = writer.prepareCommit();
            commit(committer, cs);
        }
        assertThat(countRows(path)).isZero();
    }

    @Test
    @DisplayName("Two subtasks concurrently first-write distinct keys without data loss")
    void twoSubtasksConcurrentFirstWrite() throws Exception {
        String path = tempDir.resolve("concurrent_first").toString();

        List<LanceUpsertCommittable> all = new ArrayList<>();
        try (LanceUpsertWriter w1 =
                     new LanceUpsertWriter(options(path), rowType(), PRIMARY_KEYS, 0);
             LanceUpsertWriter w2 =
                     new LanceUpsertWriter(options(path), rowType(), PRIMARY_KEYS, 1)) {
            write(w1, row(1L, "one", RowKind.INSERT));
            write(w2, row(2L, "two", RowKind.INSERT));
            all.addAll(w1.prepareCommit());
            all.addAll(w2.prepareCommit());
        }

        // The serial committer processes two committables: the first creates the table,
        // the second mergeInserts, with no data loss
        try (LanceUpsertCommitter committer = new LanceUpsertCommitter()) {
            commit(committer, all);
        }
        assertThat(countRows(path)).isEqualTo(2L);
    }

    // ------------------------------------------------------------------------
    // Minimal fake CommitRequest
    // ------------------------------------------------------------------------

    private static final class FakeCommitRequest
            implements Committer.CommitRequest<LanceUpsertCommittable> {

        private final LanceUpsertCommittable c;

        FakeCommitRequest(LanceUpsertCommittable c) {
            this.c = c;
        }

        @Override
        public LanceUpsertCommittable getCommittable() {
            return c;
        }

        @Override
        public int getNumberOfRetries() {
            return 0;
        }

        @Override
        public void signalFailedWithKnownReason(Throwable t) {
            throw new AssertionError("Unexpected commit failure", t);
        }

        @Override
        public void signalFailedWithUnknownReason(Throwable t) {
            throw new AssertionError("Unexpected commit failure", t);
        }

        @Override
        public void retryLater() {
            throw new AssertionError("Unexpected retry request");
        }

        @Override
        public void updateAndRetryLater(LanceUpsertCommittable committable) {
            throw new AssertionError("Unexpected retry request");
        }

        @Override
        public void signalAlreadyCommitted() {
            // no-op
        }
    }
}
