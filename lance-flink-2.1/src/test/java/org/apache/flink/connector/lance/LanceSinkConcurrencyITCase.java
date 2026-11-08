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
import org.apache.flink.connector.lance.sink.LanceCommittable;
import org.apache.flink.connector.lance.sink.LanceCommitter;
import org.apache.flink.connector.lance.sink.LanceSinkWriter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

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
 * Sink V2 version of the append concurrency ITCase: two {@link LanceSinkWriter} subtasks
 * concurrently first-write a non-existent dataset, aggregated and committed via
 * {@link LanceCommitter}, verifying they do not overwrite each other (regression: the first write
 * silently dropping the other subtask's rows when it used Overwrite).
 *
 * <p>This is the Flink 2.1 equivalent rewrite of the 1.x {@code LanceSinkConcurrencyITCase} in root
 * src (the {@code RichSinkFunction} {@code open/invoke/flush/close} lifecycle); the 1.x version in
 * root src has been deleted (the RichSinkFunction lifecycle it tested was removed in Flink 2.x).
 * This class carries the concurrent-first-write semantics test in Sink V2 form under the same
 * package and name.
 */
class LanceSinkConcurrencyITCase {

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
                new RowType.RowField("content", new VarCharType(true, VarCharType.MAX_LENGTH))));
    }

    private LanceOptions options(String path) {
        return LanceOptions.builder()
                .path(path)
                .writeBatchSize(100)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
    }

    private GenericRowData row(long id, String content) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, StringData.fromString(content));
        return row;
    }

    private long countRows(String path) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             Dataset ds = Dataset.open(path, allocator)) {
            return ds.countRows();
        }
    }

    private void write(LanceSinkWriter writer, RowData... rows) throws Exception {
        for (RowData r : rows) {
            writer.write(r, null);
        }
    }

    @Test
    @DisplayName("Two append subtasks concurrently first-write distinct rows without data loss")
    void twoSubtasksConcurrentFirstWrite() throws Exception {
        String path = tempDir.resolve("concurrent_append").toString();

        List<LanceCommittable> all = new ArrayList<>();
        try (LanceSinkWriter w1 = new LanceSinkWriter(options(path), rowType(), 0);
             LanceSinkWriter w2 = new LanceSinkWriter(options(path), rowType(), 1)) {
            write(w1, row(1L, "one"));
            write(w2, row(2L, "two"));
            all.addAll(w1.prepareCommit());
            all.addAll(w2.prepareCommit());
        }

        List<Committer.CommitRequest<LanceCommittable>> requests = new ArrayList<>();
        for (LanceCommittable c : all) {
            requests.add(new FakeCommitRequest(c));
        }

        // LanceCommitter buckets/aggregates by datasetPath; the first table creation uses Overwrite
        // with all fragments, so the second subtask's rows are not lost.
        try (LanceCommitter committer = new LanceCommitter()) {
            committer.commit(requests);
        }

        assertThat(countRows(path)).isEqualTo(2L);
    }

    private static final class FakeCommitRequest
            implements Committer.CommitRequest<LanceCommittable> {

        private final LanceCommittable c;

        FakeCommitRequest(LanceCommittable c) {
            this.c = c;
        }

        @Override
        public LanceCommittable getCommittable() {
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
        public void updateAndRetryLater(LanceCommittable committable) {
            throw new AssertionError("Unexpected retry request");
        }

        @Override
        public void signalAlreadyCommitted() {
            // no-op
        }
    }
}
