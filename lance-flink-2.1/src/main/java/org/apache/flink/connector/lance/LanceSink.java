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
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.sink.LanceCommittable;
import org.apache.flink.connector.lance.sink.LanceCommittableSerializer;
import org.apache.flink.connector.lance.sink.LanceCommitter;
import org.apache.flink.connector.lance.sink.LanceSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;

/**
 * Lance sink (Flink 2.x version).
 *
 * <p>Flink 2.1 has removed the combined {@code TwoPhaseCommittingSink} interface and instead
 * declares a two-phase-commit sink through the more orthogonal {@link Sink} +
 * {@link SupportsCommitter} mixins. This class implements both interfaces:
 * <ul>
 *   <li>{@link Sink#createWriter(WriterInitContext)} returns a
 *       {@link org.apache.flink.api.connector.sink2.CommittingSinkWriter} ({@link LanceSinkWriter})
 *       that pre-commits by batching RowData into Lance fragment data files;
 *   <li>{@link SupportsCommitter#createCommitter(CommitterInitContext)} returns
 *       {@link LanceCommitter}, which commits the fragments in a single Lance transaction.
 *   <li>{@link SupportsCommitter#getCommittableSerializer()} returns the committable's
 *       network/state serializer.
 * </ul>
 *
 * <p>Exactly-once provenance: Flink's checkpoint barrier triggers the writer {@code flush(false)},
 * which produces a committable; the runtime writes the committable into the checkpoint; after the
 * checkpoint succeeds the runtime delivers the committable to the committer, which calls Lance's
 * {@code CommitBuilder.execute(Transaction)} to actually commit. If any step fails the runtime
 * replays the committable and retries until commit succeeds — thanks to Lance's optimistic
 * concurrency control, the protocol is naturally exactly-once.
 *
 * <p>The constructor and builder signatures are kept consistent with the old 1.x
 * {@code RichSinkFunction} version so that shared upper-layer code (e.g.
 * {@code LanceDynamicTableSink}) need not be aware of version differences.
 *
 * <p><b>OVERWRITE semantics</b>: the "replace the dataset once per job" decision is owned by
 * {@link LanceCommitter} — {@link LanceSinkWriter} performs no {@code Dataset.drop}
 * pre-cleanup at all. The committer is serial and consistent across subtasks; it tags every
 * version it commits with the {@code flink.job-id} transaction property, so the job's first
 * commit replaces the previous result with an {@link org.lance.operation.Overwrite} while later
 * checkpoint commits append to it. This removes the data-loss race that OVERWRITE suffered with
 * parallelism &gt; 1 when a late-starting subtask 0 dropped peer subtasks' materialized files.
 */
public class LanceSink implements Sink<RowData>, SupportsCommitter<LanceCommittable> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;

    public LanceSink(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    // ------------------------------------------------------------------------
    // Sink V2 core methods
    // ------------------------------------------------------------------------

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        int subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
        return new LanceSinkWriter(options, rowType, subtaskId);
    }

    @Override
    public Committer<LanceCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new LanceCommitter(context.getJobInfo().getJobId().toString());
    }

    @Override
    public SimpleVersionedSerializer<LanceCommittable> getCommittableSerializer() {
        return LanceCommittableSerializer.INSTANCE;
    }

    // ------------------------------------------------------------------------
    // Backward-compatible read-only accessors (signatures kept consistent with the old
    // implementation for reuse by upper layers such as LanceDynamicTableSink)
    // ------------------------------------------------------------------------

    public RowType getRowType() {
        return rowType;
    }

    public LanceOptions getOptions() {
        return options;
    }

    // ------------------------------------------------------------------------
    // Builder (100% backward compatible with the old implementation)
    // ------------------------------------------------------------------------

    public static Builder builder() {
        return new Builder();
    }

    /** Builder with signatures consistent with the old implementation. */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private LanceOptions.WriteMode writeMode = LanceOptions.WriteMode.APPEND;
        private int maxRowsPerFile = 1000000;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder writeMode(LanceOptions.WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder maxRowsPerFile(int maxRowsPerFile) {
            this.maxRowsPerFile = maxRowsPerFile;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSink build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }
            if (rowType == null) {
                throw new IllegalArgumentException("RowType cannot be null");
            }
            LanceOptions options =
                    LanceOptions.builder()
                            .path(path)
                            .writeBatchSize(batchSize)
                            .writeMode(writeMode)
                            .writeMaxRowsPerFile(maxRowsPerFile)
                            .build();
            return new LanceSink(options, rowType);
        }
    }
}
