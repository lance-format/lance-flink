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
import org.apache.flink.connector.lance.sink.LanceUpsertCommittable;
import org.apache.flink.connector.lance.sink.LanceUpsertCommittableSerializer;
import org.apache.flink.connector.lance.sink.LanceUpsertCommitter;
import org.apache.flink.connector.lance.sink.LanceUpsertWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.util.List;

/**
 * Lance primary-key upsert sink (Flink 2.x Sink V2 version).
 *
 * <p>Corresponds to plan A of issue #90: a {@link Sink} + {@link SupportsCommitter} combination.
 * The writer buffers rows classified by {@code RowKind} in a stateless manner, and the serial
 * {@link LanceUpsertCommitter} performs {@code Dataset.mergeInsert} to realize the
 * {@code +I/+U/-D} semantics.
 *
 * <p>The structure is fully parallel to the append path {@link LanceSink}, differing in that:
 * <ul>
 *   <li>the writer produces {@link LanceUpsertCommittable} (carrying serialized row data) rather
 *       than fragment metadata;</li>
 *   <li>the committer serially calls {@code mergeInsert} rather than {@code Append}/{@code Overwrite}.</li>
 * </ul>
 */
public class LanceUpsertSink implements Sink<RowData>, SupportsCommitter<LanceUpsertCommittable> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;
    private final List<String> primaryKeys;

    public LanceUpsertSink(LanceOptions options, RowType rowType, List<String> primaryKeys) {
        this.options = options;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
    }

    @Override
    public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
        int subtaskId = context.getTaskInfo().getIndexOfThisSubtask();
        return new LanceUpsertWriter(options, rowType, primaryKeys, subtaskId);
    }

    @Override
    public Committer<LanceUpsertCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return new LanceUpsertCommitter();
    }

    @Override
    public SimpleVersionedSerializer<LanceUpsertCommittable> getCommittableSerializer() {
        return LanceUpsertCommittableSerializer.INSTANCE;
    }

    public RowType getRowType() {
        return rowType;
    }

    public LanceOptions getOptions() {
        return options;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }
}
