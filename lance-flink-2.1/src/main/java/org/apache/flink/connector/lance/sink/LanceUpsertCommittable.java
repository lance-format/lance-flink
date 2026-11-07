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

package org.apache.flink.connector.lance.sink;

import org.apache.flink.connector.lance.config.LanceOptions;

import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The product of {@link LanceUpsertWriter#prepareCommit()}, consumed by
 * {@link LanceUpsertCommitter#commit}.
 *
 * <p>Unlike the append-path {@link LanceCommittable}: {@code Dataset.mergeInsert} consumes
 * <b>row data</b> ({@code ArrowArrayStream}) rather than fragment metadata, so this class carries
 * two batches of row data serialized as Arrow IPC streams:
 *
 * <ul>
 *   <li>{@link #upsertBatch} — {@code +I/+U} rows (Arrow IPC bytes), the committer runs
 *       {@code mergeInsert(UpdateAll + InsertAll)};</li>
 *   <li>{@link #deleteBatch} — {@code -D} rows (Arrow IPC bytes), the committer runs
 *       {@code mergeInsert(Delete + DoNothing)}.</li>
 * </ul>
 *
 * <p>The two batches must be serialized separately, because a single {@code mergeInsert} can set
 * only one {@code WhenMatched} behavior (empirically confirmed by the PoC: {@code WhenMatched} is
 * a single enum, so update and delete cannot be mixed in one call).
 *
 * <p>{@code UPDATE_BEFORE} is dropped on the writer side (upsert does not need the old value) and
 * never enters either batch.
 */
public final class LanceUpsertCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String datasetPath;
    private final transient Schema arrowSchema;
    private final List<String> primaryKeys;
    private final LanceOptions.WriteMode writeMode;
    private final byte[] upsertBatch;
    private final byte[] deleteBatch;

    public LanceUpsertCommittable(
            String datasetPath,
            Schema arrowSchema,
            List<String> primaryKeys,
            LanceOptions.WriteMode writeMode,
            byte[] upsertBatch,
            byte[] deleteBatch) {
        this.datasetPath = Objects.requireNonNull(datasetPath, "datasetPath");
        this.arrowSchema = arrowSchema;
        this.primaryKeys =
                primaryKeys == null ? Collections.emptyList() : new ArrayList<>(primaryKeys);
        this.writeMode = writeMode == null ? LanceOptions.WriteMode.APPEND : writeMode;
        this.upsertBatch = upsertBatch == null ? new byte[0] : upsertBatch;
        this.deleteBatch = deleteBatch == null ? new byte[0] : deleteBatch;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public Schema getArrowSchema() {
        return arrowSchema;
    }

    public List<String> getPrimaryKeys() {
        return Collections.unmodifiableList(primaryKeys);
    }

    public LanceOptions.WriteMode getWriteMode() {
        return writeMode;
    }

    public byte[] getUpsertBatch() {
        return upsertBatch;
    }

    public byte[] getDeleteBatch() {
        return deleteBatch;
    }

    public boolean isEmpty() {
        return upsertBatch.length == 0 && deleteBatch.length == 0;
    }

    @Override
    public String toString() {
        return "LanceUpsertCommittable{path="
                + datasetPath
                + ", writeMode="
                + writeMode
                + ", primaryKeys="
                + primaryKeys
                + ", upsertBatchBytes="
                + upsertBatch.length
                + ", deleteBatchBytes="
                + deleteBatch.length
                + '}';
    }
}
