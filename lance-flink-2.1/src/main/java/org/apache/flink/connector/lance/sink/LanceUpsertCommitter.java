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

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.connector.lance.ArrowArrayStreams;
import org.apache.flink.connector.lance.PrimaryKeyPersistence;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.merge.MergeInsertParams;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lance's keyed upsert {@link Committer}: submits the serialized row data carried in
 * {@link LanceUpsertCommittable} to the Lance dataset via {@code Dataset.mergeInsert}.
 *
 * <p><b>Why serial (plan A)</b>: {@code mergeInsert} is built on hash-join + fragment
 * rewrite + OCC retry. Different primary keys may land on the same fragment, so concurrent
 * commits trigger optimistic-concurrency conflicts. A serial committer drives the conflict
 * probability to zero, with {@code mergeInsert}'s built-in {@code conflict_retries} only as
 * a safety net. The single-point throughput bottleneck is left to the #81 "hash-partitioned
 * committer by primary key" evolution path.
 *
 * <p><b>Commit decision (decided per datasetPath)</b>:
 * <ol>
 *   <li>Dataset does not exist → materialize the {@code upsertBatch} into fragments and
 *       create the table via {@link Overwrite}, then {@link PrimaryKeyPersistence#persist}
 *       the primary keys; {@code deleteBatch} is ignored (nothing to delete in an empty
 *       table);</li>
 *   <li>Dataset already exists → call {@code mergeInsert(UpdateAll + InsertAll)} on
 *       {@code upsertBatch}, and {@code mergeInsert(Delete + DoNothing)} on
 *       {@code deleteBatch}.</li>
 * </ol>
 *
 * <p><b>Idempotency</b>: both {@code mergeInsert} upsert and "deleting a non-existent key"
 * are idempotent, so replaying a committable from checkpoint has no side effect and
 * Exactly-Once holds.
 *
 * <p><b>Failure handling</b>: Lance exceptions are routed to retryLater / failed by the
 * {@link #maxRetries()} threshold, consistent with the append-path {@link LanceCommitter}
 * (the object_store JNI cannot reliably distinguish exception types).
 */
public class LanceUpsertCommitter implements Committer<LanceUpsertCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertCommitter.class);

    private static final int MAX_RETRIES = 5;

    private final BufferAllocator allocator;

    public LanceUpsertCommitter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    protected int maxRetries() {
        return MAX_RETRIES;
    }

    @Override
    public void commit(Collection<CommitRequest<LanceUpsertCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }
        // Process serially, one committable at a time, so Arrow IPC bytes from multiple
        // batches are never merged.
        for (CommitRequest<LanceUpsertCommittable> req : requests) {
            LanceUpsertCommittable c = req.getCommittable();
            if (c.isEmpty()) {
                continue;
            }
            commitOne(req, c);
        }
    }

    private void commitOne(CommitRequest<LanceUpsertCommittable> req, LanceUpsertCommittable c) {
        String path = c.getDatasetPath();
        try {
            boolean existed = datasetExists(path);
            if (!existed) {
                createDataset(path, c);
            } else {
                applyMergeInsert(path, upsertParams(c.getPrimaryKeys()), c.getUpsertBatch());
                applyMergeInsert(path, deleteParams(c.getPrimaryKeys()), c.getDeleteBatch());
            }
            // Commit succeeded: the request is implicitly successful and runtime won't replay it.
        } catch (Throwable t) {
            LOG.warn("Lance upsert commit failed on {}, retrying or failing request", path, t);
            if (req.getNumberOfRetries() < maxRetries()) {
                req.retryLater();
            } else {
                req.signalFailedWithKnownReason(t);
            }
        }
    }

    /**
     * First-write table creation: materialize upsertBatch into fragments, create the table
     * via Overwrite, and persist the primary keys.
     */
    private void createDataset(String path, LanceUpsertCommittable c) throws IOException {
        Schema schema = c.getArrowSchema();
        if (schema == null) {
            throw new IOException(
                    "Cannot create Lance dataset at " + path + ": no Arrow schema in committable");
        }

        List<FragmentMetadata> fragments = materializeFragments(path, c.getUpsertBatch());
        Operation op = Overwrite.builder().fragments(fragments).schema(schema).build();

        CommitBuilder builder = new CommitBuilder(path, allocator);
        try (Transaction txn = new Transaction.Builder().operation(op).build();
             Dataset ds = builder.execute(txn)) {
            // Persist primary keys after table creation (independent UpdateConfig operation,
            // weakly consistent, A-1 approach)
            PrimaryKeyPersistence.persist(ds, c.getPrimaryKeys());
        }
        LOG.info(
                "Lance upsert commit: CREATE dataset path={}, fragments={}",
                path,
                fragments.size());
    }

    /**
     * Deserialize upsertBatch (Arrow IPC bytes) and materialize it into Lance fragments;
     * returns an empty list for an empty batch.
     */
    private List<FragmentMetadata> materializeFragments(String path, byte[] batch)
            throws IOException {
        if (batch == null || batch.length == 0) {
            return Collections.emptyList();
        }
        try (ArrowStreamReader reader =
                new ArrowStreamReader(new ByteArrayInputStream(batch), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return Fragment.write()
                    .datasetUri(path)
                    .allocator(allocator)
                    .data(root)
                    .execute();
        }
    }

    /**
     * Deserialize the batch and execute a single mergeInsert.
     */
    private void applyMergeInsert(String path, MergeInsertParams params, byte[] batch)
            throws IOException {
        if (batch == null || batch.length == 0) {
            return;
        }
        try (Dataset ds = Dataset.open(path, allocator);
             ArrowStreamReader reader =
                     new ArrowStreamReader(new ByteArrayInputStream(batch), allocator)) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            ArrowArrayStreams.mergeInsert(ds, params, allocator, root);
        }
    }

    private static MergeInsertParams upsertParams(List<String> primaryKeys) {
        return new MergeInsertParams(primaryKeys)
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);
    }

    private static MergeInsertParams deleteParams(List<String> primaryKeys) {
        return new MergeInsertParams(primaryKeys)
                .withMatchedDelete()
                .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);
    }

    protected boolean datasetExists(String path) {
        try (Dataset ignored = Dataset.open(path, allocator)) {
            return true;
        } catch (Throwable t) {
            LOG.debug(
                    "Dataset.open({}) failed, treating as non-existent: {}",
                    path,
                    t.toString());
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing BufferAllocator in LanceUpsertCommitter", e);
            }
        }
    }
}
