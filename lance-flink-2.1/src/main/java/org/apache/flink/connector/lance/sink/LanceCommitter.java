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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.CommitBuilder;
import org.lance.Dataset;
import org.lance.FragmentMetadata;
import org.lance.Transaction;
import org.lance.operation.Append;
import org.lance.operation.Operation;
import org.lance.operation.Overwrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lance {@link Committer} that commits the accumulated fragment metadata from
 * {@link LanceCommittable} to a Lance dataset in a single transaction.
 *
 * <p>After the B1 fix, the committer owns the <b>commit decision</b> but not the
 * <b>physical directory cleanup</b>:
 * <ul>
 *   <li>The physical directory cleanup (the {@code drop} in OVERWRITE mode) is done
 *       by subtask 0 on the writer side before {@code Fragment.write()} — it must
 *       happen before any fragment is materialized, otherwise the fragment list at
 *       commit time would point to {@code .lance} files already removed by the drop;
 *   <li>The committer's job is to probe whether the target dataset currently has a
 *       manifest via {@link Dataset#open(String)}: absent → {@link Overwrite} +
 *       schema to create the table and commit all fragments; present → use
 *       {@link Append} for everything.
 * </ul>
 *
 * <p>Decision flow (per datasetPath):
 * <ol>
 *   <li>Iterate this batch of CommitRequests, bucketing fragments and schema by
 *       datasetPath;
 *   <li>For each bucket: probe whether the dataset is "openable"; the first commit
 *       goes through Overwrite, later commits go through Append.</li>
 * </ol>
 *
 * <p>Failure handling (actual semantics after the R2 simplification):
 * <ul>
 *   <li>Any exception thrown by the underlying Lance commit → split by the
 *       {@link #MAX_RETRIES} threshold:
 *       <code>req.getNumberOfRetries() &lt; MAX_RETRIES</code> → {@link CommitRequest#retryLater()};
 *       otherwise {@link CommitRequest#signalFailedWithKnownReason(Throwable)}.</li>
 *   <li>Missing schema in a bucket (needs to create the table but no committable
 *       carries a schema) → {@link CommitRequest#signalFailedWithKnownReason(Throwable)}
 *       directly, without retry (this is a job configuration error that retrying
 *       cannot fix).</li>
 * </ul>
 *
 * <p>This implementation deliberately does not distinguish known vs unknown reasons:
 * the Lance JNI backed by object_store wraps both underlying S3 / HDFS / local-FS
 * errors and optimistic concurrency conflicts into the same {@link RuntimeException},
 * which cannot be reliably distinguished on the Java side; consistently using the
 * known-reason signature is safer.
 */
public class LanceCommitter implements Committer<LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCommitter.class);

    /**
     * Maximum number of retries for a single CommitRequest, after which it becomes
     * signalFailed. If Lance optimistic concurrency write conflicts keep failing,
     * the upstream data or schema is very likely incompatible; retrying forever only
     * wedges the Flink job.
     *
     * <p>protected so unit tests can lower this value to expose boundary behavior.
     */
    protected int maxRetries() {
        return MAX_RETRIES;
    }

    private static final int MAX_RETRIES = 5;

    private final BufferAllocator allocator;

    public LanceCommitter() {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void commit(Collection<CommitRequest<LanceCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }

        // 1) Bucket by datasetPath (supports the rare "one commit batch spans multiple tables" case)
        Map<String, PathBucket> buckets = new LinkedHashMap<>();
        for (CommitRequest<LanceCommittable> req : requests) {
            LanceCommittable c = req.getCommittable();
            if (c.isEmpty()) {
                continue;
            }
            buckets.computeIfAbsent(c.getDatasetPath(), p -> new PathBucket(p, c))
                    .add(req, c);
        }

        // 2) Commit each bucket independently; one bucket failing does not affect others
        for (PathBucket bucket : buckets.values()) {
            commitBucket(bucket);
        }
    }

    /** Aggregates and commits all CommitRequests for a single datasetPath, reporting results via the CommitRequest API. */
    private void commitBucket(PathBucket bucket) {
        String path = bucket.datasetPath;
        try {
            boolean datasetExists = datasetExists(path);

            if (!datasetExists) {
                // First commit: Overwrite + schema + all fragments
                if (bucket.schema == null) {
                    IOException err =
                            new IOException(
                                    "Cannot create Lance dataset at "
                                            + path
                                            + ": no Arrow schema in any committable in this batch");
                    signalFail(bucket, err);
                    return;
                }
                Operation op =
                        Overwrite.builder()
                                .fragments(bucket.allFragments())
                                .schema(bucket.schema)
                                .build();
                executeTransaction(path, op);
                LOG.info(
                        "Lance commit: CREATE+OVERWRITE path={}, fragments={}",
                        path,
                        bucket.allFragments().size());
            } else {
                // Already exists: Append everything
                Operation op = Append.builder().fragments(bucket.allFragments()).build();
                executeTransaction(path, op);
                LOG.info(
                        "Lance commit: APPEND path={}, fragments={}",
                        path,
                        bucket.allFragments().size());
            }
            // Whole bucket committed successfully: all requests succeed implicitly and the runtime won't replay them
        } catch (Throwable t) {
            // Optimistic concurrency conflict / other recoverable errors on the Lance side: let the runtime retry on the next checkpoint cycle
            LOG.warn("Lance commit failed on {}, deciding retry vs fail per request", path, t);
            retryOrFail(bucket, t);
        }
    }

    /**
     * Determines whether the target dataset already exists; absence is detected via
     * the exception thrown by Lance.
     *
     * <p>protected so unit tests can inject stub behavior (R2 test point).
     */
    protected boolean datasetExists(String path) {
        try (Dataset ignored = Dataset.open(path, allocator)) {
            return true;
        } catch (Throwable t) {
            // The Lance Java API throws RuntimeException for both "manifest missing" and
            // "path missing"; treat both as "does not exist" here. Real IO errors surface
            // in the next commit step and take the retry path.
            LOG.debug("Dataset.open({}) failed, treating as non-existent: {}", path, t.toString());
            return false;
        }
    }

    /**
     * Executes a single Lance transaction.
     *
     * <p>protected so unit tests can inject simulated failures (R2 test point) —
     * real conflicts are hard to reproduce without spinning up a Lance JNI handle.
     */
    protected void executeTransaction(String path, Operation op) throws IOException {
        CommitBuilder builder = new CommitBuilder(path, allocator);
        try (Transaction txn = new Transaction.Builder().operation(op).build();
             Dataset ignored = builder.execute(txn)) {
            // Dataset.execute returns the new Dataset handle after this commit; close it immediately
        } catch (Exception e) {
            throw new IOException("Lance commit failed on " + path, e);
        }
    }

    /**
     * Splits all requests in the bucket into retryLater / signalFailed by the retry
     * count threshold. It does not rely on exception type — the Lance JNI backed by
     * object_store wraps connection failures / permission failures / optimistic
     * concurrency conflicts into the same {@link RuntimeException}, which is hard to
     * distinguish on the Java side.
     */
    private void retryOrFail(PathBucket bucket, Throwable err) {
        for (CommitRequest<LanceCommittable> req : bucket.requests) {
            if (req.getNumberOfRetries() < maxRetries()) {
                req.retryLater();
            } else {
                req.signalFailedWithKnownReason(err);
            }
        }
    }

    private void signalFail(PathBucket bucket, Throwable err) {
        for (CommitRequest<LanceCommittable> req : bucket.requests) {
            req.signalFailedWithKnownReason(err);
        }
    }

    @Override
    public void close() throws Exception {
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Error closing BufferAllocator in LanceCommitter", e);
            }
        }
    }

    /** Commit aggregation bucket for a single datasetPath. */
    private static final class PathBucket {
        final String datasetPath;
        final Schema schema; // schema from the first schema-carrying committable, used for potential table creation
        final List<CommitRequest<LanceCommittable>> requests = new ArrayList<>();
        final List<FragmentMetadata> fragments = new ArrayList<>();

        PathBucket(String path, LanceCommittable first) {
            this.datasetPath = path;
            this.schema = first.getArrowSchema();
        }

        void add(CommitRequest<LanceCommittable> req, LanceCommittable c) {
            requests.add(req);
            fragments.addAll(c.getFragments());
        }

        List<FragmentMetadata> allFragments() {
            return fragments;
        }
    }
}
