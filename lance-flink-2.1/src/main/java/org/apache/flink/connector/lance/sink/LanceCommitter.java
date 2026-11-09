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
import org.apache.flink.connector.lance.config.LanceOptions;

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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Lance {@link Committer} that commits the accumulated fragment metadata from
 * {@link LanceCommittable} to a Lance dataset in a single transaction.
 *
 * <p>The committer owns every <b>commit decision</b>, including the "replace once per job"
 * semantics of {@link LanceOptions.WriteMode#OVERWRITE}:
 * <ul>
 *   <li>the writer no longer performs any physical directory cleanup ({@code Dataset.drop})
 *       and only materializes fragments;
 *   <li>the committer decides Overwrite-vs-Append from the target dataset's manifest via
 *       {@link Dataset#open(String)}, and tags every commit it makes with the {@code flink.job-id}
 *       transaction property. In OVERWRITE mode: dataset absent → {@link Overwrite} + schema to
 *       create the table; dataset present but its latest version was not committed by this job →
 *       {@link Overwrite} to replace it; dataset present and its latest version was committed by
 *       this job → {@link Append} (a peer subtask, or an earlier checkpoint of this job, already
 *       replaced the dataset).
 * </ul>
 *
 * <p>Because the committer is serial and sees all subtasks' committables, moving the decision here
 * removes the race where a late-starting subtask 0 deleted {@code .lance} files that a peer subtask
 * had already materialized.
 *
 * <p>Decision flow (per datasetPath):
 * <ol>
 *   <li>Iterate this batch of CommitRequests, bucketing fragments, schema and write mode by
 *       datasetPath;
 *   <li>For each bucket: probe whether the dataset is "openable", then pick Overwrite or Append
 *       according to the write mode.</li>
 * </ol>
 *
 * <p>Failure handling (actual semantics after the R2 simplification):
 * <ul>
 *   <li>Any exception thrown by the underlying Lance commit → split by the
 *       {@link #MAX_RETRIES} threshold:
 *       <code>req.getNumberOfRetries() &lt; MAX_RETRIES</code> →
 *       {@link CommitRequest#retryLater()}; otherwise
 *       {@link CommitRequest#signalFailedWithKnownReason(Throwable)}.</li>
 *   <li>Missing schema in a bucket (needs to create the table but no committable
 *       carries a schema) → {@link CommitRequest#signalFailedWithKnownReason(Throwable)}
 *       directly, without retry (this is a job configuration error that retrying
 *       cannot fix).</li>
 *   <li>A failure to read the latest version's transaction properties is <b>not</b> treated as
 *       "this job has not committed yet": that would turn a transient I/O error into a
 *       destructive Overwrite of already-committed data. It propagates to the retry path
 *       instead.</li>
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

    /**
     * Transaction property recording the Flink job that committed a version. In overwrite mode it
     * lets the serial committer distinguish "this job has already committed (append)" from "the
     * dataset still holds an older job's result (overwrite)", which is what makes the replacement
     * happen once per job.
     */
    static final String JOB_ID_PROPERTY = "flink.job-id";

    private final BufferAllocator allocator;
    private final String jobId;

    public LanceCommitter() {
        this(null);
    }

    public LanceCommitter(String jobId) {
        this.jobId = jobId;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void commit(Collection<CommitRequest<LanceCommittable>> requests)
            throws IOException, InterruptedException {
        if (requests.isEmpty()) {
            return;
        }

        // 1) Bucket by datasetPath (supports the rare "one batch spans multiple tables" case)
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

    /**
     * Aggregates and commits all CommitRequests for a single datasetPath, reporting results via
     * the CommitRequest API.
     */
    private void commitBucket(PathBucket bucket) {
        String path = bucket.datasetPath;
        try {
            boolean datasetExists = datasetExists(path);
            boolean overwrite = shouldOverwrite(bucket, datasetExists);
            // Every commit made under OVERWRITE mode is tagged with the job id, including the
            // appends that follow the single replacement. Otherwise the next commit would read an
            // untagged latest version, conclude "this job has not replaced the dataset yet" and
            // overwrite again, discarding everything committed by earlier checkpoints.
            Map<String, String> props =
                    jobId != null ? Collections.singletonMap(JOB_ID_PROPERTY, jobId) : null;

            if (overwrite) {
                // Create the table, or replace the previous (other job's) result: Overwrite + schema
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
                executeTransaction(path, op, props);
                LOG.info(
                        "Lance commit: OVERWRITE path={}, fragments={}, jobTagged={}",
                        path,
                        bucket.allFragments().size(),
                        jobId != null);
            } else {
                // Already exists and (in overwrite mode) already replaced by this job: Append
                Operation op = Append.builder().fragments(bucket.allFragments()).build();
                executeTransaction(path, op, props);
                LOG.info(
                        "Lance commit: APPEND path={}, fragments={}, jobTagged={}",
                        path,
                        bucket.allFragments().size(),
                        jobId != null);
            }
            // Whole bucket committed successfully: all requests succeed implicitly, so the runtime
            // will not replay them.
        } catch (Throwable t) {
            // Optimistic concurrency conflict / other recoverable Lance-side errors: let the runtime
            // retry on the next checkpoint cycle.
            LOG.warn("Lance commit failed on {}, deciding retry vs fail per request", path, t);
            retryOrFail(bucket, t);
        }
    }

    /**
     * Decides whether this commit must replace the dataset instead of appending to it.
     *
     * <p>APPEND mode keeps the original semantics: only an absent dataset goes through
     * {@link Overwrite} (to create the table). OVERWRITE mode replaces the dataset when it does not
     * exist, or when its latest version was not committed by this job; once this job has replaced
     * it, later commits append so that rows from other subtasks and earlier checkpoints survive.
     */
    private boolean shouldOverwrite(PathBucket bucket, boolean datasetExists) throws IOException {
        if (bucket.writeMode != LanceOptions.WriteMode.OVERWRITE) {
            return !datasetExists;
        }
        if (!datasetExists) {
            return true;
        }
        return !latestVersionCommittedByThisJob(bucket.datasetPath);
    }

    /**
     * Reads the latest version's transaction properties and reports whether this job committed it.
     *
     * <p>{@code jobId == null} (no job context, e.g. unit tests) conservatively reports
     * {@code false}, i.e. the dataset is treated as needing a replace.
     *
     * <p>A failure to read the version is propagated instead of being reported as
     * {@code false}: the dataset was just proven to exist, so an I/O error must send the request
     * back through the retry path rather than trigger a destructive Overwrite.
     *
     * <p>protected so unit tests can inject read failures.
     */
    protected boolean latestVersionCommittedByThisJob(String path) throws IOException {
        if (jobId == null) {
            return false;
        }
        try (Dataset ds = Dataset.open(path, allocator)) {
            Optional<Transaction> txn = ds.readTransaction();
            if (txn.isPresent()) {
                try (Transaction t = txn.get()) {
                    Map<String, String> props = t.transactionProperties().orElse(null);
                    return props != null && jobId.equals(props.get(JOB_ID_PROPERTY));
                }
            }
            return false;
        } catch (Exception e) {
            throw new IOException(
                    "Failed to read the latest version transaction properties of " + path, e);
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
        executeTransaction(path, op, null);
    }

    /**
     * Executes a single Lance transaction, optionally tagging it with transaction properties.
     *
     * <p>protected so unit tests can inject simulated failures (R2 test point) — real conflicts
     * are hard to reproduce without spinning up a Lance JNI handle.
     */
    protected void executeTransaction(String path, Operation op, Map<String, String> properties)
            throws IOException {
        CommitBuilder builder = new CommitBuilder(path, allocator);
        Transaction.Builder txnBuilder = new Transaction.Builder().operation(op);
        if (properties != null && !properties.isEmpty()) {
            txnBuilder.transactionProperties(properties);
        }
        try (Transaction txn = txnBuilder.build();
             Dataset ignored = builder.execute(txn)) {
            // Dataset.execute returns the new Dataset handle after this commit; close it now.
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
        final Schema schema; // from the first schema-carrying committable, used for create
        final LanceOptions.WriteMode writeMode; // write mode declared by the first committable
        final List<CommitRequest<LanceCommittable>> requests = new ArrayList<>();
        final List<FragmentMetadata> fragments = new ArrayList<>();

        PathBucket(String path, LanceCommittable first) {
            this.datasetPath = path;
            this.schema = first.getArrowSchema();
            this.writeMode = first.getWriteMode();
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
