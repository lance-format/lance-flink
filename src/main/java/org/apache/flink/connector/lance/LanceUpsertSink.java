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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.merge.MergeInsertParams;
import org.lance.merge.MergeInsertResult;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.connector.lance.util.LanceAllocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Keyed sink for Lance tables declared with a primary key.
 *
 * <p>Unlike {@link LanceSink} (append-only), this sink supports the CDC changelog kinds
 * {@code +I}/{@code +U}/{@code -D}. It relies on {@code DataStream#keyBy} upstream to guarantee
 * that all events for a given primary key arrive at the same subtask in order; it then collapses
 * the buffered events per key to a single final action and applies it at checkpoint boundaries:
 *
 * <ul>
 *   <li>{@code INSERT}/{@code UPDATE_AFTER} &rarr; native upsert via
 *       {@link Dataset#mergeInsert} ({@code WhenMatched.UpdateAll} +
 *       {@code WhenNotMatched.InsertAll}).</li>
 *   <li>{@code DELETE} &rarr; native key-only {@link Dataset#mergeInsert}
 *       ({@code WhenMatched.Delete} + {@code WhenNotMatched.DoNothing}); see
 *       {@link #deleteRows}.</li>
 *   <li>{@code UPDATE_BEFORE} &rarr; dropped (upsert has no need for the old value).</li>
 * </ul>
 *
 * <h3>Hot primary keys</h3>
 * <p>{@code LanceDynamicTableSink} routes CDC events through {@code keyBy(PrimaryKeySelector)} so
 * every event for a key lands on one subtask in order. That ordering is what makes the per-flush
 * delete/upsert sequence correct, but it also means throughput for a key is bounded by a single
 * subtask. If the key distribution is skewed — one {@code tenant_id} carrying most of the traffic,
 * say — that subtask becomes the pipeline's ceiling while its peers idle.
 *
 * <p>The per-key buffer collapsing below partially absorbs this for update-heavy workloads, since
 * repeated writes to one key within a checkpoint fold into a single operation. It does not help
 * when the hot key receives many <em>distinct</em> keys' worth of volume.
 *
 * <p>Mitigation is schema-level: prefer a composite primary key that includes a
 * higher-cardinality column, or salt the key, when the natural key is known to be skewed.
 * Two-level hashing is deliberately not offered — it would break in-key ordering and with it the
 * correctness of the flush sequence.
 *
 * <h3>Consistency model</h3>
 * <p>This sink provides <b>at-least-once</b> semantics, not exactly-once:
 * <ul>
 *   <li>Persistence boundary is the Flink checkpoint (via {@link #snapshotState}), plus {@link
 *       #finish()} once the input is exhausted. The in-memory
 *       buffer is <em>not</em> checkpointed; recovery relies on the upstream source being
 *       replayable (Kafka / Debezium / CDC connectors are fine; unbounded non-replayable sources
 *       such as socket/file will lose in-flight rows on TM failure).</li>
 *   <li>Within a single {@code flush()}, {@code DELETE} operations are applied <b>before</b>
 *       {@code UPSERT} operations, so that a half-failed flush never leaves a superseded row
 *       behind. The per-key collapsing in the buffer additionally ensures replay idempotency for
 *       both operations.</li>
 *   <li>{@code close()} does <b>not</b> flush: Flink invokes {@code close()} on cancellation and
 *       recovery as well, and writing on those paths would violate the "checkpoint is the
 *       persistence boundary" contract. Un-checkpointed rows in the buffer are dropped on close
 *       and will be re-delivered by the source on restart. A graceful end-of-input is handled by
 *       {@code finish()} instead, which Flink calls only on that path; this is what makes a
 *       bounded or batch job durable, since it never takes a checkpoint.</li>
 * </ul>
 *
 * <h3>Concurrency and first-write</h3>
 * <p>The sink uses an <b>open-or-create</b> strategy in {@link #open}: it first tries
 * {@link Dataset#open}, and if that fails it falls back to
 * {@link Dataset#create(BufferAllocator, String, Schema, WriteParams)} which is atomic in the
 * Lance native layer. Concurrent first-writes from multiple subtasks therefore either observe
 * the same dataset (both {@code open} succeeds) or race on {@code create} (one wins, others fall
 * back to {@code open}); either way no {@code Overwrite} clobber can occur.
 *
 * <p>This deliberately replaces the previous {@code Files.exists()} check, which is only correct
 * for the local filesystem and would silently mis-classify remote paths (s3://, tbdsfs://) as
 * non-existent, causing every subtask to re-create and clobber the dataset.
 *
 * <h3>Hot keys and buffering</h3>
 * <p>Because {@code keyBy} routes all events for a given primary key to a single subtask, a
 * skewed key distribution (one dominant key) will bottleneck the whole pipeline on that
 * subtask. Two-level hashing is not applicable — it would break in-key ordering, which the
 * delete-before-upsert per-flush invariant depends on. Callers with a known-skewed natural PK
 * should salt or composite the key at the SQL layer.
 *
 * <p>The per-key buffer is bounded by {@code write.batch-size}: once the collapsed key count
 * reaches the threshold, {@link #flush} is invoked between events (never mid-flush), keeping
 * heap usage predictable in the gap between checkpoints.
 */
public class LanceUpsertSink extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertSink.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final List<String> primaryKeys;
    private final int[] keyIndices;
    private final LogicalType[] keyTypes;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient Schema arrowSchema;
    private transient Schema keyArrowSchema;
    private transient Map<RowData, RowData> buffer;
    private transient long totalWrittenRows;

    public LanceUpsertSink(LanceOptions options, RowType rowType, List<String> primaryKeys, int[] keyIndices) {
        this.options = options;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.keyIndices = keyIndices;
        // Pre-resolve key types once so extractKey doesn't dispatch through rowType per event,
        // and so the buffer key and the keyBy routing key (PrimaryKeySelector) share the exact
        // same projection logic (see PrimaryKeySelector#project).
        this.keyTypes = new LogicalType[keyIndices.length];
        for (int i = 0; i < keyIndices.length; i++) {
            this.keyTypes[i] = rowType.getTypeAt(keyIndices[i]);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }
        LOG.info("Opening Lance Upsert Sink: {}", datasetPath);

        this.allocator = LanceAllocators.create(
                "lance-upsert-sink", options.getArrowAllocatorMaxBytes());
        this.buffer = new LinkedHashMap<>();
        this.totalWrittenRows = 0;
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        // Projection of arrowSchema down to the primary-key columns, used as the source batch for
        // delete. Derived from arrowSchema (rather than converted separately from rowType) so the
        // key fields are guaranteed byte-identical to their counterparts in the full schema — a
        // mismatch there would make the native join silently fail to match.
        this.keyArrowSchema = projectKeySchema(this.arrowSchema);

        // OVERWRITE mode: only supported for local filesystem paths. For remote storage the
        // truncation must be performed at DDL time (e.g. via catalog CREATE OR REPLACE) — the
        // sink cannot reliably drop a remote dataset without an SDK-provided API.
        if (options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            if (isLocalPath(datasetPath)) {
                Path path = Paths.get(datasetPath);
                if (Files.exists(path)) {
                    LOG.info("Overwrite mode, deleting existing local dataset: {}", datasetPath);
                    deleteDirectory(path);
                }
            } else {
                throw new UnsupportedOperationException(
                        "write.mode=overwrite is not supported for remote storage in the upsert "
                                + "sink. Please drop/recreate the table via the catalog before writing. "
                                + "Path: " + datasetPath);
            }
        }

        // Open-or-create: atomic in the Lance native layer, safe under concurrent first-writes.
        this.dataset = openOrCreate(datasetPath);

        // Persist the primary keys into the dataset config. Idempotent; safe to call on every open.
        PrimaryKeyPersistence.persist(dataset, primaryKeys);

        LOG.info("Lance Upsert Sink opened, primary keys: {}", primaryKeys);
    }

    /**
     * Return an open {@link Dataset} handle, creating an empty dataset first if it does not yet
     * exist. This unifies first-write and steady-state paths so that both go through
     * {@code mergeInsert}/{@code delete}, eliminating the previous {@code Overwrite}-based
     * first-write that could clobber peer subtasks' commits.
     */
    private Dataset openOrCreate(String datasetPath) throws IOException {
        try {
            return Dataset.open(datasetPath, allocator);
        } catch (Exception openFailure) {
            LOG.debug("Dataset.open failed ({}), attempting create for: {}",
                    openFailure.getMessage(), datasetPath);
            try {
                return Dataset.create(
                        allocator, datasetPath, arrowSchema, new WriteParams.Builder().build());
            } catch (Exception createFailure) {
                // A peer subtask likely won the create race; try open once more.
                try {
                    return Dataset.open(datasetPath, allocator);
                } catch (Exception reopenFailure) {
                    IOException io = new IOException(
                            "Failed to open or create Lance dataset: " + datasetPath, reopenFailure);
                    io.addSuppressed(createFailure);
                    io.addSuppressed(openFailure);
                    throw io;
                }
            }
        }
    }

    /**
     * Project the full Arrow schema down to just the primary-key fields, preserving their order in
     * {@link #keyIndices}.
     *
     * <p>Fields are taken from the already-built full schema by index so the delete source batch
     * and the table share identical field definitions (type, nullability, metadata). Building the
     * key schema independently would risk a subtle divergence that the native join would express
     * as "nothing matched" rather than as an error.
     */
    private Schema projectKeySchema(Schema fullSchema) {
        List<Field> keyFields = new ArrayList<>(keyIndices.length);
        for (int keyIndex : keyIndices) {
            keyFields.add(fullSchema.getFields().get(keyIndex));
        }
        return new Schema(keyFields);
    }

    @Override
    public void invoke(RowData value, Context context) throws IOException {
        RowKind kind = value.getRowKind();
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
            case DELETE:
                buffer.put(extractKey(value), value);
                break;
            case UPDATE_BEFORE:
                // upsert has no use for the old value
                return;
            default:
                LOG.warn("Ignoring unsupported RowKind: {}", kind);
                return;
        }
        // Bound the buffer: without this, a large gap between checkpoints combined with a wide
        // key space produces unbounded heap growth. This early flush is safe because it happens
        // between events (never mid-flush), so the delete-before-upsert invariant per flush and
        // the per-key collapsing invariant within one flush are both preserved.
        if (buffer.size() >= options.getWriteBatchSize()) {
            flush();
        }
    }

    /**
     * Flush the collapsed per-key buffer to Lance.
     *
     * <p>Deletes are applied <b>before</b> upserts within a flush: this guarantees that if the
     * flush half-fails, the target never contains a stale row that should have been superseded.
     * Combined with per-key collapsing (only the last event per key is kept), the operation is
     * idempotent under upstream replay.
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }

        List<RowData> upserts = new ArrayList<>();
        List<RowData> deletes = new ArrayList<>();
        for (RowData row : buffer.values()) {
            if (row.getRowKind() == RowKind.DELETE) {
                deletes.add(row);
            } else {
                upserts.add(row);
            }
        }

        // Delete first, so a partial failure never leaves a row we intended to remove.
        if (!deletes.isEmpty()) {
            deleteRows(deletes);
        }
        if (!upserts.isEmpty()) {
            mergeInsertRows(upserts);
        }

        totalWrittenRows += upserts.size() + deletes.size();
        buffer.clear();
    }

    /**
     * Apply native upsert via {@code mergeInsert}. Also handles the "insert into empty dataset"
     * case, since the dataset was materialized empty in {@link #open}.
     */
    private void mergeInsertRows(List<RowData> rows) throws IOException {
        MergeInsertParams params = new MergeInsertParams(primaryKeys)
                .withMatchedUpdateAll()
                .withNotMatched(MergeInsertParams.WhenNotMatched.InsertAll);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rows, root);
            MergeInsertResult result =
                    ArrowArrayStreams.mergeInsert(dataset, params, allocator, root);
            adoptResultHandle(result, "upsert");
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to merge-insert Lance rows", e);
        }
    }

    /**
     * Delete rows by primary key using a native key-only {@code mergeInsert}.
     *
     * <p>This replaces the previous {@code Dataset#delete(String)} call that built an
     * {@code (k1 = v1 AND ...) OR (...)} SQL predicate from the buffered rows. That approach had
     * four problems, all of which are structurally eliminated here rather than patched:
     *
     * <ul>
     *   <li><b>Injection surface.</b> Column names were interpolated unescaped and values were
     *       hand-quoted, so a primary key containing SQL metacharacters was only as safe as the
     *       quoting helper. No SQL text is produced at all now.</li>
     *   <li><b>Predicate size.</b> The predicate grew O(N) with the number of deleted keys, so a
     *       large flush produced a multi-megabyte string that had to be parsed and planned. The
     *       keys now travel as a columnar Arrow batch.</li>
     *   <li><b>Type coverage.</b> {@code formatSqlValue} only handled the integral types, float,
     *       double, boolean and VARCHAR; DATE/TIME/TIMESTAMP/DECIMAL/VARBINARY keys threw
     *       {@code UnsupportedOperationException}. Encoding is now delegated to the same
     *       {@link RowDataConverter} used for upserts, so every type the connector can write it
     *       can also delete by.</li>
     *   <li><b>Non-finite floats.</b> {@code NaN}/{@code Infinity} have no valid SQL literal and
     *       needed an explicit rejection. As Arrow values they round-trip natively, so the
     *       special case is gone.</li>
     * </ul>
     *
     * <p>{@code WhenNotMatched.DoNothing} is essential: it makes a delete of an absent key a
     * clean no-op instead of inserting the key-only probe row. Together with the per-key
     * collapsing in {@link #flush} this keeps deletes idempotent under upstream replay.
     */
    private void deleteRows(List<RowData> rows) throws IOException {
        validateNoNullKeys(rows);

        MergeInsertParams params = new MergeInsertParams(primaryKeys)
                .withMatchedDelete()
                .withNotMatched(MergeInsertParams.WhenNotMatched.DoNothing);

        // Only the primary-key columns are sent. The converter looks vectors up by name and skips
        // fields absent from the root, so a key-only root needs no separate conversion path.
        try (VectorSchemaRoot root = VectorSchemaRoot.create(keyArrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rows, root);
            LOG.debug("Deleting {} row(s) by primary key {}", rows.size(), primaryKeys);
            MergeInsertResult result =
                    ArrowArrayStreams.mergeInsert(dataset, params, allocator, root);
            adoptResultHandle(result, "delete");
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to delete Lance rows by primary key", e);
        }
    }

    /**
     * Reject NULL primary-key values before they reach the native layer.
     *
     * <p>Retained from the predicate-based implementation, but for a different reason. Under SQL
     * semantics {@code col = NULL} is UNKNOWN, so a NULL key silently matched nothing. Under
     * {@code mergeInsert} a NULL key would instead participate in join matching and could match
     * other NULL-keyed rows, making the delete over-broad. Both behaviours are wrong, so the
     * value is rejected outright and the failure is attributed to a named column.
     */
    private void validateNoNullKeys(List<RowData> rows) {
        List<String> fieldNames = rowType.getFieldNames();
        for (RowData row : rows) {
            for (int keyIndex : keyIndices) {
                if (row.isNullAt(keyIndex)) {
                    throw new IllegalStateException(
                            "NULL primary-key value is not supported for DELETE on column '"
                                    + fieldNames.get(keyIndex) + "'");
                }
            }
        }
    }

    /**
     * Adopt the post-merge dataset handle returned by {@code mergeInsert}.
     *
     * <p>{@code mergeInsert} does not mutate the handle it is invoked on: it commits a new dataset
     * version and returns a <em>new</em> handle, leaving the original pinned to its old snapshot.
     * Writes are not lost by ignoring the return value (Lance resolves each merge against the
     * latest committed state), but the long-lived {@link #dataset} field would never observe its
     * own prior flush — so any read through it, now or after a future refactor, would silently see
     * stale data. Swapping the field keeps read-your-own-write correct.
     *
     * <p>The superseded handle is closed; failure to close is logged rather than propagated, since
     * the merge itself has already been committed durably.
     */
    private void adoptResultHandle(MergeInsertResult result, String operation) {
        if (result == null) {
            return;
        }
        Dataset updated = result.dataset();
        if (updated == null || updated == dataset) {
            return;
        }
        Dataset superseded = dataset;
        dataset = updated;
        if (superseded != null) {
            try {
                superseded.close();
            } catch (Exception e) {
                LOG.warn("Failed to close superseded dataset handle after {}", operation, e);
            }
        }
    }

    /**
     * Project the primary-key columns into a key that honors equals/hashCode.
     */
    /**
     * Project the primary-key columns into a key that honors equals/hashCode. Delegates to
     * {@link PrimaryKeySelector#project} so the buffer key here is byte-for-byte identical to
     * the {@code keyBy} routing key upstream.
     */
    private RowData extractKey(RowData value) {
        return PrimaryKeySelector.project(value, keyIndices, keyTypes);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("Snapshot state, checkpointId: {}", context.getCheckpointId());
        // Persistence boundary: only checkpoint triggers a flush.
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        LOG.debug("Initialize state, isRestored: {}", context.isRestored());
    }

    @Override
    public void finish() throws Exception {
        // Called once the input is exhausted and only on the normal completion path, unlike
        // close(), which also runs on cancel and failover. A batch job has no checkpoint, so
        // without this the buffered rows of a keyed write would never reach the dataset.
        LOG.info("Input finished, flushing remaining {} buffered key(s)", buffer.size());
        flush();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Lance Upsert Sink");
        // Do NOT flush here: close() runs on cancel/restart as well; writing on those paths
        // would violate the "checkpoint is the persistence boundary" contract. Rows still in
        // buffer are dropped and will be re-delivered by the (replayable) source on restart.
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("Failed to close dataset", e);
            }
            dataset = null;
        }
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close allocator", e);
            }
            allocator = null;
        }
        LOG.info("Lance Upsert Sink closed, total written {} rows", totalWrittenRows);
        super.close();
    }

    /**
     * Whether the given path denotes the local filesystem (as opposed to s3://, tbdsfs://, hdfs://
     * etc.). A path with no scheme, or the explicit {@code file:} scheme, is considered local.
     */
    private static boolean isLocalPath(String path) {
        try {
            URI uri = new URI(path);
            String scheme = uri.getScheme();
            return scheme == null || "file".equalsIgnoreCase(scheme);
        } catch (URISyntaxException e) {
            // A parse failure means it's almost certainly a plain local path.
            return true;
        }
    }

    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (java.util.stream.Stream<Path> children = Files.list(path)) {
                children.forEach(child -> {
                    try {
                        deleteDirectory(child);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete file: " + child, e);
                    }
                });
            }
        }
        Files.deleteIfExists(path);
    }

    public RowType getRowType() {
        return rowType;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public long getTotalWrittenRows() {
        return totalWrittenRows;
    }
}
