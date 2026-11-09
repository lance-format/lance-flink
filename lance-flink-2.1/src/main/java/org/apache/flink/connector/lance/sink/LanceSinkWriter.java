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

import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lance's {@link CommittingSinkWriter} (the Flink 2.1 2PC writer contract).
 *
 * <p>The writer is stateless and only materializes RowData batches into Lance fragment data files:
 *
 * <ul>
 *   <li>It performs <b>no</b> OVERWRITE pre-cleanup (no {@code Dataset.drop}) and no
 *       "first write / create the dataset" decision — those are delegated to the serial
 *       {@link LanceCommitter}, which probes the target's manifest and tags every version it
 *       commits with the {@code flink.job-id} transaction property to make the overwrite happen
 *       once per job.
 *   <li>Every committable carries {@code schema} and {@code writeMode}, so the committer can always
 *       create the table on first commit (a committable built by a new writer instance after
 *       failover can too).
 * </ul>
 *
 * <p><b>Orphan-file protection (#87)</b>: this class no longer materializes {@code .lance} files
 * inside {@link #write(RowData, Context)} when {@code write.batch-size} is reached; rows are
 * buffered in memory first, and materialization is deferred uniformly to
 * {@link #flush(boolean)} / {@link #prepareCommit()}. As a result, when the TM crashes between
 * {@code write()} and {@code prepareCommit()}, there is no file on disk that can become an orphan,
 * and the writer stays stateless. Materialization only happens early when the estimated in-memory
 * buffer exceeds {@code write.memory-cap-bytes} (the OOM safety valve, which reopens a narrow
 * orphan window; the default -1 means unbounded, so write() never materializes).
 */
public class LanceSinkWriter implements CommittingSinkWriter<RowData, LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSinkWriter.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final int subtaskId;

    private final BufferAllocator allocator;
    private final RowDataConverter converter;
    private final Schema arrowSchema;

    /** Fragments flushed and pending commit within the current checkpoint window. */
    private final List<FragmentMetadata> pendingFragments = new ArrayList<>();

    /** Row-level in-memory buffer: materialized only on flush/prepareCommit or memory-cap. */
    private final List<RowData> rowBuffer;

    /** Estimated bytes per row (one-time estimate from rowType field types, for memory-cap). */
    private final long rowBytesEstimate;

    /** Cumulative estimated bytes currently buffered in rowBuffer. */
    private long estimatedBufferedBytes;

    private long totalRowsWritten;

    public LanceSinkWriter(LanceOptions options, RowType rowType, int subtaskId) {
        this.options = options;
        this.rowType = rowType;
        this.subtaskId = subtaskId;

        String path = options.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }

        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        this.rowBuffer = new ArrayList<>(options.getWriteBatchSize());
        this.rowBytesEstimate = estimateRowBytes(rowType);

        LOG.info(
                "LanceSinkWriter[{}] opened: path={}, batchSize={}, writeMode={}",
                subtaskId,
                path,
                options.getWriteBatchSize(),
                options.getWriteMode());
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        rowBuffer.add(element);
        estimatedBufferedBytes += rowBytesEstimate;

        // #87: no longer materialize when write.batch-size is reached; only materialize early
        // when the in-memory buffer exceeds write.memory-cap-bytes (the OOM safety valve). The
        // default -1 (unbounded) means write() never materializes, fully eliminating the orphan
        // file window between write() and prepareCommit().
        long memoryCap = options.getWriteMemoryCapBytes();
        if (memoryCap >= 0 && estimatedBufferedBytes >= memoryCap) {
            writeBufferAsFragment();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // Flink runtime calls this at a checkpoint barrier or end-of-input;
        // drain the row-level buffer into a fragment, then prepareCommit packages it in one shot.
        writeBufferAsFragment();
    }

    @Override
    public Collection<LanceCommittable> prepareCommit() throws IOException, InterruptedException {
        // Fallback: some runtime versions may skip flush and go straight to prepareCommit
        writeBufferAsFragment();

        if (pendingFragments.isEmpty()) {
            return Collections.emptyList();
        }

        // Stateless packaging: every committable carries schema and writeMode, letting the
        // Committer decide Overwrite-create vs Append. The writer no longer does createDataset.
        LanceCommittable committable =
                new LanceCommittable(
                        options.getPath(),
                        arrowSchema,
                        options.getWriteMode(),
                        new ArrayList<>(pendingFragments));
        pendingFragments.clear();

        LOG.debug("LanceSinkWriter[{}] prepareCommit -> {}", subtaskId, committable);
        return Collections.singletonList(committable);
    }

    /**
     * Writes the data in rowBuffer to disk as a Lance Fragment, producing FragmentMetadata;
     * does not commit.
     */
    private void writeBufferAsFragment() throws IOException {
        if (rowBuffer.isEmpty()) {
            return;
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rowBuffer, root);

            WriteParams writeParams =
                    new WriteParams.Builder()
                            .withMaxRowsPerFile(options.getWriteMaxRowsPerFile())
                            .build();

            List<FragmentMetadata> fragments =
                    Fragment.write()
                            .datasetUri(options.getPath())
                            .allocator(allocator)
                            .data(root)
                            .writeParams(writeParams)
                            .execute();

            pendingFragments.addAll(fragments);
            totalRowsWritten += rowBuffer.size();
            LOG.debug(
                    "LanceSinkWriter[{}] wrote fragment: rows={}, totalRows={}, newFragments={}",
                    subtaskId,
                    rowBuffer.size(),
                    totalRowsWritten,
                    fragments.size());
        } catch (Exception e) {
            throw new IOException(
                    "Failed to write Lance fragment for path " + options.getPath(), e);
        } finally {
            rowBuffer.clear();
            estimatedBufferedBytes = 0;
        }
    }

    /**
     * Estimates the logical byte size of a single row (used for the memory-cap trigger decision,
     * not an exact value). Each field gets a fixed estimate based on its rowType field type, with
     * conservative defaults for variable-length types (string/binary/composite etc.).
     */
    private static long estimateRowBytes(RowType rowType) {
        long bytes = 16; // fixed overhead of the RowData object itself + its internal array
        for (LogicalType type : rowType.getChildren()) {
            bytes += estimateTypeBytes(type.getTypeRoot());
        }
        return Math.max(1, bytes);
    }

    private static long estimateTypeBytes(LogicalTypeRoot root) {
        switch (root) {
            case TINYINT:
            case BOOLEAN:
                return 1;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return 4;
            case BIGINT:
            case DOUBLE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return 8;
            case DECIMAL:
                return 16;
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return 32; // conservative estimate for variable-length types
            default:
                // ARRAY / MAP / ROW / MULTISET / RAW / DISTINCT / STRUCTURED_TYPE / NULL etc.
                return 64; // conservative estimate for composite types
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info(
                "Closing LanceSinkWriter[{}], totalRowsWritten={}, pendingFragmentsNotCommitted={}",
                subtaskId,
                totalRowsWritten,
                pendingFragments.size());
        try {
            allocator.close();
        } catch (Exception e) {
            LOG.warn("Error closing BufferAllocator", e);
        }
    }
}
