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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lance 的 keyed upsert {@link CommittingSinkWriter}（Flink 2.1 2PC Writer 契约）。
 *
 * <p>与 append 路径的 {@link LanceSinkWriter}（物化 fragment）不同，本 writer 把
 * {@link RowData} 按 {@link RowKind} 分成两批，在 {@link #prepareCommit()} 时用
 * Arrow IPC stream 序列化（{@link ArrowIpcCodec}）后打包进 {@link LanceUpsertCommittable}，
 * 由 {@link LanceUpsertCommitter} 统一执行 {@code Dataset.mergeInsert}。
 *
 * <p>RowKind 处理：
 * <ul>
 *   <li>{@code INSERT}/{@code UPDATE_AFTER} → {@code upsertBatch}（UpdateAll + InsertAll）；</li>
 *   <li>{@code DELETE} → {@code deleteBatch}（Delete + DoNothing）；</li>
 *   <li>{@code UPDATE_BEFORE} → 丢弃（upsert 不需要旧值）。</li>
 * </ul>
 *
 * <p>Writer 无状态：行只缓冲在内存，checkpoint 时随 committable 进入 checkpoint 状态，
 * commit 成功后 runtime 不再回放；失败则回放 committable 重试，得益于
 * {@code mergeInsert} 的幂等性，Exactly-Once 成立。
 */
public class LanceUpsertWriter
        implements CommittingSinkWriter<RowData, LanceUpsertCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceUpsertWriter.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final List<String> primaryKeys;
    private final int subtaskId;

    private final BufferAllocator allocator;
    private final RowDataConverter converter;
    private final Schema arrowSchema;

    private final List<RowData> upsertBuffer = new ArrayList<>();
    private final List<RowData> deleteBuffer = new ArrayList<>();

    private long totalRowsWritten;

    public LanceUpsertWriter(
            LanceOptions options, RowType rowType, List<String> primaryKeys, int subtaskId) {
        this.options = options;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys == null ? Collections.emptyList() : primaryKeys;
        this.subtaskId = subtaskId;

        String path = options.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Lance dataset path cannot be empty");
        }
        if (this.primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("Lance upsert sink requires at least one primary key");
        }

        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);

        LOG.info(
                "LanceUpsertWriter[{}] opened: path={}, primaryKeys={}",
                subtaskId,
                path,
                this.primaryKeys);
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        RowKind kind = element.getRowKind();
        switch (kind) {
            case INSERT:
            case UPDATE_AFTER:
                upsertBuffer.add(element);
                break;
            case DELETE:
                deleteBuffer.add(element);
                break;
            case UPDATE_BEFORE:
                // upsert has no use for the old value
                break;
            default:
                LOG.warn("LanceUpsertWriter[{}] ignoring unsupported RowKind: {}", subtaskId, kind);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // 无状态：行保留在 buffer，prepareCommit 时序列化；flush 无需动作。
    }

    @Override
    public Collection<LanceUpsertCommittable> prepareCommit()
            throws IOException, InterruptedException {
        if (upsertBuffer.isEmpty() && deleteBuffer.isEmpty()) {
            return Collections.emptyList();
        }

        byte[] upsertBatch = serializeBatch(upsertBuffer);
        byte[] deleteBatch = serializeBatch(deleteBuffer);

        int upsertCount = upsertBuffer.size();
        int deleteCount = deleteBuffer.size();
        upsertBuffer.clear();
        deleteBuffer.clear();
        totalRowsWritten += upsertCount + deleteCount;

        LanceUpsertCommittable committable =
                new LanceUpsertCommittable(
                        options.getPath(),
                        arrowSchema,
                        primaryKeys,
                        options.getWriteMode(),
                        upsertBatch,
                        deleteBatch);

        LOG.debug(
                "LanceUpsertWriter[{}] prepareCommit -> upsert={}, delete={}, totalRows={}",
                subtaskId,
                upsertCount,
                deleteCount,
                totalRowsWritten);
        return Collections.singletonList(committable);
    }

    /**
     * 把一批 RowData 转成 VectorSchemaRoot 并序列化为 Arrow IPC stream 字节。
     */
    private byte[] serializeBatch(List<RowData> rows) throws IOException {
        if (rows.isEmpty()) {
            return new byte[0];
        }
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            converter.toVectorSchemaRoot(rows, root);
            return ArrowIpcCodec.serialize(root);
        } catch (Exception e) {
            throw new IOException("Failed to serialize upsert row batch", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info(
                "Closing LanceUpsertWriter[{}], totalRowsWritten={}",
                subtaskId,
                totalRowsWritten);
        try {
            allocator.close();
        } catch (Exception e) {
            LOG.warn("Error closing BufferAllocator in LanceUpsertWriter", e);
        }
    }
}
