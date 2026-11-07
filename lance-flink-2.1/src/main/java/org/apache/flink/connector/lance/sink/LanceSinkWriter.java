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
import org.lance.Dataset;
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
 * Lance 的 {@link CommittingSinkWriter}（Flink 2.1 的 2PC Writer 契约）。
 *
 * <p>B1 修复策略（Writer 几乎无状态 + OVERWRITE 预清理前移）：
 * <ul>
 *   <li>Writer 构造时**仅** subtask 0 执行一次性 OVERWRITE 预清理，而且是**在任何
 *       {@code Fragment.write()} 落盘之前完成**。这是 Lance 存储模型的物理约束：
 *       {@code Dataset.drop(path)} 会把 {@code <path>/data/} 下已经由 Writer
 *       物化的 {@code .lance} 文件一并清掉；如果把 drop 放到 Committer 阶段，
 *       同一次 commit 的 fragments 清单会指向被 drop 删掉的 {@code .lance}，
 *       导致后续 scan 报 "no such file"。
 *   <li>Writer **不再**判定 "是否首次写入" / "是否 createDataset"——这些决策统一交给
 *       {@link LanceCommitter}，它基于 {@link Dataset#open(String)} 探测目标是否
 *       有 manifest，天然串行且跨 subtask 一致。
 *   <li>每个 committable 都带上 {@code schema}，让 Committer 不管何时首次 commit
 *       都能用它建表（failover 后新 Writer 实例构造出的 committable 同样可建表）。
 * </ul>
 *
 * <p><b>孤儿文件防护（#87）</b>：本类不再在 {@link #write(RowData, Context)} 内因
 * {@code write.batch-size} 满而物化 {@code .lance} 文件；行先缓冲在内存，物化统一延迟到
 * {@link #flush(boolean)} / {@link #prepareCommit()}。这样 TM 在 {@code write()} 与
 * {@code prepareCommit()} 之间 crash 时，磁盘上还没有任何文件可成为孤儿，writer 保持无状态。
 * 仅当估算的内存缓冲超过 {@code write.memory-cap-bytes} 时才会提前物化（OOM 安全阀，
 * 会重新打开一个窄的 orphan 窗口；默认 -1 无上限，永不提前物化）。
 */
public class LanceSinkWriter implements CommittingSinkWriter<RowData, LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSinkWriter.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final int subtaskId;

    private final BufferAllocator allocator;
    private final RowDataConverter converter;
    private final Schema arrowSchema;

    /** 当前 checkpoint 窗口内已 flush 产生、待 commit 的 fragments。 */
    private final List<FragmentMetadata> pendingFragments = new ArrayList<>();

    /** 行级内存 buffer：仅在 flush/prepareCommit 或 memory-cap 触发时物化。 */
    private final List<RowData> rowBuffer;

    /** 每行的估算字节数（基于 rowType 字段类型的一次性估算，用于 memory-cap 触发）。 */
    private final long rowBytesEstimate;

    /** 当前 rowBuffer 的累计估算字节数。 */
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

        // OVERWRITE 预清理：必须发生在 Fragment.write() 之前。只允许 subtask 0 执行，
        // 其它 subtask 可能稍晚启动，等它们开始 write 时 drop 已完成。这里走 lance 侧
        // Dataset.drop 以兼容本地 FS / HDFS / S3（Lance JNI 内部处理）。
        if (subtaskId == 0 && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            try (Dataset ignored = Dataset.open(path, allocator)) {
                LOG.info(
                        "OVERWRITE mode, dropping existing dataset at {} (subtask 0, pre-write cleanup)",
                        path);
                Dataset.drop(path, Collections.emptyMap());
            } catch (Throwable t) {
                // dataset 不存在是常态，不记日志以降低噪声；drop 其它错误则 warn
                LOG.debug("OVERWRITE pre-cleanup skip (dataset not open-able at {}): {}", path, t.toString());
            }
        }

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

        // #87: 不再因 write.batch-size 满而物化；仅当内存缓冲超过 write.memory-cap-bytes
        // 时提前物化（OOM 安全阀）。默认 -1（无上限）→ write() 阶段永不物化，彻底消除
        // write() 与 prepareCommit() 之间的 orphan 文件窗口。
        long memoryCap = options.getWriteMemoryCapBytes();
        if (memoryCap >= 0 && estimatedBufferedBytes >= memoryCap) {
            writeBufferAsFragment();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        // Flink runtime 在 checkpoint barrier 或 end-of-input 时调用；
        // 把行级 buffer 清空成 fragment，后续 prepareCommit 一次性打包。
        writeBufferAsFragment();
    }

    @Override
    public Collection<LanceCommittable> prepareCommit() throws IOException, InterruptedException {
        // 兜底：某些 runtime 版本可能跳过 flush 直接走 prepareCommit
        writeBufferAsFragment();

        if (pendingFragments.isEmpty()) {
            return Collections.emptyList();
        }

        // 无状态打包：每个 committable 都带上 schema 和 writeMode，由 Committer 决定
        // Overwrite 建表 vs Append。Writer 侧不再做 createDataset 判定。
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
     * 把 rowBuffer 里的数据写成一个 Lance Fragment 落盘，产生 FragmentMetadata；
     * 不做 commit。
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
     * 估算单行的逻辑字节数（用于 memory-cap 触发判断，非精确值）。基于 rowType 的字段
     * 类型给每个字段一个固定估算，可变长类型（字符串/二进制/复合类型等）取保守默认值。
     */
    private static long estimateRowBytes(RowType rowType) {
        long bytes = 16; // RowData 对象本身 + 内部数组的固定 overhead
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
                return 32; // 可变长类型取保守估算
            default:
                // ARRAY / MAP / ROW / MULTISET / RAW / DISTINCT / STRUCTURED_TYPE / NULL 等
                return 64; // 复合类型取保守估算
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
