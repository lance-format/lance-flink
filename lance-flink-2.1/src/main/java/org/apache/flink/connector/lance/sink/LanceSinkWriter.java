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
 * <p>孤儿文件：{@link Fragment#write()} 落盘的 {@code .lance} 文件在 TM crash 的
 * pre-commit 窗口内不被 commit 时会成为孤儿；交由 Lance 外部 compaction/cleanup
 * 清理。此行为与根 src/1.x 版 {@code LanceSink} 完全一致。
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

    /** 行级 buffer：满 batchSize 或 flush 时触发 Lance 写。 */
    private final List<RowData> rowBuffer;

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
        if (rowBuffer.size() >= options.getWriteBatchSize()) {
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
