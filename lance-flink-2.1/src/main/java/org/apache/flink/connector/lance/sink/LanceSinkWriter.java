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
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.WriteParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Lance 的 {@link CommittingSinkWriter}（Flink 2.1 的 2PC Writer 契约）。
 *
 * <p>负责 2PC 的「预提交」阶段：把输入的 {@link RowData} 按 batchSize 攒批，调用
 * {@link Fragment#write()} 写出 Lance 数据文件（产生 {@link FragmentMetadata}），
 * 但**不**调用 CommitBuilder/Transaction —— 提交由 {@link LanceCommitter} 统一完成。
 *
 * <p>{@code createDataset} 标志的决定逻辑：
 * <ul>
 *   <li>首次 write 之前，若目标路径不存在 → 本 subtask 在本 checkpoint 窗口的 committable
 *       打 {@code createDataset=true}，Committer 用 Overwrite 建表；
 *   <li>若 WriteMode=OVERWRITE 且目标已存在 → 等价于「先删后建」，同样首个 committable 打
 *       {@code createDataset=true}；这里采用更保守的策略：保留旧实现「打开前删除整个目录」
 *       的副作用，避免 Committer 需要在多并行度下协商谁来删。
 *   <li>之后所有 committable（包括本 subtask 的后续窗口、其它 subtask 的任意窗口）全部
 *       {@code createDataset=false} 走 Append。
 * </ul>
 *
 * <p>Fragment 真正写出到磁盘的时机有二：
 * <ol>
 *   <li>buffer 大小达到 {@code write.batch-size} —— 对应旧实现的隐式 flush；
 *   <li>Flink runtime 调用 {@link #flush(boolean)} 或 {@link #prepareCommit()}
 *       —— 对应 checkpoint barrier 或 end-of-input。
 * </ol>
 */
public class LanceSinkWriter implements CommittingSinkWriter<RowData, LanceCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceSinkWriter.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final int subtaskId;

    private final BufferAllocator allocator;
    private final RowDataConverter converter;
    private final Schema arrowSchema;

    // 当前 checkpoint 窗口内已 flush 产生、待 commit 的 fragments
    private final List<FragmentMetadata> pendingFragments = new ArrayList<>();
    // 行级 buffer：满 batchSize 或 flush 时触发 Lance 写
    private final List<RowData> rowBuffer;

    /**
     * 本 subtask 目前为止是否已写出过 fragment：
     * 若本 subtask 从未写过且 dataset 原本不存在，则**本窗口首个 committable** 要打 createDataset=true。
     */
    private boolean subtaskHasWritten;

    /**
     * 本次 sink 实例（作业启动时）判定目标 dataset 是否已存在。
     * 配合 {@link #subtaskHasWritten} 决定首个 committable 是 Overwrite 还是 Append。
     */
    private final boolean datasetExistedAtStart;

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

        // Overwrite 模式：旧实现在 open() 里会把目标路径递归删除——本写法保留该语义。
        // 仅 subtask 0 执行删除，避免多并发删除竞争；其它 subtask 稍后感知为「dataset 不存在」。
        boolean existedAtStart;
        try {
            java.nio.file.Path p = Paths.get(path);
            if (subtaskId == 0
                    && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE
                    && Files.exists(p)) {
                LOG.info("Overwrite mode, deleting existing dataset: {}", path);
                deleteDirectory(p);
            }
            existedAtStart = Files.exists(p);
        } catch (Exception e) {
            LOG.warn(
                    "Cannot test existence of path {} (treating as nonexistent): {}",
                    path,
                    e.toString());
            existedAtStart = false;
        }
        this.datasetExistedAtStart = existedAtStart;

        LOG.info(
                "LanceSinkWriter[{}] opened: path={}, batchSize={}, datasetExistedAtStart={}",
                subtaskId,
                path,
                options.getWriteBatchSize(),
                datasetExistedAtStart);
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

        // 本 subtask 本窗口的第一个 committable 是否承担 createDataset 职责：
        //   - 作业启动时 dataset 不存在（含 Overwrite 已删）
        //   - 并且本 subtask 之前没写过（这里简化：只要本次 pendingFragments 非空，就当「本 subtask 首个窗口」）
        //   - 为避免多 subtask 竞争建表，只允许 subtask 0 打 createDataset=true，其他 subtask 一律 append。
        //     多 subtask 并发首次写同一张表时，subtask 0 的 Overwrite 先提交，其余 Append 排在其后，
        //     由 Lance 的 CommitBuilder 做乐观并发重试。
        boolean createDataset =
                !datasetExistedAtStart && !subtaskHasWritten && subtaskId == 0;

        LanceCommittable committable =
                new LanceCommittable(
                        options.getPath(),
                        createDataset ? arrowSchema : null,
                        createDataset,
                        new ArrayList<>(pendingFragments));
        pendingFragments.clear();
        subtaskHasWritten = true;

        LOG.debug(
                "LanceSinkWriter[{}] prepareCommit -> {}", subtaskId, committable);
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
        // 注意：prepareCommit 之后 runtime 才会调 close；此时 pendingFragments 应已空。
        // 若非空（例如异常路径），这些 fragment 的数据文件已落盘但未被 commit，
        // 会成为孤儿文件；交由后续 Lance compaction/cleanup 清理。
        try {
            allocator.close();
        } catch (Exception e) {
            LOG.warn("Error closing BufferAllocator", e);
        }
    }

    /** 递归删除目录——保留与旧 LanceSink 行为一致的 Overwrite 清理语义。 */
    private static void deleteDirectory(java.nio.file.Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (java.util.stream.Stream<java.nio.file.Path> stream = Files.list(path)) {
                for (java.nio.file.Path child : (Iterable<java.nio.file.Path>) stream::iterator) {
                    deleteDirectory(child);
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
