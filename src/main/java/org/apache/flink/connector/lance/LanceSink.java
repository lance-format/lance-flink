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
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.FragmentOperation;
import com.lancedb.lance.WriteParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Lance Sink 实现。
 * 
 * <p>将 Flink RowData 写入 Lance 数据集，支持批量写入和 Checkpoint。
 * 
 * <p>使用示例：
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .writeBatchSize(1024)
 *     .writeMode(WriteMode.APPEND)
 *     .build();
 * 
 * LanceSink sink = new LanceSink(options, rowType);
 * dataStream.addSink(sink);
 * }</pre>
 */
public class LanceSink extends RichSinkFunction<RowData> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceSink.class);

    private final LanceOptions options;
    private final RowType rowType;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient Schema arrowSchema;
    private transient List<RowData> buffer;
    private transient long totalWrittenRows;
    private transient boolean datasetExists;
    private transient boolean isFirstWrite;

    /**
     * 创建 LanceSink
     *
     * @param options Lance 配置选项
     * @param rowType Flink RowType
     */
    public LanceSink(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("打开 Lance Sink: {}", options.getPath());
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.buffer = new ArrayList<>(options.getWriteBatchSize());
        this.totalWrittenRows = 0;
        this.isFirstWrite = true;
        
        // 初始化转换器和 Schema
        this.converter = new RowDataConverter(rowType);
        this.arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        
        // 检查数据集是否存在
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance 数据集路径不能为空");
        }
        
        Path path = Paths.get(datasetPath);
        this.datasetExists = Files.exists(path);
        
        // 如果是覆盖模式且数据集存在，先删除
        if (datasetExists && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
            LOG.info("覆盖模式，删除现有数据集: {}", datasetPath);
            deleteDirectory(path);
            this.datasetExists = false;
        }
        
        LOG.info("Lance Sink 已打开，Schema: {}", rowType);
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        buffer.add(value);
        
        // 当缓冲区达到批次大小时，执行写入
        if (buffer.size() >= options.getWriteBatchSize()) {
            flush();
        }
    }

    /**
     * 刷新缓冲区，将数据写入 Lance 数据集
     */
    public void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        
        LOG.debug("刷新缓冲区，行数: {}", buffer.size());
        
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            // 将 RowData 转换为 VectorSchemaRoot
            converter.toVectorSchemaRoot(buffer, root);
            
            String datasetPath = options.getPath();
            
            // 构建写入参数
            WriteParams writeParams = new WriteParams.Builder()
                    .withMaxRowsPerFile(options.getWriteMaxRowsPerFile())
                    .build();
            
            // 创建 Fragment
            List<FragmentMetadata> fragments = Fragment.create(
                    datasetPath,
                    allocator,
                    root,
                    writeParams
            );
            
            if (!datasetExists) {
                // 创建新数据集（使用 Overwrite 操作）
                FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, arrowSchema);
                dataset = overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                datasetExists = true;
                isFirstWrite = false;
                LOG.info("创建新数据集: {}", datasetPath);
            } else {
                // 追加数据
                if (isFirstWrite && options.getWriteMode() == LanceOptions.WriteMode.OVERWRITE) {
                    // 第一次写入且为覆盖模式
                    FragmentOperation.Overwrite overwrite = new FragmentOperation.Overwrite(fragments, arrowSchema);
                    dataset = overwrite.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                    isFirstWrite = false;
                } else {
                    // 追加模式
                    FragmentOperation.Append append = new FragmentOperation.Append(fragments);
                    dataset = append.commit(allocator, datasetPath, Optional.empty(), Collections.emptyMap());
                }
            }
            
            totalWrittenRows += buffer.size();
            LOG.debug("已写入 {} 行，总计: {} 行", buffer.size(), totalWrittenRows);
            
            buffer.clear();
        } catch (Exception e) {
            throw new IOException("写入 Lance 数据集失败", e);
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭 Lance Sink");
        // 刷新剩余数据
        try {
            flush();
        } catch (Exception e) {
            LOG.warn("关闭时刷新数据失败", e);
        }
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("关闭数据集失败", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭分配器失败", e);
            }
            allocator = null;
        }
        
        LOG.info("Lance Sink 已关闭，总计写入 {} 行", totalWrittenRows);
        
        super.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.debug("快照状态，checkpointId: {}", context.getCheckpointId());
        
        // 在 Checkpoint 时刷新所有缓冲数据
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.debug("初始化状态，isRestored: {}", context.isRestored());
        // 状态初始化（如果需要恢复）
    }

    /**
     * 获取 RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * 获取配置选项
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * 获取已写入的总行数
     */
    public long getTotalWrittenRows() {
        return totalWrittenRows;
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("删除文件失败: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    /**
     * Builder 模式构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSink 构建器
     */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private LanceOptions.WriteMode writeMode = LanceOptions.WriteMode.APPEND;
        private int maxRowsPerFile = 1000000;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder writeMode(LanceOptions.WriteMode writeMode) {
            this.writeMode = writeMode;
            return this;
        }

        public Builder maxRowsPerFile(int maxRowsPerFile) {
            this.maxRowsPerFile = maxRowsPerFile;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSink build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("数据集路径不能为空");
            }
            
            if (rowType == null) {
                throw new IllegalArgumentException("RowType 不能为空");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .writeBatchSize(batchSize)
                    .writeMode(writeMode)
                    .writeMaxRowsPerFile(maxRowsPerFile)
                    .build();

            return new LanceSink(options, rowType);
        }
    }
}
