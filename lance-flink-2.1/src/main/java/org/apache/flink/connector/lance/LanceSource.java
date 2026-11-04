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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.source.LanceEnumState;
import org.apache.flink.connector.lance.source.LanceEnumStateSerializer;
import org.apache.flink.connector.lance.source.LanceSourceReader;
import org.apache.flink.connector.lance.source.LanceSplit;
import org.apache.flink.connector.lance.source.LanceSplitEnumerator;
import org.apache.flink.connector.lance.source.LanceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * Lance 数据源（Flink 2.x 版本，基于 FLIP-27 Source V2）。
 *
 * <p>构造函数与 Builder 签名刻意保持与旧 1.x 实现兼容，使得上层
 * {@link org.apache.flink.connector.lance.table.LanceDynamicTableSource} 等调用方
 * 在同一份共享源码下无需变更即可编译到两套 Flink 版本。
 *
 * <p>使用示例：
 * <pre>{@code
 * LanceSource source = LanceSource.builder()
 *     .path("/path/to/lance/dataset")
 *     .batchSize(1024)
 *     .limit(100L)
 *     .rowType(rowType)
 *     .build();
 * DataStream<RowData> stream = env.fromSource(
 *     source, WatermarkStrategy.noWatermarks(), "lance-source");
 * }</pre>
 */
public class LanceSource implements Source<RowData, LanceSplit, LanceEnumState> {

    private static final long serialVersionUID = 1L;

    private final LanceOptions options;
    private final RowType rowType;

    public LanceSource(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
    }

    public LanceSource(LanceOptions options) {
        this(options, null);
    }

    @Override
    public Boundedness getBoundedness() {
        // Lance connector 当前仅支持批模式读
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, LanceSplit> createReader(SourceReaderContext readerContext) {
        return new LanceSourceReader(readerContext, options, rowType);
    }

    @Override
    public SplitEnumerator<LanceSplit, LanceEnumState> createEnumerator(
            SplitEnumeratorContext<LanceSplit> enumContext) {
        return new LanceSplitEnumerator(enumContext, options, null);
    }

    @Override
    public SplitEnumerator<LanceSplit, LanceEnumState> restoreEnumerator(
            SplitEnumeratorContext<LanceSplit> enumContext, LanceEnumState checkpoint) {
        return new LanceSplitEnumerator(enumContext, options, checkpoint.getRemainingSplits());
    }

    @Override
    public SimpleVersionedSerializer<LanceSplit> getSplitSerializer() {
        return LanceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<LanceEnumState> getEnumeratorCheckpointSerializer() {
        return LanceEnumStateSerializer.INSTANCE;
    }

    // ---------- getters（保持与旧实现一致的对外契约） ----------

    public RowType getRowType() {
        return rowType;
    }

    public LanceOptions getOptions() {
        return options;
    }

    public String[] getSelectedColumns() {
        List<String> cols = options.getReadColumns();
        return cols != null && !cols.isEmpty() ? cols.toArray(new String[0]) : null;
    }

    /**
     * {@code TypeInformation} 导出接口（部分调用方可能需要）。此处留空由
     * {@link org.apache.flink.table.runtime.typeutils.InternalTypeInfo#of(RowType)}
     * 的调用方自行构造；保持 API 兼容性即可。
     */
    @SuppressWarnings("unused")
    private TypeInformation<RowData> producedTypeInfo() {
        return null;
    }

    // ---------- Builder ----------

    public static Builder builder() {
        return new Builder();
    }

    /** 与旧实现签名一致的 Builder。 */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private List<String> columns;
        private String filter;
        private Long limit;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder limit(Long limit) {
            this.limit = limit;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSource build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Dataset path cannot be empty");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .readBatchSize(batchSize)
                    .readColumns(columns)
                    .readFilter(filter)
                    .readLimit(limit)
                    .build();

            return new LanceSource(options, rowType);
        }
    }
}
