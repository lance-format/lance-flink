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
 * Lance source (Flink 2.x version, based on FLIP-27 Source V2).
 *
 * <p>The constructor and Builder signatures are deliberately kept compatible with the old 1.x
 * implementation so that upper-layer callers such as
 * {@link org.apache.flink.connector.lance.table.LanceDynamicTableSource} can compile against both
 * Flink versions without changes in the shared source.
 *
 * <p>Usage example:
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
        // The Lance connector currently only supports batch-mode reads
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

    // ---------- getters (kept consistent with the old implementation's public contract) ----------

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
     * {@code TypeInformation} export hook (some callers may need it). Left empty here so that
     * callers of {@link org.apache.flink.table.runtime.typeutils.InternalTypeInfo#of(RowType)}
     * construct it themselves; keeping API compatibility is sufficient.
     */
    @SuppressWarnings("unused")
    private TypeInformation<RowData> producedTypeInfo() {
        return null;
    }

    // ---------- Builder ----------

    public static Builder builder() {
        return new Builder();
    }

    /** Builder with signatures consistent with the old implementation. */
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
