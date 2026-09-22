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

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Extracts the primary-key projection of a {@link RowData} so that Flink can route all events
 * sharing the same key to the same subtask via {@code DataStream#keyBy}.
 *
 * <p>The returned key is a freshly allocated {@link GenericRowData} (which implements
 * {@code equals}/{@code hashCode}) holding only the primary-key columns, preserving their order.
 */
public class PrimaryKeySelector implements KeySelector<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final int[] keyIndices;
    private final LogicalType[] keyTypes;

    public PrimaryKeySelector(int[] keyIndices, LogicalType[] keyTypes) {
        if (keyIndices == null || keyIndices.length == 0) {
            throw new IllegalArgumentException("Primary-key indices must not be empty");
        }
        if (keyTypes == null || keyTypes.length != keyIndices.length) {
            throw new IllegalArgumentException("Primary-key types must match the key indices");
        }
        this.keyIndices = keyIndices;
        this.keyTypes = keyTypes;
    }

    @Override
    public RowData getKey(RowData value) {
        GenericRowData key = new GenericRowData(keyIndices.length);
        for (int i = 0; i < keyIndices.length; i++) {
            key.setField(i, RowDataFieldAccessor.readField(value, keyIndices[i], keyTypes[i]));
        }
        return key;
    }
}
