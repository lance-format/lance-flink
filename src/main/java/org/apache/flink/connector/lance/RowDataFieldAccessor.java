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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VarBinaryType;

/**
 * Type-aware field accessor for {@link RowData}.
 *
 * <p>{@link RowData} exposes typed getters rather than a generic {@code getField(int)}, so this
 * helper centralizes the dispatch for the scalar types commonly used as primary-key columns.
 */
public final class RowDataFieldAccessor {

    private RowDataFieldAccessor() {
        // utility class
    }

    /**
     * Read a field value honoring nullability.
     *
     * @return the boxed value, or {@code null} if the field is null
     * @throws UnsupportedOperationException for types not supported as a primary-key column
     */
    public static Object readField(RowData row, int index, LogicalType type) {
        if (row.isNullAt(index)) {
            return null;
        }
        if (type instanceof BooleanType) {
            return row.getBoolean(index);
        }
        if (type instanceof TinyIntType) {
            return row.getByte(index);
        }
        if (type instanceof SmallIntType) {
            return row.getShort(index);
        }
        if (type instanceof IntType) {
            return row.getInt(index);
        }
        if (type instanceof BigIntType) {
            return row.getLong(index);
        }
        if (type instanceof FloatType) {
            return row.getFloat(index);
        }
        if (type instanceof DoubleType) {
            return row.getDouble(index);
        }
        if (type instanceof VarCharType) {
            return row.getString(index);
        }
        if (type instanceof VarBinaryType) {
            return row.getBinary(index);
        }
        throw new UnsupportedOperationException(
                "Unsupported field type for primary key: " + type.getClass().getSimpleName());
    }
}
