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
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VarBinaryType;

import java.util.Arrays;

/**
 * Type-aware field accessor for {@link RowData}.
 *
 * <p>{@link RowData} exposes typed getters rather than a generic {@code getField(int)}, so this
 * helper centralizes the dispatch for the scalar types usable as a primary-key column.
 *
 * <h3>Why the type coverage here matters twice</h3>
 * <p>This accessor feeds two distinct paths, and a missing type breaks both:
 * <ul>
 *   <li>{@link PrimaryKeySelector#project} — the {@code keyBy} routing key, evaluated on every
 *       event. An unsupported type fails at {@code invoke()} time, before any write is attempted.</li>
 *   <li>{@code LanceUpsertSink}'s in-memory buffer key, which collapses events per key.</li>
 * </ul>
 *
 * <p>Consequently a key type absent from this dispatch is unusable as a primary key regardless of
 * how DELETE or UPSERT is implemented downstream. DATE and TIMESTAMP were previously missing, so
 * they were rejected here even though Lance and the Arrow converter both handle them.
 *
 * <p>Values returned for a given type must honour {@code equals}/{@code hashCode}, since they are
 * placed into a {@code GenericRowData} used as a map key. Binary types are therefore wrapped so
 * they compare by content rather than by array identity.
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
        if (type instanceof VarCharType || type instanceof CharType) {
            return row.getString(index);
        }
        // DATE is an int (days since epoch) and TIME is an int (millis of day) in RowData's
        // internal representation; both are naturally comparable and hashable as Integer.
        if (type instanceof DateType || type instanceof TimeType) {
            return row.getInt(index);
        }
        // TimestampData implements equals/hashCode over (millis, nanoOfMillis), so it is safe to
        // use directly as a map key component.
        if (type instanceof TimestampType) {
            return row.getTimestamp(index, ((TimestampType) type).getPrecision());
        }
        if (type instanceof LocalZonedTimestampType) {
            return row.getTimestamp(index, ((LocalZonedTimestampType) type).getPrecision());
        }
        // DecimalData implements equals/hashCode.
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
        }
        // byte[] uses identity equals/hashCode, which would silently break per-key collapsing and
        // keyBy routing (two equal keys would hash differently). Wrap for content semantics.
        if (type instanceof VarBinaryType || type instanceof BinaryType) {
            return new BinaryKey(row.getBinary(index));
        }
        throw new UnsupportedOperationException(
                "Unsupported field type for primary key: " + type.getClass().getSimpleName());
    }

    /**
     * Content-comparable wrapper around a binary primary-key value.
     *
     * <p>Required because a raw {@code byte[]} compares by reference: using it directly would make
     * two logically identical binary keys hash to different buckets, breaking both {@code keyBy}
     * routing and the sink's per-key collapsing. {@link #unwrap()} exposes the original array for
     * the Arrow write path.
     */
    public static final class BinaryKey {

        private final byte[] value;
        private final int hash;

        BinaryKey(byte[] value) {
            this.value = value;
            this.hash = Arrays.hashCode(value);
        }

        public byte[] unwrap() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BinaryKey)) {
                return false;
            }
            return Arrays.equals(value, ((BinaryKey) o).value);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public String toString() {
            return "BinaryKey(" + (value == null ? "null" : value.length + " bytes") + ")";
        }
    }
}
