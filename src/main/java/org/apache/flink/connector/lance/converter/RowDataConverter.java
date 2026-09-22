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

package org.apache.flink.connector.lance.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

/**
 * Converter between RowData and Arrow data.
 * 
 * <p>Responsible for bidirectional conversion between Arrow VectorSchemaRoot and Flink RowData.
 */
public class RowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RowDataConverter.class);

    private final RowType rowType;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    public RowDataConverter(RowType rowType) {
        this.rowType = rowType;
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.fieldTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
    }

    /**
     * Convert Arrow VectorSchemaRoot to RowData list
     *
     * @param root Arrow VectorSchemaRoot
     * @return RowData list
     */
    public List<RowData> toRowDataList(VectorSchemaRoot root) {
        List<RowData> rows = new ArrayList<>();
        int rowCount = root.getRowCount();
        
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            GenericRowData rowData = new GenericRowData(fieldTypes.length);
            
            for (int fieldIndex = 0; fieldIndex < fieldTypes.length; fieldIndex++) {
                String fieldName = fieldNames[fieldIndex];
                FieldVector vector = root.getVector(fieldName);
                
                if (vector == null) {
                    rowData.setField(fieldIndex, null);
                    continue;
                }
                
                Object value = readValue(vector, rowIndex, fieldTypes[fieldIndex]);
                rowData.setField(fieldIndex, value);
            }
            
            rows.add(rowData);
        }
        
        return rows;
    }

    /**
     * Write RowData list to Arrow VectorSchemaRoot
     *
     * @param rows RowData list
     * @param root Arrow VectorSchemaRoot
     */
    public void toVectorSchemaRoot(List<RowData> rows, VectorSchemaRoot root) {
        root.allocateNew();
        
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            RowData rowData = rows.get(rowIndex);
            
            for (int fieldIndex = 0; fieldIndex < fieldTypes.length; fieldIndex++) {
                String fieldName = fieldNames[fieldIndex];
                FieldVector vector = root.getVector(fieldName);
                
                if (vector == null) {
                    continue;
                }
                
                Object value = getFieldValue(rowData, fieldIndex, fieldTypes[fieldIndex]);
                writeValue(vector, rowIndex, value, fieldTypes[fieldIndex]);
            }
        }
        
        root.setRowCount(rows.size());
    }

    /**
     * Create VectorSchemaRoot
     *
     * @param allocator Memory allocator
     * @return VectorSchemaRoot
     */
    public VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
        Schema arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }

    /**
     * Read value from Arrow Vector
     */
    private Object readValue(FieldVector vector, int index, LogicalType logicalType) {
        if (vector.isNull(index)) {
            return null;
        }

        if (logicalType instanceof TinyIntType) {
            return ((TinyIntVector) vector).get(index);
        } else if (logicalType instanceof SmallIntType) {
            return ((SmallIntVector) vector).get(index);
        } else if (logicalType instanceof IntType) {
            return ((IntVector) vector).get(index);
        } else if (logicalType instanceof BigIntType) {
            return ((BigIntVector) vector).get(index);
        } else if (logicalType instanceof FloatType) {
            return ((Float4Vector) vector).get(index);
        } else if (logicalType instanceof DoubleType) {
            return ((Float8Vector) vector).get(index);
        } else if (logicalType instanceof VarCharType) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return StringData.fromBytes(bytes);
        } else if (logicalType instanceof BooleanType) {
            return ((BitVector) vector).get(index) == 1;
        } else if (logicalType instanceof VarBinaryType) {
            return ((VarBinaryVector) vector).get(index);
        } else if (logicalType instanceof BinaryType) {
            return ((FixedSizeBinaryVector) vector).get(index);
        } else if (logicalType instanceof DateType) {
            int daysSinceEpoch = ((DateDayVector) vector).get(index);
            return daysSinceEpoch;
        } else if (logicalType instanceof TimeType) {
            return readTime(vector, index);
        } else if (logicalType instanceof TimestampType) {
            return readTimestamp(vector, index, (TimestampType) logicalType);
        } else if (logicalType instanceof LocalZonedTimestampType) {
            return readLocalZonedTimestamp(vector, index);
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            BigDecimal value = ((DecimalVector) vector).getObject(index);
            return DecimalData.fromBigDecimal(
                    value, decimalType.getPrecision(), decimalType.getScale());
        } else if (logicalType instanceof ArrayType) {
            return readArray(vector, index, (ArrayType) logicalType);
        } else if (logicalType instanceof MapType) {
            return readMap(vector, index, (MapType) logicalType);
        } else if (logicalType instanceof RowType) {
            return readStruct(vector, index, (RowType) logicalType);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported read type: " + logicalType.getClass().getSimpleName());
    }

    /**
     * Read timestamp value
     */
    private TimestampData readTimestamp(FieldVector vector, int index, TimestampType tsType) {
        long value;
        int precision = tsType.getPrecision();

        if (vector instanceof TimeStampSecVector) {
            value = ((TimeStampSecVector) vector).get(index);
            return TimestampData.fromEpochMillis(value * 1000);
        } else if (vector instanceof TimeStampMilliVector) {
            value = ((TimeStampMilliVector) vector).get(index);
            return TimestampData.fromEpochMillis(value);
        } else if (vector instanceof TimeStampMicroVector) {
            value = ((TimeStampMicroVector) vector).get(index);
            return TimestampData.fromEpochMillis(value / 1000, (int) ((value % 1000) * 1000));
        } else if (vector instanceof TimeStampNanoVector) {
            value = ((TimeStampNanoVector) vector).get(index);
            return TimestampData.fromEpochMillis(value / 1000000, (int) (value % 1000000));
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported timestamp Vector type: " + vector.getClass().getSimpleName());
    }

    /**
     * Read a TIME value as milliseconds since midnight.
     *
     * <p>Flink represents TIME as an int holding milliseconds of the day, so the sub-millisecond
     * units Arrow allows are narrowed down to that resolution here.
     */
    private int readTime(FieldVector vector, int index) {
        if (vector instanceof TimeSecVector) {
            return ((TimeSecVector) vector).get(index) * 1000;
        } else if (vector instanceof TimeMilliVector) {
            return ((TimeMilliVector) vector).get(index);
        } else if (vector instanceof TimeMicroVector) {
            return (int) (((TimeMicroVector) vector).get(index) / 1000L);
        } else if (vector instanceof TimeNanoVector) {
            return (int) (((TimeNanoVector) vector).get(index) / 1_000_000L);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported time Vector type: " + vector.getClass().getSimpleName());
    }

    /**
     * Read a TIMESTAMP_LTZ value.
     *
     * <p>Arrow exposes zoned timestamps through dedicated *TZ vectors, so these are distinct
     * classes from the ones {@link #readTimestamp} handles. Values are epoch-based, which is
     * exactly what {@link TimestampData#fromEpochMillis} expects, so no zone shifting is applied.
     */
    private TimestampData readLocalZonedTimestamp(FieldVector vector, int index) {
        if (vector instanceof TimeStampSecTZVector) {
            return TimestampData.fromEpochMillis(((TimeStampSecTZVector) vector).get(index) * 1000L);
        } else if (vector instanceof TimeStampMilliTZVector) {
            return TimestampData.fromEpochMillis(((TimeStampMilliTZVector) vector).get(index));
        } else if (vector instanceof TimeStampMicroTZVector) {
            long micros = ((TimeStampMicroTZVector) vector).get(index);
            return TimestampData.fromEpochMillis(
                    Math.floorDiv(micros, 1000L), (int) Math.floorMod(micros, 1000L) * 1000);
        } else if (vector instanceof TimeStampNanoTZVector) {
            long nanos = ((TimeStampNanoTZVector) vector).get(index);
            return TimestampData.fromEpochMillis(
                    Math.floorDiv(nanos, 1_000_000L), (int) Math.floorMod(nanos, 1_000_000L));
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported zoned timestamp Vector type: " + vector.getClass().getSimpleName());
    }

    /**
     * Read array value
     */
    private ArrayData readArray(FieldVector vector, int index, ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        
        if (vector instanceof FixedSizeListVector) {
            FixedSizeListVector listVector = (FixedSizeListVector) vector;
            int listSize = listVector.getListSize();
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = index * listSize;
            
            return readArrayData(dataVector, startIndex, listSize, elementType);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            int startIndex = listVector.getElementStartIndex(index);
            int endIndex = listVector.getElementEndIndex(index);
            int listSize = endIndex - startIndex;
            FieldVector dataVector = listVector.getDataVector();
            
            return readArrayData(dataVector, startIndex, listSize, elementType);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported array Vector type: " + vector.getClass().getSimpleName());
    }

    /**
     * Read map value.
     *
     * <p>An Arrow map is a list of non-nullable {@code entries} structs, each holding a {@code key}
     * and a {@code value} child. The offsets come from the enclosing list, so the key and value
     * slices are read from the same index range.
     */
    private MapData readMap(FieldVector vector, int index, MapType mapType) {
        if (!(vector instanceof MapVector)) {
            // Note this cannot be relaxed to ListVector: a plain list carries no key child, and
            // treating one as a map would read garbage out of the element vector.
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported map Vector type: " + vector.getClass().getSimpleName());
        }

        MapVector mapVector = (MapVector) vector;
        int startIndex = mapVector.getElementStartIndex(index);
        int endIndex = mapVector.getElementEndIndex(index);
        int size = endIndex - startIndex;

        StructVector entries = (StructVector) mapVector.getDataVector();
        FieldVector keyVector = entries.getChild(MapVector.KEY_NAME);
        FieldVector valueVector = entries.getChild(MapVector.VALUE_NAME);
        if (keyVector == null || valueVector == null) {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Map entries struct must expose '" + MapVector.KEY_NAME + "' and '"
                            + MapVector.VALUE_NAME + "' children");
        }

        ArrayData keys = readArrayData(keyVector, startIndex, size, mapType.getKeyType());
        ArrayData values = readArrayData(valueVector, startIndex, size, mapType.getValueType());
        return new GenericMapData(toJavaMap(keys, values, mapType));
    }

    /**
     * Materialize key/value slices into the map {@link GenericMapData} expects.
     *
     * <p>Duplicate keys collapse to the last occurrence, matching how Flink's own map
     * implementations behave when a duplicate reaches them.
     */
    private Map<Object, Object> toJavaMap(ArrayData keys, ArrayData values, MapType mapType) {
        Map<Object, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            Object key = elementAt(keys, i, mapType.getKeyType());
            Object value = elementAt(values, i, mapType.getValueType());
            result.put(key, value);
        }
        return result;
    }

    /** Extract one element out of an {@link ArrayData} as the object GenericMapData stores. */
    private Object elementAt(ArrayData array, int i, LogicalType elementType) {
        if (array.isNullAt(i)) {
            return null;
        }
        return ArrayData.createElementGetter(elementType).getElementOrNull(array, i);
    }

    /**
     * Read array data
     */
    private ArrayData readArrayData(FieldVector dataVector, int startIndex, int size, LogicalType elementType) {
        if (elementType instanceof FloatType) {
            Float4Vector float4Vector = (Float4Vector) dataVector;
            Float[] values = new Float[size];
            for (int i = 0; i < size; i++) {
                if (float4Vector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = float4Vector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof DoubleType) {
            Float8Vector float8Vector = (Float8Vector) dataVector;
            Double[] values = new Double[size];
            for (int i = 0; i < size; i++) {
                if (float8Vector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = float8Vector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof IntType) {
            IntVector intVector = (IntVector) dataVector;
            Integer[] values = new Integer[size];
            for (int i = 0; i < size; i++) {
                if (intVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = intVector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof BigIntType) {
            BigIntVector bigIntVector = (BigIntVector) dataVector;
            Long[] values = new Long[size];
            for (int i = 0; i < size; i++) {
                if (bigIntVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = bigIntVector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof VarCharType) {
            VarCharVector varCharVector = (VarCharVector) dataVector;
            StringData[] values = new StringData[size];
            for (int i = 0; i < size; i++) {
                if (varCharVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = StringData.fromBytes(varCharVector.get(startIndex + i));
                }
            }
            return new GenericArrayData(values);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported array element type: " + elementType.getClass().getSimpleName());
    }

    /**
     * Read struct value
     */
    private RowData readStruct(FieldVector vector, int index, RowType rowType) {
        StructVector structVector = (StructVector) vector;
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            FieldVector childVector = structVector.getChild(field.getName());
            if (childVector == null) {
                rowData.setField(i, null);
            } else {
                Object value = readValue(childVector, index, field.getType());
                rowData.setField(i, value);
            }
        }

        return rowData;
    }

    /**
     * Get field value from RowData
     */
    private Object getFieldValue(RowData rowData, int index, LogicalType logicalType) {
        if (rowData.isNullAt(index)) {
            return null;
        }

        if (logicalType instanceof TinyIntType) {
            return rowData.getByte(index);
        } else if (logicalType instanceof SmallIntType) {
            return rowData.getShort(index);
        } else if (logicalType instanceof IntType) {
            return rowData.getInt(index);
        } else if (logicalType instanceof BigIntType) {
            return rowData.getLong(index);
        } else if (logicalType instanceof FloatType) {
            return rowData.getFloat(index);
        } else if (logicalType instanceof DoubleType) {
            return rowData.getDouble(index);
        } else if (logicalType instanceof VarCharType) {
            return rowData.getString(index);
        } else if (logicalType instanceof BooleanType) {
            return rowData.getBoolean(index);
        } else if (logicalType instanceof VarBinaryType || logicalType instanceof BinaryType) {
            return rowData.getBinary(index);
        } else if (logicalType instanceof DateType) {
            return rowData.getInt(index);
        } else if (logicalType instanceof TimeType) {
            return rowData.getInt(index);
        } else if (logicalType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) logicalType;
            return rowData.getTimestamp(index, tsType.getPrecision());
        } else if (logicalType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) logicalType;
            return rowData.getTimestamp(index, ltzType.getPrecision());
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            return rowData.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
        } else if (logicalType instanceof ArrayType) {
            return rowData.getArray(index);
        } else if (logicalType instanceof MapType) {
            return rowData.getMap(index);
        } else if (logicalType instanceof RowType) {
            RowType nestedRowType = (RowType) logicalType;
            return rowData.getRow(index, nestedRowType.getFieldCount());
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "Unsupported get type: " + logicalType.getClass().getSimpleName());
    }

    /**
     * Write value to Arrow Vector
     */
    private void writeValue(FieldVector vector, int index, Object value, LogicalType logicalType) {
        if (value == null) {
            setNull(vector, index);
            return;
        }

        if (logicalType instanceof TinyIntType) {
            ((TinyIntVector) vector).setSafe(index, (byte) value);
        } else if (logicalType instanceof SmallIntType) {
            ((SmallIntVector) vector).setSafe(index, (short) value);
        } else if (logicalType instanceof IntType) {
            ((IntVector) vector).setSafe(index, (int) value);
        } else if (logicalType instanceof BigIntType) {
            ((BigIntVector) vector).setSafe(index, (long) value);
        } else if (logicalType instanceof FloatType) {
            ((Float4Vector) vector).setSafe(index, (float) value);
        } else if (logicalType instanceof DoubleType) {
            ((Float8Vector) vector).setSafe(index, (double) value);
        } else if (logicalType instanceof VarCharType) {
            StringData stringData = (StringData) value;
            ((VarCharVector) vector).setSafe(index, stringData.toBytes());
        } else if (logicalType instanceof BooleanType) {
            ((BitVector) vector).setSafe(index, (boolean) value ? 1 : 0);
        } else if (logicalType instanceof VarBinaryType) {
            ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        } else if (logicalType instanceof BinaryType) {
            ((FixedSizeBinaryVector) vector).setSafe(index, (byte[]) value);
        } else if (logicalType instanceof DateType) {
            ((DateDayVector) vector).setSafe(index, (int) value);
        } else if (logicalType instanceof TimeType) {
            writeTime(vector, index, (int) value);
        } else if (logicalType instanceof TimestampType) {
            writeTimestamp(vector, index, (TimestampData) value, (TimestampType) logicalType);
        } else if (logicalType instanceof LocalZonedTimestampType) {
            writeLocalZonedTimestamp(vector, index, (TimestampData) value);
        } else if (logicalType instanceof DecimalType) {
            ((DecimalVector) vector).setSafe(index, ((DecimalData) value).toBigDecimal());
        } else if (logicalType instanceof ArrayType) {
            writeArray(vector, index, (ArrayData) value, (ArrayType) logicalType);
        } else if (logicalType instanceof MapType) {
            writeMap(vector, index, (MapData) value, (MapType) logicalType);
        } else if (logicalType instanceof RowType) {
            writeStruct(vector, index, (RowData) value, (RowType) logicalType);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported write type: " + logicalType.getClass().getSimpleName());
        }
    }

    /**
     * Set null value
     */
    private void setNull(FieldVector vector, int index) {
        if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setNull(index);
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setNull(index);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setNull(index);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setNull(index);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setNull(index);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setNull(index);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setNull(index);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setNull(index);
        } else if (vector instanceof VarBinaryVector) {
            ((VarBinaryVector) vector).setNull(index);
        } else if (vector instanceof FixedSizeBinaryVector) {
            ((FixedSizeBinaryVector) vector).setNull(index);
        } else if (vector instanceof DateDayVector) {
            ((DateDayVector) vector).setNull(index);
        } else if (vector instanceof TimeStampSecVector) {
            ((TimeStampSecVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMicroVector) {
            ((TimeStampMicroVector) vector).setNull(index);
        } else if (vector instanceof TimeStampNanoVector) {
            ((TimeStampNanoVector) vector).setNull(index);
        } else if (vector instanceof TimeStampSecTZVector) {
            ((TimeStampSecTZVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMilliTZVector) {
            ((TimeStampMilliTZVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMicroTZVector) {
            ((TimeStampMicroTZVector) vector).setNull(index);
        } else if (vector instanceof TimeStampNanoTZVector) {
            ((TimeStampNanoTZVector) vector).setNull(index);
        } else if (vector instanceof TimeSecVector) {
            ((TimeSecVector) vector).setNull(index);
        } else if (vector instanceof TimeMilliVector) {
            ((TimeMilliVector) vector).setNull(index);
        } else if (vector instanceof TimeMicroVector) {
            ((TimeMicroVector) vector).setNull(index);
        } else if (vector instanceof TimeNanoVector) {
            ((TimeNanoVector) vector).setNull(index);
        } else if (vector instanceof DecimalVector) {
            ((DecimalVector) vector).setNull(index);
        } else if (vector instanceof FixedSizeListVector) {
            ((FixedSizeListVector) vector).setNull(index);
        } else if (vector instanceof MapVector) {
            // Must precede ListVector: MapVector extends ListVector, so the ListVector branch
            // would otherwise swallow it and null the map as if it were a plain list.
            ((MapVector) vector).setNull(index);
        } else if (vector instanceof ListVector) {
            ((ListVector) vector).setNull(index);
        } else if (vector instanceof StructVector) {
            ((StructVector) vector).setNull(index);
        } else {
            // Falling through silently is data corruption, not a harmless no-op. The validity
            // bit of a slot that already holds a value stays set, so the previous row's value is
            // emitted as this row's value with no error anywhere. A freshly allocated vector
            // hides it -- the zeroed validity buffer reads back as null while getNullCount()
            // still reports 0 -- which is why this went unnoticed. readValue, getFieldValue and
            // writeValue all reject unknown types; this branch makes setNull consistent.
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Cannot write NULL: unsupported Arrow vector "
                            + vector.getClass().getSimpleName()
                            + " for field '" + vector.getField().getName() + "'");
        }
    }

    /**
     * Write timestamp value
     */
    private void writeTimestamp(FieldVector vector, int index, TimestampData tsData, TimestampType tsType) {
        long millis = tsData.getMillisecond();
        int nanos = tsData.getNanoOfMillisecond();

        if (vector instanceof TimeStampSecVector) {
            ((TimeStampSecVector) vector).setSafe(index, millis / 1000);
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setSafe(index, millis);
        } else if (vector instanceof TimeStampMicroVector) {
            long micros = millis * 1000 + nanos / 1000;
            ((TimeStampMicroVector) vector).setSafe(index, micros);
        } else if (vector instanceof TimeStampNanoVector) {
            long totalNanos = millis * 1000000 + nanos;
            ((TimeStampNanoVector) vector).setSafe(index, totalNanos);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported timestamp Vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Write a TIME value given as milliseconds since midnight.
     *
     * <p>The incoming value is always millisecond-resolution because that is Flink's internal
     * representation, so the finer Arrow units are scaled up rather than truncated.
     */
    private void writeTime(FieldVector vector, int index, int millisOfDay) {
        if (vector instanceof TimeSecVector) {
            ((TimeSecVector) vector).setSafe(index, millisOfDay / 1000);
        } else if (vector instanceof TimeMilliVector) {
            ((TimeMilliVector) vector).setSafe(index, millisOfDay);
        } else if (vector instanceof TimeMicroVector) {
            ((TimeMicroVector) vector).setSafe(index, millisOfDay * 1000L);
        } else if (vector instanceof TimeNanoVector) {
            ((TimeNanoVector) vector).setSafe(index, millisOfDay * 1_000_000L);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported time Vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Write a TIMESTAMP_LTZ value into one of Arrow's zoned timestamp vectors.
     *
     * <p>{@link TimestampData} already holds an epoch-based instant for this type, so the value is
     * written as-is; applying a zone offset here would shift the instant.
     */
    private void writeLocalZonedTimestamp(FieldVector vector, int index, TimestampData tsData) {
        long millis = tsData.getMillisecond();
        int nanos = tsData.getNanoOfMillisecond();

        if (vector instanceof TimeStampSecTZVector) {
            ((TimeStampSecTZVector) vector).setSafe(index, millis / 1000);
        } else if (vector instanceof TimeStampMilliTZVector) {
            ((TimeStampMilliTZVector) vector).setSafe(index, millis);
        } else if (vector instanceof TimeStampMicroTZVector) {
            ((TimeStampMicroTZVector) vector).setSafe(index, millis * 1000L + nanos / 1000);
        } else if (vector instanceof TimeStampNanoTZVector) {
            ((TimeStampNanoTZVector) vector).setSafe(index, millis * 1_000_000L + nanos);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported zoned timestamp Vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Write array value
     */
    private void writeArray(FieldVector vector, int index, ArrayData arrayData, ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        int size = arrayData.size();

        if (vector instanceof FixedSizeListVector) {
            FixedSizeListVector listVector = (FixedSizeListVector) vector;
            int listSize = listVector.getListSize();
            
            if (size != listSize) {
                throw new IllegalArgumentException(
                        "Array size " + size + " does not match FixedSizeList size " + listSize);
            }
            
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = index * listSize;
            
            writeArrayData(dataVector, startIndex, arrayData, elementType);
            listVector.setNotNull(index);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            listVector.startNewValue(index);
            
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = listVector.getElementStartIndex(index);
            
            writeArrayData(dataVector, startIndex, arrayData, elementType);
            listVector.endValue(index, size);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported array Vector type: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * Write map value.
     *
     * <p>Mirrors {@link #writeArray}'s list handling for the offsets, with two additions specific
     * to maps: each {@code entries} slot has to be marked defined or the struct reads back as NULL
     * even though key and value were written, and a NULL key is rejected because Arrow does not
     * allow one.
     */
    private void writeMap(FieldVector vector, int index, MapData mapData, MapType mapType) {
        if (!(vector instanceof MapVector)) {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported map Vector type: " + vector.getClass().getSimpleName());
        }

        MapVector mapVector = (MapVector) vector;
        ArrayData keys = mapData.keyArray();
        ArrayData values = mapData.valueArray();
        int size = mapData.size();

        for (int i = 0; i < size; i++) {
            if (keys.isNullAt(i)) {
                throw new IllegalArgumentException(
                        "MAP key must not be NULL: Arrow map keys are non-nullable, so a NULL key "
                                + "cannot be written (entry " + i + ")");
            }
        }

        mapVector.startNewValue(index);

        StructVector entries = (StructVector) mapVector.getDataVector();
        FieldVector keyVector = entries.getChild(MapVector.KEY_NAME);
        FieldVector valueVector = entries.getChild(MapVector.VALUE_NAME);
        int startIndex = mapVector.getElementStartIndex(index);

        writeArrayData(keyVector, startIndex, keys, mapType.getKeyType());
        writeArrayData(valueVector, startIndex, values, mapType.getValueType());

        // Without this the entries struct keeps a zero validity bit and the whole entry reads back
        // as NULL, which looks like data loss rather than a missing flag.
        for (int i = 0; i < size; i++) {
            entries.setIndexDefined(startIndex + i);
        }
        entries.setValueCount(startIndex + size);

        mapVector.endValue(index, size);
    }

    /**
     * Write array data
     */
    private void writeArrayData(FieldVector dataVector, int startIndex, ArrayData arrayData, LogicalType elementType) {
        int size = arrayData.size();

        if (elementType instanceof FloatType) {
            Float4Vector float4Vector = (Float4Vector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    float4Vector.setNull(startIndex + i);
                } else {
                    float4Vector.setSafe(startIndex + i, arrayData.getFloat(i));
                }
            }
        } else if (elementType instanceof DoubleType) {
            Float8Vector float8Vector = (Float8Vector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    float8Vector.setNull(startIndex + i);
                } else {
                    float8Vector.setSafe(startIndex + i, arrayData.getDouble(i));
                }
            }
        } else if (elementType instanceof IntType) {
            IntVector intVector = (IntVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    intVector.setNull(startIndex + i);
                } else {
                    intVector.setSafe(startIndex + i, arrayData.getInt(i));
                }
            }
        } else if (elementType instanceof BigIntType) {
            BigIntVector bigIntVector = (BigIntVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    bigIntVector.setNull(startIndex + i);
                } else {
                    bigIntVector.setSafe(startIndex + i, arrayData.getLong(i));
                }
            }
        } else if (elementType instanceof VarCharType) {
            VarCharVector varCharVector = (VarCharVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    varCharVector.setNull(startIndex + i);
                } else {
                    StringData stringData = arrayData.getString(i);
                    varCharVector.setSafe(startIndex + i, stringData.toBytes());
                }
            }
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "Unsupported array element type: " + elementType.getClass().getSimpleName());
        }
    }

    /**
     * Write struct value
     */
    private void writeStruct(FieldVector vector, int index, RowData rowData, RowType rowType) {
        StructVector structVector = (StructVector) vector;
        List<RowType.RowField> fields = rowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            FieldVector childVector = structVector.getChild(field.getName());
            if (childVector != null) {
                Object value = getFieldValue(rowData, i, field.getType());
                writeValue(childVector, index, value, field.getType());
            }
        }

        structVector.setIndexDefined(index);
    }

    /**
     * Convert float array to ArrayData
     *
     * @param vector float array
     * @return ArrayData
     */
    public static ArrayData toArrayData(float[] vector) {
        if (vector == null) {
            return null;
        }
        Float[] boxed = new Float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            boxed[i] = vector[i];
        }
        return new GenericArrayData(boxed);
    }

    /**
     * Convert double array to ArrayData
     *
     * @param vector double array
     * @return ArrayData
     */
    public static ArrayData toArrayData(double[] vector) {
        if (vector == null) {
            return null;
        }
        Double[] boxed = new Double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            boxed[i] = vector[i];
        }
        return new GenericArrayData(boxed);
    }

    /**
     * Convert ArrayData to float array
     *
     * @param arrayData ArrayData
     * @return float array
     */
    public static float[] toFloatArray(ArrayData arrayData) {
        if (arrayData == null) {
            return null;
        }
        int size = arrayData.size();
        float[] result = new float[size];
        for (int i = 0; i < size; i++) {
            result[i] = arrayData.getFloat(i);
        }
        return result;
    }

    /**
     * Convert ArrayData to double array
     *
     * @param arrayData ArrayData
     * @return double array
     */
    public static double[] toDoubleArray(ArrayData arrayData) {
        if (arrayData == null) {
            return null;
        }
        int size = arrayData.size();
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = arrayData.getDouble(i);
        }
        return result;
    }

    /**
     * Get RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * Get field name array
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * Get field type array
     */
    public LogicalType[] getFieldTypes() {
        return fieldTypes;
    }
}
