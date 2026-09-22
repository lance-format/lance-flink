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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
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

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Type converter between Lance/Arrow and Flink types.
 * 
 * <p>Supported type mappings:
 * <ul>
 *   <li>Int8 <-> TINYINT</li>
 *   <li>Int16 <-> SMALLINT</li>
 *   <li>Int32 <-> INT</li>
 *   <li>Int64 <-> BIGINT</li>
 *   <li>Float32 <-> FLOAT</li>
 *   <li>Float64 <-> DOUBLE</li>
 *   <li>String/LargeString <-> STRING</li>
 *   <li>Boolean <-> BOOLEAN</li>
 *   <li>Binary/LargeBinary <-> BYTES</li>
 *   <li>Date32 <-> DATE</li>
 *   <li>Time32/Time64 <-> TIME</li>
 *   <li>Timestamp (no timezone) <-> TIMESTAMP</li>
 *   <li>Timestamp (UTC timezone) <-> TIMESTAMP_LTZ</li>
 *   <li>Decimal128 <-> DECIMAL</li>
 *   <li>FixedSizeList<Float32> <-> ARRAY<FLOAT></li>
 *   <li>FixedSizeList<Float64> <-> ARRAY<DOUBLE></li>
 * </ul>
 */
public class LanceTypeConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceTypeConverter.class);

    /**
     * Bit width used for Arrow decimals.
     *
     * <p>Flink's DECIMAL tops out at precision 38, which fits Decimal128, so widening to 256 is
     * never required.
     */
    private static final int DECIMAL_BIT_WIDTH = 128;

    /**
     * Convert Arrow Schema to Flink RowType
     *
     * @param schema Arrow Schema
     * @return Flink RowType
     */
    public static RowType toFlinkRowType(Schema schema) {
        List<RowType.RowField> fields = new ArrayList<>();
        for (Field field : schema.getFields()) {
            LogicalType logicalType = arrowTypeToFlinkType(field);
            fields.add(new RowType.RowField(field.getName(), logicalType));
        }
        return new RowType(fields);
    }

    /**
     * Convert Flink RowType to Arrow Schema
     *
     * @param rowType Flink RowType
     * @return Arrow Schema
     */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RowType.RowField rowField : rowType.getFields()) {
            Field arrowField = flinkTypeToArrowField(rowField.getName(), rowField.getType());
            fields.add(arrowField);
        }
        return new Schema(fields);
    }

    /**
     * Convert Arrow Field to Flink LogicalType
     *
     * @param field Arrow Field
     * @return Flink LogicalType
     */
    public static LogicalType arrowTypeToFlinkType(Field field) {
        ArrowType arrowType = field.getType();
        boolean nullable = field.isNullable();

        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            int bitWidth = intType.getBitWidth();
            switch (bitWidth) {
                case 8:
                    return new TinyIntType(nullable);
                case 16:
                    return new SmallIntType(nullable);
                case 32:
                    return new IntType(nullable);
                case 64:
                    return new BigIntType(nullable);
                default:
                    throw new UnsupportedTypeException("Unsupported Arrow Int bit width: " + bitWidth);
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            FloatingPointPrecision precision = fpType.getPrecision();
            switch (precision) {
                case SINGLE:
                    return new FloatType(nullable);
                case DOUBLE:
                    return new DoubleType(nullable);
                default:
                    throw new UnsupportedTypeException("Unsupported Arrow floating point precision: " + precision);
            }
        } else if (arrowType instanceof ArrowType.Utf8 || arrowType instanceof ArrowType.LargeUtf8) {
            return new VarCharType(nullable, VarCharType.MAX_LENGTH);
        } else if (arrowType instanceof ArrowType.Bool) {
            return new BooleanType(nullable);
        } else if (arrowType instanceof ArrowType.Binary) {
            return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
        } else if (arrowType instanceof ArrowType.LargeBinary) {
            return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
        } else if (arrowType instanceof ArrowType.FixedSizeBinary) {
            ArrowType.FixedSizeBinary fixedBinary = (ArrowType.FixedSizeBinary) arrowType;
            return new BinaryType(nullable, fixedBinary.getByteWidth());
        } else if (arrowType instanceof ArrowType.Date) {
            return new DateType(nullable);
        } else if (arrowType instanceof ArrowType.Time) {
            ArrowType.Time timeType = (ArrowType.Time) arrowType;
            return new TimeType(nullable, getTimestampPrecision(timeType.getUnit()));
        } else if (arrowType instanceof ArrowType.Timestamp) {
            ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
            // Determine precision based on time unit
            int precision = getTimestampPrecision(tsType.getUnit());
            // A timezone marks an absolute instant, which is TIMESTAMP_LTZ on the Flink side.
            // Without this split the zoned and unzoned forms would collapse into one.
            if (tsType.getTimezone() != null) {
                return new LocalZonedTimestampType(nullable, precision);
            }
            return new TimestampType(nullable, precision);
        } else if (arrowType instanceof ArrowType.Decimal) {
            ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
            return new DecimalType(nullable, decimalType.getPrecision(), decimalType.getScale());
        } else if (arrowType instanceof ArrowType.FixedSizeList) {
            // Vector type: FixedSizeList<Float32/Float64>
            ArrowType.FixedSizeList listType = (ArrowType.FixedSizeList) arrowType;
            List<Field> children = field.getChildren();
            if (children != null && !children.isEmpty()) {
                LogicalType elementType = arrowTypeToFlinkType(children.get(0));
                return new ArrayType(nullable, elementType);
            }
            throw new UnsupportedTypeException("FixedSizeList must contain child type");
        } else if (arrowType instanceof ArrowType.Map) {
            // Must precede the List branch. An Arrow map is physically a list of entry structs, so
            // a List-first check would map it to ARRAY<ROW<key, value>> and the column would stop
            // round-tripping as a MAP.
            List<Field> children = field.getChildren();
            if (children == null || children.isEmpty()) {
                throw new UnsupportedTypeException("Map must contain an entries child");
            }
            Field entries = children.get(0);
            List<Field> keyValue = entries.getChildren();
            if (keyValue == null || keyValue.size() != 2) {
                throw new UnsupportedTypeException(
                        "Map entries must contain exactly key and value children, found "
                                + (keyValue == null ? 0 : keyValue.size()));
            }
            LogicalType keyType = arrowTypeToFlinkType(keyValue.get(0));
            LogicalType valueType = arrowTypeToFlinkType(keyValue.get(1));
            // Arrow guarantees a non-null key; carry that through so a round-trip does not hand
            // back a MAP the converter would then refuse on the way in.
            return new MapType(nullable, keyType.copy(false), valueType);
        } else if (arrowType instanceof ArrowType.List || arrowType instanceof ArrowType.LargeList) {
            // Regular list type
            List<Field> children = field.getChildren();
            if (children != null && !children.isEmpty()) {
                LogicalType elementType = arrowTypeToFlinkType(children.get(0));
                return new ArrayType(nullable, elementType);
            }
            throw new UnsupportedTypeException("List must contain child type");
        } else if (arrowType instanceof ArrowType.Struct) {
            // Struct type
            List<RowType.RowField> structFields = new ArrayList<>();
            for (Field child : field.getChildren()) {
                LogicalType childType = arrowTypeToFlinkType(child);
                structFields.add(new RowType.RowField(child.getName(), childType));
            }
            return new RowType(nullable, structFields);
        } else if (arrowType instanceof ArrowType.Null) {
            // Null type, map to nullable string
            LOG.warn("Arrow Null type mapped to nullable STRING type");
            return new VarCharType(true, VarCharType.MAX_LENGTH);
        }

        throw new UnsupportedTypeException("Unsupported Arrow type: " + arrowType.getClass().getSimpleName());
    }

    /**
     * Convert Flink LogicalType to Arrow Field
     *
     * @param name Field name
     * @param logicalType Flink LogicalType
     * @return Arrow Field
     */
    public static Field flinkTypeToArrowField(String name, LogicalType logicalType) {
        boolean nullable = logicalType.isNullable();
        ArrowType arrowType;
        List<Field> children = null;

        if (logicalType instanceof TinyIntType) {
            arrowType = new ArrowType.Int(8, true);
        } else if (logicalType instanceof SmallIntType) {
            arrowType = new ArrowType.Int(16, true);
        } else if (logicalType instanceof IntType) {
            arrowType = new ArrowType.Int(32, true);
        } else if (logicalType instanceof BigIntType) {
            arrowType = new ArrowType.Int(64, true);
        } else if (logicalType instanceof FloatType) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (logicalType instanceof DoubleType) {
            arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (logicalType instanceof VarCharType) {
            arrowType = ArrowType.Utf8.INSTANCE;
        } else if (logicalType instanceof BooleanType) {
            arrowType = ArrowType.Bool.INSTANCE;
        } else if (logicalType instanceof VarBinaryType) {
            arrowType = ArrowType.Binary.INSTANCE;
        } else if (logicalType instanceof BinaryType) {
            BinaryType binaryType = (BinaryType) logicalType;
            arrowType = new ArrowType.FixedSizeBinary(binaryType.getLength());
        } else if (logicalType instanceof DateType) {
            arrowType = new ArrowType.Date(DateUnit.DAY);
        } else if (logicalType instanceof TimeType) {
            // Flink TIME is time-of-day without date. Arrow splits this across bit widths:
            // Time32 carries SECOND/MILLISECOND, Time64 carries MICROSECOND/NANOSECOND.
            TimeType timeType = (TimeType) logicalType;
            TimeUnit timeUnit = getArrowTimeUnit(timeType.getPrecision());
            arrowType = new ArrowType.Time(timeUnit, getTimeBitWidth(timeUnit));
        } else if (logicalType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) logicalType;
            TimeUnit timeUnit = getArrowTimeUnit(tsType.getPrecision());
            arrowType = new ArrowType.Timestamp(timeUnit, null);
        } else if (logicalType instanceof LocalZonedTimestampType) {
            // TIMESTAMP_LTZ denotes an absolute instant. Tagging the Arrow type with UTC keeps it
            // distinguishable from a plain TIMESTAMP, which is what makes the round-trip lossless.
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) logicalType;
            TimeUnit timeUnit = getArrowTimeUnit(ltzType.getPrecision());
            arrowType = new ArrowType.Timestamp(timeUnit, "UTC");
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            arrowType =
                    new ArrowType.Decimal(
                            decimalType.getPrecision(), decimalType.getScale(), DECIMAL_BIT_WIDTH);
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            LogicalType elementType = arrayType.getElementType();
            Field childField = flinkTypeToArrowField("item", elementType);
            children = new ArrayList<>();
            children.add(childField);
            // For vector types, use List type
            arrowType = ArrowType.List.INSTANCE;
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            LogicalType keyType = mapType.getKeyType();
            // Arrow requires map keys to be non-null, while Flink's MapType allows a nullable key
            // type. Silently widening it would let a NULL key reach the encoder, so reject it here
            // where the message can still name the offending column.
            if (keyType.isNullable()) {
                throw new UnsupportedTypeException(
                        "MAP key must be NOT NULL for column '" + name + "': Arrow map keys cannot "
                                + "be nullable. Declare the key as e.g. MAP<STRING NOT NULL, INT>.");
            }
            children = new ArrayList<>();
            children.add(mapEntriesField(name, keyType, mapType.getValueType()));
            // keysSorted=false: nothing in the write path sorts entries, and claiming otherwise
            // would let a reader skip its own ordering work on unordered data.
            arrowType = new ArrowType.Map(false);
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            children = new ArrayList<>();
            for (RowType.RowField rowField : rowType.getFields()) {
                Field childField = flinkTypeToArrowField(rowField.getName(), rowField.getType());
                children.add(childField);
            }
            arrowType = ArrowType.Struct.INSTANCE;
        } else {
            throw new UnsupportedTypeException("Unsupported Flink type: " + logicalType.getClass().getSimpleName());
        }

        FieldType fieldType = new FieldType(nullable, arrowType, null);
        return new Field(name, fieldType, children);
    }

    /**
     * Build the {@code entries} struct that backs an Arrow map field.
     *
     * <p>Arrow fixes both the names and the nullability here: the struct is called {@code entries}
     * and must itself be non-nullable, and {@code key} must be non-nullable. Only {@code value} may
     * be null. Getting any of that wrong surfaces later as an opaque IPC or encoder error, so the
     * names come from {@link MapVector}'s constants rather than string literals -- note that
     * {@code MapVector.DATA_VECTOR_NAME} is {@code entries}, whereas the inherited
     * {@code BaseRepeatedValueVector.DATA_VECTOR_NAME} is {@code $data$}.
     */
    private static Field mapEntriesField(
            String mapColumnName, LogicalType keyType, LogicalType valueType) {
        // Arrow accepts far more element types here than the read/write path can actually move, so
        // an unchecked MAP<STRING, DATE> would create a table whose first write fails deep in the
        // converter. Reject it at DDL time instead, where the message can name the column.
        requireSupportedMapElement(mapColumnName, "key", keyType);
        requireSupportedMapElement(mapColumnName, "value", valueType);

        Field keyField = flinkTypeToArrowField(MapVector.KEY_NAME, keyType);
        if (keyField.isNullable()) {
            // Defensive: the caller already rejects a nullable key type, but a converter that
            // widened nullability on the way out would otherwise produce a schema Arrow refuses.
            keyField =
                    new Field(
                            MapVector.KEY_NAME,
                            new FieldType(false, keyField.getType(), null),
                            keyField.getChildren());
        }
        Field valueField = flinkTypeToArrowField(MapVector.VALUE_NAME, valueType);

        List<Field> entryChildren = new ArrayList<>();
        entryChildren.add(keyField);
        entryChildren.add(valueField);

        return new Field(
                MapVector.DATA_VECTOR_NAME,
                new FieldType(false, ArrowType.Struct.INSTANCE, null),
                entryChildren);
    }

    /**
     * Element types the map read/write path can carry.
     *
     * <p>This mirrors what {@code RowDataConverter}'s array element helpers implement, since map
     * keys and values reuse them. It is narrower than the set of types allowed for a top-level
     * column, and widening it means extending those helpers first.
     */
    private static void requireSupportedMapElement(
            String mapColumnName, String role, LogicalType elementType) {
        boolean supported =
                elementType instanceof IntType
                        || elementType instanceof BigIntType
                        || elementType instanceof FloatType
                        || elementType instanceof DoubleType
                        || elementType instanceof VarCharType;
        if (!supported) {
            throw new UnsupportedTypeException(
                    "Unsupported MAP "
                            + role
                            + " type for column '"
                            + mapColumnName
                            + "': "
                            + elementType.getClass().getSimpleName()
                            + ". MAP keys and values support INT, BIGINT, FLOAT, DOUBLE and STRING. "
                            + "Arrow would accept more, but the connector's map read/write path "
                            + "would then fail on the first write rather than here.");
        }
    }

    /**
     * Create vector field (FixedSizeList&lt;Float32&gt;)
     *
     * @param name Field name
     * @param dimension Vector dimension
     * @param nullable Whether nullable
     * @return Arrow Field
     */
    public static Field createVectorField(String name, int dimension, boolean nullable) {
        ArrowType elementType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        Field elementField = new Field("item", new FieldType(false, elementType, null), null);
        
        ArrowType listType = new ArrowType.FixedSizeList(dimension);
        List<Field> children = new ArrayList<>();
        children.add(elementField);
        
        return new Field(name, new FieldType(nullable, listType, null), children);
    }

    /**
     * Create Float64 vector field (FixedSizeList<Float64>)
     *
     * @param name Field name
     * @param dimension Vector dimension
     * @param nullable Whether nullable
     * @return Arrow Field
     */
    public static Field createFloat64VectorField(String name, int dimension, boolean nullable) {
        ArrowType elementType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        Field elementField = new Field("item", new FieldType(false, elementType, null), null);
        
        ArrowType listType = new ArrowType.FixedSizeList(dimension);
        List<Field> children = new ArrayList<>();
        children.add(elementField);
        
        return new Field(name, new FieldType(nullable, listType, null), children);
    }

    /**
     * Check if field is vector type (FixedSizeList<Float32/Float64>)
     *
     * @param field Arrow Field
     * @return Whether vector type
     */
    public static boolean isVectorField(Field field) {
        ArrowType arrowType = field.getType();
        if (!(arrowType instanceof ArrowType.FixedSizeList)) {
            return false;
        }
        
        List<Field> children = field.getChildren();
        if (children == null || children.isEmpty()) {
            return false;
        }
        
        ArrowType childType = children.get(0).getType();
        if (childType instanceof ArrowType.FloatingPoint) {
            FloatingPointPrecision precision = ((ArrowType.FloatingPoint) childType).getPrecision();
            return precision == FloatingPointPrecision.SINGLE || precision == FloatingPointPrecision.DOUBLE;
        }
        
        return false;
    }

    /**
     * Get vector field dimension
     *
     * @param field Arrow Field
     * @return Vector dimension, returns -1 if not vector field
     */
    public static int getVectorDimension(Field field) {
        ArrowType arrowType = field.getType();
        if (arrowType instanceof ArrowType.FixedSizeList) {
            return ((ArrowType.FixedSizeList) arrowType).getListSize();
        }
        return -1;
    }

    /**
     * Convert Flink DataType to LogicalType
     *
     * @param dataType Flink DataType
     * @return LogicalType
     */
    public static LogicalType toLogicalType(DataType dataType) {
        return dataType.getLogicalType();
    }

    /**
     * Convert LogicalType to Flink DataType
     *
     * @param logicalType Flink LogicalType
     * @return Flink DataType
     */
    public static DataType toDataType(LogicalType logicalType) {
        if (logicalType instanceof TinyIntType) {
            return DataTypes.TINYINT();
        } else if (logicalType instanceof SmallIntType) {
            return DataTypes.SMALLINT();
        } else if (logicalType instanceof IntType) {
            return DataTypes.INT();
        } else if (logicalType instanceof BigIntType) {
            return DataTypes.BIGINT();
        } else if (logicalType instanceof FloatType) {
            return DataTypes.FLOAT();
        } else if (logicalType instanceof DoubleType) {
            return DataTypes.DOUBLE();
        } else if (logicalType instanceof VarCharType) {
            return DataTypes.STRING();
        } else if (logicalType instanceof BooleanType) {
            return DataTypes.BOOLEAN();
        } else if (logicalType instanceof VarBinaryType) {
            return DataTypes.BYTES();
        } else if (logicalType instanceof BinaryType) {
            BinaryType binaryType = (BinaryType) logicalType;
            return DataTypes.BINARY(binaryType.getLength());
        } else if (logicalType instanceof DateType) {
            return DataTypes.DATE();
        } else if (logicalType instanceof TimeType) {
            return DataTypes.TIME(((TimeType) logicalType).getPrecision());
        } else if (logicalType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) logicalType;
            return DataTypes.TIMESTAMP(tsType.getPrecision());
        } else if (logicalType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType ltzType = (LocalZonedTimestampType) logicalType;
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(ltzType.getPrecision());
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            DataType elementDataType = toDataType(arrayType.getElementType());
            return DataTypes.ARRAY(elementDataType);
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            DataType keyDataType = toDataType(mapType.getKeyType());
            DataType valueDataType = toDataType(mapType.getValueType());
            // The key stays NOT NULL to match Arrow, which does not allow a nullable map key.
            return DataTypes.MAP(keyDataType.notNull(), valueDataType);
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            DataTypes.Field[] fields = rowType.getFields().stream()
                    .map(f -> DataTypes.FIELD(f.getName(), toDataType(f.getType())))
                    .toArray(DataTypes.Field[]::new);
            return DataTypes.ROW(fields);
        }
        
        throw new UnsupportedTypeException("Unsupported LogicalType: " + logicalType.getClass().getSimpleName());
    }

    /**
     * Get Flink Timestamp precision based on Arrow TimeUnit
     */
    private static int getTimestampPrecision(TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECOND:
                return 0;
            case MILLISECOND:
                return 3;
            case MICROSECOND:
                return 6;
            case NANOSECOND:
                return 9;
            default:
                return 6; // Default microsecond precision
        }
    }

    /**
     * Get Arrow TimeUnit based on Flink Timestamp precision
     */
    private static TimeUnit getArrowTimeUnit(int precision) {
        if (precision <= 0) {
            return TimeUnit.SECOND;
        } else if (precision <= 3) {
            return TimeUnit.MILLISECOND;
        } else if (precision <= 6) {
            return TimeUnit.MICROSECOND;
        } else {
            return TimeUnit.NANOSECOND;
        }
    }

    /**
     * Get the Arrow Time bit width required by a time unit.
     *
     * <p>Arrow only allows Time32 for SECOND/MILLISECOND and Time64 for MICROSECOND/NANOSECOND;
     * pairing a unit with the wrong width is rejected when the field is constructed.
     */
    private static int getTimeBitWidth(TimeUnit timeUnit) {
        switch (timeUnit) {
            case SECOND:
            case MILLISECOND:
                return 32;
            default:
                return 64;
        }
    }

    /**
     * Unsupported type exception
     */
    public static class UnsupportedTypeException extends RuntimeException {
        public UnsupportedTypeException(String message) {
            super(message);
        }

        public UnsupportedTypeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
