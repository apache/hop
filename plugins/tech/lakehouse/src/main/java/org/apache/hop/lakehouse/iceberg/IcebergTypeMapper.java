/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.lakehouse.iceberg;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Converts between Hop value types and Iceberg types, and between Hop values and the Java objects
 * Iceberg's generic records use. Dates and timestamps without a zone hold a local date and time, so
 * they are taken in the JVM time zone, the same way Parquet Input reads them: a date of 2026-10-01
 * is midnight on that day and a timestamp of 12:00 shows as 12:00, in every zone. Timestamps with a
 * zone are instants.
 */
public final class IcebergTypeMapper {

  /** Precision and scale used for BigNumber fields that don't declare them. */
  public static final int DEFAULT_DECIMAL_PRECISION = 38;

  public static final int DEFAULT_DECIMAL_SCALE = 10;

  private IcebergTypeMapper() {}

  /** Builds an Iceberg schema from a Hop row layout, used when creating a table. */
  public static Schema toIcebergSchema(IRowMeta rowMeta) throws HopException {
    List<Types.NestedField> columns = new ArrayList<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      columns.add(Types.NestedField.optional(i + 1, valueMeta.getName(), toIcebergType(valueMeta)));
    }
    return new Schema(columns);
  }

  public static Type toIcebergType(IValueMeta valueMeta) throws HopException {
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING, IValueMeta.TYPE_JSON, IValueMeta.TYPE_INET:
        return Types.StringType.get();
      case IValueMeta.TYPE_INTEGER:
        return Types.LongType.get();
      case IValueMeta.TYPE_NUMBER:
        return Types.DoubleType.get();
      case IValueMeta.TYPE_BIGNUMBER:
        int precision =
            valueMeta.getLength() > 0 ? valueMeta.getLength() : DEFAULT_DECIMAL_PRECISION;
        int scale =
            valueMeta.getPrecision() >= 0 && valueMeta.getLength() > 0
                ? valueMeta.getPrecision()
                : DEFAULT_DECIMAL_SCALE;
        return Types.DecimalType.of(Math.min(precision, 38), scale);
      case IValueMeta.TYPE_BOOLEAN:
        return Types.BooleanType.get();
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP:
        return Types.TimestampType.withoutZone();
      case IValueMeta.TYPE_BINARY:
        return Types.BinaryType.get();
      default:
        throw new HopException(
            "Hop type "
                + valueMeta.getTypeDesc()
                + " of field '"
                + valueMeta.getName()
                + "' can't be stored in an Iceberg table");
    }
  }

  /** The Hop value meta used to read an Iceberg column. */
  public static IValueMeta toHopValueMeta(String name, Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return new ValueMetaBoolean(name);
      case INTEGER, LONG:
        return new ValueMetaInteger(name);
      case FLOAT, DOUBLE:
        return new ValueMetaNumber(name);
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) type;
        return new ValueMetaBigNumber(name, decimal.precision(), decimal.scale());
      case DATE:
        return new ValueMetaDate(name);
      case TIMESTAMP, TIMESTAMP_NANO:
        return new ValueMetaTimestamp(name);
      case BINARY, FIXED:
        return new ValueMetaBinary(name);
      default:
        // string, uuid, time, and nested types (as JSON-like text for now)
        return new ValueMetaString(name);
    }
  }

  /** Converts a Hop value to the object an Iceberg generic record expects for {@code type}. */
  public static Object toIcebergValue(IValueMeta valueMeta, Object value, Type type)
      throws HopValueException {
    if (value == null || valueMeta.isNull(value)) {
      return null;
    }
    switch (type.typeId()) {
      case BOOLEAN:
        return valueMeta.getBoolean(value);
      case INTEGER:
        return Math.toIntExact(valueMeta.getInteger(value));
      case LONG:
        return valueMeta.getInteger(value);
      case FLOAT:
        return valueMeta.getNumber(value).floatValue();
      case DOUBLE:
        return valueMeta.getNumber(value);
      case DECIMAL:
        return toDecimal(valueMeta, value, (Types.DecimalType) type);
      case STRING:
        return valueMeta.getString(value);
      case UUID:
        return UUID.fromString(valueMeta.getString(value));
      case DATE:
        return toInstant(valueMeta, value).atZone(ZoneId.systemDefault()).toLocalDate();
      case TIME:
        return LocalTime.parse(valueMeta.getString(value));
      case TIMESTAMP, TIMESTAMP_NANO:
        Instant instant = toInstant(valueMeta, value);
        if (adjustedToUtc(type)) {
          return instant.atOffset(ZoneOffset.UTC);
        }
        return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
      case BINARY:
        return ByteBuffer.wrap(valueMeta.getBinary(value));
      case FIXED:
        return valueMeta.getBinary(value);
      default:
        throw new HopValueException(
            "Writing values of Iceberg type " + type + " is not supported yet");
    }
  }

  /** Converts a value read from an Iceberg generic record to the matching Hop value. */
  public static Object toHopValue(Object value, Type type) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case INTEGER:
        return ((Integer) value).longValue();
      case FLOAT:
        return ((Float) value).doubleValue();
      case STRING, UUID, TIME:
        return value.toString();
      case DATE:
        return Date.from(((LocalDate) value).atStartOfDay(ZoneId.systemDefault()).toInstant());
      case TIMESTAMP, TIMESTAMP_NANO:
        if (value instanceof OffsetDateTime offsetDateTime) {
          return Timestamp.from(offsetDateTime.toInstant());
        }
        return Timestamp.valueOf((LocalDateTime) value);
      case BINARY:
        ByteBuffer buffer = ((ByteBuffer) value).duplicate();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
      case BOOLEAN, LONG, DOUBLE, DECIMAL, FIXED:
        return value;
      default:
        return value.toString();
    }
  }

  private static BigDecimal toDecimal(
      IValueMeta valueMeta, Object value, Types.DecimalType decimalType) throws HopValueException {
    BigDecimal decimal =
        valueMeta.getBigNumber(value).setScale(decimalType.scale(), RoundingMode.HALF_UP);
    if (decimal.precision() > decimalType.precision()) {
      throw new HopValueException(
          "Value "
              + decimal
              + " of field '"
              + valueMeta.getName()
              + "' doesn't fit in "
              + decimalType);
    }
    return decimal;
  }

  private static boolean adjustedToUtc(Type type) {
    if (type instanceof Types.TimestampNanoType nano) {
      return nano.shouldAdjustToUTC();
    }
    return ((Types.TimestampType) type).shouldAdjustToUTC();
  }

  private static Instant toInstant(IValueMeta valueMeta, Object value) throws HopValueException {
    if (value instanceof Timestamp timestamp) {
      return timestamp.toInstant();
    }
    Date date = valueMeta.getDate(value);
    if (date instanceof Timestamp timestamp) {
      return timestamp.toInstant();
    }
    return date.toInstant();
  }
}
