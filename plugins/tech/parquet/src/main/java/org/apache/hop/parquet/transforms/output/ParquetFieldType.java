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

package org.apache.hop.parquet.transforms.output;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

/**
 * Parquet type a field can be written as. The code is stored in the pipeline and shown in the
 * fields grid.
 *
 * <p>Date and the time-of-day types use the calendar fields in the JVM time zone, the same zone
 * Parquet Input uses when it reads those columns back. Timestamps are instants adjusted to UTC.
 */
public enum ParquetFieldType implements IEnumHasCode {
  Utf8("UTF8"),
  Boolean("Boolean"),
  Int32("Int32"),
  Int64("Int64"),
  Float("Float"),
  Double("Double"),
  Binary("Binary"),
  Date("Date"),
  TimeMillis("TimeMillis"),
  TimeMicros("TimeMicros"),
  TimestampMillis("TimestampMillis"),
  TimestampMicros("TimestampMicros"),
  Decimal("Decimal"),
  Json("JSON"),
  Uuid("UUID");

  private static final LogicalTypeAnnotation TIMESTAMP_MICROS =
      LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS);

  private static final int UUID_LENGTH = 16;

  private final String code;

  ParquetFieldType(String code) {
    this.code = code;
  }

  @Override
  public String getCode() {
    return code;
  }

  public static String[] codes() {
    return IEnumHasCode.getCodes(ParquetFieldType.class);
  }

  /**
   * @return the type for this code, or null when the code is empty or not one of the types
   */
  public static ParquetFieldType fromCode(String code) {
    if (code == null) {
      return null;
    }
    String trimmed = code.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    return IEnumHasCode.lookupCode(ParquetFieldType.class, trimmed, null);
  }

  /**
   * The type Get Fields proposes. A Hop date is proposed as {@link #Date} (a calendar date, which
   * warehouses such as BigQuery load as a DATE). Every other Hop type keeps the column the writer
   * builds when the Parquet type is left empty.
   *
   * @param valueMeta the incoming field, or null
   * @return the proposed type, or null when this Hop type has no Parquet type
   */
  public static ParquetFieldType forValueMeta(IValueMeta valueMeta) {
    if (valueMeta == null) {
      return null;
    }
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING -> Utf8;
      case IValueMeta.TYPE_BOOLEAN -> Boolean;
      case IValueMeta.TYPE_INTEGER -> Int64;
      case IValueMeta.TYPE_NUMBER -> Double;
      case IValueMeta.TYPE_BIGNUMBER -> decimalFits(valueMeta) ? Decimal : Utf8;
      case IValueMeta.TYPE_BINARY -> Binary;
      case IValueMeta.TYPE_DATE -> Date;
      case IValueMeta.TYPE_TIMESTAMP -> TimestampMicros;
      case IValueMeta.TYPE_JSON -> Json;
      case IValueMeta.TYPE_UUID -> Uuid;
      default -> null;
    };
  }

  /**
   * Same rule as {@link ParquetOutput#avroType}: a big number with a usable length is a DECIMAL,
   * otherwise it is written as text.
   */
  private static boolean decimalFits(IValueMeta valueMeta) {
    int length = valueMeta.getLength();
    int scale = Math.max(valueMeta.getPrecision(), 0);
    return length > 0 && length <= ParquetOutput.MAX_DECIMAL_PRECISION && scale <= length;
  }

  /** The optional column of this type. */
  Type column(String name, ParquetField field, IValueMeta valueMeta) throws HopException {
    return switch (this) {
      case Utf8 ->
          Types.optional(PrimitiveTypeName.BINARY)
              .as(LogicalTypeAnnotation.stringType())
              .named(name);
      case Boolean -> Types.optional(PrimitiveTypeName.BOOLEAN).named(name);
      case Int32 -> Types.optional(PrimitiveTypeName.INT32).named(name);
      case Int64 -> Types.optional(PrimitiveTypeName.INT64).named(name);
      case Float -> Types.optional(PrimitiveTypeName.FLOAT).named(name);
      case Double -> Types.optional(PrimitiveTypeName.DOUBLE).named(name);
      case Binary -> Types.optional(PrimitiveTypeName.BINARY).named(name);
      case Date ->
          Types.optional(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named(name);
      case TimeMillis ->
          Types.optional(PrimitiveTypeName.INT32)
              .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS))
              .named(name);
      case TimeMicros ->
          Types.optional(PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
              .named(name);
      case TimestampMillis ->
          Types.optional(PrimitiveTypeName.INT64)
              .as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
              .named(name);
      case TimestampMicros ->
          Types.optional(PrimitiveTypeName.INT64).as(TIMESTAMP_MICROS).named(name);
      case Decimal ->
          Types.optional(PrimitiveTypeName.BINARY).as(decimal(field, valueMeta)).named(name);
      case Json ->
          Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.jsonType()).named(name);
      case Uuid ->
          Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
              .length(UUID_LENGTH)
              .as(LogicalTypeAnnotation.uuidType())
              .named(name);
    };
  }

  /** Writes one non-null value of this type. The caller has already opened the field. */
  void write(RecordConsumer consumer, ParquetField field, IValueMeta valueMeta, Object valueData)
      throws HopException {
    String name = nameOf(field, valueMeta);
    switch (this) {
      case Utf8, Json ->
          consumer.addBinary(
              org.apache.parquet.io.api.Binary.fromString(requireString(valueMeta, valueData)));
      case Boolean -> consumer.addBoolean(requireBoolean(valueMeta, valueData));
      case Int32 -> consumer.addInteger(int32(name, valueMeta, valueData));
      case Int64 -> consumer.addLong(requireLong(valueMeta, valueData));
      case Float -> consumer.addFloat((float) requireDouble(valueMeta, valueData));
      case Double -> consumer.addDouble(requireDouble(valueMeta, valueData));
      case Binary ->
          consumer.addBinary(
              org.apache.parquet.io.api.Binary.fromConstantByteArray(
                  requireBytes(valueMeta, valueData)));
      case Date -> consumer.addInteger(dateDays(name, valueMeta, valueData));
      case TimeMillis -> consumer.addInteger(timeOfDayMillis(valueMeta, valueData));
      case TimeMicros -> consumer.addLong(timeOfDayMicros(valueMeta, valueData));
      case TimestampMillis ->
          consumer.addLong(ParquetWriteSupport.epochValue(valueMeta, valueData, null));
      case TimestampMicros ->
          consumer.addLong(ParquetWriteSupport.epochValue(valueMeta, valueData, TIMESTAMP_MICROS));
      case Decimal ->
          consumer.addBinary(
              ParquetWriteSupport.decimalBytes(
                  name, valueMeta, valueMeta.getBigNumber(valueData), decimal(field, valueMeta)));
      case Uuid ->
          consumer.addBinary(ParquetWriteSupport.uuidBytes(requireString(valueMeta, valueData)));
    }
  }

  /**
   * DECIMAL annotation for this field. An explicit precision and scale win; otherwise the length
   * and precision of the source field are used.
   */
  DecimalLogicalTypeAnnotation decimal(ParquetField field, IValueMeta valueMeta)
      throws HopException {
    String name = nameOf(field, valueMeta);
    Integer precisionText =
        wholeNumber(field == null ? null : field.getPrecision(), name, "Precision");
    Integer scaleText = wholeNumber(field == null ? null : field.getScale(), name, "Scale");
    int precision = precisionText != null ? precisionText : valueMeta.getLength();
    int scale = scaleText != null ? scaleText : Math.max(valueMeta.getPrecision(), 0);
    if (precision < 1
        || precision > ParquetOutput.MAX_DECIMAL_PRECISION
        || scale < 0
        || scale > precision) {
      throw new HopException(
          "Field '"
              + name
              + "' is written as Decimal with precision "
              + precision
              + " and scale "
              + scale
              + ". Set a precision from 1 to "
              + ParquetOutput.MAX_DECIMAL_PRECISION
              + " and a scale from 0 to that precision, or set the length and precision of the source field.");
    }
    return (DecimalLogicalTypeAnnotation) LogicalTypeAnnotation.decimalType(scale, precision);
  }

  private static int dateDays(String name, IValueMeta valueMeta, Object valueData)
      throws HopException {
    long days = zoned(valueMeta, valueData).toLocalDate().toEpochDay();
    if (days < Integer.MIN_VALUE || days > Integer.MAX_VALUE) {
      throw new HopException(
          "Value of field '" + name + "' does not fit in a Parquet Date (days since the epoch)");
    }
    return (int) days;
  }

  private static int timeOfDayMillis(IValueMeta valueMeta, Object valueData) throws HopException {
    return zoned(valueMeta, valueData).get(ChronoField.MILLI_OF_DAY);
  }

  private static long timeOfDayMicros(IValueMeta valueMeta, Object valueData) throws HopException {
    return zoned(valueMeta, valueData).getLong(ChronoField.MICRO_OF_DAY);
  }

  /** Calendar fields of the value in the JVM time zone. */
  private static ZonedDateTime zoned(IValueMeta valueMeta, Object valueData) throws HopException {
    if (valueMeta instanceof ValueMetaTimestamp timestampMeta) {
      java.sql.Timestamp timestamp = timestampMeta.getTimestamp(valueData);
      if (timestamp == null) {
        throw new HopException(
            "Unable to convert field '" + valueMeta.getName() + "' to a timestamp");
      }
      return timestamp.toInstant().atZone(ZoneId.systemDefault());
    }
    Date date = valueMeta.getDate(valueData);
    if (date == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to a date");
    }
    return Instant.ofEpochMilli(date.getTime()).atZone(ZoneId.systemDefault());
  }

  private static int int32(String name, IValueMeta valueMeta, Object valueData)
      throws HopException {
    long value = requireLong(valueMeta, valueData);
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new HopException("Value " + value + " of field '" + name + "' does not fit in Int32");
    }
    return (int) value;
  }

  private static long requireLong(IValueMeta valueMeta, Object valueData) throws HopException {
    Long value = valueMeta.getInteger(valueData);
    if (value == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to an integer");
    }
    return value;
  }

  private static double requireDouble(IValueMeta valueMeta, Object valueData) throws HopException {
    Double value = valueMeta.getNumber(valueData);
    if (value == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to a number");
    }
    return value;
  }

  private static boolean requireBoolean(IValueMeta valueMeta, Object valueData)
      throws HopException {
    Boolean value = valueMeta.getBoolean(valueData);
    if (value == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to a boolean");
    }
    return value;
  }

  private static String requireString(IValueMeta valueMeta, Object valueData) throws HopException {
    String value = valueMeta.getString(valueData);
    if (value == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to text");
    }
    return value;
  }

  private static byte[] requireBytes(IValueMeta valueMeta, Object valueData) throws HopException {
    byte[] value = valueMeta.getBinary(valueData);
    if (value == null) {
      throw new HopException("Unable to convert field '" + valueMeta.getName() + "' to bytes");
    }
    return value;
  }

  private static Integer wholeNumber(String text, String fieldName, String what)
      throws HopException {
    if (Utils.isEmpty(text) || text.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.valueOf(text.trim());
    } catch (NumberFormatException e) {
      throw new HopException(
          what + " '" + text + "' of field '" + fieldName + "' is not a whole number");
    }
  }

  private static String nameOf(ParquetField field, IValueMeta valueMeta) {
    if (field != null && !Utils.isEmpty(field.getTargetFieldName())) {
      return field.getTargetFieldName();
    }
    if (field != null && !Utils.isEmpty(field.getSourceFieldName())) {
      return field.getSourceFieldName();
    }
    return valueMeta == null ? "" : valueMeta.getName();
  }
}
