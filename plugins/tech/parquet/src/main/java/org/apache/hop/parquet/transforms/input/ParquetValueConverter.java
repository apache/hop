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

package org.apache.hop.parquet.transforms.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * Converts the values of one Parquet column into the Hop type of the field it is mapped to.
 *
 * <p>The logical type of the column decides what the stored value means (a DECIMAL long is an
 * unscaled number, a DATE int a day count, an unsigned int never negative, ...). That meaning is
 * worked out first and only then converted to the Hop type the user picked for the field.
 *
 * @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">Parquet logical
 *     types</a>
 */
public class ParquetValueConverter extends PrimitiveConverter {

  private final RowMetaAndData group;
  private final IValueMeta valueMeta;
  private final int rowIndex;
  private final PrimitiveType primitiveType;
  private final LogicalTypeAnnotation logicalTypeAnnotation;

  public ParquetValueConverter(RowMetaAndData group, int rowIndex, PrimitiveType primitiveType) {
    this.group = group;
    this.valueMeta = group.getValueMeta(rowIndex);
    this.rowIndex = rowIndex;
    this.primitiveType = primitiveType;
    // A column which isn't mapped to a field is never converted, so it is allowed to come in
    // without a type.
    this.logicalTypeAnnotation =
        primitiveType == null ? null : primitiveType.getLogicalTypeAnnotation();
  }

  /**
   * Build an error which names both ends of the conversion: the Parquet column with its physical
   * type and the Hop field with its type. Files read by a single transform all have to match the
   * configured fields, so a mismatch here usually means the files don't share the same schema.
   *
   * @return the exception to throw
   */
  private HopRuntimeException conversionError() {
    return new HopRuntimeException(
        "Unable to convert Parquet column '"
            + primitiveType.getName()
            + "' of type "
            + primitiveType.getPrimitiveTypeName()
            + (logicalTypeAnnotation == null ? "" : " (" + logicalTypeAnnotation + ")")
            + " to field '"
            + valueMeta.getName()
            + "' of type "
            + valueMeta.getTypeDesc()
            + ". Please verify that all the files being read have the same schema.");
  }

  @Override
  public void addBinary(Binary value) {
    if (rowIndex < 0) {
      return;
    }
    // Whatever the column holds, a Binary field gets the stored bytes as they are.
    //
    if (valueMeta.getType() == IValueMeta.TYPE_BINARY) {
      set(value.getBytes());
      return;
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimal) {
      // A DECIMAL column holds the unscaled two's complement value, never text.
      setDecimal(binaryToDecimal(value, decimal.getPrecision(), decimal.getScale()));
      return;
    }
    if (logicalTypeAnnotation instanceof Float16LogicalTypeAnnotation) {
      // IEEE 754 half precision, little endian.
      short bits = ByteBuffer.wrap(value.getBytes()).order(ByteOrder.LITTLE_ENDIAN).getShort();
      addDouble(Float.float16ToFloat(bits));
      return;
    }
    if (logicalTypeAnnotation instanceof UUIDLogicalTypeAnnotation) {
      setUuid(value);
      return;
    }

    boolean int96 = primitiveType.getPrimitiveTypeName() == PrimitiveTypeName.INT96;
    if (int96) {
      // A legacy timestamp, the only thing it can be read as.
      int type = valueMeta.getType();
      if (type != IValueMeta.TYPE_TIMESTAMP && type != IValueMeta.TYPE_DATE) {
        throw conversionError();
      }
      set(int96ToTimestamp(value));
      return;
    }
    // Only text can be read as text. Without a logical type the bytes are taken to be text too:
    // older writers leave strings unannotated.
    //
    if (!(logicalTypeAnnotation == null
        || logicalTypeAnnotation instanceof StringLogicalTypeAnnotation
        || logicalTypeAnnotation instanceof EnumLogicalTypeAnnotation
        || logicalTypeAnnotation instanceof JsonLogicalTypeAnnotation)) {
      throw conversionError();
    }

    Object object =
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_STRING -> value.toStringUsingUTF8();
          case IValueMeta.TYPE_BIGNUMBER -> {
            try {
              // Hop itself writes big numbers without a length as strings.
              yield new BigDecimal(value.toStringUsingUTF8());
            } catch (NumberFormatException e) {
              if (logicalTypeAnnotation != null) {
                throw conversionError();
              }
              // Unannotated bytes which aren't a number string: an unscaled decimal, with the
              // length and precision of the field standing in for its precision and scale.
              yield binaryToDecimal(
                  value, valueMeta.getLength(), Math.max(valueMeta.getPrecision(), 0));
            }
          }
          case IValueMeta.TYPE_JSON -> {
            try {
              yield new ObjectMapper().readTree(value.toStringUsingUTF8());
            } catch (Exception e) {
              throw new HopRuntimeException("Unable to parse an json value : " + e.getMessage());
            }
          }
          default -> throw conversionError();
        };
    set(object);
  }

  @Override
  public void addInt(int value) {
    // An unsigned INT(8/16/32) column uses the full 32 bits, so read it as the unsigned value.
    //
    if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation intType && !intType.isSigned()) {
      addLong(Integer.toUnsignedLong(value));
    } else {
      addLong(value);
    }
  }

  @Override
  public void addLong(long value) {
    if (rowIndex < 0) {
      return;
    }
    if (logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimal) {
      // The long (or int) is the unscaled value.
      setDecimal(BigDecimal.valueOf(value, decimal.getScale()));
      return;
    }
    if (logicalTypeAnnotation instanceof IntLogicalTypeAnnotation intType
        && !intType.isSigned()
        && intType.getBitWidth() == 64) {
      // An unsigned 64-bit value doesn't fit in a Java long once it passes Long.MAX_VALUE.
      setDecimal(new BigDecimal(Long.toUnsignedString(value)));
      return;
    }
    int type = valueMeta.getType();
    if (type == IValueMeta.TYPE_DATE || type == IValueMeta.TYPE_TIMESTAMP) {
      set(toDateOrTimestamp(value, type));
      return;
    }

    Object object =
        switch (type) {
          case IValueMeta.TYPE_INTEGER -> value;
            // An integer column read into a Number field: the same column can be stored as an
            // integer in one file and as a double in the next one.
          case IValueMeta.TYPE_NUMBER -> (double) value;
          case IValueMeta.TYPE_STRING -> Long.toString(value);
          case IValueMeta.TYPE_BIGNUMBER -> BigDecimal.valueOf(value);
          default -> throw conversionError();
        };
    set(object);
  }

  @Override
  public void addDouble(double value) {
    if (rowIndex < 0) {
      return;
    }
    // A floating point column can be read into an Integer field: the same column can be stored
    // as a double in one file and as an integer in the next one. We round it like Hop does
    // everywhere else when converting a Number to an Integer.
    //
    Object object =
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_NUMBER -> value;
          case IValueMeta.TYPE_INTEGER -> Math.round(value);
          case IValueMeta.TYPE_STRING -> Double.toString(value);
          case IValueMeta.TYPE_BIGNUMBER -> BigDecimal.valueOf(value);
          default -> throw conversionError();
        };
    set(object);
  }

  @Override
  public void addBoolean(boolean value) {
    if (rowIndex < 0) {
      return;
    }
    Object object =
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_BOOLEAN -> value;
          case IValueMeta.TYPE_STRING -> value ? "true" : "false";
          case IValueMeta.TYPE_INTEGER -> value ? 1L : 0L;
          default -> throw conversionError();
        };
    set(object);
  }

  @Override
  public void addFloat(float value) {
    addDouble(value);
  }

  private void set(Object object) {
    group.getData()[rowIndex] = object;
  }

  /** A DECIMAL or unsigned 64-bit value, into the type of the field. */
  private void setDecimal(BigDecimal decimal) {
    Object object =
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_BIGNUMBER -> decimal;
          case IValueMeta.TYPE_NUMBER -> decimal.doubleValue();
          case IValueMeta.TYPE_STRING -> decimal.toPlainString();
          case IValueMeta.TYPE_INTEGER -> {
            try {
              // Drop the fraction, as Hop does when converting a BigNumber to an Integer.
              yield decimal.setScale(0, RoundingMode.DOWN).longValueExact();
            } catch (ArithmeticException e) {
              throw new HopRuntimeException(
                  "Value "
                      + decimal.toPlainString()
                      + " of Parquet column '"
                      + primitiveType.getName()
                      + "' doesn't fit in Integer field '"
                      + valueMeta.getName()
                      + "'");
            }
          }
          default -> throw conversionError();
        };
    set(object);
  }

  /** A 16-byte big-endian UUID, into the type of the field. */
  private void setUuid(Binary value) {
    ByteBuffer buffer = value.toByteBuffer();
    UUID uuid = new UUID(buffer.getLong(), buffer.getLong());
    Object object =
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_STRING -> uuid.toString();
          case IValueMeta.TYPE_UUID -> uuid;
          default -> throw conversionError();
        };
    set(object);
  }

  /**
   * An integer column annotated as DATE, TIME or TIMESTAMP, read into a Date or Timestamp field.
   *
   * @param value the stored value
   * @param type the Hop type of the field: Date or Timestamp
   * @return the date or timestamp
   */
  private Date toDateOrTimestamp(long value, int type) {
    if (logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
      // Days since the epoch: midnight of that day, like any other Hop date without a time.
      Date date =
          Date.from(LocalDate.ofEpochDay(value).atStartOfDay(ZoneId.systemDefault()).toInstant());
      return type == IValueMeta.TYPE_TIMESTAMP ? new Timestamp(date.getTime()) : date;
    }
    if (logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timestamp) {
      return toTimestamp(value, timestamp.getUnit(), timestamp.isAdjustedToUTC());
    }
    if (logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time) {
      // A time of day: that time on 1970-01-01.
      return toTimestamp(value, time.getUnit(), time.isAdjustedToUTC());
    }
    throw conversionError();
  }

  /**
   * Converts a number of time units since the epoch into a Timestamp, keeping the sub-millisecond
   * part.
   *
   * @param value the number of units
   * @param unit the unit of the value
   * @param adjustedToUtc true if the value is an instant, false if it holds the fields of a local
   *     date and time, which are then taken in the time zone of the JVM
   * @return the timestamp
   */
  static Timestamp toTimestamp(long value, TimeUnit unit, boolean adjustedToUtc) {
    long unitsPerSecond =
        switch (unit) {
          case MILLIS -> 1_000L;
          case MICROS -> 1_000_000L;
          case NANOS -> 1_000_000_000L;
        };
    // Floor, not truncate: before 1970 the fraction of a second must stay positive.
    long seconds = Math.floorDiv(value, unitsPerSecond);
    int nanos = (int) (Math.floorMod(value, unitsPerSecond) * (1_000_000_000L / unitsPerSecond));
    if (adjustedToUtc) {
      return Timestamp.from(Instant.ofEpochSecond(seconds, nanos));
    }
    return Timestamp.valueOf(LocalDateTime.ofEpochSecond(seconds, nanos, ZoneOffset.UTC));
  }

  /**
   * An INT96 timestamp: nanoseconds in the day (8 bytes) and the Julian day (4 bytes), little
   * endian.
   */
  private static Timestamp int96ToTimestamp(Binary value) {
    ByteBuffer bb = ByteBuffer.wrap(value.getBytes()).order(ByteOrder.LITTLE_ENDIAN);
    long nsDay = bb.getLong();
    long julianDay = bb.getInt() & 0x00000000ffffffffL;

    // We need a big integer to prevent a long overflow resulting in negative values
    // for: nanoseconds since 1970/01/01 00:00:00
    //
    BigInteger bns =
        BigInteger.valueOf(julianDay - 2440588L)
            .multiply(BigInteger.valueOf(86400L * 1000 * 1000 * 1000))
            .add(BigInteger.valueOf(nsDay));
    BigInteger nanosPerSecond = BigInteger.valueOf(1_000_000_000L);
    BigInteger[] secondsAndNanos = bns.divideAndRemainder(nanosPerSecond);
    BigInteger seconds = secondsAndNanos[0];
    BigInteger nanos = secondsAndNanos[1];
    if (nanos.signum() < 0) {
      // Before 1970: keep the nanos positive, as Timestamp requires.
      seconds = seconds.subtract(BigInteger.ONE);
      nanos = nanos.add(nanosPerSecond);
    }
    Timestamp timestamp = new Timestamp(seconds.longValue() * 1000L);
    timestamp.setNanos(nanos.intValue());
    return timestamp;
  }

  /**
   * Decodes a DECIMAL stored as bytes: the big-endian two's complement unscaled value.
   *
   * @param value the stored bytes
   * @param precision the precision of the decimal, not needed to decode it
   * @param scale the number of digits after the decimal point
   * @return the exact decimal value
   */
  public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
    byte[] bytes = value.getBytes();
    if (bytes.length == 0) {
      return BigDecimal.ZERO.setScale(scale);
    }
    return new BigDecimal(new BigInteger(bytes), scale);
  }
}
