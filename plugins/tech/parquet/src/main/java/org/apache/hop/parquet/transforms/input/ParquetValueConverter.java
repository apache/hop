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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

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
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        object = value.toStringUsingUTF8();
        break;
      case IValueMeta.TYPE_BINARY:
        object = value.getBytes();
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        if (this.logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimal) {
          // A DECIMAL column holds the unscaled two's complement value, never text. Trying the
          // text route first would misread bytes that happen to be ASCII digits.
          object = binaryToDecimal(value, decimal.getPrecision(), decimal.getScale());
        } else {
          try {
            // Hop itself writes big numbers as strings.
            object = new BigDecimal(value.toStringUsingUTF8());
          } catch (NumberFormatException e) {
            object = binaryToDecimal(value, valueMeta.getLength(), valueMeta.getPrecision());
          }
        }
        break;
      case IValueMeta.TYPE_JSON:
        JsonNode node = null;
        try {
          ObjectMapper mapper = new ObjectMapper();
          node = mapper.readTree(value.toStringUsingUTF8());
        } catch (Exception e) {
          throw new HopRuntimeException("Unable to parse an json value : " + e.getMessage());
        }
        object = node;
        break;
      case IValueMeta.TYPE_TIMESTAMP:
        if (value.length() == 12) {
          // This is a binary form of an int96 (12-byte) Timestamp with nanosecond precision.
          // The first 8 bytes are the nanoseconds in a day.
          // The next 4 bytes are the Julian day.
          // Note: Little Endian.
          //
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
          object = timestamp;
          break;
        }
      default:
        throw conversionError();
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addLong(long value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_INTEGER:
        object = value;
        break;
      case IValueMeta.TYPE_NUMBER:
        // An integer column read into a Number field: the same column can be stored as an
        // integer in one file and as a double in the next one.
        object = (double) value;
        break;
      case IValueMeta.TYPE_STRING:
        object = Long.toString(value);
        break;
      case IValueMeta.TYPE_DATE:
        if (this.logicalTypeAnnotation instanceof DateLogicalTypeAnnotation) {
          LocalDate date = LocalDate.ofEpochDay(value);
          Date utilDate = Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
          object = utilDate;
        } else {
          object = convertToTimestamp(value, this.logicalTypeAnnotation);
        }
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        if (this.logicalTypeAnnotation instanceof DecimalLogicalTypeAnnotation decimal) {
          // The long is the unscaled value.
          object = BigDecimal.valueOf(value, decimal.getScale());
        } else {
          object = BigDecimal.valueOf(value);
        }
        break;
      case IValueMeta.TYPE_TIMESTAMP:
        object = convertToTimestamp(value, this.logicalTypeAnnotation);
        break;
      default:
        throw conversionError();
    }
    group.getData()[rowIndex] = object;
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
    group.getData()[rowIndex] = object;
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
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addFloat(float value) {
    addDouble(value);
  }

  @Override
  public void addInt(int value) {
    addLong(value);
  }

  /**
   * Converts a numeric epoch timestamp (in millis, micros, or nanos) into a java.sql.Timestamp
   * according to the logical type's time unit.
   *
   * @param value
   * @param logicalTypeAnnotation
   */
  private Timestamp convertToTimestamp(long value, LogicalTypeAnnotation logicalTypeAnnotation) {
    LogicalTypeAnnotation.TimeUnit unit = null;
    boolean isUTC = true;
    if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      unit =
          ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation).getUnit();
      isUTC =
          ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) this.logicalTypeAnnotation)
              .isAdjustedToUTC();
    } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
      unit = ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation).getUnit();
      isUTC = false;
    }
    if (unit == null) {
      throw new HopRuntimeException(
          "Unknown timestamp unit for the logical type: " + logicalTypeAnnotation);
    }
    long epochMillis =
        switch (unit) {
          case MILLIS -> value;
          case MICROS -> value / 1_000L;
          case NANOS -> value / 1_000_000L;
          default -> throw new HopRuntimeException("Unknown timestamp unit: " + unit);
        };
    // Convert the timestamp to milliseconds since the Epoch based on the original unit
    Timestamp ts = new Timestamp(epochMillis);
    // If the timestamp is local time, adjust it to UTC
    if (!isUTC) {
      int offset = TimeZone.getDefault().getOffset(epochMillis);
      ts.setTime(ts.getTime() - offset);
    }
    // Adjust nanosecond precision for microsecond or nanosecond timestamps
    if (unit == LogicalTypeAnnotation.TimeUnit.MICROS) {
      ts.setNanos((int) ((value % 1_000_000L) * 1_000L));
    } else if (unit == LogicalTypeAnnotation.TimeUnit.NANOS) {
      ts.setNanos((int) (value % 1_000_000_000L));
    }
    return ts;
  }

  /**
   * Source code from:
   *
   * <p>apache/parquet-mr/parquet-pig/src/main/java/org/apache/parquet/pig/convert/DecimalUtils.java
   *
   * @param value
   * @param precision
   * @param scale
   * @return
   */
  public static BigDecimal binaryToDecimal(Binary value, int precision, int scale) {
    /*
     * Precision <= 18 checks for the max number of digits for an unscaled long,
     * else treat with big integer conversion
     */
    if (precision <= 18) {
      ByteBuffer buffer = value.toByteBuffer();
      byte[] bytes = buffer.array();
      int start = buffer.arrayOffset() + buffer.position();
      int end = buffer.arrayOffset() + buffer.limit();
      long unscaled = 0L;
      int i = start;
      while (i < end) {
        unscaled = (unscaled << 8 | bytes[i] & 0xff);
        i++;
      }
      int bits = 8 * (end - start);
      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
      if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
        return new BigDecimal(unscaledNew);
      } else {
        return BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
      }
    } else {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }
}
