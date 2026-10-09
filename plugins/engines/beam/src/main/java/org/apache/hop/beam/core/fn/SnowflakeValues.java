/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.beam.core.fn;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HexFormat;
import java.util.Locale;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeDataType;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;

/**
 * CSV values handed to and from SnowflakeIO. Timestamps are UTC {@code yyyy-MM-dd HH:mm:ss.SSS}. An
 * empty CSV field is null, including for strings, so an empty string is not preserved. Number
 * values are IEEE doubles. BigNumber keeps the decimal text.
 */
public final class SnowflakeValues {
  static final DateTimeFormatter TIMESTAMP =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter TIMESTAMP_LOCAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private SnowflakeValues() {}

  public static SnowflakeDataType snowflakeType(IValueMeta meta) throws HopException {
    return switch (meta.getType()) {
      case IValueMeta.TYPE_STRING ->
          meta.getLength() > 0 ? SnowflakeVarchar.of(meta.getLength()) : SnowflakeVarchar.of();
      case IValueMeta.TYPE_INTEGER -> SnowflakeInteger.of();
      case IValueMeta.TYPE_NUMBER -> SnowflakeFloat.of();
      case IValueMeta.TYPE_BIGNUMBER -> {
        // Hop stores the decimal precision in length and the scale in precision.
        int precision = meta.getLength();
        int scale = Math.max(0, meta.getPrecision());
        yield precision > 0 ? SnowflakeNumber.of(precision, scale) : SnowflakeNumber.of();
      }
      case IValueMeta.TYPE_BOOLEAN -> SnowflakeBoolean.of();
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> SnowflakeTimestampNTZ.of();
      case IValueMeta.TYPE_BINARY -> SnowflakeBinary.of();
      default -> throw new HopException("Unsupported Snowflake field type: " + meta.getTypeDesc());
    };
  }

  public static int hopType(String typeName) throws HopException {
    int type = ValueMetaFactory.getIdForValueMeta(typeName);
    if (type == IValueMeta.TYPE_NONE
        || !(type == IValueMeta.TYPE_STRING
            || type == IValueMeta.TYPE_INTEGER
            || type == IValueMeta.TYPE_NUMBER
            || type == IValueMeta.TYPE_BIGNUMBER
            || type == IValueMeta.TYPE_BOOLEAN
            || type == IValueMeta.TYPE_DATE
            || type == IValueMeta.TYPE_TIMESTAMP
            || type == IValueMeta.TYPE_BINARY)) {
      throw new HopException("Unsupported Snowflake field type: " + typeName);
    }
    return type;
  }

  /** A string Beam will quote, or null for an empty CSV field. */
  public static String toCsv(IValueMeta meta, Object value) throws HopException {
    if (value == null) return null;
    try {
      return switch (meta.getType()) {
        case IValueMeta.TYPE_STRING -> value.toString();
        case IValueMeta.TYPE_INTEGER -> Long.toString(((Number) value).longValue());
        case IValueMeta.TYPE_NUMBER ->
            BigDecimal.valueOf(((Number) value).doubleValue()).stripTrailingZeros().toPlainString();
        case IValueMeta.TYPE_BIGNUMBER -> decimal(value).toPlainString();
        case IValueMeta.TYPE_BOOLEAN -> Boolean.TRUE.equals(value) ? "true" : "false";
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> {
          if (value instanceof Date date) {
            yield TIMESTAMP.format(date.toInstant());
          }
          fail(meta);
          yield null;
        }
        case IValueMeta.TYPE_BINARY -> {
          if (value instanceof byte[] bytes) {
            yield HexFormat.of().formatHex(bytes);
          }
          fail(meta);
          yield null;
        }
        default -> {
          fail(meta);
          yield null;
        }
      };
    } catch (HopException e) {
      throw e;
    } catch (RuntimeException e) {
      fail(meta);
      return null;
    }
  }

  public static Object fromCsv(IValueMeta meta, String text) throws HopException {
    if (text == null || text.isEmpty()) return null;
    if (meta.getType() == IValueMeta.TYPE_STRING) return text;
    String trimmed = text.trim();
    if (trimmed.isEmpty()) return null;
    try {
      return switch (meta.getType()) {
        case IValueMeta.TYPE_INTEGER ->
            new BigDecimal(trimmed).stripTrailingZeros().longValueExact();
        case IValueMeta.TYPE_NUMBER -> finiteDouble(new BigDecimal(trimmed));
        case IValueMeta.TYPE_BIGNUMBER -> new BigDecimal(trimmed);
        case IValueMeta.TYPE_BOOLEAN -> bool(trimmed);
        case IValueMeta.TYPE_DATE -> parseDate(trimmed);
        case IValueMeta.TYPE_TIMESTAMP -> new java.sql.Timestamp(parseDate(trimmed).getTime());
        case IValueMeta.TYPE_BINARY -> HexFormat.of().parseHex(trimmed);
        default -> {
          fail(meta);
          yield null;
        }
      };
    } catch (HopException e) {
      throw e;
    } catch (RuntimeException e) {
      fail(meta);
      return null;
    }
  }

  private static BigDecimal decimal(Object value) {
    if (value instanceof BigDecimal decimal) return decimal;
    return new BigDecimal(value.toString());
  }

  private static Double finiteDouble(BigDecimal number) {
    double value = number.doubleValue();
    if (!Double.isFinite(value)) throw new IllegalArgumentException("outside double range");
    return value;
  }

  private static Boolean bool(String text) {
    return switch (text.toLowerCase(Locale.ROOT)) {
      case "true", "1" -> Boolean.TRUE;
      case "false", "0" -> Boolean.FALSE;
      default -> throw new IllegalArgumentException("boolean");
    };
  }

  private static Date parseDate(String text) {
    if (text.length() == 10) {
      return Date.from(LocalDate.parse(text).atStartOfDay(ZoneOffset.UTC).toInstant());
    }
    Instant instant = LocalDateTime.parse(text, TIMESTAMP_LOCAL).toInstant(ZoneOffset.UTC);
    return Date.from(instant);
  }

  private static void fail(IValueMeta meta) throws HopException {
    throw new HopException("Unable to convert Snowflake field " + meta.getName());
  }
}
