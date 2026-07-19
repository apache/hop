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

package org.apache.hop.spark.core;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Bidirectional conversion between Hop {@link IRowMeta}/{@code Object[]} and Spark {@link
 * StructType}/{@link Row}.
 */
public final class HopSparkRowConverter {

  private HopSparkRowConverter() {}

  public static StructType toStructType(IRowMeta rowMeta) throws HopException {
    if (rowMeta == null) {
      return new StructType();
    }
    List<StructField> fields = new ArrayList<>(rowMeta.size());
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      fields.add(
          new StructField(valueMeta.getName(), toDataType(valueMeta), true, Metadata.empty()));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  public static DataType toDataType(IValueMeta valueMeta) throws HopException {
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING, IValueMeta.TYPE_INET -> DataTypes.StringType;
      case IValueMeta.TYPE_INTEGER -> DataTypes.LongType;
      case IValueMeta.TYPE_NUMBER -> DataTypes.DoubleType;
      case IValueMeta.TYPE_BIGNUMBER -> DataTypes.createDecimalType(38, 18);
      case IValueMeta.TYPE_BOOLEAN -> DataTypes.BooleanType;
      case IValueMeta.TYPE_DATE -> DataTypes.TimestampType;
      case IValueMeta.TYPE_TIMESTAMP -> DataTypes.TimestampType;
      case IValueMeta.TYPE_BINARY -> DataTypes.BinaryType;
      case IValueMeta.TYPE_NONE, IValueMeta.TYPE_SERIALIZABLE -> DataTypes.StringType;
      default ->
          throw new HopException(
              "Unsupported Hop type for Spark conversion: "
                  + valueMeta.getTypeDesc()
                  + " ("
                  + valueMeta.getName()
                  + ")");
    };
  }

  public static Row toSparkRow(IRowMeta rowMeta, Object[] hopRow) throws HopException {
    Object[] values = new Object[rowMeta.size()];
    for (int i = 0; i < rowMeta.size(); i++) {
      Object hopValue = hopRow == null || i >= hopRow.length ? null : hopRow[i];
      values[i] = toSparkValue(rowMeta.getValueMeta(i), hopValue);
    }
    return RowFactory.create(values);
  }

  /**
   * Spark row with a leading target-branch tag column ({@link HopSparkUtil#TARGET_TAG_COLUMN}) for
   * multi-output transforms (Filter Rows, Switch/Case).
   */
  public static Row toTaggedSparkRow(String targetTag, IRowMeta rowMeta, Object[] hopRow)
      throws HopException {
    Object[] values = new Object[rowMeta.size() + 1];
    values[0] = targetTag != null ? targetTag : HopSparkUtil.MAIN_TARGET_TAG;
    for (int i = 0; i < rowMeta.size(); i++) {
      Object hopValue = hopRow == null || i >= hopRow.length ? null : hopRow[i];
      values[i + 1] = toSparkValue(rowMeta.getValueMeta(i), hopValue);
    }
    return RowFactory.create(values);
  }

  /** Schema for {@link #toTaggedSparkRow}: tag string + payload fields. */
  public static StructType toTaggedStructType(IRowMeta rowMeta) throws HopException {
    StructType payload = toStructType(rowMeta);
    StructField tagField =
        new StructField(
            HopSparkUtil.TARGET_TAG_COLUMN, DataTypes.StringType, false, Metadata.empty());
    StructField[] fields = new StructField[payload.size() + 1];
    fields[0] = tagField;
    for (int i = 0; i < payload.size(); i++) {
      fields[i + 1] = payload.fields()[i];
    }
    return new StructType(fields);
  }

  public static Object[] toHopRow(IRowMeta rowMeta, Row sparkRow) throws HopException {
    Object[] hopRow = new Object[rowMeta.size()];
    if (sparkRow == null) {
      return hopRow;
    }
    // Tolerate short Spark rows (schema drift) — pad remaining Hop fields with null.
    int available = Math.min(rowMeta.size(), sparkRow.length());
    for (int i = 0; i < available; i++) {
      hopRow[i] =
          toHopValue(rowMeta.getValueMeta(i), sparkRow.isNullAt(i) ? null : sparkRow.get(i));
    }
    return hopRow;
  }

  static Object toSparkValue(IValueMeta valueMeta, Object hopValue) throws HopException {
    if (hopValue == null) {
      return null;
    }
    try {
      return switch (valueMeta.getType()) {
        case IValueMeta.TYPE_STRING, IValueMeta.TYPE_INET -> valueMeta.getString(hopValue);
        case IValueMeta.TYPE_INTEGER -> valueMeta.getInteger(hopValue);
        case IValueMeta.TYPE_NUMBER -> valueMeta.getNumber(hopValue);
        case IValueMeta.TYPE_BIGNUMBER -> valueMeta.getBigNumber(hopValue);
        case IValueMeta.TYPE_BOOLEAN -> valueMeta.getBoolean(hopValue);
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> {
          Date date = valueMeta.getDate(hopValue);
          yield date == null ? null : new Timestamp(date.getTime());
        }
        case IValueMeta.TYPE_BINARY -> valueMeta.getBinary(hopValue);
        default -> valueMeta.getString(hopValue);
      };
    } catch (Exception e) {
      throw new HopException(
          "Error converting Hop value to Spark for field '" + valueMeta.getName() + "'", e);
    }
  }

  static Object toHopValue(IValueMeta valueMeta, Object sparkValue) throws HopException {
    if (sparkValue == null) {
      return null;
    }
    try {
      return switch (valueMeta.getType()) {
        case IValueMeta.TYPE_STRING, IValueMeta.TYPE_INET -> sparkValue.toString();
        case IValueMeta.TYPE_INTEGER -> {
          if (sparkValue instanceof Number number) {
            yield number.longValue();
          }
          yield Long.parseLong(sparkValue.toString());
        }
        case IValueMeta.TYPE_NUMBER -> {
          if (sparkValue instanceof Number number) {
            yield number.doubleValue();
          }
          yield Double.parseDouble(sparkValue.toString());
        }
        case IValueMeta.TYPE_BIGNUMBER -> {
          if (sparkValue instanceof BigDecimal bigDecimal) {
            yield bigDecimal;
          }
          if (sparkValue instanceof Number number) {
            yield BigDecimal.valueOf(number.doubleValue());
          }
          yield new BigDecimal(sparkValue.toString());
        }
        case IValueMeta.TYPE_BOOLEAN -> {
          if (sparkValue instanceof Boolean bool) {
            yield bool;
          }
          yield Boolean.parseBoolean(sparkValue.toString());
        }
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> {
          if (sparkValue instanceof Timestamp timestamp) {
            yield new Date(timestamp.getTime());
          }
          if (sparkValue instanceof Date date) {
            yield date;
          }
          if (sparkValue instanceof java.time.Instant instant) {
            yield Date.from(instant);
          }
          yield sparkValue;
        }
        case IValueMeta.TYPE_BINARY -> {
          if (sparkValue instanceof byte[] bytes) {
            yield bytes;
          }
          if (sparkValue instanceof ByteBuffer buffer) {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            yield bytes;
          }
          yield sparkValue;
        }
        default -> sparkValue.toString();
      };
    } catch (Exception e) {
      throw new HopException(
          "Error converting Spark value to Hop for field '" + valueMeta.getName() + "'", e);
    }
  }

  /** Whether a Spark DataType is a decimal (for tests). */
  public static boolean isDecimal(DataType dataType) {
    return dataType instanceof DecimalType;
  }
}
