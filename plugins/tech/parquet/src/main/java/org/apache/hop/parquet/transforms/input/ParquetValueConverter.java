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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IValueMeta;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class ParquetValueConverter extends PrimitiveConverter {

  private final RowMetaAndData group;
  private final IValueMeta valueMeta;
  private final int rowIndex;

  public ParquetValueConverter(RowMetaAndData group, int rowIndex) {
    this.group = group;
    this.valueMeta = group.getValueMeta(rowIndex);
    this.rowIndex = rowIndex;
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
        try {
          object = new BigDecimal(value.toStringUsingUTF8());
        } catch (NumberFormatException e) {
          object = binaryToDecimal(value, valueMeta.getLength(), valueMeta.getPrecision());
        }
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
          BigInteger bms = bns.divide(BigInteger.valueOf(1000000));
          long ms = bms.longValue();
          int nanos = (int) (ms % 1000000000);
          Timestamp timestamp = new Timestamp(ms);
          timestamp.setNanos(nanos);
          object = timestamp;
          break;
        }

      default:
        throw new RuntimeException(
            "Unable to convert Binary source data to type " + valueMeta.getTypeDesc());
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
      case IValueMeta.TYPE_STRING:
        object = Long.toString(value);
        break;
      case IValueMeta.TYPE_DATE:
        object = new Date(value);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        object = new BigDecimal(value);
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Long source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addDouble(double value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_NUMBER:
        object = value;
        break;
      case IValueMeta.TYPE_STRING:
        object = Double.toString(value);
        break;
      case IValueMeta.TYPE_BIGNUMBER:
        object = BigDecimal.valueOf(value);
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Double/Float source data to type " + valueMeta.getTypeDesc());
    }
    group.getData()[rowIndex] = object;
  }

  @Override
  public void addBoolean(boolean value) {
    if (rowIndex < 0) {
      return;
    }
    Object object;
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_BOOLEAN:
        object = value;
        break;
      case IValueMeta.TYPE_STRING:
        object = value ? "true" : "false";
        break;
      case IValueMeta.TYPE_INTEGER:
        object = value ? 1L : 0L;
        break;
      default:
        throw new RuntimeException(
            "Unable to convert Boolean source data to type " + valueMeta.getTypeDesc());
    }
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
