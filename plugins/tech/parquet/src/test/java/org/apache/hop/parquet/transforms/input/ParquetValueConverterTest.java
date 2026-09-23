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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Unit test for {@link ParquetValueConverter}: every Parquet primitive into every Hop type. */
class ParquetValueConverterTest {

  private RowMetaAndData group;

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    // The value types need to be registered for IValueMeta.getTypeDesc() to name them.
    HopClientEnvironment.init();
  }

  private ParquetValueConverter converter(IValueMeta valueMeta, LogicalTypeAnnotation annotation) {
    return converter(valueMeta, PrimitiveTypeName.BINARY, annotation);
  }

  private ParquetValueConverter converter(
      IValueMeta valueMeta, PrimitiveTypeName typeName, LogicalTypeAnnotation annotation) {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta);
    group = new RowMetaAndData(rowMeta, new Object[1]);
    PrimitiveType column =
        new PrimitiveType(Type.Repetition.OPTIONAL, typeName, "column")
            .withLogicalTypeAnnotation(annotation);
    return new ParquetValueConverter(group, 0, column);
  }

  private Object value() {
    return group.getData()[0];
  }

  @Test
  void binaryIntoStringBinaryAndBigNumber() {
    converter(new ValueMetaString("s"), null).addBinary(Binary.fromString("héllo"));
    assertEquals("héllo", value());

    converter(new ValueMetaBinary("b"), null)
        .addBinary(Binary.fromConstantByteArray(new byte[] {1, 2, 3}));
    assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) value());

    converter(new ValueMetaBigNumber("n"), null).addBinary(Binary.fromString("12345.678"));
    assertEquals(new BigDecimal("12345.678"), value());
  }

  @Test
  void binaryDecimalUsesTheAnnotationScale() {
    // Unscaled 12345 with scale 2, big-endian two's complement as Parquet stores it.
    Binary unscaled = Binary.fromConstantByteArray(new byte[] {0x30, 0x39});
    converter(new ValueMetaBigNumber("n"), LogicalTypeAnnotation.decimalType(2, 9))
        .addBinary(unscaled);
    assertEquals(0, new BigDecimal("123.45").compareTo((BigDecimal) value()));

    // Without an annotation, bytes that are not a number string are decoded with the value
    // meta's length/precision standing in for precision/scale.
    ValueMetaBigNumber valueMeta = new ValueMetaBigNumber("n");
    valueMeta.setLength(9);
    valueMeta.setPrecision(1);
    converter(valueMeta, null)
        .addBinary(Binary.fromConstantByteArray(new byte[] {0x04, (byte) 0xD2}));
    assertEquals(0, new BigDecimal("123.4").compareTo((BigDecimal) value()));

    // More than 18 digits of precision goes through BigInteger.
    converter(new ValueMetaBigNumber("n"), LogicalTypeAnnotation.decimalType(0, 38))
        .addBinary(
            Binary.fromConstantByteArray(
                new BigDecimal("1234567890123456789012").unscaledValue().toByteArray()));
    assertEquals(new BigDecimal("1234567890123456789012"), value());
  }

  @Test
  void binaryIntoJson() {
    converter(new ValueMetaJson("j"), null).addBinary(Binary.fromString("{\"a\":1}"));
    assertEquals(1, ((JsonNode) value()).get("a").asInt());

    ParquetValueConverter bad = converter(new ValueMetaJson("j"), null);
    assertThrows(HopRuntimeException.class, () -> bad.addBinary(Binary.fromString("{not json")));
  }

  @Test
  void int96IntoTimestampKeepsNanoseconds() {
    // 2024-01-01T00:00:01.123456789Z as INT96: nanos-in-day (little endian long) + Julian day.
    long julianDay = 2460311L; // 2024-01-01
    long nanosInDay = 1_123_456_789L;
    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(nanosInDay).putInt((int) julianDay);

    converter(new ValueMetaTimestamp("ts"), PrimitiveTypeName.INT96, null)
        .addBinary(Binary.fromConstantByteArray(buffer.array()));

    Timestamp timestamp = (Timestamp) value();
    assertEquals(
        Timestamp.valueOf("2024-01-01 00:00:01.123456789").getNanos(), timestamp.getNanos());
    assertEquals(1704067201_000L + 123, timestamp.getTime());
  }

  @Test
  void int96BeforeTheEpochStaysValid() {
    // 1969-12-31T23:59:59.5Z: Julian day 2440587, half a second before midnight.
    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(86_399_500_000_000L).putInt(2440587);

    converter(new ValueMetaTimestamp("ts"), PrimitiveTypeName.INT96, null)
        .addBinary(Binary.fromConstantByteArray(buffer.array()));

    Timestamp timestamp = (Timestamp) value();
    assertEquals(-500L, timestamp.getTime());
    assertEquals(500_000_000, timestamp.getNanos());
  }

  @Test
  void binaryIntoAnUnsupportedTypeFails() {
    ParquetValueConverter c = converter(new ValueMetaInteger("i"), null);
    assertThrows(HopRuntimeException.class, () -> c.addBinary(Binary.fromString("1")));
  }

  @Test
  void longIntoIntegerStringAndBigNumber() {
    converter(new ValueMetaInteger("i"), null).addLong(42L);
    assertEquals(42L, value());

    converter(new ValueMetaString("s"), null).addLong(42L);
    assertEquals("42", value());

    converter(new ValueMetaBigNumber("n"), null).addLong(42L);
    assertEquals(BigDecimal.valueOf(42L), value());

    // A decimal-annotated long is the unscaled value.
    converter(new ValueMetaBigNumber("n"), LogicalTypeAnnotation.decimalType(3, 18))
        .addLong(1234567L);
    assertEquals(new BigDecimal("1234.567"), value());

    converter(new ValueMetaInteger("i"), null).addInt(7);
    assertEquals(7L, value());
  }

  @Test
  void longIntoDateAndTimestamp() {
    // DATE annotation: days since the epoch, at local midnight.
    converter(new ValueMetaDate("d"), LogicalTypeAnnotation.dateType()).addLong(19723L);
    Date expected =
        Date.from(LocalDate.ofEpochDay(19723L).atStartOfDay(ZoneId.systemDefault()).toInstant());
    assertEquals(expected, value());

    // TIMESTAMP annotations in every unit, adjusted to UTC.
    converter(
            new ValueMetaTimestamp("ts"),
            LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
        .addLong(1_700_000_000_123L);
    assertEquals(1_700_000_000_123L, ((Timestamp) value()).getTime());

    converter(
            new ValueMetaTimestamp("ts"),
            LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS))
        .addLong(1_700_000_000_123_456L);
    assertEquals(1_700_000_000_123L, ((Timestamp) value()).getTime());
    assertEquals(123_456_000, ((Timestamp) value()).getNanos());

    converter(
            new ValueMetaTimestamp("ts"), LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS))
        .addLong(1_700_000_000_123_456_789L);
    assertEquals(1_700_000_000_123L, ((Timestamp) value()).getTime());
    assertEquals(123_456_789, ((Timestamp) value()).getNanos());

    // A Date target with a timestamp annotation goes through the same conversion.
    converter(new ValueMetaDate("d"), LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS))
        .addLong(1_700_000_000_123L);
    assertEquals(1_700_000_000_123L, ((Date) value()).getTime());
  }

  @Test
  void localTimestampsAreShiftedToUtc() {
    long millis = 1_700_000_000_000L;
    converter(
            new ValueMetaTimestamp("ts"),
            LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS))
        .addLong(millis);
    long offset = TimeZone.getDefault().getOffset(millis);
    assertEquals(millis - offset, ((Timestamp) value()).getTime());

    converter(new ValueMetaTimestamp("ts"), LogicalTypeAnnotation.timeType(false, TimeUnit.MILLIS))
        .addLong(millis);
    assertEquals(millis - offset, ((Timestamp) value()).getTime());
  }

  @Test
  void longWithoutATimestampAnnotationCannotBecomeATimestamp() {
    ParquetValueConverter c = converter(new ValueMetaTimestamp("ts"), null);
    assertThrows(HopRuntimeException.class, () -> c.addLong(1L));

    ParquetValueConverter b = converter(new ValueMetaBoolean("b"), null);
    assertThrows(HopRuntimeException.class, () -> b.addLong(1L));
  }

  @Test
  void doubleIntoNumberStringAndBigNumber() {
    converter(new ValueMetaNumber("n"), null).addDouble(1.5D);
    assertEquals(1.5D, value());

    converter(new ValueMetaString("s"), null).addDouble(1.5D);
    assertEquals("1.5", value());

    converter(new ValueMetaBigNumber("b"), null).addDouble(1.5D);
    assertEquals(BigDecimal.valueOf(1.5D), value());

    converter(new ValueMetaNumber("n"), null).addFloat(2.5F);
    assertEquals(2.5D, value());

    // The same column can be a double in one file and an integer in the next one (#3598).
    converter(new ValueMetaInteger("i"), null).addDouble(1.5D);
    assertEquals(2L, value());

    converter(new ValueMetaInteger("i"), null).addFloat(41.4F);
    assertEquals(41L, value());

    ParquetValueConverter c = converter(new ValueMetaBoolean("b"), null);
    assertThrows(HopRuntimeException.class, () -> c.addDouble(1.5D));
  }

  @Test
  void booleanIntoBooleanStringAndInteger() {
    converter(new ValueMetaBoolean("b"), null).addBoolean(true);
    assertEquals(true, value());

    converter(new ValueMetaString("s"), null).addBoolean(false);
    assertEquals("false", value());

    converter(new ValueMetaInteger("i"), null).addBoolean(true);
    assertEquals(1L, value());

    ParquetValueConverter c = converter(new ValueMetaNumber("n"), null);
    assertThrows(HopRuntimeException.class, () -> c.addBoolean(true));
  }

  @Test
  void unmappedColumnsAreIgnored() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("s"));
    group = new RowMetaAndData(rowMeta, new Object[1]);
    ParquetValueConverter ignored = new ParquetValueConverter(group, -1, null);

    ignored.addBinary(Binary.fromString("x"));
    ignored.addLong(1L);
    ignored.addDouble(1D);
    ignored.addBoolean(true);

    assertNull(group.getData()[0]);
  }

  @Test
  void binaryToDecimalHandlesNegativeValues() {
    // -1 as a single two's complement byte, scale 1.
    BigDecimal small =
        ParquetValueConverter.binaryToDecimal(
            Binary.fromConstantByteArray(new byte[] {(byte) 0xFF}), 9, 1);
    assertEquals(0, new BigDecimal("-0.1").compareTo(small));

    // The same byte through the BigInteger path used for more than 18 digits.
    BigDecimal large =
        ParquetValueConverter.binaryToDecimal(
            Binary.fromConstantByteArray(new byte[] {(byte) 0xFF}), 38, 1);
    assertEquals(0, new BigDecimal("-0.1").compareTo(large));
  }

  /** The same column can be an integer in one file and a double in the next one (#3598). */
  @Test
  void longAndIntIntoNumber() {
    converter(new ValueMetaNumber("n"), PrimitiveTypeName.INT64, null).addLong(9081496L);
    assertEquals(9081496.0D, value());

    converter(new ValueMetaNumber("n"), PrimitiveTypeName.INT32, null).addInt(42);
    assertEquals(42.0D, value());
  }

  /**
   * A conversion which really can't be made names the Parquet column, its type, the Hop field and
   * its type, so the schema mismatch behind it can be found (#3598).
   */
  @Test
  void unsupportedConversionNamesColumnAndField() {
    ParquetValueConverter c =
        converter(new ValueMetaBoolean("flag"), PrimitiveTypeName.DOUBLE, null);

    HopRuntimeException e = assertThrows(HopRuntimeException.class, () -> c.addDouble(1.5D));

    String message = e.getMessage();
    assertTrue(message.contains("'column'"), message);
    assertTrue(message.contains("DOUBLE"), message);
    assertTrue(message.contains("'flag'"), message);
    assertTrue(message.contains("Boolean"), message);
    assertTrue(message.contains("same schema"), message);
  }
}
