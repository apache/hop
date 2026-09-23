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

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicContainer.dynamicContainer;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

/**
 * What every Parquet column type is read as, for every Hop type a field can have: the matrix of the
 * Parquet to Hop conversion.
 *
 * <p>Each column lists the Hop type Get Fields proposes for it and what it reads as in the Hop
 * types that accept it. Every Hop type a column doesn't list must fail with a conversion error, so
 * the matrix has no gaps. Values are rendered as their Java class and value; the JVM time zone is
 * Europe/Brussels (UTC+1 in January) so that UTC and local time can't be confused.
 *
 * @see <a href="https://parquet.apache.org/docs/file-format/types/logicaltypes/">Parquet logical
 *     types</a>
 */
class ParquetToHopTypeMatrixTest {

  /** The Hop types a field can be read into, as named in the matrix. */
  private static final Map<String, Integer> HOP_TYPES = new LinkedHashMap<>();

  static {
    HOP_TYPES.put("String", IValueMeta.TYPE_STRING);
    HOP_TYPES.put("Integer", IValueMeta.TYPE_INTEGER);
    HOP_TYPES.put("Number", IValueMeta.TYPE_NUMBER);
    HOP_TYPES.put("BigNumber", IValueMeta.TYPE_BIGNUMBER);
    HOP_TYPES.put("Date", IValueMeta.TYPE_DATE);
    HOP_TYPES.put("Timestamp", IValueMeta.TYPE_TIMESTAMP);
    HOP_TYPES.put("Boolean", IValueMeta.TYPE_BOOLEAN);
    HOP_TYPES.put("Binary", IValueMeta.TYPE_BINARY);
    HOP_TYPES.put("JSON", IValueMeta.TYPE_JSON);
    HOP_TYPES.put("UUID", IValueMeta.TYPE_UUID);
  }

  private static TimeZone defaultTimeZone;

  @BeforeAll
  static void setUpBeforeAll() throws Exception {
    HopClientEnvironment.init();
    defaultTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
  }

  @AfterAll
  static void tearDownAfterAll() {
    TimeZone.setDefault(defaultTimeZone);
  }

  private static List<Column> matrix() {
    return List.of(
        column("BOOLEAN true", BOOLEAN, null, c -> c.addBoolean(true))
            .getFields("Boolean")
            .reads("String", "String true")
            .reads("Integer", "Long 1")
            .reads("Boolean", "Boolean true"),
        column("INT32 42", INT32, null, c -> c.addInt(42))
            .getFields("Integer")
            .reads("String", "String 42")
            .reads("Integer", "Long 42")
            .reads("Number", "Double 42.0")
            .reads("BigNumber", "BigDecimal 42"),
        column("INT32 INT(8,signed) -5", INT32, intType(8, true), c -> c.addInt(-5))
            .getFields("Integer")
            .reads("String", "String -5")
            .reads("Integer", "Long -5")
            .reads("Number", "Double -5.0")
            .reads("BigNumber", "BigDecimal -5"),
        column("INT32 INT(32,unsigned) 4294967295", INT32, intType(32, false), c -> c.addInt(-1))
            .getFields("Integer")
            .reads("String", "String 4294967295")
            .reads("Integer", "Long 4294967295")
            .reads("Number", "Double 4.294967295E9")
            .reads("BigNumber", "BigDecimal 4294967295"),
        column("INT64 42", INT64, null, c -> c.addLong(42L))
            .getFields("Integer")
            .reads("String", "String 42")
            .reads("Integer", "Long 42")
            .reads("Number", "Double 42.0")
            .reads("BigNumber", "BigDecimal 42"),
        column(
                "INT64 INT(64,unsigned) 18446744073709551615",
                INT64,
                intType(64, false),
                c -> c.addLong(-1L))
            .getFields("BigNumber")
            .reads("String", "String 18446744073709551615")
            .reads("Number", "Double 1.8446744073709552E19")
            .reads("BigNumber", "BigDecimal 18446744073709551615"),
        column("INT32 DECIMAL(9,2) 12345.67", INT32, decimalType(2, 9), c -> c.addInt(1234567))
            .getFields("BigNumber")
            .reads("String", "String 12345.67")
            .reads("Integer", "Long 12345")
            .reads("Number", "Double 12345.67")
            .reads("BigNumber", "BigDecimal 12345.67"),
        column("INT64 DECIMAL(18,2) 1234.56", INT64, decimalType(2, 18), c -> c.addLong(123456L))
            .getFields("BigNumber")
            .reads("String", "String 1234.56")
            .reads("Integer", "Long 1234")
            .reads("Number", "Double 1234.56")
            .reads("BigNumber", "BigDecimal 1234.56"),
        column(
                "FIXED[8] DECIMAL(17,2) 123456789012345.67",
                fixed(8),
                decimalType(2, 17),
                c -> c.addBinary(unscaled("123456789012345.67", 8)))
            .getFields("BigNumber")
            .reads("String", "String 123456789012345.67")
            .reads("Integer", "Long 123456789012345")
            .reads("Number", "Double 1.2345678901234567E14")
            .reads("BigNumber", "BigDecimal 123456789012345.67")
            .reads("Binary", "bytes 002bdc545d6b4b87"),
        column(
                "BINARY DECIMAL(25,3) -1234567890123456789012.345",
                BINARY,
                decimalType(3, 25),
                c -> c.addBinary(unscaled("-1234567890123456789012.345", 0)))
            .getFields("BigNumber")
            .reads("String", "String -1234567890123456789012.345")
            .reads("Number", "Double -1.2345678901234568E21")
            .reads("BigNumber", "BigDecimal -1234567890123456789012.345")
            .reads("Binary", "bytes fefa91f0c959bbc21d2087"),
        column("FLOAT 1.5", FLOAT, null, c -> c.addFloat(1.5F))
            .getFields("Number")
            .reads("String", "String 1.5")
            .reads("Integer", "Long 2")
            .reads("Number", "Double 1.5")
            .reads("BigNumber", "BigDecimal 1.5"),
        column("DOUBLE 2.5", DOUBLE, null, c -> c.addDouble(2.5D))
            .getFields("Number")
            .reads("String", "String 2.5")
            .reads("Integer", "Long 3")
            .reads("Number", "Double 2.5")
            .reads("BigNumber", "BigDecimal 2.5"),
        column("FIXED[2] FLOAT16 1.5", fixed(2), float16Type(), c -> c.addBinary(bytes(0x00, 0x3E)))
            .getFields("Number")
            .reads("String", "String 1.5")
            .reads("Integer", "Long 2")
            .reads("Number", "Double 1.5")
            .reads("BigNumber", "BigDecimal 1.5")
            .reads("Binary", "bytes 003e"),
        column("INT32 DATE 2024-01-01", INT32, dateType(), c -> c.addInt(19723))
            .getFields("Date")
            .reads("String", "String 19723")
            .reads("Integer", "Long 19723")
            .reads("Number", "Double 19723.0")
            .reads("BigNumber", "BigDecimal 19723")
            .reads("Date", "Date 2024-01-01 00:00:00.000")
            .reads("Timestamp", "Timestamp 2024-01-01 00:00:00.0"),
        column(
                "INT32 TIME(MILLIS,UTC) 01:00Z",
                INT32,
                timeType(true, TimeUnit.MILLIS),
                c -> c.addInt(3_600_000))
            .getFields("Timestamp")
            .reads("String", "String 3600000")
            .reads("Integer", "Long 3600000")
            .reads("Number", "Double 3600000.0")
            .reads("BigNumber", "BigDecimal 3600000")
            .reads("Date", "Timestamp 1970-01-01 02:00:00.0")
            .reads("Timestamp", "Timestamp 1970-01-01 02:00:00.0"),
        column(
                "INT64 TIME(MICROS,local) 01:00",
                INT64,
                timeType(false, TimeUnit.MICROS),
                c -> c.addLong(3_600_000_000L))
            .getFields("Timestamp")
            .reads("String", "String 3600000000")
            .reads("Integer", "Long 3600000000")
            .reads("Number", "Double 3.6E9")
            .reads("BigNumber", "BigDecimal 3600000000")
            .reads("Date", "Timestamp 1970-01-01 01:00:00.0")
            .reads("Timestamp", "Timestamp 1970-01-01 01:00:00.0"),
        column(
                "INT64 TIMESTAMP(MILLIS,UTC) 2024-01-01T00:00:00.123Z",
                INT64,
                timestampType(true, TimeUnit.MILLIS),
                c -> c.addLong(1_704_067_200_123L))
            .getFields("Timestamp")
            .reads("String", "String 1704067200123")
            .reads("Integer", "Long 1704067200123")
            .reads("Number", "Double 1.704067200123E12")
            .reads("BigNumber", "BigDecimal 1704067200123")
            .reads("Date", "Timestamp 2024-01-01 01:00:00.123")
            .reads("Timestamp", "Timestamp 2024-01-01 01:00:00.123"),
        column(
                "INT64 TIMESTAMP(MICROS,UTC) 2024-01-01T00:00:00.123456Z",
                INT64,
                timestampType(true, TimeUnit.MICROS),
                c -> c.addLong(1_704_067_200_123_456L))
            .getFields("Timestamp")
            .reads("String", "String 1704067200123456")
            .reads("Integer", "Long 1704067200123456")
            .reads("Number", "Double 1.704067200123456E15")
            .reads("BigNumber", "BigDecimal 1704067200123456")
            .reads("Date", "Timestamp 2024-01-01 01:00:00.123456")
            .reads("Timestamp", "Timestamp 2024-01-01 01:00:00.123456"),
        column(
                "INT64 TIMESTAMP(NANOS,UTC) 2024-01-01T00:00:00.123456789Z",
                INT64,
                timestampType(true, TimeUnit.NANOS),
                c -> c.addLong(1_704_067_200_123_456_789L))
            .getFields("Timestamp")
            .reads("String", "String 1704067200123456789")
            .reads("Integer", "Long 1704067200123456789")
            .reads("Number", "Double 1.7040672001234568E18")
            .reads("BigNumber", "BigDecimal 1704067200123456789")
            .reads("Date", "Timestamp 2024-01-01 01:00:00.123456789")
            .reads("Timestamp", "Timestamp 2024-01-01 01:00:00.123456789"),
        column(
                "INT64 TIMESTAMP(MICROS,local) 2024-01-01 00:00:00.123456",
                INT64,
                timestampType(false, TimeUnit.MICROS),
                c -> c.addLong(1_704_067_200_123_456L))
            .getFields("Timestamp")
            .reads("String", "String 1704067200123456")
            .reads("Integer", "Long 1704067200123456")
            .reads("Number", "Double 1.704067200123456E15")
            .reads("BigNumber", "BigDecimal 1704067200123456")
            .reads("Date", "Timestamp 2024-01-01 00:00:00.123456")
            .reads("Timestamp", "Timestamp 2024-01-01 00:00:00.123456"),
        column(
                "INT64 TIMESTAMP(MICROS,UTC) 1969-12-31T23:59:59.999999Z",
                INT64,
                timestampType(true, TimeUnit.MICROS),
                c -> c.addLong(-1L))
            .getFields("Timestamp")
            .reads("String", "String -1")
            .reads("Integer", "Long -1")
            .reads("Number", "Double -1.0")
            .reads("BigNumber", "BigDecimal -1")
            .reads("Date", "Timestamp 1970-01-01 00:59:59.999999")
            .reads("Timestamp", "Timestamp 1970-01-01 00:59:59.999999"),
        column(
                "INT96 2024-01-01T01:02:03Z",
                INT96,
                null,
                c -> c.addBinary(int96(2460311, 3_723_000_000_000L)))
            .getFields("Timestamp")
            .reads("Date", "Timestamp 2024-01-01 02:02:03.0")
            .reads("Timestamp", "Timestamp 2024-01-01 02:02:03.0")
            .reads("Binary", "bytes 00ae17d462030000978a2500"),
        column(
                "BINARY STRING héllo",
                BINARY,
                stringType(),
                c -> c.addBinary(Binary.fromString("héllo")))
            .getFields("String")
            .reads("String", "String héllo")
            .reads("Binary", "bytes 68c3a96c6c6f"),
        column("BINARY ENUM RED", BINARY, enumType(), c -> c.addBinary(Binary.fromString("RED")))
            .getFields("String")
            .reads("String", "String RED")
            .reads("Binary", "bytes 524544"),
        column(
                "BINARY (no annotation) abc",
                BINARY,
                null,
                c -> c.addBinary(Binary.fromString("abc")))
            .getFields("Binary")
            .reads("String", "String abc")
            .reads("BigNumber", "BigDecimal 6382179")
            .reads("Binary", "bytes 616263"),
        column(
                "BINARY JSON {\"a\":1}",
                BINARY,
                jsonType(),
                c -> c.addBinary(Binary.fromString("{\"a\":1}")))
            .getFields("JSON")
            .reads("String", "String {\"a\":1}")
            .reads("Binary", "bytes 7b2261223a317d")
            .reads("JSON", "JSON {\"a\":1}"),
        column("BINARY BSON", BINARY, bsonType(), c -> c.addBinary(bytes(5, 0, 0, 0, 0)))
            .getFields("Binary")
            .reads("Binary", "bytes 0500000000"),
        column(
                "FIXED[16] UUID 00112233-4455-6677-8899-aabbccddeeff",
                fixed(16),
                uuidType(),
                c -> c.addBinary(uuid("00112233-4455-6677-8899-aabbccddeeff")))
            .getFields("String")
            .reads("String", "String 00112233-4455-6677-8899-aabbccddeeff")
            .reads("Binary", "bytes 00112233445566778899aabbccddeeff")
            .reads("UUID", "UUID 00112233-4455-6677-8899-aabbccddeeff"),
        column(
                "FIXED[4] (no annotation) abcd",
                fixed(4),
                null,
                c -> c.addBinary(Binary.fromString("abcd")))
            .getFields("Binary")
            .reads("String", "String abcd")
            .reads("BigNumber", "BigDecimal 1633837924")
            .reads("Binary", "bytes 61626364"),
        column(
                "FIXED[12] INTERVAL",
                fixed(12),
                IntervalLogicalTypeAnnotation.getInstance(),
                c -> c.addBinary(bytes(1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0)))
            .getFields("Binary")
            .reads("Binary", "bytes 010000000200000003000000"));
  }

  @TestFactory
  Stream<DynamicNode> parquetToHop() {
    return matrix().stream()
        .map(
            column -> {
              List<DynamicNode> cells = new ArrayList<>();
              cells.add(
                  dynamicTest(
                      "Get Fields -> " + column.getFields,
                      () ->
                          assertEquals(
                              column.getFields,
                              ParquetInputMeta.hopValueMeta("col", column.type).getTypeDesc())));
              for (String hopType : HOP_TYPES.keySet()) {
                String expected = column.reads.get(hopType);
                cells.add(
                    dynamicTest(
                        "-> " + hopType + ": " + (expected == null ? "conversion error" : expected),
                        () -> {
                          if (expected == null) {
                            assertThrows(HopRuntimeException.class, () -> read(column, hopType));
                          } else {
                            assertEquals(expected, render(read(column, hopType)));
                          }
                        }));
              }
              return dynamicContainer(column.label, cells);
            });
  }

  private static Object read(Column column, String hopType) throws Exception {
    int type = HOP_TYPES.get(hopType);
    // The UUID value type is a plugin which isn't on the class path of this test.
    IValueMeta valueMeta =
        type == IValueMeta.TYPE_UUID
            ? new UuidValueMeta()
            : ValueMetaFactory.createValueMeta("field", type);
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(valueMeta);
    RowMetaAndData group = new RowMetaAndData(rowMeta, new Object[1]);
    column.value.accept(new ParquetValueConverter(group, 0, column.type));
    return group.getData()[0];
  }

  /** A value as its Java class and value, so the matrix shows the type as well. */
  private static String render(Object value) {
    if (value instanceof Timestamp timestamp) {
      return "Timestamp " + timestamp;
    }
    if (value instanceof Date date) {
      return "Date " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
    }
    if (value instanceof byte[] bytes) {
      return "bytes " + HexFormat.of().formatHex(bytes);
    }
    if (value instanceof JsonNode json) {
      return "JSON " + json;
    }
    if (value instanceof BigDecimal decimal) {
      return "BigDecimal " + decimal.toPlainString();
    }
    return value.getClass().getSimpleName() + " " + value;
  }

  private static final class Column {
    private final String label;
    private final PrimitiveType type;
    private final Consumer<ParquetValueConverter> value;
    private final Map<String, String> reads = new LinkedHashMap<>();
    private String getFields;

    private Column(String label, PrimitiveType type, Consumer<ParquetValueConverter> value) {
      this.label = label;
      this.type = type;
      this.value = value;
    }

    Column getFields(String hopType) {
      this.getFields = hopType;
      return this;
    }

    Column reads(String hopType, String expected) {
      if (!HOP_TYPES.containsKey(hopType)) {
        throw new IllegalArgumentException("Not a Hop type of the matrix: " + hopType);
      }
      reads.put(hopType, expected);
      return this;
    }
  }

  private static Column column(
      String label,
      PrimitiveTypeName physicalType,
      LogicalTypeAnnotation logicalType,
      Consumer<ParquetValueConverter> value) {
    return column(label, physicalType, 0, logicalType, value);
  }

  private static Column column(
      String label,
      Fixed fixed,
      LogicalTypeAnnotation logicalType,
      Consumer<ParquetValueConverter> value) {
    return column(label, FIXED_LEN_BYTE_ARRAY, fixed.length, logicalType, value);
  }

  private static Column column(
      String label,
      PrimitiveTypeName physicalType,
      int length,
      LogicalTypeAnnotation logicalType,
      Consumer<ParquetValueConverter> value) {
    PrimitiveType type =
        new PrimitiveType(Type.Repetition.OPTIONAL, physicalType, length, "col")
            .withLogicalTypeAnnotation(logicalType);
    return new Column(label, type, value);
  }

  private record Fixed(int length) {}

  private static Fixed fixed(int length) {
    return new Fixed(length);
  }

  private static Binary bytes(int... values) {
    byte[] bytes = new byte[values.length];
    for (int i = 0; i < values.length; i++) {
      bytes[i] = (byte) values[i];
    }
    return Binary.fromConstantByteArray(bytes);
  }

  /** The big-endian two's complement unscaled value of a decimal, sign-extended to a length. */
  private static Binary unscaled(String decimal, int length) {
    byte[] unscaled = new BigDecimal(decimal).unscaledValue().toByteArray();
    if (length == 0) {
      return Binary.fromConstantByteArray(unscaled);
    }
    byte[] bytes = new byte[length];
    byte sign = unscaled[0] < 0 ? (byte) -1 : 0;
    java.util.Arrays.fill(bytes, sign);
    System.arraycopy(unscaled, 0, bytes, length - unscaled.length, unscaled.length);
    return Binary.fromConstantByteArray(bytes);
  }

  private static Binary uuid(String uuid) {
    UUID value = UUID.fromString(uuid);
    return Binary.fromConstantByteArray(
        ByteBuffer.allocate(16)
            .putLong(value.getMostSignificantBits())
            .putLong(value.getLeastSignificantBits())
            .array());
  }

  /** An INT96 timestamp: nanoseconds in the day and the Julian day, little endian. */
  private static Binary int96(long julianDay, long nanosOfDay) {
    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(nanosOfDay).putInt((int) julianDay);
    return Binary.fromConstantByteArray(buffer.array());
  }

  /** Stands in for the UUID value type plugin. */
  private static final class UuidValueMeta extends ValueMetaBase {
    UuidValueMeta() {
      super("field", IValueMeta.TYPE_UUID);
    }
  }
}
