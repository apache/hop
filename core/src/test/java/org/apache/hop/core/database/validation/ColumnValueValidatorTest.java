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
package org.apache.hop.core.database.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ColumnValueValidatorTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void nullRejectedOnNotNullable() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("id");
    spec.setNullable(false);
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "id", new ValueMetaInteger("id"), null, false);
    assertEquals(1, errors.size());
    assertEquals(ColumnValueErrorCode.NULL_NOT_ALLOWED, errors.get(0).code());
  }

  @Test
  void nullAllowedWhenNullable() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("id");
    spec.setNullable(true);
    assertTrue(
        ColumnValueValidator.validate(spec, "id", new ValueMetaInteger("id"), null, false)
            .isEmpty());
  }

  @Test
  void varcharLengthUsesCodePointsNotCharUnits() {
    ColumnValueConstraints spec = varchar("name", 1);
    ValueMetaString meta = new ValueMetaString("name");
    List<ColumnValueError> bmp = ColumnValueValidator.validate(spec, "name", meta, "ab", false);
    assertEquals(ColumnValueErrorCode.STRING_TOO_LONG, bmp.get(0).code());

    // One emoji is two UTF-16 code units and one code point: varchar(1) accepts it.
    List<ColumnValueError> emoji = ColumnValueValidator.validate(spec, "name", meta, "😀", false);
    assertTrue(emoji.isEmpty());
  }

  @Test
  void nulCharacterRejected() {
    ColumnValueConstraints spec = varchar("txt", 100);
    spec.setRejectNulChar(true);
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "txt", new ValueMetaString("txt"), "a\0b", false);
    assertEquals(ColumnValueErrorCode.INVALID_ENCODING, errors.get(0).code());
    assertTrue(errors.get(0).message().contains("NUL"));
  }

  @Test
  void unpairedSurrogateRejectedAsUtf8() {
    ColumnValueConstraints spec = varchar("txt", 100);
    String unpaired = new String(new char[] {'a', 0xD800});
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "txt", new ValueMetaString("txt"), unpaired, false);
    assertFalse(errors.isEmpty());
    assertEquals(ColumnValueErrorCode.INVALID_ENCODING, errors.get(0).code());
  }

  @Test
  void numericOverflowAfterRounding() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("amount");
    spec.setNativeTypeName("numeric");
    spec.setHopType(IValueMeta.TYPE_BIGNUMBER);
    spec.setNumericPrecision(5);
    spec.setNumericScale(2);
    spec.setTargetValueMeta(new ValueMetaBigNumber("amount"));
    ValueMetaBigNumber meta = new ValueMetaBigNumber("amount");
    assertTrue(
        ColumnValueValidator.validate(spec, "amount", meta, new BigDecimal("123.45"), false)
            .isEmpty());
    List<ColumnValueError> overflow =
        ColumnValueValidator.validate(spec, "amount", meta, new BigDecimal("1234.5"), false);
    assertEquals(ColumnValueErrorCode.NUMERIC_OVERFLOW, overflow.get(0).code());
    List<ColumnValueError> rounded =
        ColumnValueValidator.validate(spec, "amount", meta, new BigDecimal("999.999"), false);
    assertEquals(ColumnValueErrorCode.NUMERIC_OVERFLOW, rounded.get(0).code());
  }

  @Test
  void conversionFailure() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("qty");
    spec.setNativeTypeName("int4");
    spec.setHopType(IValueMeta.TYPE_INTEGER);
    spec.setTargetValueMeta(new ValueMetaInteger("qty"));
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "qty", new ValueMetaString("qty"), "abc", false);
    assertEquals(ColumnValueErrorCode.CONVERSION, errors.get(0).code());
  }

  @Test
  void integerRangeInt2() {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName("s");
    spec.setNativeTypeName("int2");
    spec.setHopType(IValueMeta.TYPE_INTEGER);
    spec.setIntegerMin(-32768L);
    spec.setIntegerMax(32767L);
    spec.setTargetValueMeta(new ValueMetaInteger("s"));
    ValueMetaInteger meta = new ValueMetaInteger("s");
    assertTrue(ColumnValueValidator.validate(spec, "s", meta, 32767L, false).isEmpty());
    List<ColumnValueError> errors = ColumnValueValidator.validate(spec, "s", meta, 32768L, false);
    assertEquals(ColumnValueErrorCode.INTEGER_RANGE, errors.get(0).code());
  }

  @Test
  void uuidAndJson() {
    ColumnValueConstraints uuid = new ColumnValueConstraints();
    uuid.setColumnName("id");
    uuid.setUuid(true);
    uuid.setHopType(IValueMeta.TYPE_STRING);
    uuid.setTargetValueMeta(new ValueMetaString("id"));
    ValueMetaString meta = new ValueMetaString("id");
    assertTrue(
        ColumnValueValidator.validate(
                uuid, "id", meta, "550e8400-e29b-41d4-a716-446655440000", false)
            .isEmpty());
    assertEquals(
        ColumnValueErrorCode.INVALID_UUID,
        ColumnValueValidator.validate(uuid, "id", meta, "not-a-uuid", false).get(0).code());

    ColumnValueConstraints json = new ColumnValueConstraints();
    json.setColumnName("payload");
    json.setJson(true);
    json.setHopType(IValueMeta.TYPE_STRING);
    json.setTargetValueMeta(new ValueMetaString("payload"));
    assertTrue(ColumnValueValidator.validate(json, "payload", meta, "{\"a\":1}", false).isEmpty());
    assertEquals(
        ColumnValueErrorCode.INVALID_JSON,
        ColumnValueValidator.validate(json, "payload", meta, "{", false).get(0).code());
  }

  @Test
  void omitValuesHidesFailedData() {
    ColumnValueConstraints spec = varchar("name", 1);
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(
            spec, "name", new ValueMetaString("name"), "secret-value", true);
    assertFalse(errors.get(0).message().contains("secret-value"));
    assertTrue(errors.get(0).message().contains("?"));
  }

  @Test
  void multipleErrorsOnOneFieldStaySeparate() {
    ColumnValueConstraints spec = varchar("txt", 1);
    spec.setRejectNulChar(true);
    List<ColumnValueError> errors =
        ColumnValueValidator.validate(spec, "txt", new ValueMetaString("txt"), "ab\0c", false);
    assertTrue(errors.size() >= 2);
  }

  private static ColumnValueConstraints varchar(String name, int length) {
    ColumnValueConstraints spec = new ColumnValueConstraints();
    spec.setColumnName(name);
    spec.setNativeTypeName("varchar");
    spec.setSqlType(Types.VARCHAR);
    spec.setHopType(IValueMeta.TYPE_STRING);
    spec.setStringMaxLength(length);
    spec.setLengthUnit(StringLengthUnit.CHARACTERS);
    spec.setCharacterSet("UTF-8");
    spec.setTargetValueMeta(new ValueMetaString(name));
    return spec;
  }
}
