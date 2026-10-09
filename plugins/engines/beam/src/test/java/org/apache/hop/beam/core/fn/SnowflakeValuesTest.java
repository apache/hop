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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import org.apache.hop.beam.core.BeamHop;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SnowflakeValuesTest {
  @BeforeAll
  static void init() throws Exception {
    BeamHop.init();
  }

  @Test
  void convertsCsvValuesWithoutKeepingAnEmptyString() throws Exception {
    assertNull(SnowflakeValues.toCsv(new ValueMetaString("s"), null));
    assertNull(SnowflakeValues.fromCsv(new ValueMetaString("s"), ""));
    assertNull(SnowflakeValues.fromCsv(new ValueMetaInteger("n"), "   "));
    assertEquals(" a, b ", SnowflakeValues.toCsv(new ValueMetaString("s"), " a, b "));
    assertEquals(" a, b ", SnowflakeValues.fromCsv(new ValueMetaString("s"), " a, b "));
    assertEquals("7", SnowflakeValues.toCsv(new ValueMetaInteger("n"), 7L));
    assertEquals(1L, SnowflakeValues.fromCsv(new ValueMetaInteger("n"), "1.0"));
    assertEquals(1.25d, SnowflakeValues.fromCsv(new ValueMetaNumber("n"), "1.25"));
    assertEquals("1.25", SnowflakeValues.toCsv(new ValueMetaNumber("n"), 1.25d));
    BigDecimal decimal = new BigDecimal("1.2300");
    assertEquals(
        0,
        decimal.compareTo(
            (BigDecimal) SnowflakeValues.fromCsv(new ValueMetaBigNumber("b"), "1.2300")));
    assertEquals("1.2300", SnowflakeValues.toCsv(new ValueMetaBigNumber("b"), decimal));
    assertEquals(Boolean.TRUE, SnowflakeValues.fromCsv(new ValueMetaBoolean("b"), "TRUE"));
    assertEquals(Boolean.FALSE, SnowflakeValues.fromCsv(new ValueMetaBoolean("b"), "0"));
    assertEquals("true", SnowflakeValues.toCsv(new ValueMetaBoolean("b"), Boolean.TRUE));
    assertEquals("false", SnowflakeValues.toCsv(new ValueMetaBoolean("b"), Boolean.FALSE));
    Date date = Date.from(Instant.parse("2020-01-02T03:04:05.006Z"));
    assertEquals("2020-01-02 03:04:05.006", SnowflakeValues.toCsv(new ValueMetaDate("d"), date));
    assertEquals(
        date.getTime(),
        ((Date) SnowflakeValues.fromCsv(new ValueMetaDate("d"), "2020-01-02 03:04:05.006"))
            .getTime());
    assertEquals(
        date.getTime(),
        ((Timestamp)
                SnowflakeValues.fromCsv(new ValueMetaTimestamp("t"), "2020-01-02 03:04:05.006"))
            .getTime());
    assertInstanceOf(
        Timestamp.class, SnowflakeValues.fromCsv(new ValueMetaTimestamp("t"), "2020-01-02"));
    byte[] bytes = new byte[] {0, (byte) 0xff, 0x10};
    assertEquals("00ff10", SnowflakeValues.toCsv(new ValueMetaBinary("b"), bytes));
    assertArrayEquals(bytes, (byte[]) SnowflakeValues.fromCsv(new ValueMetaBinary("b"), "00ff10"));
  }

  @Test
  void conversionFailureNamesTheFieldAndDropsTheCell() {
    HopException integer =
        assertThrows(
            HopException.class,
            () -> SnowflakeValues.fromCsv(new ValueMetaInteger("id"), "secret-cell"));
    assertTrue(integer.getMessage().contains("Unable to convert Snowflake field id"));
    assertNull(integer.getCause());
    assertFalse(integer.getMessage().contains("secret-cell"));
    HopException fraction =
        assertThrows(
            HopException.class, () -> SnowflakeValues.fromCsv(new ValueMetaInteger("id"), "1.5"));
    assertFalse(fraction.getMessage().contains("1.5"));
  }

  @Test
  void extraCsvColumnIsRejectedWithoutTheCellText() throws Exception {
    var rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    var mapper = new SnowflakeCsvToHop("read", rowMeta.getMetaXml());
    HopException extra =
        assertThrows(HopException.class, () -> mapper.mapRow(new String[] {"ok", "secret-extra"}));
    assertTrue(
        extra.getMessage().contains("Snowflake CSV row has more columns than the field list"));
    assertFalse(extra.getMessage().contains("secret-extra"));
    assertNull(extra.getCause());
  }

  @Test
  void outputMapperDropsTheCellFromAConversionError() throws Exception {
    var rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    var mapper = new HopToSnowflakeRow("write", rowMeta.getMetaXml());
    HopRuntimeException failed =
        assertThrows(
            HopRuntimeException.class,
            () -> mapper.mapRow(new HopRow(new Object[] {"secret-cell"})));
    assertTrue(failed.getMessage().contains("Unable to convert Snowflake field id"));
    assertNull(failed.getCause());
    assertFalse(failed.getMessage().contains("secret-cell"));
  }

  @Test
  void mapsHopTypesOntoSnowflakeTypes() throws Exception {
    assertEquals(IValueMeta.TYPE_TIMESTAMP, SnowflakeValues.hopType("Timestamp"));
    var text = new ValueMetaString("s");
    text.setLength(12);
    assertEquals("VARCHAR(12)", SnowflakeValues.snowflakeType(text).sql());
    assertEquals("FLOAT", SnowflakeValues.snowflakeType(new ValueMetaNumber("n")).sql());
    assertEquals("NUMBER(38,0)", SnowflakeValues.snowflakeType(new ValueMetaInteger("i")).sql());
    var decimal = new ValueMetaBigNumber("b");
    decimal.setLength(10, 2);
    assertEquals("NUMBER(10,2)", SnowflakeValues.snowflakeType(decimal).sql());
    HopException unsupported =
        assertThrows(
            HopException.class,
            () ->
                SnowflakeValues.snowflakeType(
                    ValueMetaFactory.createValueMeta("payload", IValueMeta.TYPE_SERIALIZABLE)));
    assertTrue(unsupported.getMessage().contains("Serializable"));
  }
}
