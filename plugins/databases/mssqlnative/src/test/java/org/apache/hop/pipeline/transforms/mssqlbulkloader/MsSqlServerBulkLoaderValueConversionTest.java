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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.Test;

/**
 * The bulk copy protocol casts every value straight to the class its column's JDBC type uses, so
 * the shape has to be exact. These tests pin that mapping down, along with the four things the old
 * CSV based Kettle step got wrong: nulls, booleans, dates, and values holding its separator.
 */
class MsSqlServerBulkLoaderValueConversionTest {

  /** The separator the old Kettle step joined values with. */
  private static final String OLD_DELIMITER = ",§;";

  @Test
  void anIntColumnGetsAnIntegerAndNotALong() throws Exception {
    // Hop has one integer type and it is a Long. Handing that to an int column threw
    // "class java.lang.Long cannot be cast to class java.lang.Integer" inside the driver.
    Object converted =
        MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), 42L, Types.INTEGER);

    assertInstanceOf(Integer.class, converted);
    assertEquals(42, converted);
  }

  @Test
  void smallerWholeNumberColumnsAlsoGetAnInteger() throws Exception {
    assertInstanceOf(
        Integer.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), 7L, Types.SMALLINT));
    assertInstanceOf(
        Integer.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), 7L, Types.TINYINT));
  }

  @Test
  void aBigintColumnKeepsTheLong() throws Exception {
    Object converted =
        MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), 9_000_000_000L, Types.BIGINT);

    assertInstanceOf(Long.class, converted);
    assertEquals(9_000_000_000L, converted);
  }

  @Test
  void aNumberTooLargeForTheColumnIsReportedRatherThanTruncated() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                MsSqlServerBulkLoader.convertValue(
                    new ValueMetaInteger("big"), 9_000_000_000L, Types.INTEGER));
    assertEquals(true, e.getMessage().contains("big"));
  }

  @Test
  void theTargetColumnDecidesTheShapeNotTheIncomingType() throws Exception {
    // A Hop string bound for an int column is coerced the same way a Table Output would coerce it.
    assertInstanceOf(
        Integer.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaString("s"), "42", Types.INTEGER));
    assertEquals(
        new BigDecimal("42"),
        MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), 42L, Types.DECIMAL));
  }

  @Test
  void floatingPointColumnsGetTheWidthTheyAskFor() throws Exception {
    assertInstanceOf(
        Float.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaNumber("n"), 1.5d, Types.REAL));
    assertInstanceOf(
        Float.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaNumber("n"), 1.5d, Types.FLOAT));
    assertInstanceOf(
        Double.class,
        MsSqlServerBulkLoader.convertValue(new ValueMetaNumber("n"), 1.5d, Types.DOUBLE));
    assertInstanceOf(
        BigDecimal.class,
        MsSqlServerBulkLoader.convertValue(
            new ValueMetaBigNumber("bn"), new BigDecimal("1.2345"), Types.DECIMAL));
  }

  @Test
  void aNullStaysANullForEveryType() throws Exception {
    assertNull(MsSqlServerBulkLoader.convertValue(new ValueMetaString("s"), null, Types.VARCHAR));
    assertNull(MsSqlServerBulkLoader.convertValue(new ValueMetaInteger("i"), null, Types.INTEGER));
    assertNull(MsSqlServerBulkLoader.convertValue(new ValueMetaBoolean("b"), null, Types.BIT));
    assertNull(MsSqlServerBulkLoader.convertValue(new ValueMetaDate("d"), null, Types.TIMESTAMP));
    assertNull(MsSqlServerBulkLoader.convertValue(new ValueMetaBinary("x"), null, Types.VARBINARY));
  }

  @Test
  void aBooleanStaysABooleanInsteadOfBecomingOneOrZero() throws Exception {
    assertEquals(
        Boolean.TRUE,
        MsSqlServerBulkLoader.convertValue(new ValueMetaBoolean("b"), Boolean.TRUE, Types.BIT));
    assertEquals(
        Boolean.FALSE,
        MsSqlServerBulkLoader.convertValue(new ValueMetaBoolean("b"), Boolean.FALSE, Types.BIT));
  }

  @Test
  void aValueContainingTheOldSeparatorSurvivesIntact() throws Exception {
    String value = "before" + OLD_DELIMITER + "after";
    assertEquals(
        value, MsSqlServerBulkLoader.convertValue(new ValueMetaString("s"), value, Types.VARCHAR));
  }

  @Test
  void aValueContainingALineBreakSurvivesIntact() throws Exception {
    // The old step ended every row with a newline, so a value holding one cut the row short.
    String value = "two\nlines";
    assertEquals(
        value, MsSqlServerBulkLoader.convertValue(new ValueMetaString("s"), value, Types.VARCHAR));
  }

  @Test
  void aDateBecomesTheTemporalTypeTheTargetColumnUses() throws Exception {
    Date date = new Date(1_700_000_000_000L);
    ValueMetaDate valueMeta = new ValueMetaDate("d");

    assertInstanceOf(
        java.sql.Date.class, MsSqlServerBulkLoader.convertValue(valueMeta, date, Types.DATE));
    assertInstanceOf(
        java.sql.Time.class, MsSqlServerBulkLoader.convertValue(valueMeta, date, Types.TIME));
    assertInstanceOf(
        Timestamp.class, MsSqlServerBulkLoader.convertValue(valueMeta, date, Types.TIMESTAMP));

    assertEquals(
        date.getTime(),
        ((java.util.Date) MsSqlServerBulkLoader.convertValue(valueMeta, date, Types.TIMESTAMP))
            .getTime());
  }

  @Test
  void aTimestampKeepsItsSubMillisecondPrecision() throws Exception {
    Timestamp timestamp = new Timestamp(1_700_000_000_000L);
    timestamp.setNanos(123_456_789);

    Object converted =
        MsSqlServerBulkLoader.convertValue(
            new ValueMetaTimestamp("ts"), timestamp, Types.TIMESTAMP);

    assertInstanceOf(Timestamp.class, converted);
    assertEquals(123_456_789, ((Timestamp) converted).getNanos());
  }

  @Test
  void binaryStringStorageIsResolvedBeforeItIsSent() throws Exception {
    // Rows arriving from a lazily converted reader hold byte arrays, not the value itself.
    ValueMetaString valueMeta = new ValueMetaString("s");
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    valueMeta.setStorageMetadata(new ValueMetaString("s"));

    Object converted =
        MsSqlServerBulkLoader.convertValue(
            valueMeta, "hello".getBytes(StandardCharsets.UTF_8), Types.VARCHAR);

    assertEquals("hello", converted);
  }

  @Test
  void binaryValuesArePassedThroughAsBytes() throws Exception {
    byte[] bytes = {1, 2, 3};
    assertArrayEquals(
        bytes,
        (byte[])
            MsSqlServerBulkLoader.convertValue(new ValueMetaBinary("x"), bytes, Types.VARBINARY));
  }
}
