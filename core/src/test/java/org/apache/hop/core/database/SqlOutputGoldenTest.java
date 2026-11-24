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

package org.apache.hop.core.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the exact text {@link Database#getSqlOutput(String, String, IRowMeta, Object[], String)}
 * produces, so that precomputing the quoted names and date formatters cannot change the output.
 */
class SqlOutputGoldenTest {

  static class Dialect extends ConcreteBaseDatabaseMeta {
    private final boolean oracle;

    Dialect(boolean oracle) {
      this.oracle = oracle;
    }

    @Override
    public String[] getReservedWords() {
      return new String[] {"SELECT", "ORDER", "USER", "GROUP"};
    }

    @Override
    public boolean isOracleVariant() {
      return oracle;
    }
  }

  private static final Date DATE = new GregorianCalendar(2024, 0, 15, 13, 45, 30).getTime();

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  private static Database database(boolean oracle) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new Dialect(oracle));
    Variables variables = new Variables();
    variables.setVariable("SCHEMA", "user");
    return new Database(mock(ILoggingObject.class), variables, databaseMeta);
  }

  private static IRowMeta rowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("order"));
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    IValueMeta number = new ValueMetaNumber("first amount");
    number.setConversionMask("0.00");
    number.setDecimalSymbol(".");
    number.setGroupingSymbol("");
    rowMeta.addValueMeta(number);
    IValueMeta bigNumber = new ValueMetaBigNumber("big");
    bigNumber.setDecimalSymbol(".");
    bigNumber.setGroupingSymbol("");
    rowMeta.addValueMeta(bigNumber);
    rowMeta.addValueMeta(new ValueMetaDate("Select"));
    rowMeta.addValueMeta(new ValueMetaBoolean("flag"));
    rowMeta.addValueMeta(new ValueMetaString("missing"));
    rowMeta.addValueMeta(new ValueMetaDate("no_date"));
    return rowMeta;
  }

  private static Object[] row() {
    return new Object[] {
      "O'Brien\nline2", "first", 42L, 3.14159, new BigDecimal("12345.678"), DATE, true, null, null
    };
  }

  private static final String COLUMNS =
      "(name, \"order\", id, \"first amount\", big, \"Select\", flag, missing, no_date)";

  @Test
  void genericRowWithoutDateFormat() throws Exception {
    assertEquals(
        "INSERT INTO \"user\".\"group\""
            + COLUMNS
            + " VALUES ('O''Brien\\nline2','first',42,3.14,12345.678,'2024/01/15 13:45:30.000',"
            + "'Y',null,null)",
        database(false).getSqlOutput("${SCHEMA}", "group", rowMeta(), row(), null));
  }

  @Test
  void genericRowWithDateFormat() throws Exception {
    assertEquals(
        "INSERT INTO customer"
            + COLUMNS
            + " VALUES ('O''Brien\\nline2','first',42,3.14,12345.678,'15-01-2024 13:45',"
            + "'Y',null,null)",
        database(false).getSqlOutput(null, "customer", rowMeta(), row(), "dd-MM-yyyy HH:mm"));
  }

  @Test
  void oracleRowWithoutDateFormat() throws Exception {
    assertEquals(
        "INSERT INTO sales.customer"
            + COLUMNS
            + " VALUES ('O''Brien\\nline2','first',42,3.14,12345.678,"
            + "TO_DATE('2024/01/15 13:45:30', 'YYYY/MM/DD HH24:MI:SS'),'Y',null,null)",
        database(true).getSqlOutput("sales", "customer", rowMeta(), row(), ""));
  }

  @Test
  void oracleRowWithDateFormat() throws Exception {
    assertEquals(
        "INSERT INTO sales.customer"
            + COLUMNS
            + " VALUES ('O''Brien\\nline2','first',42,3.14,12345.678,'2024-01-15',"
            + "'Y',null,null)",
        database(true).getSqlOutput("sales", "customer", rowMeta(), row(), "yyyy-MM-dd"));
  }

  @Test
  void allNullRow() throws Exception {
    assertEquals(
        "INSERT INTO customer" + COLUMNS + " VALUES (null,null,null,null,null,null,null,null,null)",
        database(false)
            .getSqlOutput(null, "customer", rowMeta(), new Object[9], "not a 'valid pattern"));
  }

  @Test
  void invalidDateFormatFailsOnlyForADateValue() {
    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () ->
                database(false)
                    .getSqlOutput(null, "customer", rowMeta(), row(), "not a 'valid pattern"));
    assertEquals(HopDatabaseException.class, e.getCause().getClass());
  }

  @Test
  void repeatedCallsAreStable() throws Exception {
    Database db = database(true);
    IRowMeta rowMeta = rowMeta();
    String first = db.getSqlOutput("sales", "customer", rowMeta, row(), null);
    for (int i = 0; i < 5; i++) {
      assertEquals(first, db.getSqlOutput("sales", "customer", rowMeta, row(), null));
    }
  }

  @Test
  void templateRendersTheSameTextForEveryRow() throws Exception {
    for (boolean oracle : new boolean[] {false, true}) {
      for (String dateFormat : new String[] {null, "", "yyyy-MM-dd HH:mm:ss"}) {
        Database db = database(oracle);
        IRowMeta rowMeta = rowMeta();
        SqlInsertTemplate template =
            db.getSqlOutputTemplate("${SCHEMA}", "order", rowMeta, dateFormat);
        Object[][] rows = {row(), new Object[9], row()};
        for (Object[] r : rows) {
          assertEquals(
              db.getSqlOutput("${SCHEMA}", "order", rowMeta, r, dateFormat), template.getSql(r));
          StringBuilder sb = new StringBuilder("x");
          template.appendSql(sb, r);
          assertEquals("x" + template.getSql(r), sb.toString());
        }
      }
    }
  }

  private static final Locale[] LOCALES = {
    Locale.US,
    Locale.GERMANY,
    Locale.forLanguageTag("sv-SE"),
    Locale.forLanguageTag("ar-EG"),
    Locale.forLanguageTag("th-TH"),
    Locale.forLanguageTag("hi-IN")
  };

  /** Number, integer and date columns in default and explicit masks, as the GUI leaves them. */
  private static IRowMeta numbersAndDatesRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("i_default"));
    rowMeta.addValueMeta(new ValueMetaInteger("i_length", 5, 0));
    rowMeta.addValueMeta(new ValueMetaNumber("n_default"));
    rowMeta.addValueMeta(new ValueMetaNumber("n_length", 10, 2));
    IValueMeta numberMask = new ValueMetaNumber("n_mask");
    numberMask.setConversionMask("#,##0.00");
    numberMask.setDecimalSymbol(".");
    numberMask.setGroupingSymbol(",");
    rowMeta.addValueMeta(numberMask);
    IValueMeta numberDot = new ValueMetaNumber("n_dot");
    numberDot.setDecimalSymbol(".");
    rowMeta.addValueMeta(numberDot);
    rowMeta.addValueMeta(new ValueMetaBigNumber("b_default"));
    rowMeta.addValueMeta(new ValueMetaDate("d_default"));
    IValueMeta iso = new ValueMetaDate("d_iso");
    iso.setConversionMask("yyyy-MM-dd'T'HH:mm:ss");
    rowMeta.addValueMeta(iso);
    IValueMeta named = new ValueMetaDate("d_named");
    named.setConversionMask("dd-MMM-yyyy");
    rowMeta.addValueMeta(named);
    return rowMeta;
  }

  private static List<Object[]> numbersAndDatesRows() {
    long[] integers = {0L, -1L, 42L, Long.MAX_VALUE, Long.MIN_VALUE, 1234567L};
    double[] numbers = {
      0.0,
      -0.0,
      12.99,
      -3.5,
      1.0 / 3,
      262143.75,
      262144.5,
      1.0E7,
      1.0E-5,
      0.001,
      123456.123456789,
      Double.NaN,
      Double.POSITIVE_INFINITY,
      -98765.4321
    };
    String[] bigs = {
      "12345.678",
      "-0.00",
      "1E+3",
      "1E-30",
      "123456789012345678901234567890.123456789",
      "0.1",
      "-7.25",
      "0.00000000000000000001"
    };
    Date[] dates = {
      new GregorianCalendar(2024, 6, 15, 13, 45, 30).getTime(),
      new Date(new GregorianCalendar(2023, 11, 31, 23, 59, 59).getTimeInMillis() + 999),
      new GregorianCalendar(1500, 2, 1, 12, 0, 0).getTime(),
      new GregorianCalendar(12000, 0, 1, 0, 0, 0).getTime(),
      new GregorianCalendar(2024, 2, 31, 3, 30, 0).getTime(),
      new Date(-1L)
    };
    List<Object[]> rows = new ArrayList<>();
    for (int k = 0; k < numbers.length; k++) {
      long integer = integers[k % integers.length];
      Date date = dates[k % dates.length];
      rows.add(
          new Object[] {
            integer,
            integer,
            numbers[k],
            numbers[k],
            numbers[k],
            numbers[k],
            new BigDecimal(bigs[k % bigs.length]),
            date,
            date,
            date
          });
    }
    return rows;
  }

  /** All rows, rendered with and without a date format and for an Oracle dialect. */
  private static String renderNumbersAndDates() throws Exception {
    StringBuilder out = new StringBuilder();
    Object[][] variants = {{false, null}, {false, "dd.MM.yyyy HH:mm:ss.SSS"}, {true, null}};
    for (Object[] variant : variants) {
      Database db = database((Boolean) variant[0]);
      IRowMeta rowMeta = numbersAndDatesRowMeta();
      SqlInsertTemplate template = db.getSqlOutputTemplate(null, "t", rowMeta, (String) variant[1]);
      for (Object[] r : numbersAndDatesRows()) {
        out.append(template.getSql(r).replace("INSERT INTO t(" + NUMBERS_COLUMNS + ") ", ""))
            .append('\n');
      }
    }
    return out.toString();
  }

  private static final String NUMBERS_COLUMNS =
      "i_default, i_length, n_default, n_length, n_mask, n_dot, b_default, d_default, d_iso,"
          + " d_named";

  /** SHA-256 of {@link #renderNumbersAndDates()}, the same under each of {@link #LOCALES}. */
  private static final String NUMBERS_AND_DATES_SHA256 =
      "c4e60b5f88c2407df79cb82b36fbf8c7a76c64aebe9726c1a54cab74625dfc00";

  /**
   * Pins numbers and dates in default and explicit masks under several default locales. The text is
   * the same in every locale (#9677): a period as decimal separator, no grouping, an ASCII minus
   * sign, NaN and infinity as en_US always wrote them, Gregorian years and English month names. The
   * full text is pinned by a digest, a few lines literally.
   */
  @Test
  void numbersAndDatesInSeveralLocales() throws Exception {
    Locale savedLocale = Locale.getDefault();
    Locale savedFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale savedDisplayLocale = Locale.getDefault(Locale.Category.DISPLAY);
    TimeZone savedZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
      Locale.setDefault(Locale.US);
      String expected = renderNumbersAndDates();
      String[] lines = expected.split("\n");
      assertEquals(
          "VALUES (0,0,0.0,0.0,0.00,0.0,12345.678,'2024/07/15 13:45:30.000',"
              + "'2024-07-15T13:45:30','15-Jul-2024')",
          lines[0]);
      // the grouping of the n_mask column is gone
      assertEquals(
          "VALUES (-9223372036854775808,-9223372036854775808,123456.123456789,"
              + "123456.123456789,123456.12,123456.123456789,1000.0,"
              + "'2024/03/31 03:30:00.000','2024-03-31T03:30:00','31-Mar-2024')",
          lines[10]);
      // NaN and infinity
      assertEquals(
          "VALUES (1234567,1234567,NaN,NaN,NaN,NaN,0.0,'1970/01/01 00:59:59.999',"
              + "'1970-01-01T00:59:59','01-Jan-1970')",
          lines[11]);
      assertEquals(
          "VALUES (-1,-1,-0.0,-0.0,-0.00,-0.0,0.0,"
              + "'31.12.2023 23:59:59.999','31.12.2023 23:59:59.999',"
              + "'31.12.2023 23:59:59.999')",
          lines[15]);
      assertEquals(
          "VALUES (42,42,12.99,12.99,12.99,12.99,1000.0,"
              + "TO_DATE('1500/03/01 12:00:00', 'YYYY/MM/DD HH24:MI:SS'),"
              + "TO_DATE('1500/03/01 12:00:00', 'YYYY/MM/DD HH24:MI:SS'),"
              + "TO_DATE('1500/03/01 12:00:00', 'YYYY/MM/DD HH24:MI:SS'))",
          lines[30]);
      assertEquals(NUMBERS_AND_DATES_SHA256, sha256(expected));
      for (Locale locale : LOCALES) {
        Locale.setDefault(locale);
        assertEquals(expected, renderNumbersAndDates(), "locale " + locale);
      }
    } finally {
      Locale.setDefault(savedLocale);
      Locale.setDefault(Locale.Category.FORMAT, savedFormatLocale);
      Locale.setDefault(Locale.Category.DISPLAY, savedDisplayLocale);
      TimeZone.setDefault(savedZone);
    }
  }

  private static String sha256(String text) throws Exception {
    byte[] digest =
        MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
    StringBuilder hex = new StringBuilder();
    for (byte b : digest) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
