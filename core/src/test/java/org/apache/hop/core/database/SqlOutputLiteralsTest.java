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
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins the literal text {@link Database#getSqlOutput(String, String, IRowMeta, Object[], String)},
 * {@link SqlInsertTemplate} and {@link BaseDatabaseMeta#getSqlValue(IValueMeta, Object, String)}
 * write for every value type, one value at a time. Unless a test says otherwise the default locale
 * is en_US and the time zone Europe/Brussels.
 */
class SqlOutputLiteralsTest {

  /** A dialect with a few reserved words, optionally an Oracle variant or bracket quoting. */
  static class Dialect extends ConcreteBaseDatabaseMeta {
    private final boolean oracle;
    private final boolean brackets;

    Dialect(boolean oracle, boolean brackets) {
      this.oracle = oracle;
      this.brackets = brackets;
    }

    @Override
    public String[] getReservedWords() {
      return new String[] {"SELECT", "ORDER", "USER", "GROUP"};
    }

    @Override
    public boolean isOracleVariant() {
      return oracle;
    }

    @Override
    public String getStartQuote() {
      return brackets ? "[" : super.getStartQuote();
    }

    @Override
    public String getEndQuote() {
      return brackets ? "]" : super.getEndQuote();
    }
  }

  private static final TimeZone ZONE = TimeZone.getTimeZone("Europe/Brussels");

  /** 2024-01-15 13:45:30 in Europe/Brussels. */
  private static final Date DATE = date(2024, 0, 15, 13, 45, 30);

  private static Date date(int year, int month, int day, int hour, int minute, int second) {
    GregorianCalendar calendar = new GregorianCalendar(ZONE, Locale.US);
    calendar.clear();
    calendar.set(year, month, day, hour, minute, second);
    return calendar.getTime();
  }

  private Locale savedLocale;
  private Locale savedFormatLocale;
  private Locale savedDisplayLocale;
  private TimeZone savedZone;

  @BeforeAll
  static void setUpOnce() throws HopException {
    DatabasePluginType.getInstance().searchPlugins();
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    savedLocale = Locale.getDefault();
    savedFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
    savedDisplayLocale = Locale.getDefault(Locale.Category.DISPLAY);
    savedZone = TimeZone.getDefault();
    Locale.setDefault(Locale.US);
    TimeZone.setDefault(ZONE);
  }

  @AfterEach
  void tearDown() {
    Locale.setDefault(savedLocale);
    Locale.setDefault(Locale.Category.FORMAT, savedFormatLocale);
    Locale.setDefault(Locale.Category.DISPLAY, savedDisplayLocale);
    TimeZone.setDefault(savedZone);
  }

  private static DatabaseMeta databaseMeta(boolean oracle, boolean brackets) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new Dialect(oracle, brackets));
    return databaseMeta;
  }

  private static Database database(DatabaseMeta databaseMeta) {
    return new Database(mock(ILoggingObject.class), new Variables(), databaseMeta);
  }

  /**
   * The literal a single-column INSERT statement holds for the value. Checks that the template and
   * getSqlOutput agree.
   */
  private static String literal(IValueMeta meta, Object value, String dateFormat, boolean oracle)
      throws Exception {
    Database db = database(databaseMeta(oracle, false));
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(meta);
    String sql = db.getSqlOutput(null, "t", rowMeta, new Object[] {value}, dateFormat);
    assertEquals(
        sql, db.getSqlOutputTemplate(null, "t", rowMeta, dateFormat).getSql(new Object[] {value}));
    String prefix = "INSERT INTO t(" + meta.getName() + ") VALUES (";
    assertEquals(prefix, sql.substring(0, prefix.length()));
    assertEquals(')', sql.charAt(sql.length() - 1));
    return sql.substring(prefix.length(), sql.length() - 1);
  }

  private static String literal(IValueMeta meta, Object value) throws Exception {
    return literal(meta, value, null, false);
  }

  private static String sqlValue(IValueMeta meta, Object value, String dateFormat)
      throws Exception {
    return new Dialect(false, false).getSqlValue(meta, value, dateFormat);
  }

  /**
   * Asserts the literal of each value, in the INSERT statement and from getSqlValue (without a date
   * format).
   */
  private static void assertLiterals(IValueMeta meta, Object[] values, String... expected)
      throws Exception {
    assertEquals(values.length, expected.length);
    for (int i = 0; i < values.length; i++) {
      assertEquals(expected[i], literal(meta, values[i]), meta.getName() + " " + values[i]);
      assertEquals(expected[i], sqlValue(meta, values[i], null), meta.getName() + " " + values[i]);
    }
  }

  private static IValueMeta withMask(IValueMeta meta, String mask) {
    meta.setConversionMask(mask);
    return meta;
  }

  private interface LocaleBlock {
    void run() throws Exception;
  }

  /** Runs the block with the default locale (every category) set to the locale. */
  private static void inLocale(Locale locale, LocaleBlock block) throws Exception {
    Locale before = Locale.getDefault();
    Locale beforeFormat = Locale.getDefault(Locale.Category.FORMAT);
    Locale beforeDisplay = Locale.getDefault(Locale.Category.DISPLAY);
    try {
      Locale.setDefault(locale);
      block.run();
    } finally {
      Locale.setDefault(before);
      Locale.setDefault(Locale.Category.FORMAT, beforeFormat);
      Locale.setDefault(Locale.Category.DISPLAY, beforeDisplay);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Behaviour that stays as it is
  // ---------------------------------------------------------------------------------------------

  @Test
  void stringsAreQuotedAndEscaped() throws Exception {
    assertLiterals(
        new ValueMetaString("s"),
        new Object[] {"O'Brien", "", "a\\b", "l1\nl2\r", "tab\t", "üñ", "''"},
        "'O''Brien'",
        "null",
        "'a\\b'",
        "'l1\\nl2\\r'",
        "'tab\t'",
        "'üñ'",
        "''''''");
  }

  @Test
  void booleansAreWrittenAsYAndN() throws Exception {
    assertLiterals(new ValueMetaBoolean("b"), new Object[] {true, false}, "'Y'", "'N'");
  }

  @Test
  void nullsOfEveryTypeAreWrittenAsNull() throws Exception {
    for (IValueMeta meta :
        new IValueMeta[] {
          new ValueMetaString("s"),
          new ValueMetaBoolean("b"),
          new ValueMetaInteger("i"),
          new ValueMetaNumber("n"),
          new ValueMetaBigNumber("bn"),
          new ValueMetaDate("d")
        }) {
      assertEquals("null", literal(meta, null), meta.getName());
      assertEquals("null", literal(meta, null, "yyyy-MM-dd", false), meta.getName());
      assertEquals("null", literal(meta, null, null, true), meta.getName());
      assertEquals("null", sqlValue(meta, null, null), meta.getName());
    }
  }

  @Test
  void integersInTheDefaultMask() throws Exception {
    Object[] values = {0L, -1L, 42L, Long.MAX_VALUE, Long.MIN_VALUE, 1234567L};
    String[] expected = {"0", "-1", "42", "9223372036854775807", "-9223372036854775808", "1234567"};
    assertLiterals(new ValueMetaInteger("i"), values, expected);
    assertLiterals(new ValueMetaInteger("i5", 5, 0), values, expected);
  }

  @Test
  void numbersInTheDefaultMask() throws Exception {
    Object[] values = {
      0.0,
      -0.0,
      1.5,
      12.99,
      -3.5,
      1.0 / 3,
      0.125,
      262143.75,
      262144.5,
      1.0E7,
      1.0E-5,
      0.001,
      123456.123456789,
      -98765.4321,
      1234567.25,
      1.0E20
    };
    String[] expected = {
      "0.0",
      "-0.0",
      "1.5",
      "12.99",
      "-3.5",
      "0.3333333333",
      "0.125",
      "262143.75",
      "262144.5",
      "10000000.0",
      "0.00001",
      "0.001",
      "123456.123456789",
      "-98765.4321",
      "1234567.25",
      "100000000000000000000.0"
    };
    assertLiterals(new ValueMetaNumber("n"), values, expected);
    assertLiterals(new ValueMetaNumber("n10", 10, 2), values, expected);
  }

  @Test
  void numberMasksRoundToTheirFractionDigits() throws Exception {
    Object[] values = {0.0, -0.0, 1.5, 1.0 / 3, 0.125, 0.135, 1.0E-5, -98765.4321, 1.0E20};
    assertLiterals(
        withMask(new ValueMetaNumber("n"), "0.00"),
        values,
        "0.00",
        "-0.00",
        "1.50",
        "0.33",
        "0.12",
        "0.14",
        "0.00",
        "-98765.43",
        "100000000000000000000.00");
    assertLiterals(
        withMask(new ValueMetaNumber("n"), "#.##"),
        values,
        "0",
        "-0",
        "1.5",
        "0.33",
        "0.12",
        "0.14",
        "0",
        "-98765.43",
        "100000000000000000000");
    IValueMeta halfUp = withMask(new ValueMetaNumber("n"), "0.00");
    halfUp.setRoundingType("half_up");
    assertLiterals(
        halfUp,
        values,
        "0.00",
        "-0.00",
        "1.50",
        "0.33",
        "0.13",
        "0.14",
        "0.00",
        "-98765.43",
        "100000000000000000000.00");
  }

  @Test
  void bigNumbersInTheDefaultMask() throws Exception {
    assertLiterals(
        new ValueMetaBigNumber("bn"),
        new Object[] {
          new BigDecimal("12345.678"),
          new BigDecimal("-0.00"),
          new BigDecimal("1E+3"),
          new BigDecimal("1E-30"),
          new BigDecimal("123456789012345678901234567890.123456789"),
          new BigDecimal("-7.25"),
          new BigDecimal("0.00000000000000000001"),
          new BigDecimal("1234567.125")
        },
        "12345.678",
        "0.0",
        "1000.0",
        "0.0",
        "123456789012345678901234567890.123456789",
        "-7.25",
        "0.00000000000000000001",
        "1234567.125");
  }

  @Test
  void bigNumberMasksRoundToTheirFractionDigits() throws Exception {
    assertLiterals(
        withMask(new ValueMetaBigNumber("bn"), "0.00"),
        new Object[] {
          new BigDecimal("12345.678"),
          new BigDecimal("1E+3"),
          new BigDecimal("123456789012345678901234567890.123456789"),
          new BigDecimal("0.1"),
          new BigDecimal("1234567.125")
        },
        "12345.68",
        "1000.00",
        "123456789012345678901234567890.12",
        "0.10",
        "1234567.12");
  }

  @Test
  void datesInTheirOwnMask() throws Exception {
    Date old = date(1500, 2, 1, 12, 0, 0);
    assertLiterals(
        new ValueMetaDate("d"),
        new Object[] {DATE, old},
        "'2024/01/15 13:45:30.000'",
        "'1500/03/01 12:00:00.000'");
    assertLiterals(
        withMask(new ValueMetaDate("d"), "yyyy-MM-dd'T'HH:mm:ss"),
        new Object[] {DATE},
        "'2024-01-15T13:45:30'");
    assertLiterals(
        withMask(new ValueMetaDate("d"), "dd-MMM-yyyy"), new Object[] {DATE}, "'15-Jan-2024'");
    assertLiterals(
        withMask(new ValueMetaDate("d"), "dd MMMM yyyy G"),
        new Object[] {DATE},
        "'15 January 2024 AD'");
  }

  @Test
  void dateFormatOptionOverridesTheMask() throws Exception {
    IValueMeta meta = withMask(new ValueMetaDate("d"), "dd-MMM-yyyy");
    assertEquals("'2024-01-15 13:45:30'", literal(meta, DATE, "yyyy-MM-dd HH:mm:ss", false));
    assertEquals("'15 Jan 2024'", literal(meta, DATE, "dd MMM yyyy", false));
    assertEquals("'2024-01-15'", literal(meta, DATE, "yyyy-MM-dd", true));
    assertEquals("'2024-01-15 13:45:30'", sqlValue(meta, DATE, "yyyy-MM-dd HH:mm:ss"));
    assertEquals("'15 Jan 2024'", sqlValue(meta, DATE, "dd MMM yyyy"));
  }

  @Test
  void oracleWritesToDateWithoutADateFormat() throws Exception {
    assertEquals(
        "TO_DATE('2024/01/15 13:45:30', 'YYYY/MM/DD HH24:MI:SS')",
        literal(withMask(new ValueMetaDate("d"), "dd-MMM-yyyy"), DATE, null, true));
    assertEquals(
        "TO_DATE('2024/01/15 13:45:30', 'YYYY/MM/DD HH24:MI:SS')",
        literal(new ValueMetaDate("d"), DATE, "", true));
    // getSqlValue has no Oracle variant
    assertEquals("'2024/01/15 13:45:30.000'", sqlValue(new ValueMetaDate("d"), DATE, null));
  }

  @Test
  void dialectQuotesNames() throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("order"));
    rowMeta.addValueMeta(new ValueMetaInteger("unit price"));
    rowMeta.addValueMeta(new ValueMetaString("plain"));
    Object[] row = {"x", 1L, "y"};
    assertEquals(
        "INSERT INTO \"user\".\"group\"(\"order\", \"unit price\", plain) VALUES ('x',1,'y')",
        database(databaseMeta(false, false)).getSqlOutput("user", "group", rowMeta, row, null));
    assertEquals(
        "INSERT INTO [user].[group]([order], [unit price], plain) VALUES ('x',1,'y')",
        database(databaseMeta(false, true)).getSqlOutput("user", "group", rowMeta, row, null));
    assertEquals(
        "INSERT INTO sales.customer([order], [unit price], plain) VALUES ('x',1,'y')",
        database(databaseMeta(true, true)).getSqlOutput("sales", "customer", rowMeta, row, null));
  }

  /** The interface language (DISPLAY locale) does not reach the literals. */
  @Test
  void displayLocaleDoesNotChangeTheLiterals() throws Exception {
    Locale.setDefault(Locale.Category.DISPLAY, Locale.GERMANY);
    assertEquals("1.5", literal(new ValueMetaNumber("n"), 1.5));
    assertEquals("'15-Jan-2024'", literal(withMask(new ValueMetaDate("d"), "dd-MMM-yyyy"), DATE));
    assertEquals("'15 Jan 2024'", literal(new ValueMetaDate("d"), DATE, "dd MMM yyyy", false));
  }

  /** Locales that already write ASCII digits and a period keep doing so. */
  @Test
  void thaiNumbersAreAlreadyPlain() throws Exception {
    inLocale(
        Locale.forLanguageTag("th-TH"),
        () -> {
          assertEquals("1.5", literal(new ValueMetaNumber("n"), 1.5));
          assertEquals("-3.5", literal(new ValueMetaNumber("n"), -3.5));
          assertEquals("1.50", literal(withMask(new ValueMetaNumber("n"), "0.00"), 1.5));
          assertEquals("-1", literal(new ValueMetaInteger("i"), -1L));
        });
  }

  // ---------------------------------------------------------------------------------------------
  // #9677: numbers and dates are written the same in every locale
  // ---------------------------------------------------------------------------------------------

  /** Under de_DE a Number used to be written with a decimal comma, read as two values. */
  @Test
  void germanLocaleWritesADecimalPoint() throws Exception {
    inLocale(
        Locale.GERMANY,
        () -> {
          IValueMeta number = new ValueMetaNumber("n");
          assertEquals("1.5", literal(number, 1.5));
          assertEquals("-3.5", literal(number, -3.5));
          assertEquals("1234567.25", literal(number, 1234567.25));
          assertEquals("0.3333333333", literal(number, 1.0 / 3));
          assertEquals("1.50", literal(withMask(new ValueMetaNumber("n"), "0.00"), 1.5));
          assertEquals(
              "1234567.25", literal(withMask(new ValueMetaNumber("n"), "#,##0.00"), 1234567.25));
          assertEquals("1.5", sqlValue(number, 1.5, null));
          assertEquals("1.5", literal(new ValueMetaBigNumber("bn"), new BigDecimal("1.5")));
          assertEquals(
              "1234567.12",
              literal(
                  withMask(new ValueMetaBigNumber("bn"), "#,##0.00"),
                  new BigDecimal("1234567.125")));
        });
  }

  /** A mask with grouping used to write the grouping separator, even under en_US. */
  @Test
  void groupingMaskWritesNoGroupingSeparators() throws Exception {
    assertEquals("1234567.25", literal(withMask(new ValueMetaNumber("n"), "#,##0.00"), 1234567.25));
    assertEquals("1234567", literal(withMask(new ValueMetaInteger("i"), "#,##0"), 1234567L));
    assertEquals(
        "1234567.12",
        literal(withMask(new ValueMetaBigNumber("bn"), "#,##0.00"), new BigDecimal("1234567.125")));
    assertEquals(
        "1234567.25", sqlValue(withMask(new ValueMetaNumber("n"), "#,##0.00"), 1234567.25, null));
  }

  /**
   * Of a currency, percent or padding mask only the rounding is kept. A percent mask keeps the
   * digits it rounds the multiplied value to. A scientific mask keeps its exponent.
   */
  @Test
  void decoratedMasksKeepOnlyTheirRounding() throws Exception {
    assertEquals("1.50", literal(withMask(new ValueMetaNumber("n"), "$#,##0.00"), 1.5));
    assertEquals("-3.50", literal(withMask(new ValueMetaNumber("n"), "¤#,##0.00"), -3.5));
    assertEquals("-3.50", literal(withMask(new ValueMetaNumber("n"), "0.00 EUR"), -3.5));
    assertEquals("-3.50", literal(withMask(new ValueMetaNumber("n"), "0.00;(0.00)"), -3.5));
    assertEquals("0.125", literal(withMask(new ValueMetaNumber("n"), "0.0%"), 0.125));
    assertEquals("0.123", literal(withMask(new ValueMetaNumber("n"), "0.0%"), 0.12345));
    assertEquals("0.012", literal(withMask(new ValueMetaNumber("n"), "0‰"), 0.012345));
    assertEquals("1.235E-3", literal(withMask(new ValueMetaNumber("n"), "0.###E0"), 0.0012346));
    assertEquals("42", literal(withMask(new ValueMetaInteger("i"), "000000"), 42L));
    assertEquals("-1", literal(withMask(new ValueMetaInteger("i"), "000000"), -1L));
    assertEquals("42", literal(withMask(new ValueMetaInteger("i"), "0.00"), 42L));
    assertEquals("42", literal(withMask(new ValueMetaInteger("i"), "$#,##0"), 42L));
  }

  /** NaN and infinity used to be written as text no database reads. */
  @Test
  void nonFiniteNumbersAreWrittenAsNull() throws Exception {
    IValueMeta number = new ValueMetaNumber("n");
    assertEquals("null", literal(number, Double.NaN));
    assertEquals("null", literal(number, Double.POSITIVE_INFINITY));
    assertEquals("null", literal(number, Double.NEGATIVE_INFINITY));
    assertEquals("null", literal(withMask(new ValueMetaNumber("n"), "0.00"), Double.NaN));
    assertEquals("null", sqlValue(number, Double.NaN, null));
  }

  /** sv_SE used to write U+2212 as the minus sign, with a decimal comma. */
  @Test
  void swedishLocaleWritesAnAsciiMinus() throws Exception {
    inLocale(
        Locale.forLanguageTag("sv-SE"),
        () -> {
          assertEquals("-1", literal(new ValueMetaInteger("i"), -1L));
          assertEquals("-3.5", literal(new ValueMetaNumber("n"), -3.5));
          assertEquals("-0.0", literal(new ValueMetaNumber("n"), -0.0));
          assertEquals("-1", sqlValue(new ValueMetaInteger("i"), -1L, null));
        });
  }

  /** Locales with their own digits write ASCII digits. */
  @Test
  void nativeDigitLocalesWriteAsciiDigits() throws Exception {
    for (String tag : new String[] {"ar-EG", "hi-IN-u-nu-deva", "th-TH-u-nu-thai"}) {
      inLocale(
          Locale.forLanguageTag(tag),
          () -> {
            assertEquals("-1234567", literal(new ValueMetaInteger("i"), -1234567L), tag);
            assertEquals("-3.5", literal(new ValueMetaNumber("n"), -3.5), tag);
            assertEquals("1.5", literal(new ValueMetaBigNumber("bn"), new BigDecimal("1.5")), tag);
            assertEquals("'2024/01/15 13:45:30.000'", literal(new ValueMetaDate("d"), DATE), tag);
            assertEquals(
                "'2024-01-15'", literal(new ValueMetaDate("d"), DATE, "yyyy-MM-dd", false), tag);
          });
    }
  }

  /** th_TH used to count Buddhist years in every date path. */
  @Test
  void thaiLocaleWritesGregorianYears() throws Exception {
    inLocale(
        Locale.forLanguageTag("th-TH"),
        () -> {
          assertEquals("'2024/01/15 13:45:30.000'", literal(new ValueMetaDate("d"), DATE));
          assertEquals("'2024-01-15'", literal(new ValueMetaDate("d"), DATE, "yyyy-MM-dd", false));
          assertEquals(
              "TO_DATE('2024/01/15 13:45:30', 'YYYY/MM/DD HH24:MI:SS')",
              literal(new ValueMetaDate("d"), DATE, null, true));
          assertEquals("'2024-01-15'", sqlValue(new ValueMetaDate("d"), DATE, "yyyy-MM-dd"));
          assertEquals("'2024/01/15 13:45:30.000'", sqlValue(new ValueMetaDate("d"), DATE, null));
        });
  }

  /** Month names used to follow the regional format locale. */
  @Test
  void monthNamesAreEnglish() throws Exception {
    inLocale(
        Locale.GERMANY,
        () -> {
          assertEquals(
              "'15-Jan-2024'", literal(withMask(new ValueMetaDate("d"), "dd-MMM-yyyy"), DATE));
          assertEquals(
              "'15 January 2024'", literal(new ValueMetaDate("d"), DATE, "dd MMMM yyyy", false));
          assertEquals("'15 Jan 2024'", sqlValue(new ValueMetaDate("d"), DATE, "dd MMM yyyy"));
        });
  }

  /** A date format's own time zone is still honoured. */
  @Test
  void dateMaskKeepsItsTimeZone() throws Exception {
    inLocale(
        Locale.GERMANY,
        () -> {
          IValueMeta utc = withMask(new ValueMetaDate("d"), "yyyy-MM-dd HH:mm");
          utc.setDateFormatTimeZone(TimeZone.getTimeZone("UTC"));
          assertEquals("'2024-01-15 12:45'", literal(utc, DATE));
          assertEquals("'2024-01-15 12:45'", sqlValue(utc, DATE, null));
          // the transform's date format is in the default time zone, as before
          assertEquals("'2024-01-15 13:45'", literal(utc, DATE, "yyyy-MM-dd HH:mm", false));
        });
  }
}
