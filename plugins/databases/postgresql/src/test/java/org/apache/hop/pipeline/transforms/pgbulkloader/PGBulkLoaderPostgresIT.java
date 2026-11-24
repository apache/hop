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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Runs the PostgreSQL bulk loader against a real PostgreSQL and reads back what it stored: every
 * data type, the date modes under several regional settings, lazy-conversion numbers and client
 * encodings other than UTF-8 (issue 9678).
 */
class PGBulkLoaderPostgresIT {
  private static final String LATIN1_DATABASE = "latin1db";

  private static PostgreSQLContainer<?> container;

  private static final AtomicInteger TABLES = new AtomicInteger();

  private TimeZone originalTimeZone;
  private Locale originalLocale;
  private Locale originalFormatLocale;

  /** The root causes the loader logged as errors in the last load. */
  private final List<String> loggedErrors = new ArrayList<>();

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
    execute(
        container.getDatabaseName(),
        "CREATE DATABASE "
            + LATIN1_DATABASE
            + " ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0");
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  @BeforeEach
  void setUp() {
    originalTimeZone = TimeZone.getDefault();
    originalLocale = Locale.getDefault();
    originalFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale.setDefault(Locale.US);
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
  }

  @AfterEach
  void tearDown() {
    TimeZone.setDefault(originalTimeZone);
    Locale.setDefault(originalLocale);
    Locale.setDefault(Locale.Category.FORMAT, originalFormatLocale);
  }

  // -------------------------------------------------------------------------------------------
  // Harness
  // -------------------------------------------------------------------------------------------

  private static Connection jdbc(String database) throws Exception {
    String url =
        "jdbc:postgresql://"
            + container.getHost()
            + ":"
            + container.getMappedPort(5432)
            + "/"
            + database;
    return DriverManager.getConnection(url, container.getUsername(), container.getPassword());
  }

  private static void execute(String database, String sql) throws Exception {
    try (Connection connection = jdbc(database);
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  /** The rows of the query, each as its columns joined with " | ", null as NULL. */
  private static List<String> query(String database, String sql) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Connection connection = jdbc(database);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      int columns = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
          String value = resultSet.getString(i);
          row.append(i > 1 ? " | " : "").append(value == null ? "NULL" : value);
        }
        rows.add(row.toString());
      }
    }
    return rows;
  }

  /** Creates a table with a unique name in the database and returns that name. */
  private static String createTable(String database, String columns) throws Exception {
    String table = "t" + TABLES.incrementAndGet();
    execute(database, "CREATE TABLE " + table + " (" + columns + ")");
    return table;
  }

  /**
   * A connection to the database. With a client encoding, the driver allows the change and every
   * session sets it.
   */
  private static DatabaseMeta databaseMeta(String database, String clientEncoding) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "pg",
            "POSTGRESQL",
            "Native",
            container.getHost(),
            database,
            String.valueOf(container.getMappedPort(5432)),
            container.getUsername(),
            container.getPassword());
    if (clientEncoding != null) {
      databaseMeta.addExtraOption("POSTGRESQL", "allowEncodingChanges", "true");
      databaseMeta.setConnectSql("SET client_encoding = '" + clientEncoding + "'");
    }
    return databaseMeta;
  }

  private static IRowMeta rowMeta(IValueMeta... valueMetas) {
    IRowMeta rowMeta = new RowMeta();
    for (IValueMeta valueMeta : valueMetas) {
      rowMeta.addValueMeta(valueMeta);
    }
    return rowMeta;
  }

  private static Object[] row(Object... values) {
    return values;
  }

  private static PGBulkLoaderMappingMeta mapping(String column, String field, String dateMask) {
    return new PGBulkLoaderMappingMeta(column, field, dateMask);
  }

  private static Date brussels(String localDateTime) {
    return Date.from(
        LocalDateTime.parse(localDateTime).atZone(ZoneId.of("Europe/Brussels")).toInstant());
  }

  private static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  /** A value of another type stored as the text the input file had (lazy conversion). */
  private static <T extends IValueMeta> T lazy(T valueMeta, String storageMask) {
    ValueMetaString storage = new ValueMetaString(valueMeta.getName());
    storage.setStringEncoding("UTF-8");
    storage.setConversionMask(storageMask);
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    valueMeta.setStorageMetadata(storage);
    return valueMeta;
  }

  /** Runs the loader on the rows through a real connection and returns its error count. */
  private long load(
      DatabaseMeta databaseMeta,
      String table,
      List<PGBulkLoaderMappingMeta> mappings,
      IRowMeta rowMeta,
      Object[]... rows)
      throws Exception {
    loggedErrors.clear();
    TransformMockHelper<PGBulkLoaderMeta, PGBulkLoaderData> helper =
        new TransformMockHelper<>(
            "PostgreSQL Bulk Loader", PGBulkLoaderMeta.class, PGBulkLoaderData.class);
    try {
      when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
          .thenReturn(helper.iLogChannel);
      doAnswer(
              invocation -> {
                Throwable cause = invocation.getArgument(1);
                while (cause.getCause() != null) {
                  cause = cause.getCause();
                }
                loggedErrors.add(String.valueOf(cause.getMessage()));
                return null;
              })
          .when(helper.iLogChannel)
          .logError(anyString(), any(Throwable.class));
      when(helper.pipeline.isRunning()).thenReturn(true);
      when(helper.pipelineMeta.findDatabase(any(), any())).thenReturn(databaseMeta);

      PGBulkLoaderMeta meta = new PGBulkLoaderMeta();
      meta.setDefault();
      meta.setConnection("pg");
      meta.setSchemaName("public");
      meta.setTableName(table);
      meta.setLoadAction(PGBulkLoaderMeta.ACTION_INSERT);
      meta.setDelimiter(";");
      meta.setEnclosure("\"");
      meta.setMappings(mappings);

      PGBulkLoader loader =
          spy(
              new PGBulkLoader(
                  helper.transformMeta,
                  meta,
                  new PGBulkLoaderData(),
                  0,
                  helper.pipelineMeta,
                  helper.pipeline));
      Object[] next = new Object[rows.length];
      System.arraycopy(rows, 1, next, 0, rows.length - 1);
      doReturn(rows[0], next).when(loader).getRow();
      doReturn(rowMeta).when(loader).getInputRowMeta();
      doReturn(new Database(new LoggingObject("it"), new Variables(), databaseMeta))
          .when(loader)
          .getDatabase(any(), any());
      doAnswer(invocation -> null).when(loader).putRow(any(), any());

      assertTrue(loader.init(), "init() failed");
      int calls = 0;
      while (loader.processRow()) {
        calls++;
        assertTrue(calls <= rows.length, "processRow() did not stop");
      }
      loader.dispose();
      return loader.getErrors();
    } finally {
      helper.cleanUp();
    }
  }

  // -------------------------------------------------------------------------------------------
  // Data types
  // -------------------------------------------------------------------------------------------

  @Test
  void everyTypeRoundTrips() throws Exception {
    String database = container.getDatabaseName();
    String table =
        createTable(
            database,
            "id int, s text, i bigint, n double precision, b numeric, flag boolean, bin bytea,"
                + " d date, ts timestamp, pt timestamp");
    IRowMeta rowMeta =
        rowMeta(
            new ValueMetaInteger("id"),
            new ValueMetaString("s"),
            new ValueMetaInteger("i"),
            new ValueMetaNumber("n"),
            new ValueMetaBigNumber("b"),
            new ValueMetaBoolean("flag"),
            new ValueMetaBinary("bin"),
            new ValueMetaDate("d"));
    Date date = brussels("2024-03-05T07:08:09.010");
    Date old = brussels("1950-06-15T23:59:59.999");

    long errors =
        load(
            databaseMeta(database, null),
            table,
            List.of(
                mapping("id", "id", ""),
                mapping("s", "s", ""),
                mapping("i", "i", ""),
                mapping("n", "n", ""),
                mapping("b", "b", ""),
                mapping("flag", "flag", ""),
                mapping("bin", "bin", ""),
                mapping("d", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
                mapping("ts", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
                mapping("pt", "d", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH)),
            rowMeta,
            row(
                1L,
                "plain",
                42L,
                1.5,
                new BigDecimal("12.50"),
                true,
                new byte[] {(byte) 0xde, (byte) 0xad},
                date),
            row(
                2L,
                "say \"hi\"; a\nb é 中 😀 back\\slash",
                Long.MIN_VALUE,
                -0.1,
                new BigDecimal("123456789012345678901234567890.123456789"),
                false,
                new byte[0],
                old),
            row(3L, "", 0L, 1.0E20, new BigDecimal("-0.000"), null, null, null),
            row(4L, null, null, null, null, null, null, null));

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(
        List.of(
            "1 | plain | 42 | 1.5 | 12.50 | t | \\xdead | 2024-03-05 | 2024-03-05 07:08:09.01"
                + " | 2024-03-05 07:08:09.01",
            "2 | say \"hi\"; a\nb é 中 😀 back\\slash | -9223372036854775808 | -0.1"
                + " | 123456789012345678901234567890.123456789 | f | \\x | 1950-06-15"
                + " | 1950-06-15 23:59:59.999 | 1950-06-15 23:59:59.999",
            "3 |  | 0 | 1e+20 | 0.000 | NULL | NULL | NULL | NULL | NULL",
            "4 | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL | NULL"),
        query(
            database,
            "SELECT id, s, i, n, b, flag, bin, d, ts, pt FROM " + table + " ORDER BY id"));
  }

  // -------------------------------------------------------------------------------------------
  // Date modes and regional settings
  // -------------------------------------------------------------------------------------------

  private static final List<PGBulkLoaderMappingMeta> DATE_MODES =
      List.of(
          mapping("tag", "tag", ""),
          mapping("d", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
          mapping("ts", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME));

  /** Loads 2024-03-05 07:08:09.010 in the DATE and DATETIME modes under the format locale. */
  private List<String> loadDateModesUnder(String languageTag, List<String> errors)
      throws Exception {
    Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag(languageTag));
    String database = container.getDatabaseName();
    String table = createTable(database, "tag text, d date, ts timestamp");
    long errorCount =
        load(
            databaseMeta(database, null),
            table,
            DATE_MODES,
            rowMeta(new ValueMetaString("tag"), new ValueMetaDate("d")),
            row(languageTag, brussels("2024-03-05T07:08:09.010")));
    errors.add(errorCount + " " + loggedErrors);
    return query(database, "SELECT tag, d, ts FROM " + table);
  }

  /**
   * The DATE and DATETIME modes write the Gregorian calendar with ASCII digits in every format
   * locale. A Thai locale used to store 2567 for 2024, and Thai or Arabic-Indic digits used to fail
   * the load (issue 9678).
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "en-US",
        "de-DE",
        "fr-FR",
        "nl-BE",
        "th-TH",
        "th-TH-u-nu-thai",
        "ar-EG",
        "fa-IR",
        "ja-JP-u-ca-japanese"
      })
  void dateModesInEveryFormatLocale(String languageTag) throws Exception {
    List<String> errors = new ArrayList<>();

    List<String> rows = loadDateModesUnder(languageTag, errors);

    assertEquals("0 []", errors.get(0));
    assertEquals(List.of(languageTag + " | 2024-03-05 | 2024-03-05 07:08:09.01"), rows);
  }

  /**
   * PASS THROUGH follows the field's own format, in any locale: with a Thai format locale the
   * default mask writes the Buddhist year, and PostgreSQL stores it.
   */
  @Test
  void passThroughFollowsTheFieldsFormat() throws Exception {
    Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("th-TH"));
    String database = container.getDatabaseName();
    String table = createTable(database, "a timestamp, b timestamp");
    ValueMetaDate iso = new ValueMetaDate("iso");
    iso.setConversionMask("yyyy-MM-dd HH:mm:ss");
    iso.setDateFormatLocale(Locale.US);
    Date date = brussels("2024-03-05T07:08:09.010");

    long errors =
        load(
            databaseMeta(database, null),
            table,
            List.of(
                mapping("a", "d", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH),
                mapping("b", "iso", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH)),
            rowMeta(new ValueMetaDate("d"), iso),
            row(date, date));

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(
        List.of("2567-03-05 07:08:09.01 | 2024-03-05 07:08:09"),
        query(database, "SELECT a, b FROM " + table));
  }

  // -------------------------------------------------------------------------------------------
  // Lazy conversion
  // -------------------------------------------------------------------------------------------

  /** Lazy numbers in a form PostgreSQL reads as Hop does. */
  @Test
  void lazyPlainNumbersRoundTrip() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "i bigint, n double precision, b numeric");

    long errors =
        load(
            databaseMeta(database, null),
            table,
            List.of(mapping("i", "i", ""), mapping("n", "n", ""), mapping("b", "b", "")),
            rowMeta(
                lazy(new ValueMetaInteger("i"), "#"),
                lazy(new ValueMetaNumber("n"), null),
                lazy(new ValueMetaBigNumber("b"), "0.00")),
            row(utf8("007"), utf8("-1.25"), utf8("1234.50")),
            row(null, null, null));

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(
        List.of("7 | -1.25 | 1234.50", "NULL | NULL | NULL"),
        query(database, "SELECT i, n, b FROM " + table + " ORDER BY i NULLS LAST"));
  }

  private static <T extends IValueMeta> T german(T valueMeta, String mask) {
    T lazy = lazy(valueMeta, mask);
    lazy.getStorageMetadata().setDecimalSymbol(",");
    lazy.getStorageMetadata().setGroupingSymbol(".");
    return lazy;
  }

  /**
   * A lazy number read with a German decimal comma is converted. It used to be copied as it was
   * read, and PostgreSQL rejected 1.234,50 (issue 9678).
   */
  @Test
  void lazyNumbersWithADecimalCommaRoundTrip() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "n numeric");

    long errors =
        load(
            databaseMeta(database, null),
            table,
            List.of(mapping("n", "n", "")),
            rowMeta(german(new ValueMetaNumber("n"), "#,##0.00")),
            row(utf8("1.234,50")));

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of("1234.5"), query(database, "SELECT n FROM " + table));
  }

  /**
   * A lazy number with a grouping dot is converted. It used to be copied as it was read, and
   * PostgreSQL silently stored 1.234 where Hop reads 1234 (issue 9678).
   */
  @Test
  void lazyNumbersWithAGroupingDotRoundTrip() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "n numeric");

    long errors =
        load(
            databaseMeta(database, null),
            table,
            List.of(mapping("n", "n", "")),
            rowMeta(german(new ValueMetaBigNumber("n"), "#,##0.###")),
            row(utf8("1.234")));

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of("1234"), query(database, "SELECT n FROM " + table));
  }

  // -------------------------------------------------------------------------------------------
  // Client encodings
  // -------------------------------------------------------------------------------------------

  private long loadString(String database, String clientEncoding, String table, String value)
      throws Exception {
    return load(
        databaseMeta(database, clientEncoding),
        table,
        List.of(mapping("s", "s", "")),
        rowMeta(new ValueMetaString("s")),
        row(value));
  }

  @Test
  void nonAsciiStringsRoundTripInTheDriversUtf8ClientEncoding() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "s text");
    String text = "é ß € 中 日本語 😀 Ωmega";

    long errors = loadString(database, null, table, text);

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of(text), query(database, "SELECT s FROM " + table));
  }

  /**
   * The COPY declares UTF-8, so a WIN1252 session does not matter. The loader used to write UTF-8
   * which the server read as WIN1252, storing Ã© for é (issue 9678).
   */
  @Test
  void stringsRoundTripInAWin1252Session() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "s text");

    long errors = loadString(database, "WIN1252", table, "é ß € Ω");

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of("é ß € Ω"), query(database, "SELECT s FROM " + table));
  }

  /**
   * A LATIN1 session does not limit what a UTF-8 database stores. The loader used to write Latin-1,
   * and what it cannot represent silently became '?' (issue 9678).
   */
  @Test
  void stringsRoundTripInALatin1Session() throws Exception {
    String database = container.getDatabaseName();
    String table = createTable(database, "s text");

    long errors = loadString(database, "LATIN1", table, "é ß € Ω");

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of("é ß € Ω"), query(database, "SELECT s FROM " + table));
  }

  /** A LATIN1 database stores what Latin-1 can represent. */
  @Test
  void latin1TextRoundTripsInALatin1Database() throws Exception {
    String table = createTable(LATIN1_DATABASE, "s text");

    long errors = loadString(LATIN1_DATABASE, "LATIN1", table, "é ß ÿ");

    assertEquals(0, errors, loggedErrors::toString);
    assertEquals(List.of("é ß ÿ"), query(LATIN1_DATABASE, "SELECT s FROM " + table));
  }

  /**
   * A character a LATIN1 database cannot store fails the load. It used to become '?' silently in a
   * LATIN1 session (issue 9678).
   */
  @Test
  void anUnrepresentableCharacterInALatin1DatabaseFailsTheLoad() throws Exception {
    String table = createTable(LATIN1_DATABASE, "s text");

    long errors = loadString(LATIN1_DATABASE, "LATIN1", table, "é 中");

    assertEquals(1, errors);
    assertTrue(
        loggedErrors.toString().contains("has no equivalent in encoding \"LATIN1\""),
        loggedErrors::toString);
    assertEquals(List.of(), query(LATIN1_DATABASE, "SELECT s FROM " + table));
  }
}
