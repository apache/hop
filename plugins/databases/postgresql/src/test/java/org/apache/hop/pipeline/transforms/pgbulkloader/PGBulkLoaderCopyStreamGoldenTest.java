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

package org.apache.hop.pipeline.transforms.pgbulkloader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;

/**
 * Golden tests of the bytes the PostgreSQL bulk loader hands to the COPY stream.
 *
 * <p>The loader runs through {@code init()} / {@code processRow()} / {@code dispose()} as in a
 * pipeline. Only the database is faked: the client encoding the server reports, and the driver's
 * {@link CopyIn} operation behind the real {@link org.postgresql.copy.PGCopyOutputStream}. Every
 * byte the stream passes to the driver is captured and compared with the literal expected output,
 * as text or, for encodings other than UTF-8, as hex.
 */
class PGBulkLoaderCopyStreamGoldenTest {
  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private static final String NL_HEX =
      HexFormat.of().formatHex(Const.CR.getBytes(StandardCharsets.UTF_8));

  private TransformMockHelper<PGBulkLoaderMeta, PGBulkLoaderData> helper;
  private PGBulkLoaderMeta meta;
  private final Map<String, String> variables = new HashMap<>();
  private TimeZone originalTimeZone;
  private Locale originalLocale;
  private Locale originalFormatLocale;

  @BeforeEach
  void setUp() throws Exception {
    originalTimeZone = TimeZone.getDefault();
    originalLocale = Locale.getDefault();
    originalFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale.setDefault(Locale.US);
    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));

    PluginRegistry.getInstance()
        .registerPluginClass(
            PostgreSqlDatabaseMeta.class.getName(),
            DatabasePluginType.class,
            DatabaseMetaPlugin.class);

    helper =
        new TransformMockHelper<>(
            "PostgreSQL Bulk Loader", PGBulkLoaderMeta.class, PGBulkLoaderData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    when(helper.pipelineMeta.findDatabase(any(), any()))
        .thenReturn(
            new DatabaseMeta(
                "pg", "POSTGRESQL", "Native", "localhost", "db", "5432", "user", "password"));

    meta = new PGBulkLoaderMeta();
    meta.setDefault();
    meta.setConnection("pg");
    meta.setSchemaName("public");
    meta.setTableName("target");
    meta.setLoadAction(PGBulkLoaderMeta.ACTION_INSERT);
    meta.setDelimiter(";");
    meta.setEnclosure("\"");
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
    TimeZone.setDefault(originalTimeZone);
    Locale.setDefault(originalLocale);
    Locale.setDefault(Locale.Category.FORMAT, originalFormatLocale);
  }

  // -------------------------------------------------------------------------------------------
  // Harness
  // -------------------------------------------------------------------------------------------

  /** What one run of the loader did. */
  private static final class Copy {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final List<IRowMeta> forwardedMetas = new ArrayList<>();
    final List<Object[]> forwardedRows = new ArrayList<>();
    String command;
    PGBulkLoaderData data;
    long errors;
    long linesOutput;

    String text(Charset charset) {
      return new String(bytes.toByteArray(), charset);
    }

    String hex() {
      return HexFormat.of().formatHex(bytes.toByteArray());
    }
  }

  /**
   * Loads the rows. The server reports the client encoding given, or none when it is null. Without
   * mappings in the meta, every field is loaded into a column of the same name.
   */
  private Copy load(String clientEncoding, IRowMeta rowMeta, Object[]... rows) throws Exception {
    if (meta.getMappings().isEmpty()) {
      List<PGBulkLoaderMappingMeta> mappings = new ArrayList<>();
      for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
        mappings.add(new PGBulkLoaderMappingMeta(valueMeta.getName(), valueMeta.getName(), ""));
      }
      meta.setMappings(mappings);
    }

    Copy copy = new Copy();

    Database db = mock(Database.class);
    Connection connection = mock(Connection.class);
    when(db.getConnection()).thenReturn(connection);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    ResultSet encodingResult = mock(ResultSet.class);
    when(statement.executeQuery("show client_encoding")).thenReturn(encodingResult);
    if (clientEncoding == null) {
      when(encodingResult.next()).thenReturn(false);
    } else {
      when(encodingResult.next()).thenReturn(true);
      ResultSetMetaData encodingMetaData = mock(ResultSetMetaData.class);
      when(encodingMetaData.getColumnCount()).thenReturn(1);
      when(encodingResult.getMetaData()).thenReturn(encodingMetaData);
      when(encodingResult.getString(1)).thenReturn(clientEncoding);
    }
    PGConnection pgConnection = mock(PGConnection.class);
    when(connection.unwrap(PGConnection.class)).thenReturn(pgConnection);
    CopyManager copyManager = mock(CopyManager.class);
    when(pgConnection.getCopyAPI()).thenReturn(copyManager);
    CopyIn copyIn = mock(CopyIn.class);
    when(copyManager.copyIn(anyString()))
        .thenAnswer(
            invocation -> {
              copy.command = invocation.getArgument(0);
              return copyIn;
            });
    doAnswer(
            invocation -> {
              byte[] buffer = invocation.getArgument(0);
              int offset = invocation.getArgument(1);
              int length = invocation.getArgument(2);
              copy.bytes.write(buffer, offset, length);
              return null;
            })
        .when(copyIn)
        .writeToCopy(any(byte[].class), anyInt(), anyInt());

    PGBulkLoaderData data = new PGBulkLoaderData();
    copy.data = data;
    PGBulkLoader loader =
        spy(
            new PGBulkLoader(
                helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline));
    for (Map.Entry<String, String> variable : variables.entrySet()) {
      loader.setVariable(variable.getKey(), variable.getValue());
    }
    doReturn(db).when(loader).getDatabase(any(), any());
    Object[] next = new Object[rows.length];
    for (int i = 1; i < rows.length; i++) {
      next[i - 1] = rows[i];
    }
    if (rows.length == 0) {
      doReturn(null).when(loader).getRow();
    } else {
      next[next.length - 1] = null;
      doReturn(rows[0], next).when(loader).getRow();
    }
    doReturn(rowMeta).when(loader).getInputRowMeta();
    doAnswer(
            invocation -> {
              copy.forwardedMetas.add(invocation.getArgument(0));
              copy.forwardedRows.add(invocation.getArgument(1));
              return null;
            })
        .when(loader)
        .putRow(any(), any());

    assertTrue(loader.init(), "init() failed");
    int calls = 0;
    while (loader.processRow()) {
      calls++;
      assertTrue(calls <= rows.length, "processRow() did not stop");
    }
    loader.dispose();

    copy.errors = loader.getErrors();
    copy.linesOutput = loader.getLinesOutput();
    return copy;
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

  /** The rows, each followed by the line separator the loader writes. */
  private static String lines(String... rows) {
    StringBuilder text = new StringBuilder();
    for (String row : rows) {
      text.append(row).append(Const.CR);
    }
    return text.toString();
  }

  /** The hex of the rows, each followed by the line separator the loader writes. */
  private static String hexLines(String... rows) {
    StringBuilder text = new StringBuilder();
    for (String row : rows) {
      text.append(row.replace(" ", "")).append(NL_HEX);
    }
    return text.toString();
  }

  private static PGBulkLoaderMappingMeta mapping(String table, String stream, String dateMask) {
    return new PGBulkLoaderMappingMeta(table, stream, dateMask);
  }

  private static ValueMetaString lazyString(String name) {
    ValueMetaString storage = new ValueMetaString(name);
    storage.setStringEncoding("UTF-8");
    ValueMetaString valueMeta = new ValueMetaString(name);
    valueMeta.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    valueMeta.setStorageMetadata(storage);
    valueMeta.setStringEncoding("UTF-8");
    return valueMeta;
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

  private static byte[] utf8(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  private static Date utc(String instant) {
    return Date.from(Instant.parse(instant));
  }

  private static Date brussels(String localDateTime) {
    return Date.from(
        LocalDateTime.parse(localDateTime).atZone(ZoneId.of("Europe/Brussels")).toInstant());
  }

  private static ValueMetaDate date(String name, String mask) {
    ValueMetaDate valueMeta = new ValueMetaDate(name);
    valueMeta.setConversionMask(mask);
    return valueMeta;
  }

  // -------------------------------------------------------------------------------------------
  // COPY command
  // -------------------------------------------------------------------------------------------

  @Test
  void theCopyCommandListsTheTableColumnsInMappingOrder() throws Exception {
    meta.setMappings(
        List.of(mapping("col_b", "b", ""), mapping("Col A", "a", ""), mapping("select", "b", "")));

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("a"), new ValueMetaInteger("b")),
            row("x", 1L),
            row("y", 2L));

    assertEquals(
        "COPY \"public\".target ( col_b, \"Col A\", \"select\" )  FROM STDIN WITH (FORMAT csv,"
            + " DELIMITER ';', QUOTE '\"', ENCODING 'UTF8');"
            + Const.CR,
        copy.command);
    assertEquals(lines("1;\"x\";1", "2;\"y\";2"), copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  @Test
  void theDelimiterAndEnclosureCanBeVariables() throws Exception {
    meta.setDelimiter("${DELIM}");
    meta.setEnclosure("${QUOTE}");
    variables.put("DELIM", "|");
    variables.put("QUOTE", "'");

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("a"), new ValueMetaInteger("b")),
            row("it's", 1L),
            row("x|y", 2L));

    assertEquals(
        "COPY \"public\".target ( \"a\", b )  FROM STDIN WITH (FORMAT csv, DELIMITER '|', QUOTE"
            + " ''', ENCODING 'UTF8');"
            + Const.CR,
        copy.command);
    assertEquals(lines("'it''s'|1", "'x|y'|2"), copy.text(StandardCharsets.UTF_8));
  }

  /** No delimiter and no enclosure: the command says 'null', and the values are run together. */
  @Test
  void aMissingDelimiterAndEnclosure() throws Exception {
    meta.setDelimiter(null);
    meta.setEnclosure(null);

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("a"), new ValueMetaString("b"), new ValueMetaInteger("c")),
            row("ab", "c\"d", 1L));

    assertEquals(
        "COPY \"public\".target ( \"a\", b, \"c\" )  FROM STDIN WITH (FORMAT csv, DELIMITER 'null',"
            + " QUOTE 'null', ENCODING 'UTF8');"
            + Const.CR,
        copy.command);
    assertEquals(lines("abc\"d1"), copy.text(StandardCharsets.UTF_8));
  }

  @Test
  void anEmptyEnclosureAndATabDelimiter() throws Exception {
    meta.setDelimiter("\t");
    meta.setEnclosure("");

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("a"), new ValueMetaInteger("b"), new ValueMetaString("c")),
            row("a\"b", 1L, "x\ty"),
            row("", null, null));

    assertEquals(lines("a\"b\t1\tx\ty", "\t\t"), copy.text(StandardCharsets.UTF_8));
  }

  /** A non-ASCII delimiter is encoded in UTF-8 like the values, whatever the client encoding. */
  @Test
  void aNonAsciiDelimiterIsEncodedInUtf8() throws Exception {
    meta.setDelimiter("¦");

    Copy copy =
        load("LATIN1", rowMeta(new ValueMetaString("a"), new ValueMetaString("b")), row("é", "ß"));

    assertEquals(hexLines("22 c3a9 22 c2a6 22 c39f 22"), copy.hex());
  }

  @Test
  void noInputRowsOpenNoCopy() throws Exception {
    Copy copy = load("UTF8", rowMeta(new ValueMetaString("a")));

    assertNull(copy.command);
    assertEquals("", copy.hex());
    assertEquals(0, copy.errors);
    assertEquals(0, copy.linesOutput);
  }

  // -------------------------------------------------------------------------------------------
  // Strings and client encodings
  // -------------------------------------------------------------------------------------------

  @Test
  void stringsInUtf8() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("s"), new ValueMetaInteger("n")),
            row("plain ascii", 1L),
            row("é ß 中文 😀", 2L),
            row("semi;colon", 3L),
            row("say \"hi\"", 4L),
            row("\"", 5L),
            row("it's", 6L),
            row("back\\slash\\", 7L),
            row("cr\rlf\ncrlf\r\n", 8L),
            row("tab\there", 9L),
            row("", 10L),
            row(null, 11L),
            row("\u0000nul\u007fdel", 12L),
            row("a\uD800b", 13L),
            row("\u0080ÿĀ", 14L),
            row(" padded ", null),
            row(null, null));

    assertEquals(
        lines(
            "\"plain ascii\";1",
            "\"é ß 中文 😀\";2",
            "\"semi;colon\";3",
            "\"say \"\"hi\"\"\";4",
            "\"\"\"\";5",
            "\"it's\";6",
            "\"back\\slash\\\";7",
            "\"cr\rlf\ncrlf\r\n\";8",
            "\"tab\there\";9",
            "\"\";10",
            ";11",
            "\"\u0000nul\u007fdel\";12",
            "\"a?b\";13",
            "\"\u0080ÿĀ\";14",
            "\" padded \";",
            ";"),
        copy.text(StandardCharsets.UTF_8));
    // the same, byte for byte: 4-byte emoji, 3-byte CJK, the unpaired surrogate as '?'
    assertEquals(
        hexLines(
            "22 706c61696e206173636969 22 3b 31",
            "22 c3a9 20 c39f 20 e4b8ade69687 20 f09f9880 22 3b 32",
            "22 73656d693b636f6c6f6e 22 3b 33",
            "22 73617920 2222 6869 2222 22 3b 34",
            "22 2222 22 3b 35",
            "22 69742773 22 3b 36",
            "22 6261636b 5c 736c617368 5c 22 3b 37",
            "22 6372 0d 6c66 0a 63726c66 0d0a 22 3b 38",
            "22 746162 09 68657265 22 3b 39",
            "22 22 3b 3130",
            "3b 3131",
            "22 00 6e756c 7f 64656c 22 3b 3132",
            "22 61 3f 62 22 3b 3133",
            "22 c280 c3bf c480 22 3b 3134",
            "22 20706164646564 20 22 3b",
            "3b"),
        copy.hex());
    assertEquals(0, copy.errors);
    assertEquals(16, copy.linesOutput);
  }

  private static final Object[][] ENCODING_ROWS = {
    row("abc"),
    row("é"),
    row("ß"),
    row("€"),
    row("中"),
    row("日本語"),
    row("😀"),
    row("\u0080"),
    row("x\"y"),
    row("Ab1;\\"),
  };

  private Copy loadEncodingRows(String clientEncoding) throws Exception {
    meta.setMappings(new ArrayList<>());
    return load(clientEncoding, rowMeta(new ValueMetaString("s")), ENCODING_ROWS);
  }

  /**
   * The values are written in UTF-8 whatever the session's client encoding, and the COPY command
   * declares it, so the server converts them and nothing silently becomes '?' (issue 9678).
   */
  @Test
  void stringsAreWrittenInUtf8WhateverTheClientEncoding() throws Exception {
    String expected =
        hexLines(
            "22 616263 22",
            "22 c3a9 22",
            "22 c39f 22",
            "22 e282ac 22",
            "22 e4b8ad 22",
            "22 e697a5e69cace8aa9e 22",
            "22 f09f9880 22",
            "22 c280 22",
            "22 78 2222 79 22",
            "22 4162313b5c 22");

    for (String clientEncoding :
        new String[] {
          "UTF8",
          "LATIN1",
          "LATIN9",
          "WIN1252",
          "SQL_ASCII",
          "EUC_JP",
          "SJIS",
          "GBK",
          "BIG5",
          "EUC_KR",
          "NO_SUCH_ENCODING",
          null
        }) {
      Copy copy = loadEncodingRows(clientEncoding);
      assertEquals(expected, copy.hex(), "client encoding " + clientEncoding);
      assertEquals(0, copy.errors);
    }
  }

  /**
   * Lazy (binary) strings are decoded with their storage encoding and written in UTF-8, also in a
   * LATIN1 session; indexed strings are looked up; the trim type is applied.
   */
  @Test
  void lazyIndexedAndTrimmedStrings() throws Exception {
    ValueMetaString indexed = new ValueMetaString("indexed");
    indexed.setStorageType(IValueMeta.STORAGE_TYPE_INDEXED);
    indexed.setIndex(new Object[] {"zero", "één"});
    ValueMetaString trimmed = new ValueMetaString("trimmed");
    trimmed.setTrimType(IValueMeta.TRIM_TYPE_BOTH);

    Copy copy =
        load(
            "LATIN1",
            rowMeta(lazyString("lazy"), indexed, trimmed),
            row(utf8("héllo"), 1, "  x  "),
            row(utf8("say \"\""), 0, " "),
            row(null, null, null));

    assertEquals(
        hexLines(
            "22 68c3a96c6c6f 22 3b 22 c3a9c3a96e 22 3b 22 78 22",
            "22 736179 20 22222222 22 3b 22 7a65726f 22 3b 22 22",
            "3b 3b"),
        copy.hex());
  }

  // -------------------------------------------------------------------------------------------
  // Numbers and other types
  // -------------------------------------------------------------------------------------------

  @Test
  void integers() throws Exception {
    ValueMetaInteger masked = new ValueMetaInteger("masked");
    masked.setConversionMask("#,##0.00");

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaInteger("i"), masked, lazy(new ValueMetaInteger("lazy"), "#")),
            row(0L, 0L, utf8("0042")),
            row(-1L, 1234567L, utf8("-7")),
            row(42L, -1234567L, utf8(" 1 000 ")),
            row(Long.MIN_VALUE, Long.MAX_VALUE, utf8("x")),
            row(Long.MAX_VALUE, Long.MIN_VALUE, utf8("")),
            row(null, null, null));

    assertEquals(
        lines(
            "0;0;0042",
            "-1;1234567;-7",
            "42;-1234567; 1 000 ",
            "-9223372036854775808;9223372036854775807;x",
            "9223372036854775807;-9223372036854775808;",
            ";;"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  @Test
  void numbersHaveTheDigitsOfDoubleToString() throws Exception {
    ValueMetaNumber masked = new ValueMetaNumber("masked");
    masked.setConversionMask("0.00");
    masked.setDecimalSymbol(",");

    double[] values = {
      0.0,
      -0.0,
      0.1,
      -0.1,
      1.0 / 3,
      2.0 / 3,
      100.0,
      1.0E7,
      9999999.0,
      1.0E20,
      1.0E-7,
      0.001,
      9.999E-4,
      123456.789,
      262144.0,
      262143.99999,
      12345678.9,
      0.30000000000000004,
      1.0E-300,
      Double.MIN_VALUE,
      Double.MAX_VALUE,
      Double.NaN,
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY
    };
    Object[][] rows = new Object[values.length + 1][];
    for (int i = 0; i < values.length; i++) {
      rows[i] = row(values[i], values[i]);
    }
    rows[values.length] = row(null, null);

    Copy copy = load("UTF8", rowMeta(new ValueMetaNumber("n"), masked), rows);

    assertEquals(
        lines(
            "0.0;0.0",
            "-0.0;-0.0",
            "0.1;0.1",
            "-0.1;-0.1",
            "0.3333333333333333;0.3333333333333333",
            "0.6666666666666666;0.6666666666666666",
            "100.0;100.0",
            "1.0E7;1.0E7",
            "9999999.0;9999999.0",
            "1.0E20;1.0E20",
            "1.0E-7;1.0E-7",
            "0.001;0.001",
            "9.999E-4;9.999E-4",
            "123456.789;123456.789",
            "262144.0;262144.0",
            "262143.99999;262143.99999",
            "1.23456789E7;1.23456789E7",
            "0.30000000000000004;0.30000000000000004",
            "1.0E-300;1.0E-300",
            "4.9E-324;4.9E-324",
            "1.7976931348623157E308;1.7976931348623157E308",
            "NaN;NaN",
            "Infinity;Infinity",
            "-Infinity;-Infinity",
            ";"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  /**
   * Lazy numbers with a plain mask and a decimal point are copied as they were read, unless they
   * hold the grouping symbol: then Hop reads them, and 1,5 is no number for the mask #.# (issue
   * 9678; PostgreSQL used to reject it instead).
   */
  @Test
  void lazyNumbersAreCopiedAsTheyWereReadUnlessTheyHoldTheGroupingSymbol() throws Exception {
    IRowMeta rowMeta =
        rowMeta(lazy(new ValueMetaNumber("n"), "#.#"), lazy(new ValueMetaBigNumber("b"), "#.#"));

    Copy copy = load("UTF8", rowMeta, row(utf8("-0"), utf8("1e3")), row(null, null));
    Copy grouped = load("UTF8", rowMeta, row(utf8("1,5"), utf8("12,50")));

    assertEquals(lines("-0;1e3", ";"), copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
    assertEquals(1, grouped.errors);
  }

  @Test
  void bigNumbersAreWrittenWithBigDecimalToString() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaBigNumber("b")),
            row(new BigDecimal("0")),
            row(new BigDecimal("0.000")),
            row(new BigDecimal("-0.000")),
            row(new BigDecimal("1E+3")),
            row(new BigDecimal("1000")),
            row(new BigDecimal("123456789012345678901234567890.123456789")),
            row(new BigDecimal("0.00000001")),
            row(new BigDecimal("-12.50")),
            row(BigDecimal.valueOf(1.1)),
            row(new BigDecimal(0.1)),
            row((Object) null));

    assertEquals(
        lines(
            "0",
            "0.000",
            "0.000",
            "1E+3",
            "1000",
            "123456789012345678901234567890.123456789",
            "1E-8",
            "-12.50",
            "1.1",
            "0.1000000000000000055511151231257827021181583404541015625",
            ""),
        copy.text(StandardCharsets.UTF_8));
  }

  @Test
  void booleans() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaBoolean("b"), lazy(new ValueMetaBoolean("lazy"), null)),
            row(true, utf8("Y")),
            row(false, utf8("N")),
            row(null, null));

    assertEquals(lines("t;t", "f;f", ";"), copy.text(StandardCharsets.UTF_8));
  }

  @Test
  void binaryIsWrittenAsByteaHex() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaBinary("bin"), new ValueMetaInteger("n")),
            row(new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}, 1L),
            row(new byte[0], 2L),
            row(new byte[] {0x00, (byte) 0xff, 0x22, 0x3b}, 3L),
            row(null, 4L));

    assertEquals(
        lines("\\xdeadbeef;1", "\\x;2", "\\x00ff223b;3", ";4"), copy.text(StandardCharsets.UTF_8));
  }

  /** A stand-in for the UUID value type plugin, which is not on this module's class path. */
  @ValueMetaPlugin(id = "32", name = "UUID", description = "UUID stand-in for the tests")
  public static class TestUuidValueMeta extends ValueMetaBase {
    public TestUuidValueMeta() {
      super(null, 32);
    }

    public TestUuidValueMeta(String name) {
      super(name, 32);
    }

    @Override
    public String getString(Object object) throws HopValueException {
      if (object == null) {
        return null;
      }
      if (isStorageBinaryString()) {
        return new String((byte[]) object, StandardCharsets.UTF_8);
      }
      return object.toString();
    }
  }

  @Test
  void uuids() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            TestUuidValueMeta.class.getName(), ValueMetaPluginType.class, ValueMetaPlugin.class);
    TestUuidValueMeta lazyUuid = new TestUuidValueMeta("lazy");
    lazyUuid.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazyUuid.setStorageMetadata(new ValueMetaString("lazy"));

    Copy copy =
        load(
            "LATIN1",
            rowMeta(new TestUuidValueMeta("u"), lazyUuid),
            row(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), utf8("not-a-uuid-é")),
            row(null, null));

    assertEquals(
        lines("123e4567-e89b-12d3-a456-426614174000;not-a-uuid-é", ";"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  @Test
  void anUnsupportedTypeStopsTheLoad() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaInteger("n"), new ValueMetaInternetAddress("ip")),
            row(1L, InetAddress.getByAddress(new byte[] {127, 0, 0, 1})),
            row(2L, null));

    assertEquals(1, copy.errors);
    assertEquals("", copy.hex());
    assertEquals(0, copy.forwardedRows.size());
  }

  // -------------------------------------------------------------------------------------------
  // Dates and timestamps
  // -------------------------------------------------------------------------------------------

  /**
   * Every date mask mode, on dates with different masks, locales and time zones, for instants
   * around DST changes, the epoch, before 1900, before 1600, before the Gregorian change, BC and
   * after 9999. The JVM time zone is Europe/Brussels.
   */
  @Test
  void datesInEveryMaskMode() throws Exception {
    ValueMetaDate kathmandu = date("dKtm", "yyyy-MM-dd HH:mm:ss.SSS");
    kathmandu.setDateFormatTimeZone(TimeZone.getTimeZone("Asia/Kathmandu"));
    ValueMetaDate thai = date("dThai", "yyyy-MM-dd");
    thai.setDateFormatLocale(Locale.forLanguageTag("th-TH"));
    IRowMeta rowMeta =
        rowMeta(
            new ValueMetaDate("d"),
            date("dIso", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            date("dShort", "dd/MM/yy HH:mm"),
            date("dZone", "yyyy-MM-dd HH:mm:ss XXX"),
            date("dSingle", "y-M-d H:m:s.S"),
            kathmandu,
            thai,
            date("dNames", "EEE d MMM yyyy"));
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "d", ""),
            mapping("c2", "dIso", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH),
            mapping("c3", "dShort", ""),
            mapping("c4", "dZone", ""),
            mapping("c5", "dSingle", ""),
            mapping("c6", "dKtm", ""),
            mapping("c7", "dThai", ""),
            mapping("c8", "dNames", ""),
            mapping("c9", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c10", "d", "date"),
            mapping("c11", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c12", "dKtm", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c13", "d", "something else")));

    Date[] instants = {
      brussels("2024-03-05T07:08:09.010"),
      utc("2024-10-27T00:30:00Z"),
      utc("2024-10-27T01:30:00Z"),
      utc("2024-03-31T01:00:00Z"),
      utc("1970-01-01T00:00:00Z"),
      utc("1969-12-31T23:59:59.999Z"),
      brussels("2024-02-29T23:59:59.999"),
      utc("1850-06-15T12:00:00Z"),
      utc("1899-12-31T23:59:59.999Z"),
      utc("1599-12-31T23:00:00Z"),
      utc("1599-12-31T22:59:59.999Z"),
      utc("1582-10-15T00:00:00Z"),
      utc("1582-10-04T00:00:00Z"),
      utc("0001-01-01T00:00:00Z"),
      utc("-0001-06-01T00:00:00Z"),
      brussels("9999-12-31T23:59:59.999"),
      utc("+10000-01-01T00:00:00Z"),
      null
    };
    Object[][] rows = new Object[instants.length][];
    for (int i = 0; i < instants.length; i++) {
      Date d = instants[i];
      rows[i] = row(d, d, d, d, d, d, d, d);
    }

    Copy copy = load("UTF8", rowMeta, rows);

    assertEquals(
        lines(
            "2024/03/05 07:08:09.010|2024-03-05T07:08:09.010|05/03/24 07:08"
                + "|2024-03-05 07:08:09 +01:00|2024-3-5 7:8:9.10|2024-03-05 11:53:09.010"
                + "|2567-03-05|Tue 5 Mar 2024|2024-03-05|2024-03-05"
                + "|2024-03-05 07:08:09.010|2024-03-05 07:08:09.010"
                + "|2024/03/05 07:08:09.010",
            "2024/10/27 02:30:00.000|2024-10-27T02:30:00.000|27/10/24 02:30"
                + "|2024-10-27 02:30:00 +02:00|2024-10-27 2:30:0.0"
                + "|2024-10-27 06:15:00.000|2567-10-27|Sun 27 Oct 2024|2024-10-27"
                + "|2024-10-27|2024-10-27 02:30:00.000|2024-10-27 02:30:00.000"
                + "|2024/10/27 02:30:00.000",
            "2024/10/27 02:30:00.000|2024-10-27T02:30:00.000|27/10/24 02:30"
                + "|2024-10-27 02:30:00 +01:00|2024-10-27 2:30:0.0"
                + "|2024-10-27 07:15:00.000|2567-10-27|Sun 27 Oct 2024|2024-10-27"
                + "|2024-10-27|2024-10-27 02:30:00.000|2024-10-27 02:30:00.000"
                + "|2024/10/27 02:30:00.000",
            "2024/03/31 03:00:00.000|2024-03-31T03:00:00.000|31/03/24 03:00"
                + "|2024-03-31 03:00:00 +02:00|2024-3-31 3:0:0.0|2024-03-31 06:45:00.000"
                + "|2567-03-31|Sun 31 Mar 2024|2024-03-31|2024-03-31"
                + "|2024-03-31 03:00:00.000|2024-03-31 03:00:00.000"
                + "|2024/03/31 03:00:00.000",
            "1970/01/01 01:00:00.000|1970-01-01T01:00:00.000|01/01/70 01:00"
                + "|1970-01-01 01:00:00 +01:00|1970-1-1 1:0:0.0|1970-01-01 05:30:00.000"
                + "|2513-01-01|Thu 1 Jan 1970|1970-01-01|1970-01-01"
                + "|1970-01-01 01:00:00.000|1970-01-01 01:00:00.000"
                + "|1970/01/01 01:00:00.000",
            "1970/01/01 00:59:59.999|1970-01-01T00:59:59.999|01/01/70 00:59"
                + "|1970-01-01 00:59:59 +01:00|1970-1-1 0:59:59.999"
                + "|1970-01-01 05:29:59.999|2513-01-01|Thu 1 Jan 1970|1970-01-01"
                + "|1970-01-01|1970-01-01 00:59:59.999|1970-01-01 00:59:59.999"
                + "|1970/01/01 00:59:59.999",
            "2024/02/29 23:59:59.999|2024-02-29T23:59:59.999|29/02/24 23:59"
                + "|2024-02-29 23:59:59 +01:00|2024-2-29 23:59:59.999"
                + "|2024-03-01 04:44:59.999|2567-02-29|Thu 29 Feb 2024|2024-02-29"
                + "|2024-02-29|2024-02-29 23:59:59.999|2024-02-29 23:59:59.999"
                + "|2024/02/29 23:59:59.999",
            "1850/06/15 13:00:00.000|1850-06-15T13:00:00.000|15/06/50 13:00"
                + "|1850-06-15 13:00:00 +01:00|1850-6-15 13:0:0.0|1850-06-15 17:45:00.000"
                + "|2393-06-15|Sat 15 Jun 1850|1850-06-15|1850-06-15"
                + "|1850-06-15 13:00:00.000|1850-06-15 13:00:00.000"
                + "|1850/06/15 13:00:00.000",
            "1900/01/01 00:59:59.999|1900-01-01T00:59:59.999|01/01/00 00:59"
                + "|1900-01-01 00:59:59 +01:00|1900-1-1 0:59:59.999"
                + "|1900-01-01 05:44:59.999|2443-01-01|Mon 1 Jan 1900|1900-01-01"
                + "|1900-01-01|1900-01-01 00:59:59.999|1900-01-01 00:59:59.999"
                + "|1900/01/01 00:59:59.999",
            "1600/01/01 00:00:00.000|1600-01-01T00:00:00.000|01/01/00 00:00"
                + "|1600-01-01 00:00:00 +01:00|1600-1-1 0:0:0.0|1600-01-01 04:45:00.000"
                + "|2143-01-01|Sat 1 Jan 1600|1600-01-01|1600-01-01"
                + "|1600-01-01 00:00:00.000|1600-01-01 00:00:00.000"
                + "|1600/01/01 00:00:00.000",
            "1599/12/31 23:59:59.999|1599-12-31T23:59:59.999|31/12/99 23:59"
                + "|1599-12-31 23:59:59 +01:00|1599-12-31 23:59:59.999"
                + "|1600-01-01 04:44:59.999|2142-12-31|Fri 31 Dec 1599|1599-12-31"
                + "|1599-12-31|1599-12-31 23:59:59.999|1599-12-31 23:59:59.999"
                + "|1599/12/31 23:59:59.999",
            "1582/10/15 01:00:00.000|1582-10-15T01:00:00.000|15/10/82 01:00"
                + "|1582-10-15 01:00:00 +01:00|1582-10-15 1:0:0.0|1582-10-15 05:45:00.000"
                + "|2125-10-15|Fri 15 Oct 1582|1582-10-15|1582-10-15"
                + "|1582-10-15 01:00:00.000|1582-10-15 01:00:00.000"
                + "|1582/10/15 01:00:00.000",
            "1582/09/24 01:00:00.000|1582-09-24T01:00:00.000|24/09/82 01:00"
                + "|1582-09-24 01:00:00 +01:00|1582-9-24 1:0:0.0|1582-09-24 05:45:00.000"
                + "|2125-09-24|Mon 24 Sep 1582|1582-09-24|1582-09-24"
                + "|1582-09-24 01:00:00.000|1582-09-24 01:00:00.000"
                + "|1582/09/24 01:00:00.000",
            "0001/01/03 01:00:00.000|0001-01-03T01:00:00.000|03/01/01 01:00"
                + "|0001-01-03 01:00:00 +01:00|1-1-3 1:0:0.0|0001-01-03 05:45:00.000"
                + "|0544-01-03|Mon 3 Jan 0001|0001-01-03|0001-01-03"
                + "|0001-01-03 01:00:00.000|0001-01-03 01:00:00.000"
                + "|0001/01/03 01:00:00.000",
            "0002/06/03 01:00:00.000|0002-06-03T01:00:00.000|03/06/02 01:00"
                + "|0002-06-03 01:00:00 +01:00|2-6-3 1:0:0.0|0002-06-03 05:45:00.000"
                + "|0545-06-03|Tue 3 Jun 0002|0002-06-03|0002-06-03"
                + "|0002-06-03 01:00:00.000|0002-06-03 01:00:00.000"
                + "|0002/06/03 01:00:00.000",
            "9999/12/31 23:59:59.999|9999-12-31T23:59:59.999|31/12/99 23:59"
                + "|9999-12-31 23:59:59 +01:00|9999-12-31 23:59:59.999"
                + "|10000-01-01 04:44:59.999|10542-12-31|Fri 31 Dec 9999|9999-12-31"
                + "|9999-12-31|9999-12-31 23:59:59.999|9999-12-31 23:59:59.999"
                + "|9999/12/31 23:59:59.999",
            "10000/01/01 01:00:00.000|10000-01-01T01:00:00.000|01/01/00 01:00"
                + "|10000-01-01 01:00:00 +01:00|10000-1-1 1:0:0.0"
                + "|10000-01-01 05:45:00.000|10543-01-01|Sat 1 Jan 10000|10000-01-01"
                + "|10000-01-01|10000-01-01 01:00:00.000|10000-01-01 01:00:00.000"
                + "|10000/01/01 01:00:00.000",
            "||||||||||||"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  @Test
  void timestampsInEveryMaskMode() throws Exception {
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "ts", ""),
            mapping("c2", "ts", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c3", "ts", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c4", "tsMask", ""),
            mapping("c5", "dateHoldingTimestamp", ""),
            mapping("c6", "dateHoldingTimestamp", PGBulkLoaderMeta.DATE_MASK_DATETIME)));
    ValueMetaTimestamp masked = new ValueMetaTimestamp("tsMask");
    masked.setConversionMask("yyyy-MM-dd HH:mm:ss.SSSSSS");

    Timestamp nanos = Timestamp.valueOf("2024-03-05 07:08:09.123456789");
    Timestamp beforeNineteenHundred = Timestamp.valueOf("1899-12-31 23:59:59.999999999");
    Timestamp sixteenHundred = Timestamp.valueOf("1600-01-01 00:00:00");
    Timestamp lastOfNineNineNineNine = Timestamp.valueOf("9999-12-31 23:59:59.999999999");
    Timestamp wholeSecond = Timestamp.valueOf("2024-10-27 02:30:00");

    Copy copy =
        load(
            "UTF8",
            rowMeta(
                new ValueMetaTimestamp("ts"), masked, new ValueMetaDate("dateHoldingTimestamp")),
            row(nanos, nanos, nanos),
            row(beforeNineteenHundred, beforeNineteenHundred, beforeNineteenHundred),
            row(sixteenHundred, sixteenHundred, sixteenHundred),
            row(lastOfNineNineNineNine, lastOfNineNineNineNine, lastOfNineNineNineNine),
            row(wholeSecond, wholeSecond, wholeSecond),
            row(null, null, null));

    assertEquals(
        lines(
            "2024/03/05 07:08:09.123456789|2024-03-05|2024-03-05 07:08:09.123"
                + "|2024-03-05 07:08:09.123456|2024/03/05 07:08:09.123"
                + "|2024-03-05 07:08:09.123",
            "1899/12/31 23:59:59.999999999|1899-12-31|1899-12-31 23:59:59.999"
                + "|1899-12-31 23:59:59.999999|1899/12/31 23:59:59.999"
                + "|1899-12-31 23:59:59.999",
            "1600/01/01 00:00:00.000000000|1600-01-01|1600-01-01 00:00:00.000"
                + "|1600-01-01 00:00:00.000000|1600/01/01 00:00:00.000"
                + "|1600-01-01 00:00:00.000",
            "9999/12/31 23:59:59.999999999|9999-12-31|9999-12-31 23:59:59.999"
                + "|9999-12-31 23:59:59.999999|9999/12/31 23:59:59.999"
                + "|9999-12-31 23:59:59.999",
            "2024/10/27 02:30:00.000000000|2024-10-27|2024-10-27 02:30:00.000"
                + "|2024-10-27 02:30:00.000000|2024/10/27 02:30:00.000"
                + "|2024-10-27 02:30:00.000",
            "|||||"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
  }

  @Test
  void lazyDatesArePassedThroughOrConverted() throws Exception {
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "d", ""),
            mapping("c2", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c3", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c4", "ts", ""),
            mapping("c5", "ts", PGBulkLoaderMeta.DATE_MASK_DATETIME)));

    Copy copy =
        load(
            "UTF8",
            rowMeta(
                lazy(new ValueMetaDate("d"), "yyyyMMdd"),
                lazy(new ValueMetaTimestamp("ts"), "yyyy-MM-dd HH:mm:ss")),
            row(utf8("20240305"), utf8("2024-03-05 07:08:09")),
            row(utf8("18991231"), utf8("1899-12-31 23:59:59")),
            row(null, null));

    assertEquals(
        lines(
            "20240305|2024-03-05|2024-03-05 00:00:00.000|2024-03-05 07:08:09"
                + "|2024-03-05 07:08:09.000",
            "18991231|1899-12-31|1899-12-31 00:00:00.000|1899-12-31 23:59:59"
                + "|1899-12-31 23:59:59.000",
            "||||"),
        copy.text(StandardCharsets.UTF_8));
  }

  /** DATE and DATETIME follow the JVM time zone, also when it has odd offsets. */
  @Test
  void datesInOtherJvmTimeZones() throws Exception {
    String[] zones = {
      "UTC", "America/St_Johns", "Asia/Kathmandu", "Pacific/Chatham", "America/Sao_Paulo"
    };
    Date[] instants = {
      utc("1970-01-01T00:00:00Z"),
      utc("2024-07-01T12:00:00Z"),
      utc("2024-01-01T12:00:00Z"),
      utc("1850-01-01T00:00:00Z"),
      utc("1650-05-05T05:05:05.005Z")
    };
    StringBuilder actual = new StringBuilder();
    for (String zone : zones) {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      meta.setDelimiter("|");
      meta.setMappings(
          List.of(
              mapping("c1", "d", ""),
              mapping("c2", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
              mapping("c3", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME)));
      Object[][] rows = new Object[instants.length][];
      for (int i = 0; i < instants.length; i++) {
        rows[i] = row(instants[i]);
      }
      Copy copy = load("UTF8", rowMeta(new ValueMetaDate("d")), rows);
      actual.append(zone).append(Const.CR).append(copy.text(StandardCharsets.UTF_8));
    }

    assertEquals(
        lines(
            "UTC",
            "1970/01/01 00:00:00.000|1970-01-01|1970-01-01 00:00:00.000",
            "2024/07/01 12:00:00.000|2024-07-01|2024-07-01 12:00:00.000",
            "2024/01/01 12:00:00.000|2024-01-01|2024-01-01 12:00:00.000",
            "1850/01/01 00:00:00.000|1850-01-01|1850-01-01 00:00:00.000",
            "1650/05/05 05:05:05.005|1650-05-05|1650-05-05 05:05:05.005",
            "America/St_Johns",
            "1969/12/31 20:30:00.000|1969-12-31|1969-12-31 20:30:00.000",
            "2024/07/01 09:30:00.000|2024-07-01|2024-07-01 09:30:00.000",
            "2024/01/01 08:30:00.000|2024-01-01|2024-01-01 08:30:00.000",
            "1849/12/31 20:30:00.000|1849-12-31|1849-12-31 20:30:00.000",
            "1650/05/05 01:35:05.005|1650-05-05|1650-05-05 01:35:05.005",
            "Asia/Kathmandu",
            "1970/01/01 05:30:00.000|1970-01-01|1970-01-01 05:30:00.000",
            "2024/07/01 17:45:00.000|2024-07-01|2024-07-01 17:45:00.000",
            "2024/01/01 17:45:00.000|2024-01-01|2024-01-01 17:45:00.000",
            "1850/01/01 05:45:00.000|1850-01-01|1850-01-01 05:45:00.000",
            "1650/05/05 10:50:05.005|1650-05-05|1650-05-05 10:50:05.005",
            "Pacific/Chatham",
            "1970/01/01 12:45:00.000|1970-01-01|1970-01-01 12:45:00.000",
            "2024/07/02 00:45:00.000|2024-07-02|2024-07-02 00:45:00.000",
            "2024/01/02 01:45:00.000|2024-01-02|2024-01-02 01:45:00.000",
            "1850/01/01 12:45:00.000|1850-01-01|1850-01-01 12:45:00.000",
            "1650/05/05 17:50:05.005|1650-05-05|1650-05-05 17:50:05.005",
            "America/Sao_Paulo",
            "1969/12/31 21:00:00.000|1969-12-31|1969-12-31 21:00:00.000",
            "2024/07/01 09:00:00.000|2024-07-01|2024-07-01 09:00:00.000",
            "2024/01/01 09:00:00.000|2024-01-01|2024-01-01 09:00:00.000",
            "1849/12/31 21:00:00.000|1849-12-31|1849-12-31 21:00:00.000",
            "1650/05/05 02:05:05.005|1650-05-05|1650-05-05 02:05:05.005"),
        actual.toString());
  }

  // -------------------------------------------------------------------------------------------
  // Field mapping, row flow and volume
  // -------------------------------------------------------------------------------------------

  @Test
  void streamFieldsAreMappedByNameInMappingOrder() throws Exception {
    meta.setMappings(
        List.of(
            mapping("col_d", "d", ""),
            mapping("col_a", "A", ""),
            mapping("col_b", "b", ""),
            mapping("col_b_again", "b", "")));
    IRowMeta rowMeta =
        rowMeta(
            new ValueMetaString("a"),
            new ValueMetaInteger("b"),
            new ValueMetaNumber("c"),
            new ValueMetaString("d"));
    Object[] first = row("a1", 1L, 1.5, "d1");
    Object[] second = row(null, null, 2.5, "d2");

    Copy copy = load("UTF8", rowMeta, first, second);

    assertEquals(
        "COPY \"public\".target ( col_d, col_a, col_b, col_b_again )  FROM STDIN WITH (FORMAT"
            + " csv, DELIMITER ';', QUOTE '\"', ENCODING 'UTF8');"
            + Const.CR,
        copy.command);
    assertEquals(lines("\"d1\";\"a1\";1;1", "\"d2\";;;"), copy.text(StandardCharsets.UTF_8));
    // the input rows are passed on unchanged
    assertEquals(2, copy.linesOutput);
    assertEquals(2, copy.forwardedRows.size());
    assertSame(first, copy.forwardedRows.get(0));
    assertSame(second, copy.forwardedRows.get(1));
    assertSame(rowMeta, copy.forwardedMetas.get(0));
    assertSame(rowMeta, copy.forwardedMetas.get(1));
  }

  /**
   * Input fields whose names were made unique, or renamed after they were added to the row, are
   * found by their current name.
   */
  @Test
  void renamedAndDeduplicatedStreamFields() throws Exception {
    IRowMeta rowMeta = rowMeta(new ValueMetaString("x"), new ValueMetaInteger("y"));
    rowMeta.getValueMeta(0).setName("dup");
    rowMeta.addValueMeta(new ValueMetaString("dup"));
    rowMeta.addValueMeta(new ValueMetaString("DUP"));
    meta.setMappings(
        List.of(
            mapping("c1", "dup_2", ""),
            mapping("c2", "dup", ""),
            mapping("c3", "y", ""),
            mapping("c4", "Dup_1", "")));

    Copy copy = load("UTF8", rowMeta, row("first", 1L, "second", "third"));

    assertEquals("dup,y,dup_1,DUP_2", String.join(",", rowMeta.getFieldNames()));
    assertEquals(lines("\"third\";\"first\";1;\"second\""), copy.text(StandardCharsets.UTF_8));
  }

  /**
   * The DATE and DATETIME modes format with the JVM's format locale. With a Thai default locale
   * that is the Buddhist calendar (and here Thai digits); a date field's own locale can bring other
   * digits or calendars. Numbers do not follow the locale.
   *
   * <p>The DATE and DATETIME modes (c2, c3) use the Gregorian calendar and ASCII digits whatever
   * the regional settings (issue 9678). PASS THROUGH (c1, c4 to c6) follows the field's own format
   * by design.
   */
  @Test
  void datesWithOtherCalendarsAndDigits() throws Exception {
    Locale.setDefault(Locale.forLanguageTag("th-TH-u-nu-thai"));
    ValueMetaDate arabic = date("dArab", "yyyy-MM-dd HH:mm");
    arabic.setDateFormatLocale(Locale.forLanguageTag("ar-EG"));
    ValueMetaDate japanese = date("dJapan", "yyyy-MM-dd");
    japanese.setDateFormatLocale(Locale.forLanguageTag("ja-JP-u-ca-japanese"));
    ValueMetaDate us = date("dUs", "yyyy-MM-dd HH:mm:ss.SSS");
    us.setDateFormatLocale(Locale.US);
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "d", ""),
            mapping("c2", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c3", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c4", "dArab", ""),
            mapping("c5", "dJapan", ""),
            mapping("c6", "dUs", ""),
            mapping("c7", "n", "")));

    Date date = brussels("2024-03-05T07:08:09.010");
    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaDate("d"), arabic, japanese, us, new ValueMetaNumber("n")),
            row(date, date, date, date, 1.5),
            row(null, null, null, null, null));

    // Thai digits and Buddhist years in PASS THROUGH but not in the DATE and DATETIME modes,
    // Arabic-Indic digits, a Japanese era year without its era
    assertEquals(
        lines(
            "๒๕๖๗/๐๓/๐๕ ๐๗:๐๘:๐๙.๐๑๐|2024-03-05|2024-03-05 07:08:09.010"
                + "|٢٠٢٤-٠٣-٠٥ ٠٧:٠٨|6-03-05|2024-03-05 07:08:09.010|1.5",
            "||||||"),
        copy.text(StandardCharsets.UTF_8));
    assertNotNull(copy.data.dateFormatters[1]);
    assertNotNull(copy.data.dateFormatters[2]);
  }

  @Test
  void aMappedFieldMissingFromTheStreamStopsTheLoad() throws Exception {
    meta.setMappings(List.of(mapping("a", "a", ""), mapping("x", "nope", "")));

    Copy copy = load("UTF8", rowMeta(new ValueMetaString("a")), row("a1"), row("a2"));

    assertEquals(1, copy.errors);
    assertEquals("", copy.hex());
    assertEquals(0, copy.forwardedRows.size());
  }

  /** Rows larger than the copy stream's 64 KiB buffer and many rows that fill it several times. */
  @Test
  void largeValuesAndManyRows() throws Exception {
    String large = "x".repeat(70_000);
    String largeNonAscii = "é".repeat(40_000);
    List<Object[]> rows = new ArrayList<>();
    rows.add(row("small", 0L));
    rows.add(row(large, 1L));
    rows.add(row(largeNonAscii, 2L));
    StringBuilder expected = new StringBuilder();
    expected.append(lines("\"small\";0", "\"" + large + "\";1", "\"" + largeNonAscii + "\";2"));
    for (long i = 3; i < 5_000; i++) {
      rows.add(row("row \"" + i + "\" é", i));
      expected.append(lines("\"row \"\"" + i + "\"\" é\";" + i));
    }

    Copy copy =
        load(
            "UTF8",
            rowMeta(new ValueMetaString("s"), new ValueMetaInteger("n")),
            rows.toArray(new Object[0][]));

    assertEquals(expected.toString(), copy.text(StandardCharsets.UTF_8));
    assertEquals(5_000, copy.linesOutput);
    assertEquals(0, copy.errors);
  }

  // -------------------------------------------------------------------------------------------
  // Regional settings and lazy conversion (issue 9678)
  // -------------------------------------------------------------------------------------------

  /** Maps d, ts, dIso and dNames in every date mode; the date fields are made under the locale. */
  private Copy loadDatesUnder(Locale formatLocale) throws Exception {
    Locale.setDefault(formatLocale);
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c2", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c3", "d", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH),
            mapping("c4", "ts", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c5", "ts", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c6", "ts", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH),
            mapping("c7", "dIso", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH),
            mapping("c8", "dNames", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH)));
    Date date = brussels("2024-03-05T07:08:09.010");
    Timestamp timestamp = Timestamp.valueOf("2024-10-27 02:30:00.123456789");
    return load(
        "UTF8",
        rowMeta(
            new ValueMetaDate("d"),
            new ValueMetaTimestamp("ts"),
            date("dIso", "yyyy-MM-dd HH:mm:ss"),
            date("dNames", "EEE d MMMM yyyy")),
        row(date, timestamp, date, date),
        row(null, null, null, null));
  }

  /** The DATE and DATETIME modes are formatted without SimpleDateFormat. */
  private static void assertTheDateModesUseTheFastFormatter(Copy copy) {
    assertNotNull(copy.data.dateFormatters[0], "DATE");
    assertNotNull(copy.data.dateFormatters[1], "DATETIME");
    assertNotNull(copy.data.dateFormatters[3], "DATE on a timestamp");
    assertNotNull(copy.data.dateFormatters[4], "DATETIME on a timestamp");
  }

  @Test
  void everyDateModeInAnAmericanFormatLocale() throws Exception {
    Copy copy = loadDatesUnder(Locale.US);

    assertEquals(
        lines(
            "2024-03-05|2024-03-05 07:08:09.010|2024/03/05 07:08:09.010"
                + "|2024-10-27|2024-10-27 02:30:00.123|2024/10/27 02:30:00.123456789"
                + "|2024-03-05 07:08:09|Tue 5 March 2024",
            "|||||||"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
    assertTheDateModesUseTheFastFormatter(copy);
  }

  /**
   * A German format locale changes nothing but the names a field's own mask asks for: PASS THROUGH
   * keeps following the field's format.
   */
  @Test
  void everyDateModeInAGermanFormatLocale() throws Exception {
    Copy copy = loadDatesUnder(Locale.GERMANY);

    assertEquals(
        lines(
            "2024-03-05|2024-03-05 07:08:09.010|2024/03/05 07:08:09.010"
                + "|2024-10-27|2024-10-27 02:30:00.123|2024/10/27 02:30:00.123456789"
                + "|2024-03-05 07:08:09|Di. 5 März 2024",
            "|||||||"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
    assertTheDateModesUseTheFastFormatter(copy);
  }

  /**
   * Only the FORMAT category of the default locale is Thai (the GUI language stays English).
   *
   * <p>The DATE and DATETIME modes write the Gregorian year with the fast formatter; they used to
   * write the Buddhist year (2567 for 2024), which PostgreSQL stored without complaint (issue
   * 9678). PASS THROUGH follows the field's own format by design.
   */
  @Test
  void dateModesIgnoreAThaiFormatLocale() throws Exception {
    Locale.setDefault(Locale.US);
    Locale.setDefault(Locale.Category.FORMAT, Locale.forLanguageTag("th-TH"));
    meta.setDelimiter("|");
    meta.setMappings(
        List.of(
            mapping("c1", "d", PGBulkLoaderMeta.DATE_MASK_DATE),
            mapping("c2", "d", PGBulkLoaderMeta.DATE_MASK_DATETIME),
            mapping("c3", "d", PGBulkLoaderMeta.DATE_MASK_PASS_THROUGH)));
    Date date = brussels("2024-03-05T07:08:09.010");

    Copy copy = load("UTF8", rowMeta(new ValueMetaDate("d")), row(date), row((Object) null));

    assertEquals(
        lines("2024-03-05|2024-03-05 07:08:09.010|2567/03/05 07:08:09.010", "||"),
        copy.text(StandardCharsets.UTF_8));
    assertNotNull(copy.data.dateFormatters[0]);
    assertNotNull(copy.data.dateFormatters[1]);
  }

  /** Lazy numbers whose text PostgreSQL reads as Hop does: the bytes go out as they came in. */
  @Test
  void lazyPlainNumbersAreCopiedAsTheyWereRead() throws Exception {
    ValueMetaNumber number = spy(lazy(new ValueMetaNumber("n"), "#.##"));
    ValueMetaNumber noMask = lazy(new ValueMetaNumber("noMask"), null);
    ValueMetaInteger integer = spy(lazy(new ValueMetaInteger("i"), null));
    ValueMetaBigNumber big = lazy(new ValueMetaBigNumber("b"), "0.000");
    ValueMetaInteger latin1 = lazy(new ValueMetaInteger("latin1"), "#");
    ((ValueMetaString) latin1.getStorageMetadata()).setStringEncoding("ISO-8859-1");
    ValueMetaNumber exponent = lazy(new ValueMetaNumber("e"), "0.###E0");

    Copy copy =
        load(
            "UTF8",
            rowMeta(number, noMask, integer, big, latin1, exponent),
            row(
                utf8("1.50"),
                utf8("-0.10"),
                utf8("007"),
                utf8("123456789012345678901234567890.123456789"),
                "42".getBytes(StandardCharsets.ISO_8859_1),
                utf8("1.5E3")),
            row(utf8("0"), utf8("2"), utf8("-9223372036854775808"), utf8("1"), null, null),
            row(null, null, null, null, null, null));

    assertEquals(
        lines(
            "1.50;-0.10;007;123456789012345678901234567890.123456789;42;1.5E3",
            "0;2;-9223372036854775808;1;;",
            ";;;;;"),
        copy.text(StandardCharsets.UTF_8));
    assertEquals(0, copy.errors);
    // no conversion: the raw bytes are copied
    verify(number, never()).getNumber(any());
    verify(integer, never()).getInteger(any());
  }

  /** A lazy number read with a German decimal comma and grouping dot. */
  private static <T extends IValueMeta> T german(T valueMeta, String mask) {
    T lazy = lazy(valueMeta, mask);
    IValueMeta storage = lazy.getStorageMetadata();
    storage.setDecimalSymbol(",");
    storage.setGroupingSymbol(".");
    return lazy;
  }

  /**
   * Lazy numbers read with a decimal comma are converted and written as numbers in normal storage
   * are. They used to be copied as the input file had them: PostgreSQL rejected 1.234,50 and read
   * 1.234 as one point two three four, where Hop reads one thousand two hundred thirty-four (issue
   * 9678).
   */
  @Test
  void lazyNumbersWithADecimalCommaAreConverted() throws Exception {
    Copy copy =
        load(
            "UTF8",
            rowMeta(
                german(new ValueMetaNumber("n"), "#,##0.00"),
                german(new ValueMetaBigNumber("b"), "#,##0.00"),
                german(new ValueMetaInteger("i"), "#,##0")),
            row(utf8("1.234,50"), utf8("1.234,50"), utf8("1.234")),
            row(utf8("1.234"), utf8("-0,5"), utf8("7")),
            row(null, null, null));

    assertEquals(
        lines("1234.5;1234.50;1234", "1234.0;-0.5;7", ";;"), copy.text(StandardCharsets.UTF_8));
  }

  /**
   * Text holding the grouping symbol, or stored in an encoding that does not keep ASCII digits as
   * ASCII, is converted. It used to be copied as it was read (issue 9678).
   */
  @Test
  void lazyNumbersWithGroupingOrInUtf16AreConverted() throws Exception {
    ValueMetaInteger grouped = lazy(new ValueMetaInteger("grouped"), "#,##0");
    ValueMetaInteger utf16 = lazy(new ValueMetaInteger("utf16"), null);
    ((ValueMetaString) utf16.getStorageMetadata()).setStringEncoding("UTF-16BE");

    Copy copy =
        load(
            "UTF8",
            rowMeta(grouped, utf16),
            row(utf8("1,234,567"), "42".getBytes(StandardCharsets.UTF_16BE)),
            row(utf8("1234"), "-7".getBytes(StandardCharsets.UTF_16BE)));

    assertEquals(lines("1234567;42", "1234;-7"), copy.text(StandardCharsets.UTF_8));
  }
}
