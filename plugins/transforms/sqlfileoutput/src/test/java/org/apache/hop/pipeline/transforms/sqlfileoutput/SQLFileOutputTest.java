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

package org.apache.hop.pipeline.transforms.sqlfileoutput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.database.BaseDatabaseMeta;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SqlInsertTemplate;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Pins the exact file the SQL File Output transform writes. */
class SQLFileOutputTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  /** A dialect with a few reserved words, so quoting shows up in the statements. */
  public static class TestDialect extends BaseDatabaseMeta {
    @Override
    public String[] getReservedWords() {
      return new String[] {"ORDER", "USER", "SELECT"};
    }

    @Override
    public String getFieldDefinition(
        IValueMeta v,
        String tk,
        String pk,
        boolean useAutoIncrement,
        boolean addFieldName,
        boolean addCr) {
      return "";
    }

    @Override
    public String getDriverClass() {
      return "";
    }

    @Override
    public String getURL(String hostname, String port, String databaseName) {
      return "";
    }

    @Override
    public String getAddColumnStatement(
        String tableName,
        IValueMeta v,
        String tk,
        boolean useAutoIncrement,
        String pk,
        boolean semicolon) {
      return "";
    }

    @Override
    public String getModifyColumnStatement(
        String tableName,
        IValueMeta v,
        String tk,
        boolean useAutoIncrement,
        String pk,
        boolean semicolon) {
      return "";
    }

    @Override
    public int[] getAccessTypeList() {
      return new int[] {DatabaseMeta.TYPE_ACCESS_NATIVE};
    }
  }

  private TransformMockHelper<SQLFileOutputMeta, SQLFileOutputData> helper;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "SQLFileOutputTest", SQLFileOutputMeta.class, SQLFileOutputData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);
    when(helper.pipeline.isRunning()).thenReturn(true);
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setName("test");
    databaseMeta.setIDatabase(new TestDialect());
    when(helper.pipelineMeta.findDatabase(anyString(), any(IVariables.class)))
        .thenReturn(databaseMeta);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void writesOneInsertPerRow(@TempDir Path tempDir) throws Exception {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();
    meta.setConnection("test");
    meta.setSchemaName("user");
    meta.setTableName("order");
    meta.setDateFormat("yyyy-MM-dd HH:mm:ss");
    meta.setStartNewLine(true);
    meta.getFile().setFileName(tempDir.resolve("out").toUri().toString());
    meta.getFile().setExtension("sql");

    SQLFileOutputData data = new SQLFileOutputData();
    SQLFileOutput transform =
        new SQLFileOutput(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("select"));
    IValueMeta amount = new ValueMetaNumber("unit price");
    amount.setConversionMask("0.00");
    amount.setDecimalSymbol(".");
    amount.setGroupingSymbol("");
    rowMeta.addValueMeta(amount);
    rowMeta.addValueMeta(new ValueMetaDate("created"));
    rowMeta.addValueMeta(new ValueMetaBoolean("active"));
    Date date = new GregorianCalendar(2024, 0, 15, 13, 45, 30).getTime();

    BlockingRowSet input = new BlockingRowSet(16);
    input.putRow(rowMeta, new Object[] {1L, "it's", 9.5, date, true});
    input.putRow(rowMeta, new Object[] {2L, null, null, null, false});
    input.putRow(rowMeta, new Object[] {3L, "plain", 0.125, date, null});
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(Collections.singletonList(input)));
    transform.setOutputRowSets(new ArrayList<>());

    assertTrue(transform.init());
    while (transform.processRow()) {
      // keep going
    }
    transform.dispose();

    String prefix =
        "INSERT INTO \"user\".\"order\"(id, \"select\", \"unit price\", created, active)";
    String cr = System.lineSeparator();
    assertEquals(
        prefix
            + " VALUES (1,'it''s',9.50,'2024-01-15 13:45:30','Y');"
            + cr
            + prefix
            + " VALUES (2,null,null,null,'N');"
            + cr
            + prefix
            + " VALUES (3,'plain',0.12,'2024-01-15 13:45:30',null);"
            + cr,
        Files.readString(tempDir.resolve("out.sql"), StandardCharsets.UTF_8));
  }

  /** The create and truncate options write their statements once, before the first insert. */
  @Test
  void writesCreateAndTruncateStatementsOnce(@TempDir Path tempDir) throws Exception {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {1L, "a"});
    rows.add(new Object[] {2L, "b"});
    String cr = System.lineSeparator();
    String inserts =
        "INSERT INTO orders_out(id, name) VALUES (1,'a');"
            + cr
            + "INSERT INTO orders_out(id, name) VALUES (2,'b');"
            + cr;
    // the test dialect has empty column definitions
    String create = "CREATE TABLE orders_out" + cr + "(" + cr + "  , )" + cr;
    String truncate = "TRUNCATE TABLE orders_out;" + cr + cr;

    assertEquals(inserts, run(tempDir, null, rowMeta, rows, false, false, false));
    assertEquals(
        create + ";" + cr + cr + inserts, run(tempDir, null, rowMeta, rows, true, false, false));
    assertEquals(truncate + inserts, run(tempDir, null, rowMeta, rows, false, true, false));
    assertEquals(
        create + ";" + cr + cr + truncate + inserts,
        run(tempDir, null, rowMeta, rows, true, true, false));
    // without inserts only together with the create statement
    assertEquals(
        create + ";" + cr + cr + truncate, run(tempDir, null, rowMeta, rows, true, true, true));
    assertEquals(truncate + inserts, run(tempDir, null, rowMeta, rows, false, true, true));
  }

  /** Runs rows through the transform into a file and returns the file's text. */
  private String run(Path tempDir, String dateFormat, RowMeta rowMeta, List<Object[]> rows)
      throws Exception {
    return run(tempDir, dateFormat, rowMeta, rows, false, false, false);
  }

  /** Runs rows through the transform into a file and returns the file's text. */
  private String run(
      Path tempDir,
      String dateFormat,
      RowMeta rowMeta,
      List<Object[]> rows,
      boolean createTable,
      boolean truncateTable,
      boolean noInserts)
      throws Exception {
    SQLFileOutputMeta meta = new SQLFileOutputMeta();
    meta.setDefault();
    meta.setCreateTable(createTable);
    meta.setTruncateTable(truncateTable);
    meta.setDoNotAddInsertStatements(noInserts);
    meta.setConnection("test");
    meta.setTableName("orders_out");
    meta.setDateFormat(dateFormat);
    meta.setStartNewLine(true);
    meta.setEncoding("UTF-8");
    meta.getFile().setFileName(tempDir.resolve("out").toUri().toString());
    meta.getFile().setExtension("sql");

    SQLFileOutputData data = new SQLFileOutputData();
    SQLFileOutput transform =
        new SQLFileOutput(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
    BlockingRowSet input = new BlockingRowSet(rows.size() + 1);
    for (Object[] row : rows) {
      input.putRow(rowMeta, row);
    }
    input.setDone();
    transform.setInputRowSets(new ArrayList<>(Collections.singletonList(input)));
    transform.setOutputRowSets(new ArrayList<>());

    assertTrue(transform.init());
    while (transform.processRow()) {
      // keep going
    }
    transform.dispose();
    return Files.readString(tempDir.resolve("out.sql"), StandardCharsets.UTF_8);
  }

  /** The column layout of the orders benchmark: no masks anywhere, as a CSV input leaves them. */
  private static RowMeta ordersRowMeta() {
    RowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("order_id"));
    rowMeta.addValueMeta(new ValueMetaInteger("quantity"));
    rowMeta.addValueMeta(new ValueMetaNumber("unit_price"));
    rowMeta.addValueMeta(new ValueMetaNumber("discount"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("total"));
    IValueMeta orderDate = new ValueMetaDate("order_date");
    orderDate.setConversionMask("yyyy-MM-dd");
    rowMeta.addValueMeta(orderDate);
    rowMeta.addValueMeta(new ValueMetaDate("shipped"));
    rowMeta.addValueMeta(new ValueMetaString("status"));
    return rowMeta;
  }

  private static Object[] orderRow(long id) {
    Date date = new Date(1_700_000_000_000L + id * 3_600_123L);
    return new Object[] {
      id,
      -id % 7,
      (id % 10_000) / 100.0 + 0.99,
      id % 3 == 0 ? 0.0 : -0.125 * (id % 5),
      new BigDecimal(id).movePointLeft(2),
      date,
      date,
      id % 2 == 0 ? "shipped" : "it's pending"
    };
  }

  /**
   * Values without masks - numbers in the default mask, dates in their own or the transform's
   * format - under two interface languages. The GUI sets its language as the DISPLAY locale only;
   * the FORMAT locale stays on the regional settings.
   */
  @Test
  void writesDefaultMasksTheSameInEveryLocale(@TempDir Path tempDir) throws Exception {
    Locale savedLocale = Locale.getDefault(Locale.Category.DISPLAY);
    TimeZone savedZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
      List<Object[]> rows = new ArrayList<>();
      rows.add(orderRow(1));
      rows.add(orderRow(4_567));
      rows.add(orderRow(1_000_003));
      rows.add(new Object[8]);
      String cr = System.lineSeparator();
      String prefix =
          "INSERT INTO orders_out(order_id, quantity, unit_price, discount, total, order_date,"
              + " shipped, status) VALUES (";
      for (Locale locale : new Locale[] {Locale.US, Locale.GERMANY}) {
        Locale.setDefault(Locale.Category.DISPLAY, locale);
        assertEquals(
            prefix
                + "1,-1,1.0,-0.125,0.01,'2023-11-15','2023/11/15 00:13:20.123','it''s pending');"
                + cr
                + prefix
                + "4567,-3,46.66,-0.25,45.67,'2024-05-23','2024/05/23 07:22:41.741','it''s pending');"
                + cr
                + prefix
                + "1000003,-4,1.02,-0.375,10000.03,'2137-12-15','2137/12/15 04:23:20.369','it''s pending');"
                + cr
                + prefix
                + "null,null,null,null,null,null,null,null);"
                + cr,
            run(tempDir, null, ordersRowMeta(), rows),
            "locale " + locale);
        assertEquals(
            prefix
                + "1,-1,1.0,-0.125,0.01,'15/11/2023 00:13','15/11/2023 00:13','it''s pending');"
                + cr
                + prefix
                + "4567,-3,46.66,-0.25,45.67,'23/05/2024 07:22','23/05/2024 07:22','it''s pending');"
                + cr
                + prefix
                + "1000003,-4,1.02,-0.375,10000.03,'15/12/2137 04:23','15/12/2137 04:23','it''s pending');"
                + cr
                + prefix
                + "null,null,null,null,null,null,null,null);"
                + cr,
            run(tempDir, "dd/MM/yyyy HH:mm", ordersRowMeta(), rows),
            "locale " + locale);
      }
    } finally {
      Locale.setDefault(Locale.Category.DISPLAY, savedLocale);
      TimeZone.setDefault(savedZone);
    }
  }

  /**
   * The regional format locale (#9677) does not reach the file either: under de_DE, sv_SE and th_TH
   * numbers keep a period and an ASCII minus sign, dates Gregorian years.
   */
  @Test
  void writesTheSameUnderEveryRegionalFormat(@TempDir Path tempDir) throws Exception {
    Locale savedLocale = Locale.getDefault();
    Locale savedFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
    Locale savedDisplayLocale = Locale.getDefault(Locale.Category.DISPLAY);
    TimeZone savedZone = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Europe/Brussels"));
      List<Object[]> rows = new ArrayList<>();
      rows.add(orderRow(1));
      rows.add(orderRow(4_567));
      rows.add(orderRow(1_000_003));
      rows.add(new Object[8]);
      Locale.setDefault(Locale.US);
      String plain = run(tempDir, null, ordersRowMeta(), rows);
      String formatted = run(tempDir, "dd/MM/yyyy HH:mm", ordersRowMeta(), rows);

      for (Locale locale :
          new Locale[] {
            Locale.GERMANY, Locale.forLanguageTag("sv-SE"), Locale.forLanguageTag("th-TH")
          }) {
        Locale.setDefault(locale);
        assertEquals(plain, run(tempDir, null, ordersRowMeta(), rows), "locale " + locale);
        assertEquals(
            formatted, run(tempDir, "dd/MM/yyyy HH:mm", ordersRowMeta(), rows), "locale " + locale);
      }

      Locale.setDefault(Locale.GERMANY);
      RowMeta rowMeta = new RowMeta();
      rowMeta.addValueMeta(new ValueMetaNumber("price"));
      IValueMeta total = new ValueMetaNumber("total");
      total.setConversionMask("#,##0.00");
      rowMeta.addValueMeta(total);
      IValueMeta shipped = new ValueMetaDate("shipped");
      shipped.setConversionMask("dd MMM yyyy");
      rowMeta.addValueMeta(shipped);
      Date date = new GregorianCalendar(2024, 2, 15, 13, 45, 30).getTime();
      List<Object[]> masked = new ArrayList<>();
      masked.add(new Object[] {1.5, -1234567.5, date});
      assertEquals(
          "INSERT INTO orders_out(price, total, shipped) VALUES (1.5,-1234567.50,'15 Mar 2024');"
              + System.lineSeparator(),
          run(tempDir, null, rowMeta, masked));
    } finally {
      Locale.setDefault(savedLocale);
      Locale.setDefault(Locale.Category.FORMAT, savedFormatLocale);
      Locale.setDefault(Locale.Category.DISPLAY, savedDisplayLocale);
      TimeZone.setDefault(savedZone);
    }
  }

  /**
   * Not a check: how long the per-row work of the transform takes for 200k rows - rendering the
   * statement and writing it through the transform's writer chain to a file - with the old (5000)
   * and the new output buffer. The transform harness itself is left out: its mocks record every
   * call and would dominate. Run with -Dhop.perf=true.
   */
  @Test
  @EnabledIfSystemProperty(named = "hop.perf", matches = "true")
  void timing(@TempDir Path tempDir) throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new TestDialect());
    Database db = new Database(mock(ILoggingObject.class), new Variables(), databaseMeta);
    RowMeta rowMeta = ordersRowMeta();
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 200_000; i++) {
      rows.add(orderRow(i));
    }
    for (int bufferSize : new int[] {5000, 65_536, 5000, 65_536}) {
      for (int round = 0; round < 4; round++) {
        SqlInsertTemplate template = db.getSqlOutputTemplate(null, "orders_out", rowMeta, null);
        StringBuilder statement = new StringBuilder();
        long start = System.nanoTime();
        try (Writer writer =
            new OutputStreamWriter(
                new BufferedOutputStream(
                    HopVfs.getOutputStream(tempDir.resolve("timing.sql").toString(), false),
                    bufferSize),
                StandardCharsets.UTF_8)) {
          for (Object[] r : rows) {
            statement.setLength(0);
            template.appendSql(statement, r);
            statement.append(';').append(System.lineSeparator());
            writer.write(statement.toString());
          }
        }
        long nanos = System.nanoTime() - start;
        System.out.printf(
            "SQL file output, buffer %d, 200k rows: %d ms, %.2f us/row%n",
            bufferSize, nanos / 1_000_000, nanos / 1000.0 / rows.size());
      }
    }
  }
}
