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

package org.apache.hop.pipeline.transforms.tableinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Table input with "Execute for each row" end to end against a real in-memory H2 database: the
 * output rows (values, Java classes and order) and the output row layout for every row, for the
 * parameter types, named parameters, specified fields, the row limit, variables and failing
 * queries.
 *
 * <p>The expected values are what the transform produces today. They are spelled out literally so
 * that a change to how the query is prepared, executed or read between input rows shows up here.
 */
class TableInputExecuteEachRowRegressionTest {

  private static final String CONNECTION_NAME = "table-input-each-row-regression";
  private static final String TRANSFORM_NAME = "table input";

  /** The layout of "SELECT ID, NAME, AMOUNT, F, D, TS, FLAG FROM T". */
  private static final String ALL_COLUMNS_LAYOUT =
      "ID:Integer(9,0)@table input"
          + " | NAME:String(20,-1)@table input"
          + " | AMOUNT:Number(10,2)@table input"
          + " | F:BigNumber(53,-1)@table input"
          + " | D:Date(-1,-1)@table input"
          + " | TS:Timestamp(6,-1)@table input"
          + " | FLAG:Boolean(-1,-1)@table input";

  private DatabaseMeta databaseMeta;
  private Database setupDb;
  private TableInputMeta meta;
  private TableInputData data;
  private TableInput tableInput;
  private boolean disposed;
  private final List<Object[]> output = new ArrayList<>();
  private final List<IRowMeta> outputMetas = new ArrayList<>();
  private final List<String> errorMessages = new ArrayList<>();

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    databaseMeta =
        new DatabaseMeta(
            CONNECTION_NAME,
            "H2",
            "Native",
            "",
            "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "",
            "",
            "");

    setupDb =
        new Database(
            new LoggingObject("TableInputExecuteEachRowRegressionTest"),
            new Variables(),
            databaseMeta);
    setupDb.connect();
    setupDb.execStatement(
        "CREATE TABLE T (ID INT, NAME VARCHAR(20), AMOUNT DECIMAL(10,2), F DOUBLE PRECISION,"
            + " D DATE, TS TIMESTAMP, FLAG BOOLEAN)");
    setupDb.execStatement(
        "INSERT INTO T VALUES"
            + " (1, 'one', 1.50, 0.1, DATE '2024-01-15', TIMESTAMP '2024-01-15 10:11:12.123',"
            + " TRUE),"
            + " (2, 'two', 2.25, 2.5, DATE '1899-12-31', TIMESTAMP '1899-12-31 23:59:59.999',"
            + " FALSE),"
            + " (2, 'two-bis', NULL, NULL, NULL, NULL, NULL),"
            + " (3, 'three', -3.00, -1.0E-7, DATE '9999-12-31',"
            + " TIMESTAMP '9999-12-31 23:59:59.999', TRUE),"
            + " (NULL, 'no-id', 0.00, 0, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00',"
            + " FALSE)");
    setupDb.execStatement("CREATE TABLE MANY (GRP INT, SEQ INT)");
    StringBuilder many = new StringBuilder("INSERT INTO MANY VALUES (8, 1)");
    for (int i = 1; i <= 250; i++) {
      many.append(", (7, ").append(i).append(')');
    }
    setupDb.execStatement(many.toString());

    meta = new TableInputMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION_NAME);
    meta.setExecuteEachInputRow(true);

    data = new TableInputData();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tableInput != null && !disposed) {
      tableInput.dispose();
    }
    setupDb.execStatement("DROP ALL OBJECTS");
    setupDb.disconnect();
  }

  // -------------------------------------------------------------------------------------------
  // Harness
  // -------------------------------------------------------------------------------------------

  /** Builds the transform with these input rows (and the end of input after them). */
  private TableInput createTransform(IRowMeta inputRowMeta, Object[]... inputRows)
      throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = new TransformMeta(TRANSFORM_NAME, meta);
    pipelineMeta.addTransform(transformMeta);

    TableInput transform =
        spy(
            new TableInput(
                transformMeta, meta, data, 0, pipelineMeta, spy(new LocalPipelineEngine())));
    doReturn(transformMeta).when(transform).getTransformMeta();
    doNothing().when(transform).logDetailed(any());
    doNothing().when(transform).logBasic(any());
    doAnswer(
            invocation -> {
              errorMessages.add(invocation.getArgument(0));
              return null;
            })
        .when(transform)
        .logError(anyString());
    doAnswer(
            invocation -> {
              outputMetas.add(invocation.getArgument(0));
              output.add(invocation.getArgument(1));
              return null;
            })
        .when(transform)
        .putRow(any(IRowMeta.class), any(Object[].class));

    if (inputRows.length == 0) {
      doReturn(null).when(transform).getRow();
    } else {
      // the rows after the first, then the end of input
      Object[] next = new Object[inputRows.length];
      for (int i = 1; i < inputRows.length; i++) {
        next[i - 1] = inputRows[i];
      }
      next[next.length - 1] = null;
      doReturn(inputRows[0], next).when(transform).getRow();
    }
    doReturn(inputRowMeta).when(transform).getInputRowMeta();
    return transform;
  }

  /** Runs init, processRow until it returns false, and dispose, like the pipeline does. */
  private void run(IRowMeta inputRowMeta, Object[]... inputRows) throws Exception {
    tableInput = createTransform(inputRowMeta, inputRows);
    assertTrue(tableInput.init(), "init() failed: " + errorMessages);
    try {
      while (tableInput.processRow()) {
        // keep reading
      }
    } finally {
      tableInput.dispose();
      disposed = true;
    }
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

  /** name:type(length,precision)@origin for every field, separated by " | ". */
  private static String layout(IRowMeta rowMeta) {
    List<String> fields = new ArrayList<>();
    for (IValueMeta valueMeta : rowMeta.getValueMetaList()) {
      fields.add(
          valueMeta.getName()
              + ":"
              + valueMeta.getTypeDesc()
              + "("
              + valueMeta.getLength()
              + ","
              + valueMeta.getPrecision()
              + ")@"
              + valueMeta.getOrigin());
    }
    return String.join(" | ", fields);
  }

  /** Every field of the row, rendered with its Java class, separated by " | ". */
  private static String values(IRowMeta rowMeta, Object[] row) {
    List<String> fields = new ArrayList<>();
    for (int i = 0; i < rowMeta.size(); i++) {
      fields.add(render(row[i]));
    }
    return String.join(" | ", fields);
  }

  private static String render(Object value) {
    if (value == null) {
      return "null";
    }
    String text;
    if (value instanceof Timestamp timestamp) {
      text =
          new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp)
              + "/"
              + timestamp.getNanos();
    } else if (value instanceof Date date) {
      text = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);
    } else if (value instanceof BigDecimal big) {
      text = big.toPlainString() + "/scale=" + big.scale();
    } else {
      text = value.toString();
    }
    return text + "[" + value.getClass().getSimpleName() + "]";
  }

  /** All output rows, one per line: the values. */
  private String outputValues() {
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < output.size(); i++) {
      lines.add(values(outputMetas.get(i), output.get(i)));
    }
    return String.join("\n", lines);
  }

  /** Asserts that every output row was handed over with this layout. */
  private void assertEveryLayout(String expectedLayout) {
    for (int i = 0; i < outputMetas.size(); i++) {
      assertEquals(expectedLayout, layout(outputMetas.get(i)), "layout of output row " + i);
    }
  }

  /** The first field of every output row, as text. */
  private List<String> firstColumn() {
    List<String> values = new ArrayList<>();
    for (Object[] row : output) {
      values.add(String.valueOf(row[0]));
    }
    return values;
  }

  private static Date localDate(String text) throws Exception {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(text);
  }

  // -------------------------------------------------------------------------------------------
  // Tests
  // -------------------------------------------------------------------------------------------

  @Test
  void eachInputRowIsQueriedWithItsOwnParameters() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row(2L), row(4L), row(3L));

    assertEquals(0, tableInput.getErrors());
    assertEquals(List.of("one", "two", "two-bis", "three"), firstColumn());
    assertEquals(4, output.size());
    assertEveryLayout("NAME:String(20,-1)@table input");
  }

  /**
   * Today H2's DECIMAL(10,2) is read as a Number (Double) and DOUBLE PRECISION as a BigNumber
   * (BigDecimal); DATE values arrive as java.sql.Timestamp objects.
   */
  @Test
  void allColumnTypesAndTheRowLayoutForEveryRow() throws Exception {
    meta.setSql("SELECT ID, NAME, AMOUNT, F, D, TS, FLAG FROM T WHERE ID = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row(2L), row(3L), row(1L));

    assertEquals(
        String.join(
            "\n",
            "1[Long] | one[String] | 1.5[Double] | 0.1/scale=1[BigDecimal]"
                + " | 2024-01-15 00:00:00.000/0[Timestamp]"
                + " | 2024-01-15 10:11:12.123/123000000[Timestamp] | true[Boolean]",
            "2[Long] | two[String] | 2.25[Double] | 2.5/scale=1[BigDecimal]"
                + " | 1899-12-31 00:00:00.000/0[Timestamp]"
                + " | 1899-12-31 23:59:59.999/999000000[Timestamp] | false[Boolean]",
            "2[Long] | two-bis[String] | null | null | null | null | null",
            "3[Long] | three[String] | -3.0[Double] | -0.00000010/scale=8[BigDecimal]"
                + " | 9999-12-31 00:00:00.000/0[Timestamp]"
                + " | 9999-12-31 23:59:59.999/999000000[Timestamp] | true[Boolean]",
            "1[Long] | one[String] | 1.5[Double] | 0.1/scale=1[BigDecimal]"
                + " | 2024-01-15 00:00:00.000/0[Timestamp]"
                + " | 2024-01-15 10:11:12.123/123000000[Timestamp] | true[Boolean]"),
        outputValues());
    assertEveryLayout(ALL_COLUMNS_LAYOUT);
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void aNullParameterIsBoundAsNull() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID IS NOT DISTINCT FROM ? ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row((Object) null), row(1L), row((Object) null));

    assertEquals(List.of("no-id", "one", "no-id"), firstColumn());
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void aNullParameterMatchesNothingWithEquals() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row((Object) null), row(3L));

    assertEquals(List.of("one", "three"), firstColumn());
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void inputRowsWithNoResultsAndWithManyResults() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");

    run(
        rowMeta(new ValueMetaInteger("grp")),
        row(99L),
        row(8L),
        row(99L),
        row(7L),
        row(99L),
        row(99L));

    assertEquals(251, output.size());
    assertEquals("8[Long] | 1[Long]", values(outputMetas.get(0), output.get(0)));
    for (int i = 1; i <= 250; i++) {
      assertEquals(
          "7[Long] | " + i + "[Long]", values(outputMetas.get(i), output.get(i)), "row " + i);
    }
    assertEveryLayout("GRP:Integer(9,0)@table input | SEQ:Integer(9,0)@table input");
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void noInputRowReturnsAnything() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");

    run(rowMeta(new ValueMetaInteger("grp")), row(99L), row(98L), row(97L));

    assertEquals(0, output.size());
    assertEquals(0, tableInput.getErrors());
    assertEquals(List.of(), errorMessages);
  }

  @Test
  void noInputRowsAtAll() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");

    run(rowMeta(new ValueMetaInteger("grp")));

    assertEquals(0, output.size());
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void theLinesInputCountAllRowsOfAllQueries() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");

    tableInput =
        createTransform(rowMeta(new ValueMetaInteger("grp")), row(8L), row(7L), row(99L), row(8L));
    assertTrue(tableInput.init());
    while (tableInput.processRow()) {
      // keep reading
    }
    assertEquals(252, tableInput.getLinesInput());
    assertEquals(252, output.size());
  }

  @Test
  void stringParameters() throws Exception {
    meta.setSql("SELECT ID, NAME FROM T WHERE NAME = ?");

    run(rowMeta(new ValueMetaString("name")), row("two"), row("nope"), row("one"), row("three"));

    assertEquals(
        String.join(
            "\n", "2[Long] | two[String]", "1[Long] | one[String]", "3[Long] | three[String]"),
        outputValues());
    assertEveryLayout("ID:Integer(9,0)@table input | NAME:String(20,-1)@table input");
  }

  @Test
  void dateParameters() throws Exception {
    meta.setSql("SELECT NAME, D FROM T WHERE D = ?");

    run(
        rowMeta(new ValueMetaDate("d")),
        row(localDate("2024-01-15 00:00:00.000")),
        row(localDate("2000-01-01 00:00:00.000")),
        row(localDate("1899-12-31 00:00:00.000")),
        row(localDate("9999-12-31 00:00:00.000")));

    assertEquals(
        String.join(
            "\n",
            "one[String] | 2024-01-15 00:00:00.000/0[Timestamp]",
            "two[String] | 1899-12-31 00:00:00.000/0[Timestamp]",
            "three[String] | 9999-12-31 00:00:00.000/0[Timestamp]"),
        outputValues());
    assertEveryLayout("NAME:String(20,-1)@table input | D:Date(-1,-1)@table input");
  }

  @Test
  void timestampParameters() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE TS = ?");

    Timestamp first = Timestamp.valueOf("2024-01-15 10:11:12.123");
    Timestamp second = Timestamp.valueOf("1899-12-31 23:59:59.999");
    run(rowMeta(new ValueMetaTimestamp("ts")), row(second), row(first));

    assertEquals(List.of("two", "one"), firstColumn());
  }

  @Test
  void numberParameters() throws Exception {
    meta.setSql("SELECT NAME, F FROM T WHERE F = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaNumber("f")), row(2.5), row(0.1), row(-1.0E-7), row(0.0), row(7.0));

    assertEquals(
        String.join(
            "\n",
            "two[String] | 2.5/scale=1[BigDecimal]",
            "one[String] | 0.1/scale=1[BigDecimal]",
            "three[String] | -0.00000010/scale=8[BigDecimal]",
            "no-id[String] | 0.0/scale=1[BigDecimal]"),
        outputValues());
    assertEveryLayout("NAME:String(20,-1)@table input | F:BigNumber(53,-1)@table input");
  }

  @Test
  void bigNumberParameters() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE AMOUNT = ? ORDER BY NAME");

    run(
        rowMeta(new ValueMetaBigNumber("amount")),
        row(new BigDecimal("2.25")),
        row(new BigDecimal("-3")),
        row(new BigDecimal("1.5000")),
        row(new BigDecimal("0")));

    assertEquals(List.of("two", "three", "one", "no-id"), firstColumn());
  }

  @Test
  void booleanParameters() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE FLAG = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaBoolean("flag")), row(true), row(false));

    assertEquals(List.of("one", "three", "no-id", "two"), firstColumn());
  }

  @Test
  void severalPositionalParametersPerRow() throws Exception {
    meta.setSql("SELECT ID, NAME FROM T WHERE ID = ? AND NAME = ?");

    run(
        rowMeta(new ValueMetaInteger("id"), new ValueMetaString("name")),
        row(2L, "two-bis"),
        row(2L, "one"),
        row(1L, "one"));

    assertEquals("2[Long] | two-bis[String]\n1[Long] | one[String]", outputValues());
  }

  /** The output is the query result only: the fields of the input row are not added to it. */
  @Test
  void namedParametersAndTheInputFieldsAreNotAppended() throws Exception {
    meta.setUseNamedParameters(true);
    meta.setSql("SELECT ID, NAME FROM T WHERE NAME <> {name} AND ID = {id} ORDER BY NAME");

    run(
        rowMeta(
            new ValueMetaInteger("id"), new ValueMetaString("name"), new ValueMetaString("unused")),
        row(2L, "two", "x"),
        row(1L, "nope", "y"),
        row(3L, "three", "z"));

    assertEquals("2[Long] | two-bis[String]\n1[Long] | one[String]", outputValues());
    assertEveryLayout("ID:Integer(9,0)@table input | NAME:String(20,-1)@table input");
    for (Object[] row : output) {
      assertTrue(row.length >= 2);
    }
  }

  @Test
  void theSameNamedParameterTwice() throws Exception {
    meta.setUseNamedParameters(true);
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = {grp} AND SEQ <= {grp} - 4 ORDER BY SEQ");

    run(rowMeta(new ValueMetaInteger("grp")), row(7L), row(8L));

    assertEquals(
        "7[Long] | 1[Long]\n7[Long] | 2[Long]\n7[Long] | 3[Long]\n8[Long] | 1[Long]",
        outputValues());
  }

  @Test
  void aQueryWithoutPlaceholdersRunsOncePerInputRow() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = 1");

    run(rowMeta(new ValueMetaInteger("id")), row(5L), row(6L), row(7L));

    assertEquals(List.of("one", "one", "one"), firstColumn());
    assertEveryLayout("NAME:String(20,-1)@table input");
  }

  /**
   * The specified fields pick and reorder the query columns and convert them. Today the format of a
   * specified String field is not used to render a date: the date's own default mask is.
   */
  @Test
  void specifiedFieldsAreConvertedAndReorderedForEveryRow() throws Exception {
    meta.setSql("SELECT ID, NAME, AMOUNT, D FROM T WHERE ID = ? ORDER BY NAME");
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            field("NAME", IValueMeta.TYPE_STRING, null),
            field("AMOUNT", IValueMeta.TYPE_NUMBER, null),
            field("ID", IValueMeta.TYPE_STRING, null),
            field("D", IValueMeta.TYPE_STRING, "dd/MM/yyyy")));

    run(rowMeta(new ValueMetaInteger("id")), row(2L), row(99L), row(1L), row(3L));

    assertEquals(
        String.join(
            "\n",
            "two[String] | 2.25[Double] | 2[String] | 1899/12/31 00:00:00.000[String]",
            "two-bis[String] | null | 2[String] | null",
            "one[String] | 1.5[Double] | 1[String] | 2024/01/15 00:00:00.000[String]",
            "three[String] | -3.0[Double] | 3[String] | 9999/12/31 00:00:00.000[String]"),
        outputValues());
    assertEveryLayout(
        "NAME:String(-1,-1)@table input"
            + " | AMOUNT:Number(-1,-1)@table input"
            + " | ID:String(-1,-1)@table input"
            + " | D:String(-1,-1)@table input");
  }

  @Test
  void specifiedFieldsWithAMismatchingTypeFailOnTheFirstRow() throws Exception {
    meta.setSql("SELECT ID, NAME FROM T WHERE ID = ?");
    meta.setSpecifyFields(true);
    meta.setValidateSpecifiedFields(true);
    meta.setFields(List.of(field("ID", IValueMeta.TYPE_STRING, null)));

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row(2L));

    assertEquals(0, output.size());
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
  }

  @Test
  void theRowLimitAppliesToEveryInputRow() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");
    meta.setRowLimit("2");

    run(rowMeta(new ValueMetaInteger("grp")), row(7L), row(8L), row(99L), row(7L));

    assertEquals(
        String.join(
            "\n",
            "7[Long] | 1[Long]",
            "7[Long] | 2[Long]",
            "8[Long] | 1[Long]",
            "7[Long] | 1[Long]",
            "7[Long] | 2[Long]"),
        outputValues());
  }

  @Test
  void theRowLimitCanBeAVariable() throws Exception {
    meta.setSql("SELECT GRP, SEQ FROM MANY WHERE GRP = ? ORDER BY SEQ");
    meta.setRowLimit("${LIMIT}");

    tableInput = createTransform(rowMeta(new ValueMetaInteger("grp")), row(7L), row(8L), row(7L));
    tableInput.setVariable("LIMIT", "3");
    assertTrue(tableInput.init());
    while (tableInput.processRow()) {
      // keep reading
    }

    assertEquals(
        String.join(
            "\n",
            "7[Long] | 1[Long]",
            "7[Long] | 2[Long]",
            "7[Long] | 3[Long]",
            "8[Long] | 1[Long]",
            "7[Long] | 1[Long]",
            "7[Long] | 2[Long]",
            "7[Long] | 3[Long]"),
        outputValues());
  }

  @Test
  void variablesAreReplacedInTheSql() throws Exception {
    meta.setVariableReplacementActive(true);
    meta.setSql("SELECT NAME FROM ${TABLE_NAME} WHERE ID = ? AND NAME <> '${SKIP}' ORDER BY NAME");

    tableInput = createTransform(rowMeta(new ValueMetaInteger("id")), row(2L), row(1L), row(2L));
    tableInput.setVariable("TABLE_NAME", "T");
    tableInput.setVariable("SKIP", "two-bis");
    assertTrue(tableInput.init());
    while (tableInput.processRow()) {
      // keep reading
    }

    assertEquals(List.of("two", "one", "two"), firstColumn());
    assertEquals(0, tableInput.getErrors());
  }

  @Test
  void withoutVariableReplacementTheSqlIsSentAsWritten() throws Exception {
    meta.setVariableReplacementActive(false);
    meta.setSql("SELECT NAME FROM T WHERE ID = ? AND NAME <> '${SKIP}' ORDER BY NAME");

    tableInput = createTransform(rowMeta(new ValueMetaInteger("id")), row(2L), row(1L));
    tableInput.setVariable("SKIP", "two-bis");
    assertTrue(tableInput.init());
    while (tableInput.processRow()) {
      // keep reading
    }

    assertEquals(List.of("two", "two-bis", "one"), firstColumn());
  }

  /**
   * A query that fails for one input row: the rows of the earlier input rows are out, the error is
   * reported once, the transform stops and does not read further input rows.
   */
  @Test
  void aQueryFailingForALaterInputRowStopsTheTransform() throws Exception {
    meta.setSql("SELECT 10 / ? AS Q");

    tableInput =
        createTransform(rowMeta(new ValueMetaInteger("divisor")), row(5L), row(0L), row(2L));
    assertTrue(tableInput.init());
    boolean more = true;
    int calls = 0;
    while (more && calls < 10) {
      more = tableInput.processRow();
      calls++;
    }

    assertEquals(List.of("2"), firstColumn());
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
    assertTrue(
        errorMessages.get(0).startsWith("SQL query failed. Please verify the SQL syntax"),
        errorMessages.get(0));
    verify(tableInput, times(1)).stopAll();
    verify(tableInput, times(2)).getRow();

    tableInput.dispose();
    disposed = true;
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
  }

  @Test
  void aQueryFailingForTheFirstInputRow() throws Exception {
    meta.setSql("SELECT 10 / ? AS Q");

    run(rowMeta(new ValueMetaInteger("divisor")), row(0L), row(5L));

    assertEquals(0, output.size());
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
    assertTrue(
        errorMessages.get(0).startsWith("SQL query failed. Please verify the SQL syntax"),
        errorMessages.get(0));
  }

  @Test
  void aParameterThatCannotBeConvertedFailsForThatRow() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaString("id")), row("1"), row("abc"), row("3"));

    assertEquals(List.of("one"), firstColumn());
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
    assertTrue(
        errorMessages.get(0).startsWith("SQL query failed. Please verify the SQL syntax"),
        errorMessages.get(0));
  }

  /** A value that does not match its own metadata cannot be bound to the statement. */
  @Test
  void aValueThatCannotBeBoundFailsForThatRow() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = ? ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row("abc"), row(3L));

    assertEquals(List.of("one"), firstColumn());
    assertEquals(1, tableInput.getErrors());
    assertEquals(1, errorMessages.size(), errorMessages.toString());
    // the cause is not an SQLException: the binding failed before the statement ran
    String message = errorMessages.get(0);
    assertTrue(message.startsWith("Failed to execute query: "), message);
    assertTrue(message.contains("An error occurred executing SQL:"), message);
    assertTrue(message.contains("SELECT NAME FROM T WHERE ID = ? ORDER BY NAME"), message);
    assertTrue(message.contains("offending row : [id Integer]"), message);
  }

  @Test
  void moreInputFieldsThanPlaceholdersFails() throws Exception {
    meta.setSql("SELECT NAME FROM T WHERE ID = ?");

    run(rowMeta(new ValueMetaInteger("id"), new ValueMetaString("extra")), row(1L, "x"));

    assertEquals(0, output.size());
    assertEquals(1, tableInput.getErrors());
  }

  /** Duplicate column names (also ignoring case) are made unique, the same way for every row. */
  @Test
  void duplicateColumnNamesAreRenamedForEveryRow() throws Exception {
    meta.setSql("SELECT ID, ID, NAME AS \"id\", NAME, NAME AS \"Name\" FROM T WHERE ID = ?");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row(3L));

    assertEquals(
        "1[Long] | 1[Long] | one[String] | one[String] | one[String]\n"
            + "3[Long] | 3[Long] | three[String] | three[String] | three[String]",
        outputValues());
    // the column names, not the aliases, are used (H2 reports NAME for NAME AS "id")
    assertEveryLayout(
        "ID:Integer(9,0)@table input"
            + " | ID_1:Integer(9,0)@table input"
            + " | NAME:String(20,-1)@table input"
            + " | NAME_1:String(20,-1)@table input"
            + " | NAME_2:String(20,-1)@table input");
  }

  /** A wide result: forty columns of the same name. */
  @Test
  void aWideResultWithManyDuplicateNames() throws Exception {
    StringBuilder sql = new StringBuilder("SELECT ID");
    for (int i = 1; i < 40; i++) {
      sql.append(", ID");
    }
    meta.setSql(sql.append(" FROM T WHERE ID = ?").toString());

    run(rowMeta(new ValueMetaInteger("id")), row(2L), row(1L));

    assertEquals(3, output.size());
    StringBuilder expected = new StringBuilder("ID:Integer(9,0)@table input");
    for (int i = 1; i < 40; i++) {
      expected.append(" | ID_").append(i).append(":Integer(9,0)@table input");
    }
    assertEveryLayout(expected.toString());
    for (Object[] row : output) {
      for (int i = 1; i < 40; i++) {
        assertEquals(row[0], row[i]);
      }
    }
  }

  /**
   * With specified fields and no type validation, a value that cannot be converted for a later
   * input row ends the transform with an exception.
   */
  @Test
  void aSpecifiedFieldConversionFailingForALaterInputRow() throws Exception {
    meta.setSql("SELECT CAST(? AS VARCHAR) AS V");
    meta.setSpecifyFields(true);
    meta.setValidateSpecifiedFields(false);
    meta.setFields(List.of(field("V", IValueMeta.TYPE_INTEGER, null)));

    tableInput =
        createTransform(rowMeta(new ValueMetaString("v")), row("1"), row("2"), row("x"), row("4"));
    assertTrue(tableInput.init());
    String failure = null;
    try {
      while (tableInput.processRow()) {
        // keep reading
      }
    } catch (Exception e) {
      failure = e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    assertEquals("1[Long]\n2[Long]", outputValues());
    // the exception leaves processRow(); the transform itself does not count the error
    assertTrue(failure != null && failure.startsWith("HopException: "), failure);
    assertTrue(
        failure.contains("Unable to convert query value for specified field 'V': "), failure);
    assertTrue(failure.contains("for value [x]"), failure);
    assertEquals(0, tableInput.getErrors());
  }

  /** Without "Execute for each row" all input rows are bound to one query, for contrast. */
  @Test
  void withoutExecuteForEachRowAllInputRowsFeedOneQuery() throws Exception {
    meta.setExecuteEachInputRow(false);
    meta.setSql("SELECT NAME FROM T WHERE ID IN (?, ?) ORDER BY NAME");

    run(rowMeta(new ValueMetaInteger("id")), row(1L), row(3L));

    assertEquals(List.of("one", "three"), firstColumn());
    assertEveryLayout("NAME:String(20,-1)@table input");
  }

  private static TableInputField field(String name, int type, String format) {
    TableInputField field = new TableInputField();
    field.setName(name);
    field.setType(type);
    field.setFormat(format);
    return field;
  }
}
