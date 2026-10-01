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

package org.apache.hop.pipeline.transforms.vertica.bulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaPluginType;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.ColumnSpec;
import org.apache.hop.pipeline.transforms.vertica.bulkloader.nativebinary.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers the way the Vertica bulk loader lines up the fields on the stream with the columns of the
 * target table, both with and without the "Specify database fields" option.
 */
class VerticaBulkLoaderTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private TransformMockHelper<VerticaBulkLoaderMeta, VerticaBulkLoaderData> mockHelper;
  private VerticaBulkLoaderMeta meta;
  private VerticaBulkLoaderData data;
  private VerticaBulkLoader transform;

  @BeforeAll
  static void setUpBeforeClass() throws HopException {
    PluginRegistry.addPluginType(ValueMetaPluginType.getInstance());
    PluginRegistry.addPluginType(DatabasePluginType.getInstance());
    PluginRegistry.init();
    HopLogStore.init();
  }

  @BeforeEach
  void setUp() {
    mockHelper =
        new TransformMockHelper<>(
            "Vertica bulk loader", VerticaBulkLoaderMeta.class, VerticaBulkLoaderData.class);
    when(mockHelper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(mockHelper.iLogChannel);
    when(mockHelper.logChannelFactory.create(any())).thenReturn(mockHelper.iLogChannel);

    meta = new VerticaBulkLoaderMeta();
    meta.setConnection("vertica");
    meta.setTableName("INVOICE");
    data = new VerticaBulkLoaderData();

    transform =
        new VerticaBulkLoader(
            mockHelper.transformMeta, meta, data, 0, mockHelper.pipelineMeta, mockHelper.pipeline);
  }

  @AfterEach
  void tearDown() {
    mockHelper.cleanUp();
  }

  /**
   * The table as the database reports it. Note that the column order deliberately differs from the
   * order of the fields on the stream.
   */
  private static IRowMeta invoiceTable() {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaInteger("INVOICE_ID"), "INTEGER", 0, 0));
    table.addValueMeta(column(new ValueMetaDate("INVOICE_RECEIPT_DATE"), "DATE", 0, 0));
    table.addValueMeta(column(new ValueMetaString("COST_CURRENCY"), "VARCHAR", 3, 0));
    return table;
  }

  private static IValueMeta column(
      IValueMeta valueMeta, String columnTypeName, int length, int precision) {
    valueMeta.setOriginalColumnTypeName(columnTypeName);
    valueMeta.setLength(length);
    valueMeta.setPrecision(precision);
    return valueMeta;
  }

  // ----------------------------------------------------------------------------------------------
  // Without "Specify database fields": the whole input row is loaded, matched on name.
  // ----------------------------------------------------------------------------------------------

  @Test
  void matchesColumnsByNameAndNotByPositionWhenFieldsAreNotSpecified() throws Exception {
    // The exact scenario of issue #4394: a string field sits on the position that the table uses
    // for a date column. Matching on position made this fail with "Field COST_CURRENCY must be a
    // Date compatible type to match target column INVOICE_RECEIPT_DATE".
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("INVOICE_ID"));
    input.addValueMeta(new ValueMetaString("COST_CURRENCY"));
    input.addValueMeta(new ValueMetaDate("INVOICE_RECEIPT_DATE"));

    transform.prepareFieldMapping(input, invoiceTable());

    assertArrayEquals(new int[] {0, 1, 2}, data.selectedRowFieldIndices);
    assertArrayEquals(
        new String[] {"INVOICE_ID", "COST_CURRENCY", "INVOICE_RECEIPT_DATE"},
        data.insertRowMeta.getFieldNames());
    assertEquals(
        List.of(ColumnType.INTEGER, ColumnType.VARCHAR, ColumnType.DATE),
        data.colSpecs.stream().map(cs -> cs.type).toList());
  }

  @Test
  void takesOverTheColumnNameAsTheDatabaseSpellsIt() throws Exception {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("cost_currency"));

    transform.prepareFieldMapping(input, invoiceTable());

    // The COPY statement is generated from these names, so they have to match the table.
    assertArrayEquals(new String[] {"COST_CURRENCY"}, data.insertRowMeta.getFieldNames());
  }

  @Test
  void loadsAnInputRowThatCoversOnlyPartOfTheTable() throws Exception {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("COST_CURRENCY"));
    input.addValueMeta(new ValueMetaInteger("INVOICE_ID"));

    transform.prepareFieldMapping(input, invoiceTable());

    assertEquals(2, data.colSpecs.size());
    assertArrayEquals(
        new String[] {"COST_CURRENCY", "INVOICE_ID"}, data.insertRowMeta.getFieldNames());
  }

  @Test
  void reportsAnInputFieldWithoutAMatchingColumn() {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("INVOICE_ID"));
    input.addValueMeta(new ValueMetaString("NOT_A_COLUMN"));

    HopException e =
        assertThrows(
            HopTransformException.class,
            () -> transform.prepareFieldMapping(input, invoiceTable()));

    // Naming the field, the table and the available columns beats an out of bounds exception.
    assertTrue(e.getMessage().contains("NOT_A_COLUMN"), e.getMessage());
    assertTrue(e.getMessage().contains("INVOICE"), e.getMessage());
    assertTrue(e.getMessage().contains("INVOICE_RECEIPT_DATE"), e.getMessage());
  }

  @Test
  void reportsAnInputRowThatIsWiderThanTheTable() {
    // Before the fix this walked off the end of the table row meta with a NullPointerException.
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("INVOICE_ID"));
    input.addValueMeta(new ValueMetaDate("INVOICE_RECEIPT_DATE"));
    input.addValueMeta(new ValueMetaString("COST_CURRENCY"));
    input.addValueMeta(new ValueMetaString("ONE_TOO_MANY"));

    HopException e =
        assertThrows(
            HopTransformException.class,
            () -> transform.prepareFieldMapping(input, invoiceTable()));
    assertTrue(e.getMessage().contains("ONE_TOO_MANY"), e.getMessage());
  }

  // ----------------------------------------------------------------------------------------------
  // With "Specify database fields".
  // ----------------------------------------------------------------------------------------------

  @Test
  void mapsStreamFieldsOntoTheirConfiguredColumns() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new VerticaBulkLoaderField("COST_CURRENCY", "currency"),
            new VerticaBulkLoaderField("INVOICE_ID", "id")));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("id"));
    input.addValueMeta(new ValueMetaDate("received"));
    input.addValueMeta(new ValueMetaString("currency"));

    transform.prepareFieldMapping(input, invoiceTable());

    assertArrayEquals(new int[] {2, 0}, data.selectedRowFieldIndices);
    assertArrayEquals(
        new String[] {"COST_CURRENCY", "INVOICE_ID"}, data.insertRowMeta.getFieldNames());
    assertEquals(
        List.of(ColumnType.VARCHAR, ColumnType.INTEGER),
        data.colSpecs.stream().map(cs -> cs.type).toList());
  }

  @Test
  void reportsAConfiguredColumnThatIsMissingFromTheTable() {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new VerticaBulkLoaderField("NOT_A_COLUMN", "id")));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("id"));

    // This used to be a NullPointerException on the unknown target column.
    HopException e =
        assertThrows(
            HopTransformException.class,
            () -> transform.prepareFieldMapping(input, invoiceTable()));
    assertTrue(e.getMessage().contains("NOT_A_COLUMN"), e.getMessage());
  }

  @Test
  void reportsAConfiguredStreamFieldThatIsMissingFromTheInput() {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new VerticaBulkLoaderField("INVOICE_ID", "not_on_the_stream")));

    HopException e =
        assertThrows(
            HopTransformException.class,
            () -> transform.prepareFieldMapping(new RowMeta(), invoiceTable()));
    assertTrue(e.getMessage().contains("not_on_the_stream"), e.getMessage());
  }

  @Test
  void usesTheColumnNameOfTheTableRatherThanTheConfiguredCasing() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new VerticaBulkLoaderField("invoice_id", "id")));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("id"));

    transform.prepareFieldMapping(input, invoiceTable());

    assertArrayEquals(new String[] {"INVOICE_ID"}, data.insertRowMeta.getFieldNames());
  }

  // ----------------------------------------------------------------------------------------------
  // Column types.
  // ----------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource({
    // The names on the left are what the Vertica driver returns from getColumnTypeName(),
    // upper cased, plus the SQL standard synonyms this transform has always accepted.
    // column type name, expected native binary type, expected width in the file header
    "INTEGER, INTEGER, 8",
    "BIGINT, INTEGER, 8",
    "BOOLEAN, BOOLEAN, 1",
    "FLOAT, FLOAT, 8",
    "DOUBLE PRECISION, FLOAT, 8",
    "CHAR, CHAR, 12",
    "VARCHAR, VARCHAR, -1",
    "CHARACTER VARYING, VARCHAR, -1",
    "LONG VARCHAR, VARCHAR, -1",
    "BINARY, BINARY, 12",
    "VARBINARY, VARBINARY, -1",
    "LONG VARBINARY, VARBINARY, -1",
    "NUMERIC, NUMERIC, -1",
  })
  void mapsColumnTypesOntoTheNativeBinaryFormat(String columnTypeName, String type, int bytes)
      throws Exception {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaString("VALUE"), columnTypeName, 12, 2));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("VALUE"));

    transform.prepareFieldMapping(input, table);

    ColumnSpec spec = data.colSpecs.get(0);
    assertEquals(ColumnType.valueOf(type), spec.type);
    assertEquals(bytes, spec.bytes);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL DAY TO SECOND"})
  void mapsTheDateTimeColumnTypes(String columnTypeName) throws Exception {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaDate("MOMENT"), columnTypeName, 0, 0));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaDate("MOMENT"));

    transform.prepareFieldMapping(input, table);

    assertEquals(8, data.colSpecs.get(0).bytes);
  }

  @Test
  void keepsThePrecisionAndScaleOfNumericColumns() throws Exception {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaBigNumber("AMOUNT"), "NUMERIC", 18, 4));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaBigNumber("AMOUNT"));

    transform.prepareFieldMapping(input, table);

    ColumnSpec spec = data.colSpecs.get(0);
    assertEquals(18, spec.getMaxLength());
    assertEquals(4, spec.scale);
  }

  @Test
  void rejectsANonDateFieldForADateColumn() {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaDate("INVOICE_RECEIPT_DATE"), "DATE", 0, 0));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("INVOICE_RECEIPT_DATE"));

    HopException e =
        assertThrows(
            HopTransformException.class, () -> transform.prepareFieldMapping(input, table));
    assertTrue(e.getMessage().contains("INVOICE_RECEIPT_DATE"), e.getMessage());
    assertTrue(e.getMessage().contains("DATE"), e.getMessage());
  }

  @Test
  void rejectsAColumnTypeItCannotEncode() {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaString("KEY"), "UUID", 16, 0));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("KEY"));

    HopException e =
        assertThrows(
            HopTransformException.class, () -> transform.prepareFieldMapping(input, table));
    assertTrue(e.getMessage().contains("UUID"), e.getMessage());
  }

  @Test
  void rejectsAColumnWithoutATypeName() {
    // A driver that does not report a column type name used to cause a NullPointerException.
    RowMeta table = new RowMeta();
    table.addValueMeta(new ValueMetaString("KEY"));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("KEY"));

    HopException e =
        assertThrows(
            HopTransformException.class, () -> transform.prepareFieldMapping(input, table));
    assertTrue(e.getMessage().contains("KEY"), e.getMessage());
  }

  @Test
  void binaryColumnsAreFixedWidthAndVarbinaryColumnsAreNot() throws Exception {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaBinary("FIXED"), "BINARY", 10, 0));
    table.addValueMeta(column(new ValueMetaBinary("VARIABLE"), "VARBINARY", 10, 0));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaBinary("FIXED"));
    input.addValueMeta(new ValueMetaBinary("VARIABLE"));

    transform.prepareFieldMapping(input, table);

    // Vertica pads a BINARY column up to its declared width, a VARBINARY carries a length prefix.
    assertEquals(ColumnType.BINARY, data.colSpecs.get(0).type);
    assertEquals(10, data.colSpecs.get(0).bytes);
    assertEquals(ColumnType.VARBINARY, data.colSpecs.get(1).type);
    assertEquals(-1, data.colSpecs.get(1).bytes);
  }

  // ----------------------------------------------------------------------------------------------
  // The generated COPY statement.
  // ----------------------------------------------------------------------------------------------

  @Test
  void copyStatementListsTheColumnsInTheOrderOfTheStream() throws Exception {
    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaString("COST_CURRENCY"));
    input.addValueMeta(new ValueMetaInteger("INVOICE_ID"));

    transform.prepareFieldMapping(input, invoiceTable());
    data.db = verticaDatabase();

    String sql = transform.buildCopyStatementSqlString();

    assertTrue(
        sql.startsWith("COPY INVOICE (\"COST_CURRENCY\", \"INVOICE_ID\") FROM STDIN NATIVE "), sql);
    assertTrue(sql.contains("ENFORCELENGTH"), sql);
  }

  @Test
  void copyStatementCastsNumericColumnsThroughAFillerColumn() throws Exception {
    RowMeta table = new RowMeta();
    table.addValueMeta(column(new ValueMetaBigNumber("AMOUNT"), "NUMERIC", 18, 4));

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaBigNumber("AMOUNT"));

    transform.prepareFieldMapping(input, table);
    data.db = verticaDatabase();

    String sql = transform.buildCopyStatementSqlString();

    assertTrue(
        sql.contains(
            "(TMPFILLERCOL0 FILLER VARCHAR(1000), \"AMOUNT\" AS CAST(TMPFILLERCOL0 AS NUMERIC))"),
        sql);
  }

  @Test
  void copyStatementHonoursTheTransformOptions() throws Exception {
    meta.setSchemaName("staging");
    meta.setDirect(true);
    meta.setAbortOnError(true);
    meta.setStreamName("nightly load");

    RowMeta input = new RowMeta();
    input.addValueMeta(new ValueMetaInteger("INVOICE_ID"));

    transform.prepareFieldMapping(input, invoiceTable());
    data.db = verticaDatabase();

    String sql = transform.buildCopyStatementSqlString();

    assertTrue(sql.startsWith("COPY staging.INVOICE "), sql);
    assertTrue(sql.contains("ABORT ON ERROR "), sql);
    assertTrue(sql.contains("DIRECT "), sql);
    assertTrue(sql.contains("STREAM NAME E'nightly load' "), sql);
  }

  private Database verticaDatabase() {
    DatabaseMeta databaseMeta =
        new DatabaseMeta("vertica", "VERTICA", "Native", "localhost", "db", "5433", "user", "pass");
    Database db = mock(Database.class);
    when(db.getDatabaseMeta()).thenReturn(databaseMeta);
    when(db.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
    return db;
  }
}
