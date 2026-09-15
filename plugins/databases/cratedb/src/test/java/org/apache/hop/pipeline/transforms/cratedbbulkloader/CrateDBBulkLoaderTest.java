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

package org.apache.hop.pipeline.transforms.cratedbbulkloader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests around the COPY statement and the field mapping of the CrateDB bulk loader. */
class CrateDBBulkLoaderTest {

  private TransformMockHelper<CrateDBBulkLoaderMeta, CrateDBBulkLoaderData> helper;
  private CrateDBBulkLoaderMeta meta;
  private CrateDBBulkLoaderData data;
  private CrateDBBulkLoader transform;

  @BeforeEach
  void setUp() {
    helper =
        new TransformMockHelper<>(
            "CrateDB bulk loader", CrateDBBulkLoaderMeta.class, CrateDBBulkLoaderData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);

    meta = new CrateDBBulkLoaderMeta();
    meta.setConnection("cratedb");
    meta.setSchemaName("doc");
    meta.setTablename("orders");
    meta.setReadFromFilename("s3://bucket/orders.csv");

    data = new CrateDBBulkLoaderData();

    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getQuotedSchemaTableCombination(any(), any(), any()))
        .thenReturn("\"doc\".\"orders\"");

    data.db = mock(Database.class);
    when(data.db.getDatabaseMeta()).thenReturn(databaseMeta);
    when(data.db.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));

    transform =
        new CrateDBBulkLoader(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  /** A local path has no "://" in it, which used to blow up on split()[1]. */
  @Test
  void buildsTheCopyStatementForAFileNameThatIsNotAUri() {
    meta.setReadFromFilename("/data/orders.csv");
    prepare(rowMeta("name"));

    String sql = assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(false));

    assertTrue(sql.contains(" FROM '/data/orders.csv'"), sql);
  }

  /** And an unset file name should not NPE either. */
  @Test
  void refusesToStartWithoutAFileName() {
    meta.setReadFromFilename(null);

    assertThrows(HopException.class, () -> transform.verifyLoadSettings());
  }

  @Test
  void refusesToStartWithoutAnHttpEndpoint() {
    meta.setUseHttpEndpoint(true);
    meta.setHttpEndpoint("");

    assertThrows(HopException.class, () -> transform.verifyLoadSettings());
  }

  /** CrateDB reads the S3 credentials from the URI, so they have to end up in it, resolved. */
  @Test
  void resolvesVariablesInTheCredentials() {
    transform.setVariable("AWS_KEY", "AKIAEXAMPLE");
    transform.setVariable("AWS_SECRET", "s3cr3t");
    meta.setAwsAccessKeyId("${AWS_KEY}");
    meta.setAwsSecretAccessKey("${AWS_SECRET}");
    prepare(rowMeta("name"));

    assertTrue(
        transform
            .buildCopyStatementSqlString(false)
            .contains(" FROM 's3://AKIAEXAMPLE:s3cr3t@bucket/orders.csv'"),
        transform.buildCopyStatementSqlString(false));
  }

  /** Without credentials the URI stays clean so CrateDB can use its own configuration. */
  @Test
  void leavesTheUriAloneWithoutCredentials() {
    prepare(rowMeta("name"));

    assertTrue(
        transform.buildCopyStatementSqlString(false).contains(" FROM 's3://bucket/orders.csv'"),
        transform.buildCopyStatementSqlString(false));
  }

  /** The statement ends up in the log on detailed level, the credentials should not. */
  @Test
  void masksTheCredentialsInTheLoggedStatement() {
    meta.setAwsAccessKeyId("AKIAEXAMPLE");
    meta.setAwsSecretAccessKey("s3cr3t");
    prepare(rowMeta("name"));

    String logged = transform.buildCopyStatementSqlString(true);

    assertFalse(logged.contains("AKIAEXAMPLE"), logged);
    assertFalse(logged.contains("s3cr3t"), logged);
  }

  /**
   * The CSV file carries no header, so the COPY statement has to name the columns. It used to emit
   * an empty "()" whenever the fields were not specified explicitly.
   */
  @Test
  void namesTheColumnsOfTheWholeInputRowWhenNoFieldsAreSpecified() {
    prepare(rowMeta("name", "amount"));

    assertTrue(
        transform.buildCopyStatementSqlString(false).contains("\"doc\".\"orders\" (name, amount)"),
        transform.buildCopyStatementSqlString(false));
  }

  @Test
  void namesTheConfiguredColumnsWhenFieldsAreSpecified() {
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new CrateDBBulkLoaderField("order_amount", "amount"),
            new CrateDBBulkLoaderField("order_name", "name")));
    prepare(rowMeta("name", "amount"));

    assertTrue(
        transform
            .buildCopyStatementSqlString(false)
            .contains("\"doc\".\"orders\" (order_amount, order_name)"),
        transform.buildCopyStatementSqlString(false));
  }

  /** The mapping picks the fields out of the row in the configured order. */
  @Test
  void mapsTheSelectedFieldsToTheirPositionOnTheStream() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new CrateDBBulkLoaderField("order_amount", "amount"),
            new CrateDBBulkLoaderField("order_name", "name")));
    prepare(rowMeta("name", "amount"));

    assertArrayEquals(new int[] {1, 0}, data.selectedRowFieldIndices);
    assertArrayEquals(new String[] {"order_amount", "order_name"}, data.columnNames);
  }

  /** Without an explicit mapping the whole row is loaded, in stream order. */
  @Test
  void mapsTheWholeRowWhenNoFieldsAreSpecified() {
    prepare(rowMeta("name", "amount"));

    assertArrayEquals(new int[] {0, 1}, data.selectedRowFieldIndices);
    assertArrayEquals(new String[] {"name", "amount"}, data.columnNames);
    assertEquals(2, data.insertRowMeta.size());
  }

  /** A configured field that is not on the stream is an error, not an empty column. */
  @Test
  void failsOnAFieldThatIsNotOnTheStream() {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new CrateDBBulkLoaderField("order_note", "note")));
    transform.setInputRowMeta(rowMeta("name"));

    assertThrows(HopException.class, () -> transform.prepareRowMapping());
  }

  /** A column that does not exist in the table has to be caught before the file is written. */
  @Test
  void refusesToStartWhenAColumnIsNotInTheTable() {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new CrateDBBulkLoaderField("order_totl", "amount")));
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"ORDER_ID", "INTEGER"});

    HopException e = assertThrows(HopException.class, () -> transform.verifyTableFields());
    assertTrue(e.getMessage().contains("order_totl"), e.getMessage());
  }

  // ------------------------------------------------------------ the CSV write path

  /** The plain case: values separated, strings enclosed. */
  @Test
  void writesTheRowAsCsv() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    assertEquals("\"Acme\",42\n", writeRow(rowMeta, new Object[] {"Acme", 42L}));
  }

  @Test
  void writesBinaryValuesAsHex() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaBinary("hash"));

    assertEquals(
        "\"deadbeef\"\n",
        writeRow(
            rowMeta,
            new Object[] {new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}}));
  }

  /** A value carrying the separator has to be enclosed or it becomes two columns. */
  @Test
  void enclosesValuesHoldingTheSeparator() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    assertEquals("\"Acme, Inc\"\n", writeRow(rowMeta, new Object[] {"Acme, Inc"}));
  }

  /** Quotes inside a value are doubled, the way the COPY statement expects. */
  @Test
  void doublesQuotesInsideAValue() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    assertEquals("\"a\"\"b\"\n", writeRow(rowMeta, new Object[] {"a\"b"}));
  }

  /**
   * JSON is not a string as far as Hop is concerned, so it used to be written bare: its commas
   * split it into extra columns. Hop pretty prints it across lines by default too.
   */
  @Test
  void enclosesJsonSoItSurvivesTheCsvRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaJson("payload"));
    JsonNode json = new ObjectMapper().readTree("{\"a\":\"x\",\"b\":{\"c\":[1,2]}}");

    String written = writeRow(rowMeta, new Object[] {json});

    assertTrue(written.startsWith("\""), written);
    assertTrue(written.endsWith("\"\n"), written);
    assertTrue(written.contains("\"\"a\"\""), written);
  }

  /** Dates are rendered with the mask the COPY statement reads them back with. */
  @Test
  void writesDatesInTheDeclaredFormat() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("event_date"));

    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.clear();
    calendar.set(2026, Calendar.AUGUST, 24, 15, 30, 45);

    assertEquals("\"2026-08-24\"\n", writeRow(rowMeta, new Object[] {calendar.getTime()}));
  }

  /** Numbers stay bare, so the common case pays nothing for the enclosure logic. */
  @Test
  void doesNotEncloseValuesThatCannotBreakTheRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    assertEquals("42\n", writeRow(rowMeta, new Object[] {42L}));
  }

  // ------------------------------------------------------------ batch size

  @Test
  void resolvesTheBatchSize() throws Exception {
    meta.setBatchSize("500");
    assertEquals(500, transform.resolveBatchSize());
  }

  @Test
  void resolvesTheBatchSizeFromAVariable() throws Exception {
    transform.setVariable("BATCH", "250");
    meta.setBatchSize("${BATCH}");
    assertEquals(250, transform.resolveBatchSize());
  }

  /** The docs say the batch size has no default, so a missing or silly one must be an error. */
  @Test
  void refusesABatchSizeThatIsNotAPositiveNumber() {
    meta.setBatchSize("");
    assertThrows(HopException.class, () -> transform.resolveBatchSize());
    meta.setBatchSize("0");
    assertThrows(HopException.class, () -> transform.resolveBatchSize());
    meta.setBatchSize("not a number");
    assertThrows(HopException.class, () -> transform.resolveBatchSize());
  }

  // ------------------------------------------------------------ existing files

  /**
   * A file somebody else wrote is the one case we cannot speak for: the trigger stream says nothing
   * about its columns, so the statement has to stay silent.
   */
  @Test
  void namesNoColumnsForAnExistingFileWithoutAMapping() throws Exception {
    meta.setStreamToS3Csv(false);
    meta.setSpecifyFields(false);
    prepare(rowMeta("trigger"));

    assertEquals(0, transform.copyColumnNames().length);
  }

  // ------------------------------------------------------------ staging folder

  @Test
  void createsTheStagingFolderWhenItDoesNotExistYet(@TempDir Path tempDir) throws Exception {
    Path missing = tempDir.resolve("not-there-yet/nested");
    assertFalse(Files.exists(missing));

    transform.ensureParentFolderExists(missing.resolve("stage.csv").toString());

    assertTrue(Files.isDirectory(missing));
  }

  private String writeRow(IRowMeta rowMeta, Object[] row) throws Exception {
    meta.setStreamToS3Csv(true);
    meta.setSpecifyFields(false);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    data.writer = out;
    transform.initBinaryDataFields();
    transform.setInputRowMeta(rowMeta);
    transform.prepareRowMapping();
    transform.writeRowToFile(row);

    return out.toString(StandardCharsets.UTF_8);
  }

  private IRowMeta rowMeta(String... names) {
    IRowMeta rowMeta = new RowMeta();
    for (String name : names) {
      rowMeta.addValueMeta(
          "amount".equals(name) ? new ValueMetaInteger(name) : new ValueMetaString(name));
    }
    return rowMeta;
  }

  private void prepare(IRowMeta rowMeta) {
    transform.setInputRowMeta(rowMeta);
    assertDoesNotThrow(() -> transform.prepareRowMapping());
  }
}
