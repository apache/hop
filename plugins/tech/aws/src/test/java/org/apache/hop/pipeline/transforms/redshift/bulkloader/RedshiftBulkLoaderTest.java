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

package org.apache.hop.pipeline.transforms.redshift.bulkloader;

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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.databases.redshift.RedshiftAuthenticationType;
import org.apache.hop.databases.redshift.RedshiftDatabaseMeta;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests around the COPY statement and the CSV the Redshift bulk loader streams to S3.
 *
 * @see <a href="https://github.com/apache/hop/issues/3621">issue 3621</a>
 */
class RedshiftBulkLoaderTest {

  private TransformMockHelper<RedshiftBulkLoaderMeta, RedshiftBulkLoaderData> helper;
  private RedshiftBulkLoaderMeta meta;
  private RedshiftBulkLoaderData data;
  private RedshiftBulkLoader transform;
  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void initHop() throws Exception {
    // Inherited secrets go through Encr, which needs the password encoder plugin.
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    helper =
        new TransformMockHelper<>(
            "Redshift bulk loader", RedshiftBulkLoaderMeta.class, RedshiftBulkLoaderData.class);
    when(helper.logChannelFactory.create(any(), any())).thenReturn(helper.iLogChannel);
    when(helper.logChannelFactory.create(any())).thenReturn(helper.iLogChannel);

    meta = new RedshiftBulkLoaderMeta();
    meta.setConnection("redshift");
    meta.setSchemaName("staging");
    meta.setTablename("orders");
    meta.setCopyFromFilename("s3://bucket/orders.csv");

    data = new RedshiftBulkLoaderData();

    databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getQuotedSchemaTableCombination(any(), any(), any()))
        .thenReturn("\"staging\".\"orders\"");
    data.databaseMeta = databaseMeta;

    data.db = mock(Database.class);
    when(data.db.getDatabaseMeta()).thenReturn(databaseMeta);
    when(data.db.resolve(anyString())).thenAnswer(invocation -> invocation.getArgument(0));

    transform =
        new RedshiftBulkLoader(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);
  }

  @AfterEach
  void tearDown() throws Exception {
    helper.cleanUp();
  }

  /**
   * The reported NullPointerException: with "stream to S3" off nothing ever set a file format, and
   * the statement builder called equals() straight on it.
   */
  @Test
  void buildsTheCopyStatementWhenNoFileFormatWasEverPicked() throws Exception {
    meta.setStreamToS3Csv(false);
    meta.setUseCredentials(true);
    meta.setAwsAccessKeyId("key");
    meta.setAwsSecretAccessKey("secret");

    String sql = assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(false));

    assertTrue(sql.startsWith("COPY \"staging\".\"orders\" FROM 's3://bucket/orders.csv'"), sql);
    // Without a format there are no CSV options to add.
    assertFalse(sql.contains("DELIMITER"), sql);
  }

  /** The same guard has to hold when the transform is asked for its configuration up front. */
  @Test
  void refusesToStartWithoutAFileFormatForAnExistingFile() throws Exception {
    meta.setStreamToS3Csv(false);

    HopException e = assertThrows(HopException.class, () -> transform.verifyFileSettings());
    assertTrue(e.getMessage().contains("file format"), e.getMessage());
  }

  /** And a COPY statement without a file to read from is never going to work either. */
  @Test
  void refusesToStartWithoutAFileName() throws Exception {
    meta.setCopyFromFilename(null);

    assertThrows(HopException.class, () -> transform.verifyFileSettings());
  }

  /** Variables in the credentials have to reach the COPY statement resolved. */
  @Test
  void resolvesVariablesInTheCredentials() throws Exception {
    transform.setVariable("AWS_KEY", "AKIAEXAMPLE");
    transform.setVariable("AWS_SECRET", "s3cr3t");
    meta.setUseCredentials(true);
    meta.setAwsAccessKeyId("${AWS_KEY}");
    meta.setAwsSecretAccessKey("${AWS_SECRET}");

    String sql = transform.buildCopyStatementSqlString(false);

    assertTrue(
        sql.contains("CREDENTIALS 'aws_access_key_id=AKIAEXAMPLE;aws_secret_access_key=s3cr3t'"),
        sql);
  }

  /** The statement ends up in the log on debug level, the credentials should not. */
  @Test
  void masksTheCredentialsInTheLoggedStatement() throws Exception {
    meta.setUseCredentials(true);
    meta.setAwsAccessKeyId("AKIAEXAMPLE");
    meta.setAwsSecretAccessKey("s3cr3t");

    String logged = transform.buildCopyStatementSqlString(true);

    assertFalse(logged.contains("AKIAEXAMPLE"), logged);
    assertFalse(logged.contains("s3cr3t"), logged);
  }

  /** An IAM role is a secret too. */
  @Test
  void masksTheIamRoleInTheLoggedStatement() throws Exception {
    meta.setUseAwsIamRole(true);
    meta.setAwsIamRole("arn:aws:iam::123456789012:role/loader");

    assertTrue(transform.buildCopyStatementSqlString(false).contains("role/loader"));
    assertFalse(transform.buildCopyStatementSqlString(true).contains("role/loader"));
  }

  /** An empty column list is not valid SQL, so it must never be emitted. */
  @Test
  void neverEmitsAnEmptyColumnList() throws Exception {
    meta.setStreamToS3Csv(true);

    assertFalse(transform.buildCopyStatementSqlString(false).contains("()"));
  }

  @Test
  void namesTheColumnsTheMappingPicked() throws Exception {
    meta.setStreamToS3Csv(true);
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new RedshiftBulkLoaderField("order_id", "id"),
            new RedshiftBulkLoaderField("order_date", "date")));

    assertTrue(
        transform.buildCopyStatementSqlString(false).contains("(order_id, order_date)"),
        transform.buildCopyStatementSqlString(false));
  }

  /**
   * Without a mapping the file still holds only the fields of the stream. Saying nothing makes
   * Redshift expect a value for every column of the table and fail the load with "Delimiter not
   * found" as soon as the table is wider than the stream.
   */
  @Test
  void namesTheStreamFieldsWhenThereIsNoMapping() throws Exception {
    meta.setStreamToS3Csv(true);
    meta.setSpecifyFields(false);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    transform.setInputRowMeta(rowMeta);
    transform.prepareRowMapping();

    assertTrue(
        transform.buildCopyStatementSqlString(false).contains("(id, name)"),
        transform.buildCopyStatementSqlString(false));
  }

  /**
   * A file somebody else wrote is the one case we cannot speak for: with no mapping there is
   * nothing to go on, so the statement stays silent and Redshift matches it positionally.
   */
  @Test
  void namesNoColumnsForAnExistingFileWithoutAMapping() throws Exception {
    meta.setStreamToS3Csv(false);
    meta.setLoadFromExistingFileFormat(RedshiftBulkLoaderMeta.FILE_FORMAT_CSV);
    meta.setSpecifyFields(false);

    String sql = transform.buildCopyStatementSqlString(false);

    assertTrue(sql.startsWith("COPY \"staging\".\"orders\" FROM"), sql);
  }

  /** Parquet files are loaded as is, without any of the CSV options. */
  @Test
  void addsTheParquetFormatForParquetFiles() throws Exception {
    meta.setStreamToS3Csv(false);
    meta.setLoadFromExistingFileFormat(RedshiftBulkLoaderMeta.FILE_FORMAT_PARQUET);
    meta.setCopyFromFilename("s3://bucket/orders.parquet");

    String sql = transform.buildCopyStatementSqlString(false);

    assertTrue(sql.endsWith(" FORMAT AS PARQUET;"), sql);
    assertFalse(sql.contains("DELIMITER"), sql);
  }

  @Test
  void writesEveryFieldOfTheRowWhenNoFieldsAreSpecified() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    assertEquals("\"Acme, Inc\",42\n", writeRow(rowMeta, new Object[] {"Acme, Inc", 42L}, false));
  }

  /** Quotes inside a value are doubled, the way the COPY statement expects them. */
  @Test
  void doublesTheQuotesInsideAValue() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    assertEquals("\"a\"\"b\"\"\"\n", writeRow(rowMeta, new Object[] {"a\"b\""}, false));
  }

  /** The selected fields are written in the order the COPY statement names them. */
  @Test
  void writesTheSelectedFieldsInTheConfiguredOrder() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new RedshiftBulkLoaderField("order_amount", "amount"),
            new RedshiftBulkLoaderField("order_name", "name")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    assertEquals("42,\"Acme\"\n", writeRow(rowMeta, new Object[] {"Acme", 42L}, true));
  }

  /**
   * Dates are written as ISO 8601 with milliseconds, which is what DATEFORMAT/TIMEFORMAT 'auto'
   * reads. A Hop Date carries a time of day, so writing it whole lets a TIMESTAMP column keep it; a
   * DATE column truncates it on Redshift's side.
   */
  @Test
  void writesDatesAsIsoTimestampsSoNoTimeOfDayIsLost() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new RedshiftBulkLoaderField("order_date", "date")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaDate("date"));

    assertEquals("2026-08-24 15:30:45.123\n", writeRow(rowMeta, new Object[] {dateTime()}, true));
  }

  /** Timestamps get the same treatment, fractional seconds included. */
  @Test
  void writesTimestampsAsIsoWithMilliseconds() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new RedshiftBulkLoaderField("order_ts", "ts")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaTimestamp("ts"));

    assertEquals(
        "2026-08-24 15:30:45.123\n",
        writeRow(rowMeta, new Object[] {new Timestamp(dateTime().getTime())}, true));
  }

  /**
   * The regression that made this visible: with "specify database fields" off the transform used a
   * separate write path that never converted dates at all, so they went out in whatever format the
   * incoming field happened to carry.
   */
  @Test
  void convertsDatesEvenWhenNoFieldsAreSpecified() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaDate("date"));

    assertEquals(
        "\"Acme\",2026-08-24 15:30:45.123\n",
        writeRow(rowMeta, new Object[] {"Acme", dateTime()}, false));
  }

  /** The COPY statement has to declare the format the rows are actually written in. */
  @Test
  void declaresTheAutoDateFormatsTheRowsAreWrittenIn() throws Exception {
    meta.setStreamToS3Csv(true);

    String sql = transform.buildCopyStatementSqlString(false);

    assertTrue(sql.contains("DATEFORMAT AS 'auto'"), sql);
    assertTrue(sql.contains("TIMEFORMAT AS 'auto'"), sql);
  }

  private java.util.Date dateTime() {
    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.clear();
    calendar.set(2026, Calendar.AUGUST, 24, 15, 30, 45);
    calendar.set(Calendar.MILLISECOND, 123);
    return calendar.getTime();
  }

  /** A field that is not on the stream is written as a null value, not as a crash. */
  @Test
  void writesAnEmptyValueForAFieldThatIsNotOnTheStream() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(
        List.of(
            new RedshiftBulkLoaderField("order_name", "name"),
            new RedshiftBulkLoaderField("order_note", "note")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));

    assertEquals("\"Acme\",\n", writeRow(rowMeta, new Object[] {"Acme"}, true));
  }

  /** Unless the user asked for that to be an error. */
  @Test
  void failsOnAMissingStreamFieldWhenErrorColumnMismatchIsSet() throws Exception {
    meta.setSpecifyFields(true);
    meta.setErrorColumnMismatch(true);
    meta.setFields(List.of(new RedshiftBulkLoaderField("order_note", "note")));

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("name"));
    transform.setInputRowMeta(rowMeta);

    assertThrows(HopException.class, () -> transform.prepareRowMapping());
  }

  /** A column that does not exist in the table has to be caught before the file is written. */
  @Test
  void refusesToStartWhenAColumnIsNotInTheTable() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new RedshiftBulkLoaderField("order_totl", "amount")));
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"ORDER_ID", "INTEGER"});

    HopException e = assertThrows(HopException.class, () -> transform.verifyTableFields());
    assertTrue(e.getMessage().contains("order_totl"), e.getMessage());
  }

  @Test
  void acceptsAColumnThatDiffersOnlyInCase() throws Exception {
    meta.setSpecifyFields(true);
    meta.setFields(List.of(new RedshiftBulkLoaderField("Order_Id", "id")));
    data.dbFields = new ArrayList<>();
    data.dbFields.add(new String[] {"ORDER_ID", "INTEGER"});

    assertDoesNotThrow(() -> transform.verifyTableFields());
  }

  /** Streaming an empty stream to S3 leaves nothing to load, so the COPY is skipped. */
  @Test
  void skipsTheCopyWhenNothingWasStreamedToS3() throws Exception {
    meta.setStreamToS3Csv(true);
    data.rowsReceived = false;

    assertFalse(transform.shouldExecuteCopy());

    data.rowsReceived = true;
    assertTrue(transform.shouldExecuteCopy());
  }

  /**
   * A file that is already on S3 is loaded even when the stream that triggers the transform is
   * empty, unless the user asked for the load to depend on the rows.
   */
  @Test
  void loadsAnExistingFileEvenOnAnEmptyStream() throws Exception {
    meta.setStreamToS3Csv(false);
    meta.setLoadFromExistingFileFormat(RedshiftBulkLoaderMeta.FILE_FORMAT_CSV);
    data.rowsReceived = false;

    assertTrue(transform.shouldExecuteCopy());

    meta.setOnlyWhenHaveRows(true);
    assertFalse(transform.shouldExecuteCopy());
  }

  /**
   * A JSON value is not a string as far as Hop is concerned, so it used to be written bare. Its
   * commas then split it into extra CSV columns and Redshift saw only a fragment, reporting
   * "End-of-input inside object or array". Hop pretty prints JSON by default, so there are line
   * breaks in there too.
   */
  @Test
  void enclosesJsonSoItSurvivesTheCsvRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaJson("config"));
    JsonNode json =
        new ObjectMapper()
            .readTree(
                "{\"metadataBaseFolder\":\"${PROJECT_HOME}/metadata\","
                    + "\"enforcingExecutionInHome\":true,"
                    + "\"config\":{\"variables\":[]}}");

    String written = writeRow(rowMeta, new Object[] {json}, false);

    // One enclosed field: quoted at both ends, with every inner quote doubled.
    assertTrue(written.startsWith("\""), written);
    assertTrue(written.endsWith("\"\n"), written);
    assertTrue(written.contains("\"\"metadataBaseFolder\"\""), written);
    // The pretty printed line breaks have to sit inside the quotes, not end the row early.
    assertEquals(1, written.split("\n").length - countNewlinesInsideQuotes(written), written);
  }

  /** Numbers must stay bare, so the common case pays nothing for the fix above. */
  @Test
  void doesNotEncloseValuesThatCannotBreakTheRow() throws Exception {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("amount"));

    assertEquals("42\n", writeRow(rowMeta, new Object[] {42L}, false));
  }

  private int countNewlinesInsideQuotes(String written) {
    // everything up to the final closing quote is one field
    int lastQuote = written.lastIndexOf('"');
    return (int) written.substring(0, lastQuote).chars().filter(c -> c == '\n').count();
  }

  /**
   * The staging file is the transform's own business, so a folder that is not there yet gets
   * created rather than refused. On S3 this is the common case: a prefix only exists while an
   * object sits under it, so any brand new path failed with "Parent directory does not exist".
   */
  @Test
  void createsTheStagingFolderWhenItDoesNotExistYet(@TempDir Path tempDir) throws Exception {
    Path missing = tempDir.resolve("not-there-yet/nested");
    assertFalse(Files.exists(missing));

    transform.ensureParentFolderExists(missing.resolve("stage.csv").toString());

    assertTrue(Files.isDirectory(missing), "the staging folder should have been created");
  }

  /** An existing folder is left exactly as it is. */
  @Test
  void leavesAnExistingStagingFolderAlone(@TempDir Path tempDir) throws Exception {
    Path existing = Files.createDirectories(tempDir.resolve("already-there"));

    transform.ensureParentFolderExists(existing.resolve("stage.csv").toString());

    assertTrue(Files.isDirectory(existing));
  }

  // ------------------------------------------------------------ inherited credentials

  /** An access key on the connection is handed straight to the COPY statement. */
  @Test
  void inheritsAnAccessKeyFromTheConnection() throws Exception {
    RedshiftDatabaseMeta connection = new RedshiftDatabaseMeta();
    connection.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    connection.setAwsAccessKeyId("AKIAFROMCONNECTION");
    connection.setAwsSecretAccessKey("connection-secret");
    useConnection(connection);
    meta.setUseConnectionCredentials(true);

    String sql = assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(false));

    assertTrue(
        sql.contains(
            "CREDENTIALS 'aws_access_key_id=AKIAFROMCONNECTION"
                + ";aws_secret_access_key=connection-secret'"),
        sql);
  }

  /** Temporary credentials need their session token to travel with them. */
  @Test
  void inheritsASessionTokenWhenTheConnectionHasOne() throws Exception {
    RedshiftDatabaseMeta connection = new RedshiftDatabaseMeta();
    connection.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    connection.setAwsAccessKeyId("AKIAFROMCONNECTION");
    connection.setAwsSecretAccessKey("connection-secret");
    connection.setAwsSessionToken("temporary-token");
    useConnection(connection);
    meta.setUseConnectionCredentials(true);

    assertTrue(
        assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(false))
            .contains(";token=temporary-token'"));
  }

  /** Variables on the connection are resolved with the transform's own variables. */
  @Test
  void resolvesVariablesInTheInheritedCredentials() throws Exception {
    transform.setVariable("CONN_KEY", "AKIARESOLVED");
    RedshiftDatabaseMeta connection = new RedshiftDatabaseMeta();
    connection.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    connection.setAwsAccessKeyId("${CONN_KEY}");
    connection.setAwsSecretAccessKey("connection-secret");
    useConnection(connection);
    meta.setUseConnectionCredentials(true);

    assertTrue(
        assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(false))
            .contains("aws_access_key_id=AKIARESOLVED"));
  }

  /** A connection signing in with a database password has nothing to lend. */
  @Test
  void refusesToInheritFromAPasswordOnlyConnection() throws Exception {
    RedshiftDatabaseMeta connection = new RedshiftDatabaseMeta();
    connection.setAuthenticationType(RedshiftAuthenticationType.DATABASE);
    useConnection(connection);
    meta.setUseConnectionCredentials(true);

    assertThrows(HopException.class, () -> transform.buildCopyStatementSqlString(false));
  }

  /** Inherited credentials are secrets like any other, so they stay out of the log. */
  @Test
  void masksInheritedCredentialsInTheLoggedStatement() throws Exception {
    RedshiftDatabaseMeta connection = new RedshiftDatabaseMeta();
    connection.setAuthenticationType(RedshiftAuthenticationType.IAM_CREDENTIALS);
    connection.setAwsAccessKeyId("AKIAFROMCONNECTION");
    connection.setAwsSecretAccessKey("connection-secret");
    useConnection(connection);
    meta.setUseConnectionCredentials(true);

    String logged = assertDoesNotThrow(() -> transform.buildCopyStatementSqlString(true));

    assertFalse(logged.contains("AKIAFROMCONNECTION"), logged);
    assertFalse(logged.contains("connection-secret"), logged);
  }

  private void useConnection(RedshiftDatabaseMeta connection) {
    when(databaseMeta.getIDatabase()).thenReturn(connection);
  }

  private String writeRow(IRowMeta rowMeta, Object[] row, boolean specifyFields) throws Exception {
    meta.setStreamToS3Csv(true);
    meta.setSpecifyFields(specifyFields);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    data.writer = out;
    transform.initBinaryDataFields();
    transform.setInputRowMeta(rowMeta);
    transform.prepareRowMapping();
    transform.writeRowToFile(data.outputRowMeta, row);

    return out.toString(StandardCharsets.UTF_8);
  }
}
