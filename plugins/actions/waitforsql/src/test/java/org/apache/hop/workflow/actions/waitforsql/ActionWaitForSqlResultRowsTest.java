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

package org.apache.hop.workflow.actions.waitforsql;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.UUID;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.actions.waitforsql.ActionWaitForSql.SuccessCondition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Reproduction for issue #2483 - "Wait for SQL: Passing date row results to pipeline".
 *
 * <p>With "Add rows to result" the action reads its values with the metadata of the live result set
 * but describes them with a second, independent derivation ({@code Database.getQueryFields}). The
 * two are married in {@code new RowMetaAndData(rowMeta, objects)} without any check that they
 * agree. When they don't, the action hands downstream a row whose {@link IRowMeta} does not
 * describe its own values. Nothing notices at that point; it surfaces much later, when the
 * execution information platform serializes the row, as
 *
 * <pre>
 * SAMPLEDATE Timestamp : There was a data type error: the data type of java.lang.String
 * object ['2023-02-27T13:00:00.000'] does not correspond to value meta [Timestamp]
 * (through reference chain: org.apache.hop.execution.ExecutionData["rowsBinaryGzipBase64Encoded"])
 * </pre>
 *
 * <p>In the reporter's case the two derivations diverge inside the SQL Server driver, which is not
 * something a unit test can force. This test drives the divergence through the other way in, which
 * is driver independent: {@code getQueryFields} is served from the process wide {@link DbCache},
 * which is keyed on nothing but the connection name and the SQL text. Two Hop projects or
 * environments that both define a connection called "warehouse" share that key, so the second run
 * gets the first run's column types. Hop only invalidates that cache when it runs the DDL itself
 * (see {@code Database.execStatement}), which does not help across databases.
 */
class ActionWaitForSqlResultRowsTest {

  private static final String CONNECTION_NAME = "issue-2483";
  private static final String CUSTOM_SQL = "SELECT ID, SAMPLEDATE FROM SAMPLE";

  private Variables variables;
  private MemoryMetadataProvider metadataProvider;
  private WorkflowMeta workflowMeta;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() {
    DbCache.clearAll();
    variables = new Variables();
    metadataProvider = new MemoryMetadataProvider();
    workflowMeta = new WorkflowMeta();
    workflowMeta.setMetadataProvider(metadataProvider);
  }

  @AfterEach
  void tearDown() {
    DbCache.clearAll();
  }

  @Test
  void resultRowMetadataDescribesTheValuesThatWereActuallyRead() throws Exception {
    // Two databases, one connection name. Think "dev" and "prod", or two projects that both call
    // their connection "warehouse", running in the same Hop GUI or Hop Server process.
    //
    DatabaseMeta timestampDb = connectionTo("mem:issue2483_ts_" + UUID.randomUUID());
    DatabaseMeta varcharDb = connectionTo("mem:issue2483_vc_" + UUID.randomUUID());

    // All DDL up front: Database.execStatement clears the DbCache for the connection name on
    // CREATE/ALTER/DROP TABLE, so doing this later would hide the effect we are after.
    //
    execute(
        timestampDb,
        "CREATE TABLE SAMPLE(ID INT, SAMPLEDATE TIMESTAMP)",
        "INSERT INTO SAMPLE VALUES(1, TIMESTAMP '2023-02-27 13:00:00')");
    execute(
        varcharDb,
        "CREATE TABLE SAMPLE(ID INT, SAMPLEDATE VARCHAR(30))",
        "INSERT INTO SAMPLE VALUES(1, '2023-02-27T13:00:00.000')");

    // Run against the database where SAMPLEDATE is a TIMESTAMP. This is the run that fills the
    // DbCache entry for (connection name, SQL).
    //
    RowMetaAndData timestampRow = runActionAgainst(timestampDb);
    assertEquals(
        "Timestamp",
        timestampRow.getRowMeta().getValueMeta(1).getTypeDesc(),
        "sanity check: an H2 TIMESTAMP column is read as a Hop Timestamp");

    // Same action, same connection name, same SQL, other database.
    //
    RowMetaAndData varcharRow = runActionAgainst(varcharDb);

    assertEquals(
        String.class,
        varcharRow.getData()[1].getClass(),
        "sanity check: the value the action read really is a String");
    assertEquals(
        "String",
        varcharRow.getRowMeta().getValueMeta(1).getTypeDesc(),
        "the result row metadata must describe the values the action actually read");

    // What the execution information platform does with every result row of an action:
    // ExecutionData.getRowsBinaryGzipBase64Encoded() -> IRowMeta.writeData(). A row whose metadata
    // lies about its own values blows up here, far away from where it was built.
    //
    assertDoesNotThrow(() -> writeTheWayExecutionDataDoes(varcharRow));
  }

  private DatabaseMeta connectionTo(String database) {
    // DB_CLOSE_DELAY=-1 keeps the in-memory database alive between connections
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            CONNECTION_NAME, "H2", "Native", "", database + ";DB_CLOSE_DELAY=-1", "", "", "");
    databaseMeta.setSupportsTimestampDataType(true);
    return databaseMeta;
  }

  private RowMetaAndData runActionAgainst(DatabaseMeta databaseMeta) throws Exception {
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);

    ActionWaitForSql action = new ActionWaitForSql("wait for sql");
    action.setParentWorkflowMeta(workflowMeta);
    action.setConnection(CONNECTION_NAME);
    action.setCustomSqlEnabled(true);
    action.setCustomSql(CUSTOM_SQL);
    action.setAddRowsResult(true);
    action.setSuccessCondition(SuccessCondition.ROWS_COUNT_GREATER);

    Result result = new Result();
    assertTrue(
        action.sqlDataOK(result, 0, null, null, CUSTOM_SQL),
        "the action should see the single row we inserted");
    assertEquals(1, result.getRows().size());
    return result.getRows().get(0);
  }

  private void writeTheWayExecutionDataDoes(RowMetaAndData row) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos)) {
      row.getRowMeta().writeMeta(dos);
      row.getRowMeta().writeData(dos, row.getData());
    }
  }

  private void execute(DatabaseMeta databaseMeta, String... statements) throws Exception {
    try (Database db =
        new Database(
            new LoggingObject("ActionWaitForSqlResultRowsTest"), variables, databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }
}
