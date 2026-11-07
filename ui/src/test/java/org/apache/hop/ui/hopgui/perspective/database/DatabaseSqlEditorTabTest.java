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

package org.apache.hop.ui.hopgui.perspective.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.row.RowMeta;
import org.junit.jupiter.api.Test;

class DatabaseSqlEditorTabTest {

  @Test
  void queryResultMessageUnderTheCap() {
    assertEquals("Query 1: 12 row(s)", DatabaseSqlEditorTab.queryResultMessage(1, 12, 1000, false));
  }

  @Test
  void queryResultMessageAboveTheCapExplainsTheLimit() {
    String message = DatabaseSqlEditorTab.queryResultMessage(1, 1000, 1000, true);
    assertTrue(message.startsWith("Query 1: 1000 row(s)"));
    assertTrue(message.contains("capped at 1000"));
    assertTrue(message.contains("Database Perspective"));
    assertTrue(message.contains("SQL LIMIT"));
  }

  @Test
  void queryResultMessageBelowCapDoesNotMentionTheOption() {
    String message = DatabaseSqlEditorTab.queryResultMessage(2, 50, 2000, false);
    assertEquals("Query 2: 50 row(s)", message);
    assertFalse(message.contains("capped"));
  }

  @Test
  void resultOfExactlyTheLimitIsNotCapped() {
    List<DatabaseResultsPanel.QueryResult> results = new ArrayList<>();
    StringBuilder messages = new StringBuilder();

    DatabaseSqlEditorTab.addQueryResult(1, new RowMeta(), rows(3), 3, results, messages);

    assertEquals(3, results.get(0).getRows().size());
    assertFalse(messages.toString().contains("capped"));
  }

  @Test
  void resultAboveTheLimitIsCutAndSaysSo() {
    List<DatabaseResultsPanel.QueryResult> results = new ArrayList<>();
    StringBuilder messages = new StringBuilder();

    // The editor reads one row more than the limit to tell the two cases apart.
    DatabaseSqlEditorTab.addQueryResult(1, new RowMeta(), rows(4), 3, results, messages);

    assertEquals(3, results.get(0).getRows().size());
    assertTrue(messages.toString().startsWith("Query 1: 3 row(s)"));
    assertTrue(messages.toString().contains("capped at 3"));
  }

  @Test
  void executedMessageReportsAffectedRowsOfDml() {
    assertEquals(
        "OK, 5 row(s) affected: UPDATE t SET v = 1",
        DatabaseSqlEditorTab.executedMessage("UPDATE t SET v = 1\n", 5));
    assertEquals(
        "OK, 0 row(s) affected: DELETE FROM t WHERE 1 = 0",
        DatabaseSqlEditorTab.executedMessage("DELETE FROM t WHERE 1 = 0", 0));
    assertEquals(
        "OK, 2 row(s) affected: WITH s AS (SELECT 1) DELETE FROM t",
        DatabaseSqlEditorTab.executedMessage("WITH s AS (SELECT 1) DELETE FROM t", 2));
  }

  @Test
  void executedMessageLeavesDdlAlone() {
    assertEquals(
        "OK: CREATE TABLE t (id INT)",
        DatabaseSqlEditorTab.executedMessage("CREATE TABLE t (id INT)", 0));
    assertEquals("OK: SET SCHEMA s", DatabaseSqlEditorTab.executedMessage("SET SCHEMA s", -1));
    // sqlite-jdbc repeats the count of the previous INSERT after a CREATE TABLE.
    assertEquals(
        "OK: CREATE TABLE u (x INT)",
        DatabaseSqlEditorTab.executedMessage("CREATE TABLE u (x INT)", 1));
  }

  private static List<Object[]> rows(int count) {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      rows.add(new Object[] {(long) i});
    }
    return rows;
  }

  @Test
  void restoredOrLoadedTextIsNotAUserEdit() {
    assertFalse(DatabaseSqlEditorTab.isModifiedFromClean("SELECT 1", "SELECT 1"));
    assertFalse(DatabaseSqlEditorTab.isModifiedFromClean(null, ""));
    assertFalse(DatabaseSqlEditorTab.isModifiedFromClean("", null));
  }

  @Test
  void typingAfterRestoreIsAUserEdit() {
    assertTrue(DatabaseSqlEditorTab.isModifiedFromClean("SELECT 2", "SELECT 1"));
    assertTrue(DatabaseSqlEditorTab.isModifiedFromClean("SELECT 1", ""));
  }

  @Test
  void sqlFailureTextUsesTheDatabaseMessage() {
    Exception cause =
        new Exception("ERROR: relation \"order_shipment_tracking\" does not exist\n  Position: 64");
    String text =
        DatabaseSqlEditorTab.sqlFailureText(
            new HopDatabaseException("An error occurred executing SQL: \nSELECT 1", cause));
    assertEquals("ERROR: relation \"order_shipment_tracking\" does not exist", text);
  }

  @Test
  void sqlFailureTextKeepsASingleLineAndSurvivesANull() {
    assertEquals(
        "relation missing",
        DatabaseSqlEditorTab.sqlFailureText(new Exception("relation missing\r\nmore")));
    assertEquals("", DatabaseSqlEditorTab.sqlFailureText(null));
  }

  @Test
  void shutdownRaceIsNotAQueryFailure() {
    assertTrue(DatabaseSqlEditorTab.isShutdownRace(new IllegalStateException("Already disposed")));
    assertTrue(
        DatabaseSqlEditorTab.isShutdownRace(new IllegalStateException("Widget is disposed")));
    assertTrue(
        DatabaseSqlEditorTab.isShutdownRace(
            new RuntimeException("wrapper", new IllegalStateException("Device is disposed"))));
    assertFalse(
        DatabaseSqlEditorTab.isShutdownRace(
            new HopDatabaseException("ERROR: relation \"t\" does not exist")));
    assertFalse(DatabaseSqlEditorTab.isShutdownRace(null));
  }

  @Test
  void generatedSqlCaretSitsOnTheLastStatement() {
    String statement = "CREATE TABLE t (id INT);";
    assertEquals(
        statement.length(), DatabaseSqlEditorTab.generatedSqlCaretOffset(statement + "\n"));
    assertEquals(
        statement.length(), DatabaseSqlEditorTab.generatedSqlCaretOffset(statement + "\r\n\n"));
    assertEquals(0, DatabaseSqlEditorTab.generatedSqlCaretOffset(null));
    assertEquals(0, DatabaseSqlEditorTab.generatedSqlCaretOffset("\r\n"));
    assertEquals(statement.length(), DatabaseSqlEditorTab.generatedSqlCaretOffset(statement));
  }
}
