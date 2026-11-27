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

package org.apache.hop.databases.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSupport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Without strict SQL mode MySQL writes a column's implicit default for a NULL in a NOT NULL column
 * of a multi-row INSERT and only warns, where a one-row INSERT fails; a table without transactions
 * does the same in strict mode. So a multi-row INSERT is only planned in strict mode, into an
 * InnoDB or NDB table, read from the session of the open connection. A statement also has to fit in
 * max_allowed_packet.
 */
class MySqlStrictModeMultiRowTest {

  private static final String INNODB =
      "CREATE TABLE `T` (\n  `ID` int NOT NULL,\n  `NAME` varchar(10) DEFAULT NULL,\n"
          + "  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  /** A connection whose session reports this sql_mode, with an InnoDB table T. */
  private static Connection connection(String sqlMode) throws SQLException {
    return connection(sqlMode, INNODB);
  }

  /** A connection whose session reports this sql_mode, and T as SHOW CREATE TABLE reports it. */
  private static Connection connection(String sqlMode, String createTable) throws SQLException {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    ResultSet mode = mock(ResultSet.class);
    when(statement.executeQuery("SELECT @@SESSION.sql_mode")).thenReturn(mode);
    when(mode.next()).thenReturn(true);
    when(mode.getString(1)).thenReturn(sqlMode);
    ResultSet table = mock(ResultSet.class);
    when(statement.executeQuery("SHOW CREATE TABLE `T`")).thenReturn(table);
    when(table.next()).thenReturn(true);
    when(table.getString(2)).thenReturn(createTable);
    ResultSet packet = mock(ResultSet.class);
    when(statement.executeQuery("SELECT @@SESSION.max_allowed_packet")).thenReturn(packet);
    when(packet.next()).thenReturn(true);
    when(packet.getLong(1)).thenReturn(67_108_864L);
    return connection;
  }

  private static DatabaseMetaData metaData(Connection connection) throws SQLException {
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getConnection()).thenReturn(connection);
    when(metaData.getDatabaseMajorVersion()).thenReturn(9);
    when(metaData.getDatabaseMinorVersion()).thenReturn(0);
    return metaData;
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MySqlDatabaseMeta());
    return databaseMeta;
  }

  private static DmlSpec insert() throws Exception {
    return DmlSpec.insert("`T`", List.of(DmlColumn.of("ID"), DmlColumn.of("NAME")));
  }

  private static DmlPlan planInsert(DatabaseMetaData metaData) throws Exception {
    return DmlSupport.planStatements(databaseMeta(), metaData, insert(), 100);
  }

  @Test
  void strictModesAreRecognised() {
    assertTrue(MySqlDatabaseMeta.isStrictSqlMode("STRICT_TRANS_TABLES"));
    assertTrue(
        MySqlDatabaseMeta.isStrictSqlMode(
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO"));
    assertTrue(MySqlDatabaseMeta.isStrictSqlMode("ANSI_QUOTES, strict_all_tables"));
    assertFalse(MySqlDatabaseMeta.isStrictSqlMode(""));
    assertFalse(MySqlDatabaseMeta.isStrictSqlMode(null));
    assertFalse(MySqlDatabaseMeta.isStrictSqlMode("NO_ENGINE_SUBSTITUTION"));
    assertFalse(MySqlDatabaseMeta.isStrictSqlMode("TRADITIONAL_STRICT"));
  }

  @Test
  void aStrictSessionGetsSeveralRowsPerInsert() throws Exception {
    DmlPlan plan = planInsert(metaData(connection("STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION")));

    assertTrue(plan.isSupported());
    assertEquals(100, plan.getRowsPerStatement());
    assertNull(plan.getNote());
    assertEquals(67_108_864L, plan.getMaxStatementBytes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "NO_ENGINE_SUBSTITUTION", "ANSI_QUOTES,ONLY_FULL_GROUP_BY"})
  void aSessionWithoutStrictModeWritesOneRowPerInsert(String sqlMode) throws Exception {
    DmlPlan plan = planInsert(metaData(connection(sqlMode)));

    assertTrue(plan.isSupported(), "one row per statement still works");
    assertEquals(1, plan.getRowsPerStatement());
    assertNotNull(plan.getNote());
    assertTrue(plan.getNote().contains("STRICT_TRANS_TABLES"), plan.getNote());
    assertTrue(plan.getNote().contains("'" + sqlMode + "'"), plan.getNote());
  }

  /**
   * In STRICT_TRANS_TABLES mode a MyISAM table takes "(2,'ok'),(3,NULL),(4,'toolong')" as 'ok', ''
   * and 'toolo' with two warnings, where the one-row INSERT of row 3 fails.
   */
  @ParameterizedTest
  @ValueSource(strings = {"MyISAM", "MEMORY", "ARCHIVE", "CSV"})
  void aTableWithoutTransactionsGetsOneRowPerInsert(String engine) throws Exception {
    String createTable = INNODB.replace("ENGINE=InnoDB", "ENGINE=" + engine);
    DmlPlan plan = planInsert(metaData(connection("STRICT_TRANS_TABLES", createTable)));

    assertEquals(1, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains(engine), plan.getNote());
    assertTrue(plan.getNote().contains("`T`"), plan.getNote());
  }

  @Test
  void aViewOrAnUnreadableEngineGetsOneRowPerInsert() throws Exception {
    String view =
        "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `T` AS select"
            + " `X`.`ID` AS `ID` from `X`";
    assertEquals(
        1, planInsert(metaData(connection("STRICT_ALL_TABLES", view))).getRowsPerStatement());

    Connection connection = connection("STRICT_ALL_TABLES");
    Statement failing = mock(Statement.class);
    ResultSet mode = mock(ResultSet.class);
    when(failing.executeQuery("SELECT @@SESSION.sql_mode")).thenReturn(mode);
    when(mode.next()).thenReturn(true);
    when(mode.getString(1)).thenReturn("STRICT_ALL_TABLES");
    when(failing.executeQuery("SHOW CREATE TABLE `T`")).thenThrow(new SQLException("denied"));
    when(connection.createStatement()).thenReturn(failing);
    DmlPlan plan = planInsert(metaData(connection));
    assertEquals(1, plan.getRowsPerStatement());
    assertTrue(plan.getNote().contains("denied"), plan.getNote());
  }

  @Test
  void theEngineIsReadAfterTheColumnDefinitions() {
    assertEquals("InnoDB", MySqlDatabaseMeta.tableEngine(INNODB));
    // An "ENGINE=" in a comment, a default or a quoted name doesn't count.
    assertEquals(
        "MyISAM",
        MySqlDatabaseMeta.tableEngine(
            "CREATE TABLE `ENGINE=InnoDB` (\n  `a` varchar(9) DEFAULT 'ENGINE=InnoDB)',\n"
                + "  `b` int COMMENT 'it''s (ENGINE=InnoDB'\n) ENGINE=MyISAM COMMENT='x'"));
    // A partitioned table names the engine of each partition after its own.
    assertEquals(
        "InnoDB",
        MySqlDatabaseMeta.tableEngine(
            "CREATE TABLE `p` (\n  `id` int NOT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n"
                + "/*!50100 PARTITION BY RANGE (`id`)\n(PARTITION p0 VALUES LESS THAN (10)"
                + " ENGINE = MyISAM) */"));
    assertNull(MySqlDatabaseMeta.tableEngine("CREATE VIEW `v` AS select 1 AS `a`"));
    assertNull(MySqlDatabaseMeta.tableEngine(null));
  }

  @Test
  void onlyAnInsertIsRefused() throws Exception {
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    Connection loose = connection("NO_ENGINE_SUBSTITUTION");
    DmlSpec upsert =
        DmlSpec.upsert(
            "`T`", List.of(DmlColumn.of("ID")), List.of(DmlColumn.of("NAME")), List.of(), false);

    assertNotNull(dialect.getMultiRowDmlRefusal(loose, insert()));
    // An upsert is written one row per statement, so the session doesn't matter for it here.
    assertNull(dialect.getMultiRowDmlRefusal(loose, upsert));
    assertNull(dialect.getMultiRowDmlRefusal(null, insert()));
  }

  @Test
  void anUnreadableModeRefusesSeveralRows() throws Exception {
    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenThrow(new SQLException("not supported"));

    String refusal = new MySqlDatabaseMeta().getMultiRowDmlRefusal(connection, insert());
    assertNotNull(refusal);
    assertTrue(refusal.contains("not supported"), refusal);
  }

  @Test
  void theStatementLimitIsTheLowerOfTheServersAndTheConnectionsPacketSize() throws Exception {
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    assertEquals(67_108_864L, dialect.getMaxDmlStatementBytes(connection("STRICT_ALL_TABLES")));

    dialect.addExtraOption("MYSQL", "maxAllowedPacket", "1m");
    assertEquals(1_048_576L, dialect.getMaxDmlStatementBytes(connection("STRICT_ALL_TABLES")));
    // Connector/J ignores a client value above the server's.
    dialect.addExtraOption("MYSQL", "maxAllowedPacket", "1g");
    assertEquals(67_108_864L, dialect.getMaxDmlStatementBytes(connection("STRICT_ALL_TABLES")));

    // Unreadable, the 4 MB default of MySQL 5.7 is assumed.
    Connection connection = mock(Connection.class);
    when(connection.createStatement()).thenThrow(new SQLException("not supported"));
    assertEquals(4_194_304L, new MySqlDatabaseMeta().getMaxDmlStatementBytes(connection));

    assertEquals(65_536L, MySqlDatabaseMeta.byteSize(" 64K "));
    assertEquals(16_777_216L, MySqlDatabaseMeta.byteSize("16777216"));
    assertEquals(0, MySqlDatabaseMeta.byteSize("lots"));
    assertEquals(0, MySqlDatabaseMeta.byteSize(null));
  }

  @Test
  void oneRowPerStatementAskedForNeedsNoQuery() throws Exception {
    Connection connection = connection("");
    DmlSpec spec = DmlSpec.insert("`T`", List.of(DmlColumn.of("ID")));

    DmlPlan plan = DmlSupport.planStatements(databaseMeta(), metaData(connection), spec, 1);

    assertEquals(1, plan.getRowsPerStatement());
    assertNull(plan.getNote());
    verify(connection, never()).createStatement();
    verify(connection, never()).prepareStatement(anyString());
  }
}
