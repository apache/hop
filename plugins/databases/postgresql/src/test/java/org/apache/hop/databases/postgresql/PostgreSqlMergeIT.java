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

package org.apache.hop.databases.postgresql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Runs the declared MERGE syntax against PostgreSQL 18, and checks that PostgreSQL 14, which has no
 * MERGE, is refused by version. Needs Docker; run with {@code -Dtest=PostgreSqlMergeIT}.
 */
class PostgreSqlMergeIT {

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new PostgreSqlDatabaseMeta());
    return databaseMeta;
  }

  private static Connection connect(PostgreSQLContainer<?> container) throws SQLException {
    return DriverManager.getConnection(
        container.getJdbcUrl(), container.getUsername(), container.getPassword());
  }

  @Test
  void postgres18() throws Exception {
    try (PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:18")) {
      container.start();
      try (Connection connection = connect(container)) {
        assertNull(
            DmlSupport.unsupportedReason(
                databaseMeta(), connection.getMetaData(), UpsertForm.MERGE));
        RealDriverMergeScenario.Report report =
            RealDriverMergeScenario.run(connection, databaseMeta(), "merge_test");
        System.out.println("PostgreSQL 18 MERGE update counts: " + report);
        report =
            RealDriverMergeScenario.run(
                connection, databaseMeta(), "merge_test", UpsertForm.ON_CONFLICT);
        System.out.println("PostgreSQL 18 ON CONFLICT update counts: " + report);

        withoutCastsTheParametersAreText(connection);
        unconstrainedTypesAreCastWithoutModifier(connection);
        typesWithoutComparisonAreUpdatedAlways(connection, UpsertForm.MERGE);
        theTableOfTheCurrentSchemaGivesTheCasts(connection);
      }
    }
  }

  /**
   * Why the dialect casts. pgjdbc sends a timestamp (and a string bound to a non-text column) as an
   * untyped parameter, and in a VALUES list PostgreSQL resolves an untyped value to text, which
   * then can't be written to the timestamp column.
   */
  private static void withoutCastsTheParametersAreText(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE merge_nocast (id INTEGER PRIMARY KEY, ts TIMESTAMP)");
    }
    try {
      List<DmlColumn> keys = List.of(DmlColumn.of("id"));
      List<DmlColumn> others = List.of(DmlColumn.of("ts"));
      DmlStatement merge =
          DmlStatementBuilder.upsert(
              databaseMeta(),
              DmlSyntax.STANDARD_MERGE,
              DmlSyntax.STANDARD_MERGE.upsert(UpsertForm.MERGE),
              DmlSpec.upsert("merge_nocast", keys, others, others, false));
      try (PreparedStatement statement = connection.prepareStatement(merge.getSql())) {
        statement.setInt(1, 1);
        statement.setTimestamp(2, java.sql.Timestamp.valueOf("2026-10-22 10:11:12"));
        SQLException e = assertThrows(SQLException.class, statement::executeUpdate);
        System.out.println("PostgreSQL MERGE without casts: " + e.getMessage());
      }
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE merge_nocast");
      }
    }
  }

  /** text, unbounded varchar and unconstrained numeric must not get a length or precision. */
  private static void unconstrainedTypesAreCastWithoutModifier(Connection connection)
      throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE TABLE merge_types (id SERIAL PRIMARY KEY, t TEXT, v VARCHAR, n NUMERIC,"
              + " c CHAR(3), d NUMERIC(12,4), ts TIMESTAMP, b BOOLEAN)");
    }
    try {
      List<DmlColumn> columns =
          DmlColumns.fromTable(
              connection.getMetaData(),
              new PostgreSqlDatabaseMeta(),
              "public",
              "MERGE_TYPES",
              List.of("id", "t", "v", "n", "c", "d", "ts", "b"));
      System.out.println("PostgreSQL native types: " + columns);
      assertEquals(
          List.of("int4", "text", "varchar", "numeric", "bpchar", "numeric", "timestamp", "bool"),
          columns.stream().map(DmlColumn::nativeType).toList());

      DmlSyntax syntax =
          PostgreSqlDatabaseMeta.class.getDeclaredConstructor().newInstance().getDmlSyntax();
      DmlStatement merge =
          DmlStatementBuilder.upsert(
              databaseMeta(),
              syntax,
              syntax.upsert(UpsertForm.MERGE),
              DmlSpec.upsert(
                  "merge_types",
                  columns.subList(0, 1),
                  columns.subList(1, columns.size()),
                  columns.subList(1, columns.size()),
                  true));
      try (PreparedStatement statement = connection.prepareStatement(merge.getSql())) {
        statement.setInt(1, 1);
        statement.setString(2, "text");
        statement.setString(3, "varchar");
        statement.setBigDecimal(4, new BigDecimal("12345.6789012345"));
        statement.setString(5, "abc");
        statement.setBigDecimal(6, new BigDecimal("1.2345"));
        statement.setTimestamp(7, java.sql.Timestamp.valueOf("2026-10-22 10:11:12"));
        statement.setBoolean(8, true);
        assertEquals(1, statement.executeUpdate());
        assertEquals(0, statement.executeUpdate(), "an identical row is not updated");
      }
      try (Statement statement = connection.createStatement();
          var resultSet = statement.executeQuery("SELECT n, c FROM merge_types")) {
        assertTrue(resultSet.next());
        assertEquals(new BigDecimal("12345.6789012345"), resultSet.getBigDecimal(1));
        assertEquals("abc", resultSet.getString(2));
      }
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE merge_types");
      }
    }
  }

  /**
   * PostgreSQL 14 has no MERGE: it is refused by version, with the version in the reason, and the
   * server really refuses the statement. ON CONFLICT, which it has, is what Automatic picks there,
   * and it works.
   */
  @Test
  void postgres14IsRefusedByVersion() throws Exception {
    try (PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:14")) {
      container.start();
      try (Connection connection = connect(container)) {
        String reason =
            DmlSupport.unsupportedReason(
                databaseMeta(), connection.getMetaData(), UpsertForm.MERGE);
        System.out.println("PostgreSQL 14: " + reason);
        assertNotNull(reason);
        assertTrue(reason.contains("14") && reason.contains("15"), reason);
        // And the server really has no MERGE.
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE merge_old (id INTEGER PRIMARY KEY)");
          assertThrows(
              SQLException.class,
              () ->
                  statement.execute(
                      "MERGE INTO merge_old t USING (VALUES (1)) AS s (id) ON (t.id = s.id)"
                          + " WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)"));
          statement.execute("DROP TABLE merge_old");
        }
        assertNull(DmlSupport.unsupportedReason(databaseMeta(), connection.getMetaData(), null));
        RealDriverMergeScenario.Report report =
            RealDriverMergeScenario.run(
                connection, databaseMeta(), "merge_test", UpsertForm.ON_CONFLICT);
        System.out.println("PostgreSQL 14 ON CONFLICT update counts: " + report);

        onConflictNeedsAKeyOfThisTable(connection);
      }
      // ON CONFLICT doesn't cast: a string reaches a json column only as an untyped parameter.
      Properties untyped = new Properties();
      untyped.setProperty("user", container.getUsername());
      untyped.setProperty("password", container.getPassword());
      untyped.setProperty("stringtype", "unspecified");
      try (Connection connection = DriverManager.getConnection(container.getJdbcUrl(), untyped)) {
        typesWithoutComparisonAreUpdatedAlways(connection, UpsertForm.ON_CONFLICT);
      }
    }
  }

  /**
   * json and xml have no {@code <>}, point has none, and citext compares ignoring case: "update
   * only changed rows" must update every matched row instead of failing every row, or of leaving a
   * change of case unwritten.
   */
  private static void typesWithoutComparisonAreUpdatedAlways(Connection connection, UpsertForm form)
      throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE EXTENSION IF NOT EXISTS citext");
    }
    String table = "merge_uncomparable";
    RealDriverMergeScenario.uncomparableColumn(
        connection, databaseMeta(), table, form, "json", "{\"v\": \"one\"}", "{\"v\": \"two\"}");
    RealDriverMergeScenario.uncomparableColumn(
        connection, databaseMeta(), table, form, "point", "(1,1)", "(2,2)");
    RealDriverMergeScenario.uncomparableColumn(
        connection, databaseMeta(), table, form, "citext", "John", "john");
    if (form == UpsertForm.MERGE) {
      RealDriverMergeScenario.uncomparableColumn(
          connection, databaseMeta(), table, form, "xml", "<a>one</a>", "<a>two</a>");
    }
  }

  /**
   * Without a schema, the casts come from the table the MERGE writes to, in the search path: not
   * from {@code hop_archive.merge_orders}, which PostgreSQL lists first and whose {@code date}
   * would drop the time of every row.
   */
  private static void theTableOfTheCurrentSchemaGivesTheCasts(Connection connection)
      throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA hop_archive");
      statement.execute(
          "CREATE TABLE hop_archive.merge_orders (id INTEGER PRIMARY KEY, created DATE)");
      statement.execute("CREATE TABLE merge_orders (id INTEGER PRIMARY KEY, created TIMESTAMP)");
    }
    try {
      List<DmlColumn> columns =
          DmlColumns.fromTable(
              connection.getMetaData(),
              new PostgreSqlDatabaseMeta(),
              null,
              "merge_orders",
              List.of("id", "created"));
      assertEquals(
          List.of("int4", "timestamp"), columns.stream().map(DmlColumn::nativeType).toList());
      DmlSyntax syntax = new PostgreSqlDatabaseMeta().getDmlSyntax();
      DmlStatement merge =
          DmlStatementBuilder.upsert(
              databaseMeta(),
              syntax,
              syntax.upsert(UpsertForm.MERGE),
              DmlSpec.upsert(
                  "merge_orders",
                  columns.subList(0, 1),
                  columns.subList(1, 2),
                  columns.subList(1, 2),
                  false));
      java.sql.Timestamp created = java.sql.Timestamp.valueOf("2026-11-25 14:15:16");
      try (PreparedStatement statement = connection.prepareStatement(merge.getSql())) {
        statement.setInt(1, 1);
        statement.setTimestamp(2, created);
        assertEquals(1, statement.executeUpdate());
      }
      try (Statement statement = connection.createStatement();
          var resultSet = statement.executeQuery("SELECT created FROM merge_orders")) {
        assertTrue(resultSet.next());
        assertEquals(created, resultSet.getTimestamp(1), "the time of day is kept");
      }
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE merge_orders");
        statement.execute("DROP SCHEMA hop_archive CASCADE");
      }
    }
  }

  /**
   * ON CONFLICT needs a unique key of the table itself: a primary key of a table of the same name
   * in another schema doesn't count, and neither does a partial unique index, which PostgreSQL
   * can't use as the arbiter of {@code ON CONFLICT (code)}. Both are refused with the reason
   * instead of failing on the first row.
   */
  private static void onConflictNeedsAKeyOfThisTable(Connection connection) throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA hop_other");
      statement.execute("CREATE TABLE hop_other.merge_keys (code VARCHAR(10) PRIMARY KEY)");
      statement.execute("CREATE TABLE merge_keys (id INTEGER, code VARCHAR(10))");
    }
    try {
      DmlSpec spec =
          DmlSpec.upsert(
              "merge_keys",
              List.of(DmlColumn.of("code")),
              List.of(DmlColumn.of("id")),
              List.of(DmlColumn.of("id")),
              false);
      DmlPlan plan =
          DmlSupport.plan(databaseMeta(), connection.getMetaData(), spec, null, "merge_keys", null);
      System.out.println("PostgreSQL 14, key in another schema: " + plan.getUnsupportedReason());
      assertFalse(plan.isSupported(), "the primary key of hop_other.merge_keys doesn't count");

      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE UNIQUE INDEX merge_keys_code ON merge_keys (code) WHERE id > 0");
        // The server really can't use it.
        assertThrows(
            SQLException.class,
            () ->
                statement.execute(
                    "INSERT INTO merge_keys (id, code) VALUES (1, 'a')"
                        + " ON CONFLICT (code) DO UPDATE SET id = EXCLUDED.id"));
      }
      plan =
          DmlSupport.plan(databaseMeta(), connection.getMetaData(), spec, null, "merge_keys", null);
      System.out.println("PostgreSQL 14, partial unique index: " + plan.getUnsupportedReason());
      assertFalse(plan.isSupported(), "a partial unique index is no key for ON CONFLICT");
      assertTrue(plan.getUnsupportedReason().contains("ON_CONFLICT"), plan.getUnsupportedReason());

      // Without the predicate it is.
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP INDEX merge_keys_code");
        statement.execute("CREATE UNIQUE INDEX merge_keys_code ON merge_keys (code)");
      }
      plan =
          DmlSupport.plan(databaseMeta(), connection.getMetaData(), spec, null, "merge_keys", null);
      assertTrue(plan.isSupported(), plan.getUnsupportedReason());
      assertEquals(UpsertForm.ON_CONFLICT, plan.getUpsert().form());
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE merge_keys");
        statement.execute("DROP SCHEMA hop_other CASCADE");
      }
    }
  }
}
