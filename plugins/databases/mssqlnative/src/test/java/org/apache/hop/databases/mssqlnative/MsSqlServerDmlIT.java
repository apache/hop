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

package org.apache.hop.databases.mssqlnative;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.databases.mssql.MsSqlServerDatabaseMeta;
import org.apache.hop.junit.dml.RealDriverDmlScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the INSERT of several rows the SQL Server dialects declare against SQL Server 2022 through a
 * multi-row batch, also at the limits of 2098 parameters and 1000 rows in an INSERT. Needs Docker
 * (the image is amd64 only, so it runs emulated on ARM); run with {@code -Dhop.it.dml=true}.
 */
@EnabledIfSystemProperty(named = "hop.it.dml", matches = "true")
class MsSqlServerDmlIT {

  private static final String PASSWORD = "Hop_Merge_2026!";
  private static GenericContainer<?> container;

  @BeforeAll
  static void startContainer() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    container =
        new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
            .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("MSSQL_SA_PASSWORD", PASSWORD)
            .withExposedPorts(1433)
            .waitingFor(
                Wait.forLogMessage(".*SQL Server is now ready for client connections.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(5)));
    container.start();
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static Connection connect() throws Exception {
    String url =
        "jdbc:sqlserver://"
            + container.getHost()
            + ":"
            + container.getMappedPort(1433)
            + ";encrypt=false;trustServerCertificate=true";
    // The server logs "ready" a moment before it accepts logins.
    for (int attempt = 0; ; attempt++) {
      try {
        return DriverManager.getConnection(url, "sa", PASSWORD);
      } catch (SQLException e) {
        if (attempt > 30) {
          throw e;
        }
        Thread.sleep(1000);
      }
    }
  }

  private static DatabaseMeta databaseMeta(boolean nativeDialect) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(
        nativeDialect ? new MsSqlServerNativeDatabaseMeta() : new MsSqlServerDatabaseMeta());
    return databaseMeta;
  }

  @ParameterizedTest
  @CsvSource({"1, true", "2, true", "5, true", "100, true", "2, false", "100, false"})
  void theDeclaredInsertWorks(int rowsPerStatement, boolean nativeDialect) throws Exception {
    try (Connection connection = connect()) {
      String server = "sqlserver" + (nativeDialect ? " native" : "") + " x" + rowsPerStatement;
      System.out.println(server + ": " + connection.getMetaData().getDatabaseProductVersion());
      RealDriverDmlScenario.run(
              connection, databaseMeta(nativeDialect), "DML_TEST", rowsPerStatement)
          .forEach(sql -> System.out.println(server + ": " + sql));
    }
  }

  @Test
  void anInsertAtTheLimitsWorks() throws Exception {
    try (Connection connection = connect()) {
      System.out.println(
          "sqlserver: "
              + RealDriverDmlScenario.runAtLimits(
                  connection, databaseMeta(true), "DML_LIMIT", 100_000));
    }
  }

  /**
   * A table of 1000 columns, 999 of them VARCHAR(5), about as wide as a row fits in a page: every
   * parameter of the VALUES list is cast to its column type, so the statement is several times
   * longer than its parameters.
   */
  @Test
  void aWideTableIsWrittenInStatementsOfSeveralRows() throws Exception {
    try (Connection connection = connect()) {
      System.out.println(
          "sqlserver: "
              + RealDriverDmlScenario.runWide(
                  connection, databaseMeta(true), "DML_WIDE", 999, "VARCHAR(5)", 5, 1000));
    }
  }

  /**
   * The limits are real: 2099 parameters, or 1001 rows in the VALUES list of an INSERT, fail. The
   * server takes 2100, and mssql-jdbc's sp_prepexec adds two of its own.
   */
  @ParameterizedTest
  @ValueSource(ints = {2098, 2099})
  void theLimitsAreReal(int parameters) throws Exception {
    try (Connection connection = connect()) {
      StringBuilder sql = new StringBuilder("SELECT 1 WHERE 1 IN (");
      for (int i = 0; i < parameters; i++) {
        sql.append(i == 0 ? "?" : ", ?");
      }
      sql.append(')');
      try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
        for (int i = 1; i <= parameters; i++) {
          statement.setInt(i, i);
        }
        if (parameters <= 2098) {
          statement.executeQuery().close();
        } else {
          SQLException e = assertThrows(SQLException.class, statement::executeQuery);
          System.out.println("SQL Server with " + parameters + " parameters: " + e.getMessage());
        }
      }

      if (parameters > 2098) {
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE DML_ROWS (ID INT)");
          StringBuilder insert = new StringBuilder("INSERT INTO DML_ROWS (ID) VALUES ");
          for (int i = 0; i < 1001; i++) {
            insert.append(i == 0 ? "(?)" : ", (?)");
          }
          try (PreparedStatement prepared = connection.prepareStatement(insert.toString())) {
            for (int i = 1; i <= 1001; i++) {
              prepared.setInt(i, i);
            }
            SQLException e = assertThrows(SQLException.class, prepared::executeUpdate);
            System.out.println("SQL Server INSERT of 1001 rows: " + e.getMessage());
          } finally {
            statement.execute("DROP TABLE DML_ROWS");
          }
        }
      }
    }
  }

  /**
   * Why every parameter of an INSERT of several rows is cast: SQL Server types a VALUES list of
   * several rows from its parameters, and two {@code BigDecimal}s of different scales overflow 38
   * digits, so the scale is cut without an error. The dialect's statement, with casts, keeps it.
   */
  @Test
  void aValuesListWithoutCastsRoundsDecimals() throws Exception {
    try (Connection connection = connect()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE DML_ROUND (ID INT IDENTITY PRIMARY KEY, N NUMERIC(10,2))");
      }
      try {
        try (PreparedStatement statement =
            connection.prepareStatement("INSERT INTO DML_ROUND (N) VALUES (?), (?)")) {
          statement.setBigDecimal(1, new BigDecimal("1"));
          statement.setBigDecimal(2, new BigDecimal("2.25"));
          statement.executeUpdate();
        }
        assertEquals(List.of(new BigDecimal("1.00"), new BigDecimal("2.00")), read(connection));

        List<DmlColumn> columns =
            DmlColumns.fromTable(
                connection.getMetaData(),
                new MsSqlServerNativeDatabaseMeta(),
                null,
                "DML_ROUND",
                List.of("N"));
        assertEquals("numeric(38,30)", columns.get(0).nativeType());
        String sql =
            DmlStatementBuilder.insert(
                    databaseMeta(true),
                    new MsSqlServerNativeDatabaseMeta().getDmlSyntax(),
                    DmlSpec.insert("DML_ROUND", columns),
                    2)
                .getSql();
        System.out.println("SQL Server multi-row INSERT: " + sql);
        try (Statement statement = connection.createStatement()) {
          statement.execute("DELETE FROM DML_ROUND");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
          statement.setBigDecimal(1, new BigDecimal("1"));
          statement.setBigDecimal(2, new BigDecimal("2.25"));
          statement.executeUpdate();
        }
        assertEquals(List.of(new BigDecimal("1.00"), new BigDecimal("2.25")), read(connection));
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE DML_ROUND");
        }
      }
    }
  }

  private static List<BigDecimal> read(Connection connection) throws SQLException {
    List<BigDecimal> values = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT N FROM DML_ROUND ORDER BY ID")) {
      while (resultSet.next()) {
        values.add(resultSet.getBigDecimal(1));
      }
    }
    return values;
  }

  /**
   * Text in columns whose collation differs from the database's (a UTF-8 collation and Greek in a
   * Latin1 database) keeps every character with several rows per statement, as with one: a VALUES
   * list casts character values to nvarchar(max), and the column converts them. A cast to
   * varchar(max) would convert them to the database's code page first, writing '?' without an
   * error. A value too long for its column is still refused.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void textKeepsEveryCharacterInAColumnOfAnotherCollation(boolean nativeDialect) throws Exception {
    try (Connection connection = connect()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE DML_COLLATION (ID INT PRIMARY KEY,"
                + " U VARCHAR(20) COLLATE Latin1_General_100_CI_AS_SC_UTF8,"
                + " G VARCHAR(20) COLLATE Greek_CI_AS, T TEXT COLLATE Greek_CI_AS,"
                + " C CHAR(5) COLLATE Greek_CI_AS)");
      }
      try {
        List<DmlColumn> columns =
            DmlColumns.fromTable(
                connection.getMetaData(),
                databaseMeta(nativeDialect).getIDatabase(),
                null,
                "DML_COLLATION",
                List.of("ID", "U", "G", "T", "C"));
        String sql =
            DmlStatementBuilder.insert(
                    databaseMeta(nativeDialect),
                    databaseMeta(nativeDialect).getIDatabase().getDmlSyntax(),
                    DmlSpec.insert("DML_COLLATION", columns),
                    2)
                .getSql();
        System.out.println("SQL Server multi-row INSERT: " + sql);
        insertTwo(connection, sql, "中文 αβγ");
        List<String> expected = List.of("1|中文 αβγ|αβγ|αβγ|αβ   ", "2|中文 αβγ|αβγ|αβγ|αβ   ");
        assertEquals(expected, readCollation(connection));

        // What the cast to varchar(max) did: the database's code page lacks these characters.
        try (Statement statement = connection.createStatement()) {
          statement.execute("DELETE FROM DML_COLLATION");
        }
        insertTwo(connection, sql.replace("nvarchar(max)", "varchar(max)"), "中文 αβγ");
        assertEquals(
            List.of(false, false),
            readCollation(connection).stream().map(line -> line.contains("αβγ")).toList());

        // Too long for VARCHAR(20) in UTF-8: refused, as by a one-row INSERT.
        try (Statement statement = connection.createStatement()) {
          statement.execute("DELETE FROM DML_COLLATION");
        }
        assertThrows(SQLException.class, () -> insertTwo(connection, sql, "中文中文中文中文"));
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE DML_COLLATION");
        }
      }
    }
  }

  /** Inserts rows 1 and 2 with the same text in every text column. */
  private static void insertTwo(Connection connection, String sql, String text)
      throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int row = 0; row < 2; row++) {
        statement.setInt(row * 5 + 1, row + 1);
        statement.setString(row * 5 + 2, text);
        statement.setString(row * 5 + 3, "αβγ");
        statement.setString(row * 5 + 4, "αβγ");
        statement.setString(row * 5 + 5, "αβ");
      }
      statement.executeUpdate();
    }
  }

  private static List<String> readCollation(Connection connection) throws SQLException {
    List<String> lines = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("SELECT ID, U, G, T, C FROM DML_COLLATION ORDER BY ID")) {
      while (resultSet.next()) {
        lines.add(
            resultSet.getInt(1)
                + "|"
                + resultSet.getString(2)
                + "|"
                + resultSet.getString(3)
                + "|"
                + resultSet.getString(4)
                + "|"
                + resultSet.getString(5));
      }
    }
    return lines;
  }

  /** The SQL Server types of a table, as the casts write them. */
  @Test
  void castTypesOfSqlServerColumns() throws Exception {
    try (Connection connection = connect()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE DML_TYPES (ID INT IDENTITY PRIMARY KEY, N NUMERIC(10,2), V VARCHAR(MAX),"
                + " NV NVARCHAR(MAX), NV2 NVARCHAR(20), VB VARBINARY(MAX), TS DATETIME2, M MONEY,"
                + " X XML, B BIT, U UNIQUEIDENTIFIER, C CHAR(3), TM TIME, DO DATETIMEOFFSET)");
      }
      try {
        List<String> names =
            List.of("ID", "N", "V", "NV", "NV2", "VB", "TS", "M", "X", "B", "U", "C", "TM", "DO");
        List<DmlColumn> columns =
            DmlColumns.fromTable(
                connection.getMetaData(),
                new MsSqlServerNativeDatabaseMeta(),
                null,
                "DML_TYPES",
                names);
        System.out.println("SQL Server cast types: " + columns);
        assertEquals(
            List.of(
                "int",
                "numeric(38,30)",
                "varchar(max)",
                "nvarchar(max)",
                "nvarchar(max)",
                "varbinary(max)",
                "datetime2",
                "money",
                "xml",
                "bit",
                "uniqueidentifier",
                "varchar(max)",
                "time",
                "datetimeoffset"),
            columns.stream().map(DmlColumn::nativeType).toList());
        // Every one of them is a valid cast.
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < columns.size(); i++) {
          sql.append(i == 0 ? "" : ", ")
              .append("CAST(NULL AS ")
              .append(columns.get(i).nativeType());
          sql.append(')');
        }
        try (Statement statement = connection.createStatement()) {
          statement.executeQuery(sql.toString()).close();
        }
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE DML_TYPES");
        }
      }
    }
  }

  /** Times 100,000 inserted rows. Run with {@code -Dhop.dml.benchmark=true}. */
  @Test
  @EnabledIfSystemProperty(named = "hop.dml.benchmark", matches = "true")
  void measure() throws Exception {
    try (Connection connection = connect()) {
      RealDriverDmlScenario.measure(connection, databaseMeta(true), "DML_BENCH", 100_000, 100, 1000)
          .forEach(line -> System.out.println("sqlserver BENCH " + line));
    }
  }
}
