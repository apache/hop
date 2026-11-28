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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.junit.batch.DownloadedJdbcDriver;
import org.apache.hop.junit.dml.RealDriverDmlScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the INSERT of several rows the MySQL dialect declares against MySQL 9 through a multi-row
 * batch, also with server-side prepared statements at the parameter limit, with the Connector/J Hop
 * downloads (it is not shipped, and not a build dependency). Needs Docker; runs with {@code
 * -Dhop.it.dml=true}.
 */
@EnabledIfSystemProperty(named = "hop.it.dml", matches = "true")
class MySqlDmlIT {

  private static final String PASSWORD = "hop-dml";
  private static GenericContainer<?> container;
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void startContainer() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    container =
        new GenericContainer<>("mysql:9")
            .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD)
            .withEnv("MYSQL_DATABASE", "hop")
            .withExposedPorts(3306)
            .waitingFor(Wait.forLogMessage(".*ready for connections.*port: 3306.*\\n", 1));
    container.start();
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    driver =
        DownloadedJdbcDriver.load(
            dialect.getDriverDownload(),
            dialect.getDriverClass(),
            Path.of("target", "jdbc-drivers"));
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }

  private static Connection connect(String options) throws Exception {
    String url =
        "jdbc:mysql://"
            + container.getHost()
            + ":"
            + container.getMappedPort(3306)
            + "/hop?allowPublicKeyRetrieval=true&useSSL=false";
    if (!options.isEmpty()) {
      url += "&" + options;
    }
    return driver.connect(url, "root", PASSWORD);
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new MySqlDatabaseMeta());
    return databaseMeta;
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "1;",
        "2;",
        "5;",
        "100;",
        "2;useServerPrepStmts=true",
        "100;useServerPrepStmts=true",
        "100;rewriteBatchedStatements=true"
      },
      delimiter = ';')
  void theDeclaredInsertWorks(int rowsPerStatement, String options) throws Exception {
    try (Connection connection = connect(options == null ? "" : options)) {
      String server = "mysql x" + rowsPerStatement + (options == null ? "" : " " + options);
      System.out.println(server + ": " + connection.getMetaData().getDatabaseProductVersion());
      RealDriverDmlScenario.run(connection, databaseMeta(), "DML_TEST", rowsPerStatement)
          .forEach(sql -> System.out.println(server + ": " + sql));
    }
  }

  @ParameterizedTest
  @CsvSource(value = {"''", "useServerPrepStmts=true"})
  void anInsertAtTheParameterLimitWorks(String options) throws Exception {
    try (Connection connection = connect(options)) {
      System.out.println(
          "mysql "
              + options
              + ": "
              + RealDriverDmlScenario.runAtLimits(
                  connection, databaseMeta(), "DML_LIMIT", 100_000));
    }
  }

  /**
   * A table of 1000 columns, 999 of them VARCHAR(5) in ASCII: InnoDB keeps short values in the row,
   * and the longest such a row can be has to fit in half a page (8126 bytes) when the table is
   * created. The parameter limit caps a statement at 65 rows.
   */
  @ParameterizedTest
  @CsvSource(value = {"''", "useServerPrepStmts=true"})
  void aWideTableIsWrittenInStatementsOfSeveralRows(String options) throws Exception {
    try (Connection connection = connect(options)) {
      System.out.println(
          "mysql "
              + options
              + ": "
              + RealDriverDmlScenario.runWide(
                  connection,
                  databaseMeta(),
                  "DML_WIDE",
                  999,
                  "VARCHAR(5) CHARACTER SET ascii",
                  5,
                  1000));
    }
  }

  /**
   * The values of one statement have to fit in one packet of the server's max_allowed_packet, where
   * a batch of one-row statements only needs each row to fit. 4 MB is the default of MySQL 5.7 and
   * of many hosted servers; 100 rows of 4 values of 64 KB make a statement of 25 MB.
   */
  @ParameterizedTest
  @CsvSource(value = {"''", "useServerPrepStmts=true"})
  void aStatementLargerThanTheServersPacketLimitIsWritten(String options) throws Exception {
    long previous;
    try (Connection admin = connect("");
        Statement statement = admin.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("SELECT @@GLOBAL.max_allowed_packet")) {
        resultSet.next();
        previous = resultSet.getLong(1);
      }
      statement.execute("SET GLOBAL max_allowed_packet = 4194304");
    }
    // The driver reads the limit when it connects.
    try (Connection connection = connect(options)) {
      System.out.println(
          "mysql max_allowed_packet=4M "
              + options
              + ": "
              + RealDriverDmlScenario.runWide(
                  connection, databaseMeta(), "DML_PACKET", 4, "MEDIUMTEXT", 65_536, 100));
    } finally {
      try (Connection admin = connect("");
          Statement statement = admin.createStatement()) {
        statement.execute("SET GLOBAL max_allowed_packet = " + previous);
      }
    }
  }

  /**
   * Outside strict SQL mode a multi-row INSERT writes the column default for a NULL in a NOT NULL
   * column and only warns, where the one-row INSERT fails. The plan then falls back to one row per
   * statement, and says why.
   */
  @Test
  void withoutStrictModeInsertsAreOneRowPerStatement() throws Exception {
    try (Connection connection = connect("")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS DML_LOOSE");
        statement.execute("CREATE TABLE DML_LOOSE (ID INT PRIMARY KEY, V VARCHAR(10) NOT NULL)");
        statement.execute("SET SESSION sql_mode = ''");
        // What the plan protects against: the bad row is changed instead of refused.
        statement.executeUpdate("INSERT INTO DML_LOOSE VALUES (1, 'a'), (2, NULL)");
        try (ResultSet rs = statement.executeQuery("SELECT V FROM DML_LOOSE WHERE ID = 2")) {
          assertTrue(rs.next());
          assertEquals("", rs.getString(1));
        }
        assertThrows(
            SQLException.class,
            () -> statement.executeUpdate("INSERT INTO DML_LOOSE VALUES (3, NULL)"));
      }
      DmlSpec insert = DmlSpec.insert("DML_LOOSE", List.of(DmlColumn.of("ID"), DmlColumn.of("V")));

      DmlPlan loose =
          DmlSupport.planStatements(databaseMeta(), connection.getMetaData(), insert, 100);
      System.out.println("mysql sql_mode='': " + loose.getNote());
      assertEquals(1, loose.getRowsPerStatement());
      assertTrue(loose.getNote().contains("STRICT_TRANS_TABLES"), loose.getNote());

      try (Statement statement = connection.createStatement()) {
        statement.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'");
      }
      DmlPlan strict =
          DmlSupport.planStatements(databaseMeta(), connection.getMetaData(), insert, 100);
      assertEquals(100, strict.getRowsPerStatement());

      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE DML_LOOSE");
      }
    }
  }

  /**
   * A table without transactions (MyISAM) writes a bad row of a multi-row INSERT with an adjusted
   * value in strict mode too, where a one-row INSERT fails. The plan gives it one row per
   * statement, and an InnoDB table several, a temporary one too.
   */
  @Test
  void aTableWithoutTransactionsGetsOneRowPerStatement() throws Exception {
    try (Connection connection = connect("")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS DML_MYISAM");
        statement.execute(
            "CREATE TABLE DML_MYISAM (ID INT PRIMARY KEY, V VARCHAR(5) NOT NULL) ENGINE=MyISAM");
        statement.execute(
            "CREATE TEMPORARY TABLE DML_TEMP (ID INT PRIMARY KEY, V VARCHAR(5) NOT NULL)"
                + " ENGINE=InnoDB");
        // What the plan protects against, in the default strict mode.
        try (ResultSet rs = statement.executeQuery("SELECT @@SESSION.sql_mode")) {
          assertTrue(rs.next());
          assertTrue(MySqlDatabaseMeta.isStrictSqlMode(rs.getString(1)), rs.getString(1));
        }
        statement.executeUpdate(
            "INSERT INTO DML_MYISAM VALUES (1, 'ok'), (2, NULL), (3, 'toolong')");
        List<String> values = new ArrayList<>();
        try (ResultSet rs = statement.executeQuery("SELECT V FROM DML_MYISAM ORDER BY ID")) {
          while (rs.next()) {
            values.add(rs.getString(1));
          }
        }
        assertEquals(List.of("ok", "", "toolo"), values);
        assertThrows(
            SQLException.class,
            () -> statement.executeUpdate("INSERT INTO DML_MYISAM VALUES (4, NULL)"));
      }
      List<DmlColumn> columns = List.of(DmlColumn.of("ID"), DmlColumn.of("V"));

      DmlPlan myisam =
          DmlSupport.planStatements(
              databaseMeta(), connection.getMetaData(), DmlSpec.insert("DML_MYISAM", columns), 100);
      System.out.println("mysql MyISAM: " + myisam.getNote());
      assertEquals(1, myisam.getRowsPerStatement());
      assertTrue(myisam.getNote().contains("MyISAM"), myisam.getNote());

      DmlPlan temporary =
          DmlSupport.planStatements(
              databaseMeta(), connection.getMetaData(), DmlSpec.insert("DML_TEMP", columns), 100);
      assertEquals(100, temporary.getRowsPerStatement(), temporary.getNote());
      assertTrue(temporary.getMaxStatementBytes() > 0);

      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE DML_MYISAM");
      }
    }
  }

  /**
   * Times 100,000 inserted rows, also with the driver rewriting a batch of one-row INSERTs into
   * INSERTs of several rows itself. Run with {@code -Dhop.dml.benchmark=true}.
   */
  @ParameterizedTest
  @CsvSource(value = {"''", "rewriteBatchedStatements=true"})
  @EnabledIfSystemProperty(named = "hop.dml.benchmark", matches = "true")
  void measure(String options) throws Exception {
    try (Connection connection = connect(options)) {
      RealDriverDmlScenario.measure(connection, databaseMeta(), "DML_BENCH", 100_000, 100, 1000)
          .forEach(line -> System.out.println("mysql " + options + " BENCH " + line));
    }
  }
}
