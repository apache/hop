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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.junit.dml.RealDriverDmlScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Runs the INSERT of several rows the PostgreSQL dialect declares against PostgreSQL 18 and 14
 * through a multi-row batch, with and without pgjdbc's {@code reWriteBatchedInserts}, and at the
 * parameter limit. Needs Docker. {@code -Dhop.dml.benchmark=true} also times 100,000 rows, one row
 * per statement against 100 and 1000.
 */
class PostgreSqlDmlIT {

  private static final String REWRITE_BATCHED_INSERTS = "reWriteBatchedInserts";

  private static final Map<String, PostgreSQLContainer<?>> CONTAINERS = new HashMap<>();

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
  }

  @AfterAll
  static void stopContainers() {
    CONTAINERS.values().forEach(PostgreSQLContainer::stop);
  }

  private static synchronized PostgreSQLContainer<?> container(String image) {
    return CONTAINERS.computeIfAbsent(
        image,
        name -> {
          PostgreSQLContainer<?> container = new PostgreSQLContainer<>(name);
          container.start();
          return container;
        });
  }

  private static Connection connect(String image, boolean rewrite) throws SQLException {
    PostgreSQLContainer<?> container = container(image);
    String url = container.getJdbcUrl();
    if (rewrite) {
      url += (url.contains("?") ? "&" : "?") + REWRITE_BATCHED_INSERTS + "=true";
    }
    return DriverManager.getConnection(url, container.getUsername(), container.getPassword());
  }

  private static DatabaseMeta databaseMeta(boolean rewrite) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new PostgreSqlDatabaseMeta());
    if (rewrite) {
      databaseMeta.addExtraOption(databaseMeta.getPluginId(), REWRITE_BATCHED_INSERTS, "true");
    }
    return databaseMeta;
  }

  @ParameterizedTest
  @CsvSource({
    "postgres:18, 1, false",
    "postgres:18, 2, false",
    "postgres:18, 5, false",
    "postgres:18, 100, false",
    "postgres:18, 2, true",
    "postgres:18, 5, true",
    "postgres:18, 100, true",
    "postgres:14, 1, false",
    "postgres:14, 5, false",
    "postgres:14, 100, false",
  })
  void theDeclaredInsertWorks(String image, int rowsPerStatement, boolean rewrite)
      throws Exception {
    try (Connection connection = connect(image, rewrite)) {
      String server = image + (rewrite ? " rewrite" : "") + " x" + rowsPerStatement;
      System.out.println(server + ": " + connection.getMetaData().getDatabaseProductVersion());
      RealDriverDmlScenario.run(connection, databaseMeta(rewrite), "dml_test", rowsPerStatement)
          .forEach(sql -> System.out.println(server + ": " + sql));
    }
  }

  @ParameterizedTest
  @CsvSource({"postgres:18, false", "postgres:18, true", "postgres:14, false"})
  void anInsertAtTheParameterLimitWorks(String image, boolean rewrite) throws Exception {
    try (Connection connection = connect(image, rewrite)) {
      System.out.println(
          image
              + (rewrite ? " rewrite" : "")
              + ": "
              + RealDriverDmlScenario.runAtLimits(
                  connection, databaseMeta(rewrite), "dml_limit", 100_000));
    }
  }

  /**
   * A table of 1000 columns, 999 of them VARCHAR(6): about as wide as a row of short values gets
   * before it no longer fits a page. The parameter limit caps a statement at 65 rows.
   */
  @ParameterizedTest
  @CsvSource({"postgres:18, false", "postgres:18, true"})
  void aWideTableIsWrittenInStatementsOfSeveralRows(String image, boolean rewrite)
      throws Exception {
    try (Connection connection = connect(image, rewrite)) {
      System.out.println(
          image
              + (rewrite ? " rewrite" : "")
              + ": "
              + RealDriverDmlScenario.runWide(
                  connection, databaseMeta(rewrite), "dml_wide", 999, "VARCHAR(6)", 6, 1000));
    }
  }

  /** 65535 parameters is the real limit: pgjdbc refuses to prepare a statement with one more. */
  @Test
  void oneParameterMoreThanTheLimitIsRefused() throws Exception {
    try (Connection connection = connect("postgres:18", false)) {
      for (int parameters : new int[] {65535, 65536}) {
        StringBuilder sql = new StringBuilder("SELECT 1 WHERE 1 IN (");
        for (int i = 0; i < parameters; i++) {
          sql.append(i == 0 ? "?" : ", ?");
        }
        sql.append(')');
        if (parameters == 65535) {
          try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            for (int i = 1; i <= parameters; i++) {
              statement.setInt(i, i);
            }
            statement.executeQuery().close();
          }
        } else {
          SQLException e =
              assertThrows(SQLException.class, () -> connection.prepareStatement(sql.toString()));
          System.out.println("PostgreSQL with " + parameters + " parameters: " + e.getMessage());
        }
      }
    }
  }

  /**
   * With {@code reWriteBatchedInserts=true} pgjdbc also folds a batch of INSERTs that already carry
   * several rows into larger ones, and reports {@code SUCCESS_NO_INFO} for each; the entry it
   * blames for a failure then counts its own statements. The dialect gives no hint for a failed
   * INSERT with a VALUES list of several rows, with or without the option, so a multi-row batch
   * never trusts such an entry number.
   */
  @Test
  void multiRowInsertsAndTheBatchEntryHint() throws Exception {
    for (boolean rewrite : new boolean[] {false, true}) {
      PostgreSqlDatabaseMeta dialect =
          (PostgreSqlDatabaseMeta) databaseMeta(rewrite).getIDatabase();
      try (Connection connection = connect("postgres:18", rewrite)) {
        connection.setAutoCommit(true);
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE IF EXISTS dml_rewrite");
          statement.execute(
              "CREATE TABLE dml_rewrite (id INTEGER PRIMARY KEY, v VARCHAR NOT NULL)");
        }
        connection.setAutoCommit(false);
        String twoRows = "INSERT INTO dml_rewrite (id, v) VALUES (?, ?), (?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(twoRows)) {
          for (int entry = 0; entry < 3; entry++) {
            statement.setInt(1, entry * 2 + 1);
            statement.setString(2, "a");
            statement.setInt(3, entry * 2 + 2);
            statement.setString(4, "b");
            statement.addBatch();
          }
          int[] counts = statement.executeBatch();
          connection.commit();
          System.out.println(
              "PostgreSQL rewrite=" + rewrite + ", 3 two-row INSERTs: " + Arrays.toString(counts));
          if (rewrite) {
            // pgjdbc folds multi-row INSERTs too, in powers of two (3 entries: 2 + 1), and then
            // only says the folded ones succeeded.
            assertArrayEquals(
                new int[] {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO, 2}, counts);
          } else {
            assertArrayEquals(new int[] {2, 2, 2}, counts);
          }

          // Entry 1 of 3 fails on its second row.
          for (int entry = 0; entry < 3; entry++) {
            statement.setInt(1, 100 + entry * 2);
            statement.setString(2, "a");
            statement.setInt(3, 101 + entry * 2);
            if (entry == 1) {
              statement.setNull(4, java.sql.Types.VARCHAR);
            } else {
              statement.setString(4, "b");
            }
            statement.addBatch();
          }
          BatchUpdateException e =
              assertThrows(BatchUpdateException.class, statement::executeBatch);
          connection.rollback();
          int hint = dialect.getFailedBatchEntry(e);
          System.out.println(
              "PostgreSQL rewrite="
                  + rewrite
                  + ", entry 1 of 3 fails: hint "
                  + hint
                  + ", message: "
                  + e.getMessage());
          // The message shows a VALUES list of several rows, which is what a rewritten batch looks
          // like: no hint, so the batch recovers without one rather than trust a wrong entry.
          assertEquals(-1, hint);
        }
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE dml_rewrite");
        }
        connection.commit();
      }
    }
  }

  /**
   * Times 100,000 inserted rows, one row per statement against 100 and 1000, commit every 1000
   * rows, with and without {@code reWriteBatchedInserts}. Run with {@code
   * -Dhop.dml.benchmark=true}.
   */
  @Test
  @EnabledIfSystemProperty(named = "hop.dml.benchmark", matches = "true")
  void measure() throws Exception {
    for (boolean rewrite : new boolean[] {false, true}) {
      try (Connection connection = connect("postgres:18", rewrite)) {
        for (int rowsPerStatement : new int[] {100, 1000}) {
          RealDriverDmlScenario.measure(
                  connection, databaseMeta(rewrite), "dml_bench", 100_000, rowsPerStatement, 1000)
              .forEach(
                  line ->
                      System.out.println(
                          "postgres:18" + (rewrite ? " rewrite" : "") + " BENCH " + line));
        }
      }
    }
  }
}
