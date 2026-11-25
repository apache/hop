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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.databases.mssql.MsSqlServerDatabaseMeta;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the MERGE syntax declared by the SQL Server dialect against SQL Server 2022, through both
 * SQL Server plugins. Needs Docker (the image is amd64 only, so it runs emulated on ARM); run with
 * {@code -Dtest=MsSqlServerMergeIT}.
 */
@EnabledIfSystemProperty(named = "hop.it.dml", matches = "true")
class MsSqlServerMergeIT {

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
      } catch (java.sql.SQLException e) {
        if (attempt > 30) {
          throw e;
        }
        Thread.sleep(1000);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theDeclaredMergeWorks(boolean nativeDialect) throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(
        nativeDialect ? new MsSqlServerNativeDatabaseMeta() : new MsSqlServerDatabaseMeta());
    try (Connection connection = connect()) {
      System.out.println(
          "SQL Server "
              + connection.getMetaData().getDatabaseProductVersion()
              + ", major "
              + connection.getMetaData().getDatabaseMajorVersion());
      assertNull(
          DmlSupport.unsupportedReason(databaseMeta, connection.getMetaData(), UpsertForm.MERGE));
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(
              connection, databaseMeta, nativeDialect ? "MERGE_TEST_NATIVE" : "MERGE_TEST");
      System.out.println("SQL Server MERGE update counts: " + report);

      String suffix = nativeDialect ? "_NATIVE" : "";
      String table = "MERGE_UNCOMPARABLE" + suffix;
      RealDriverMergeScenario.uncomparableColumn(
          connection, databaseMeta, table, UpsertForm.MERGE, "text", "one", "two");
      RealDriverMergeScenario.uncomparableColumn(
          connection, databaseMeta, table, UpsertForm.MERGE, "ntext", "one", "two");
      RealDriverMergeScenario.uncomparableColumn(
          connection, databaseMeta, table, UpsertForm.MERGE, "xml", "<a>one</a>", "<a>two</a>");
      aTableStoredInAnotherCaseIsFound(connection, databaseMeta, "OrderLine" + suffix);
    }
  }

  /**
   * The default collation is case-insensitive: SQL Server takes {@code orderline} for the table
   * created as {@code OrderLine}, and so must the column lookup, as Table Output does.
   */
  private static void aTableStoredInAnotherCaseIsFound(
      Connection connection, DatabaseMeta databaseMeta, String table) throws Exception {
    connection.setAutoCommit(true);
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE " + table + " (Id INT PRIMARY KEY, Label NVARCHAR(20))");
    }
    try {
      String typed = table.toLowerCase(Locale.ROOT);
      List<DmlColumn> columns =
          DmlColumns.fromTable(
              connection.getMetaData(),
              databaseMeta.getIDatabase(),
              null,
              typed,
              List.of("ID", "LABEL"));
      assertEquals(List.of("Id", "Label"), columns.stream().map(DmlColumn::column).toList());
      DmlSyntax syntax = databaseMeta.getIDatabase().getDmlSyntax();
      DmlStatement merge =
          DmlStatementBuilder.upsert(
              databaseMeta,
              syntax,
              syntax.upsert(UpsertForm.MERGE),
              DmlSpec.upsert(
                  typed,
                  columns.subList(0, 1),
                  columns.subList(1, 2),
                  columns.subList(1, 2),
                  false));
      for (String label : new String[] {"one", "two"}) {
        try (PreparedStatement statement = connection.prepareStatement(merge.getSql())) {
          statement.setInt(1, 1);
          statement.setString(2, label);
          assertEquals(1, statement.executeUpdate());
        }
      }
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT Label FROM " + table)) {
        assertTrue(resultSet.next());
        assertEquals("two", resultSet.getString(1));
      }
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE " + table);
      }
    }
  }
}
