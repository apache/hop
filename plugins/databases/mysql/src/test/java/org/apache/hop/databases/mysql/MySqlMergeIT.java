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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.batch.DownloadedJdbcDriver;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs the {@code INSERT ... ON DUPLICATE KEY UPDATE} declared by the MySQL dialect against MySQL
 * 8.4, which gets the row alias, and 8.0, which gets the {@code VALUES()} function, with the
 * Connector/J Hop downloads (it is not shipped, and not a build dependency). Needs Docker; runs
 * with {@code -Dhop.it.dml=true}.
 */
@EnabledIfSystemProperty(named = "hop.it.dml", matches = "true")
class MySqlMergeIT {

  private static final String PASSWORD = "hop-merge";
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(MySqlDatabaseMeta.class);
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    driver =
        DownloadedJdbcDriver.load(
            dialect.getDriverDownload(),
            dialect.getDriverClass(),
            Path.of("target", "jdbc-drivers"));
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({"mysql:8.4, true", "mysql:8.0, false"})
  void theDeclaredUpsertWorks(String image, boolean rowAlias) throws Exception {
    try (GenericContainer<?> container =
        new GenericContainer<>(image)
            .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD)
            .withEnv("MYSQL_DATABASE", "hop")
            .withExposedPorts(3306)
            .waitingFor(Wait.forLogMessage(".*ready for connections.*port: 3306.*\\n", 1))) {
      container.start();
      DatabaseMeta databaseMeta =
          new DatabaseMeta(
              "mysql",
              "MYSQL",
              "Native",
              container.getHost(),
              "hop",
              Integer.toString(container.getMappedPort(3306)),
              "root",
              PASSWORD);
      databaseMeta.addExtraOption("MYSQL", "allowPublicKeyRetrieval", "true");
      try (Connection connection =
          driver.connect(databaseMeta.getURL(new Variables()), "root", PASSWORD)) {
        System.out.println(image + ": " + connection.getMetaData().getDatabaseProductVersion());
        // The table of the scenario has its primary key as its only unique key.
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE PROBE (ID INTEGER PRIMARY KEY, LABEL VARCHAR(10))");
        }
        DmlSpec spec =
            DmlSpec.upsert(
                "PROBE",
                List.of(DmlColumn.of("ID")),
                List.of(DmlColumn.of("LABEL")),
                List.of(DmlColumn.of("LABEL")),
                false);
        DmlPlan plan =
            DmlSupport.plan(databaseMeta, connection.getMetaData(), spec, null, "PROBE", null);
        assertTrue(plan.isSupported(), plan.getUnsupportedReason());
        assertEquals(UpsertForm.ON_DUPLICATE_KEY, plan.getUpsert().form());
        assertEquals(rowAlias, plan.getUpsert().rowAlias());

        // Another unique index: ON DUPLICATE KEY would react to it too, so it is refused.
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE UNIQUE INDEX PROBE_LABEL ON PROBE (LABEL)");
        }
        plan = DmlSupport.plan(databaseMeta, connection.getMetaData(), spec, null, "PROBE", null);
        assertNotNull(plan.getUnsupportedReason());

        RealDriverMergeScenario.Report report =
            RealDriverMergeScenario.run(
                connection, databaseMeta, "MERGE_TEST", UpsertForm.ON_DUPLICATE_KEY);
        System.out.println(image + " ON DUPLICATE KEY update counts: " + report);

        aKeyInAnotherDatabaseDoesNotCount(connection, databaseMeta);
      }
    }
  }

  /**
   * Connector/J ignores the schema and, without a catalog, lists the keys of every database on the
   * server. {@code hop_prod.CUSTOMERS} has a primary key, {@code hop.CUSTOMERS} has none: ON
   * DUPLICATE KEY on the latter would insert every row again on every run, so it is refused. The
   * reverse, another key in the other database, doesn't refuse a table that has the right one.
   */
  private static void aKeyInAnotherDatabaseDoesNotCount(
      Connection connection, DatabaseMeta databaseMeta) throws Exception {
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE hop_prod");
      statement.execute(
          "CREATE TABLE hop_prod.CUSTOMERS (ID INTEGER PRIMARY KEY, CODE VARCHAR(10))");
      statement.execute("CREATE TABLE CUSTOMERS (ID INTEGER, CODE VARCHAR(10))");
    }
    try {
      DmlSpec spec =
          DmlSpec.upsert(
              "CUSTOMERS",
              List.of(DmlColumn.of("ID")),
              List.of(DmlColumn.of("CODE")),
              List.of(DmlColumn.of("CODE")),
              false);
      for (String schema : new String[] {null, "hop"}) {
        DmlPlan plan =
            DmlSupport.plan(
                databaseMeta, connection.getMetaData(), spec, schema, "CUSTOMERS", null);
        System.out.println(
            "hop.CUSTOMERS without a key, schema " + schema + ": " + plan.getUnsupportedReason());
        assertFalse(plan.isSupported(), "the key of hop_prod.CUSTOMERS doesn't count");
      }
      DmlPlan prod =
          DmlSupport.plan(
              databaseMeta, connection.getMetaData(), spec, "hop_prod", "CUSTOMERS", null);
      assertTrue(prod.isSupported(), prod.getUnsupportedReason());

      // Now this table has its key, and the other one gets a second unique key: ON DUPLICATE KEY
      // would react to it there, but not here.
      try (Statement statement = connection.createStatement()) {
        statement.execute("ALTER TABLE CUSTOMERS ADD PRIMARY KEY (ID)");
        statement.execute("CREATE UNIQUE INDEX CUSTOMERS_CODE ON hop_prod.CUSTOMERS (CODE)");
      }
      DmlPlan plan =
          DmlSupport.plan(databaseMeta, connection.getMetaData(), spec, null, "CUSTOMERS", null);
      assertTrue(plan.isSupported(), plan.getUnsupportedReason());
      assertNotNull(
          DmlSupport.plan(
                  databaseMeta, connection.getMetaData(), spec, "hop_prod", "CUSTOMERS", null)
              .getUnsupportedReason());
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE CUSTOMERS");
        statement.execute("DROP DATABASE hop_prod");
      }
    }
  }

  /**
   * A MariaDB server reached through a MySQL connection reports a version (11) that MySQL's row
   * alias would qualify for, but has no row alias: the {@code VALUES()} form is used, and works.
   */
  @Test
  void mariaDbOnAMySqlConnectionGetsNoRowAlias() throws Exception {
    try (GenericContainer<?> container =
        new GenericContainer<>("mariadb:11.4")
            .withEnv("MARIADB_ROOT_PASSWORD", PASSWORD)
            .withEnv("MARIADB_DATABASE", "hop")
            .withExposedPorts(3306)
            .waitingFor(Wait.forLogMessage(".*port: 3306.*\\n", 1))) {
      container.start();
      DatabaseMeta databaseMeta =
          new DatabaseMeta(
              "mariadb",
              "MYSQL",
              "Native",
              container.getHost(),
              "hop",
              Integer.toString(container.getMappedPort(3306)),
              "root",
              PASSWORD);
      databaseMeta.addExtraOption("MYSQL", "allowPublicKeyRetrieval", "true");
      try (Connection connection =
          driver.connect(databaseMeta.getURL(new Variables()), "root", PASSWORD)) {
        System.out.println(
            "MariaDB through Connector/J: "
                + connection.getMetaData().getDatabaseProductVersion()
                + ", major "
                + connection.getMetaData().getDatabaseMajorVersion());
        try (Statement statement = connection.createStatement()) {
          statement.execute("CREATE TABLE PROBE (ID INTEGER PRIMARY KEY, LABEL VARCHAR(10))");
        }
        DmlSpec spec =
            DmlSpec.upsert(
                "PROBE",
                List.of(DmlColumn.of("ID")),
                List.of(DmlColumn.of("LABEL")),
                List.of(DmlColumn.of("LABEL")),
                false);
        DmlPlan plan =
            DmlSupport.plan(databaseMeta, connection.getMetaData(), spec, null, "PROBE", null);
        assertTrue(plan.isSupported(), plan.getUnsupportedReason());
        assertFalse(plan.getUpsert().rowAlias(), "MariaDB has no row alias");

        RealDriverMergeScenario.Report report =
            RealDriverMergeScenario.run(
                connection, databaseMeta, "MERGE_TEST", UpsertForm.ON_DUPLICATE_KEY);
        System.out.println("MariaDB ON DUPLICATE KEY update counts: " + report);
      }
    }
  }
}
