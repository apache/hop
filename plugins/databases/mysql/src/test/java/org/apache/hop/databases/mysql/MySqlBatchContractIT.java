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

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.batch.DialectContractCheck;
import org.apache.hop.junit.batch.DownloadedJdbcDriver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * MySQL Connector/J, at the version Hop downloads, against MySQL 8.4: the driver has to behave as
 * the dialect declares for each connection option that changes it. Runs with {@code
 * -Dhop.it.batchContracts=true} and Docker.
 */
@EnabledIfSystemProperty(named = "hop.it.batchContracts", matches = "true")
class MySqlBatchContractIT {

  private static final String PASSWORD = "hop-contract";
  private static GenericContainer<?> container;
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(MySqlDatabaseMeta.class);
    container =
        new GenericContainer<>("mysql:8.4")
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
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  private static DatabaseMeta databaseMeta(String... options) {
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
    for (int i = 0; i < options.length; i += 2) {
      databaseMeta.addExtraOption("MYSQL", options[i], options[i + 1]);
    }
    return databaseMeta;
  }

  private static Connection connect(DatabaseMeta databaseMeta) throws Exception {
    return driver.connect(databaseMeta.getURL(new Variables()), "root", PASSWORD);
  }

  @Test
  void theDriversDefaultsMarkTheFailedRows() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta();
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "MY_DEF");
    }
  }

  @Test
  void serverSidePreparedStatementsMarkTheFailedRows() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta("useServerPrepStmts", "true");
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "MY_SPS");
    }
  }

  @Test
  void stoppingAtTheFirstErrorKeepsThePrefix() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta("continueBatchOnError", "false");
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "MY_CBOE");
    }
  }

  @Test
  void rewrittenBatchesBehaveDifferentlySoNothingIsDeclared() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta("rewriteBatchedStatements", "true");
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.undeclaredForAReason(
          connection,
          databaseMeta.getIDatabase().getBatchFailureContract(),
          BatchFailureContract.MARKS_FAILED_ROWS,
          "MY_RW");
    }
  }
}
