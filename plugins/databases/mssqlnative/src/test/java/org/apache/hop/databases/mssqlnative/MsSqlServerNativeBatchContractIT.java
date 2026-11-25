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
 * Microsoft's JDBC driver, at the version Hop downloads, against SQL Server 2022: the driver has to
 * behave as the dialect declares, and where an option or the connection SQL changes it, the dialect
 * must declare nothing. Runs with {@code -Dhop.it.batchContracts=true} and Docker.
 */
@EnabledIfSystemProperty(named = "hop.it.batchContracts", matches = "true")
class MsSqlServerNativeBatchContractIT {

  private static final String PASSWORD = "Hop_Contract_2022!";
  private static GenericContainer<?> container;
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(MsSqlServerNativeDatabaseMeta.class);
    container =
        new GenericContainer<>("mcr.microsoft.com/mssql/server:2022-latest")
            .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("MSSQL_SA_PASSWORD", PASSWORD)
            .withExposedPorts(1433)
            .waitingFor(
                Wait.forLogMessage(".*SQL Server is now ready for client connections.*\\n", 1));
    container.start();
    MsSqlServerNativeDatabaseMeta dialect = new MsSqlServerNativeDatabaseMeta();
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
            "mssql",
            "MSSQLNATIVE",
            "Native",
            container.getHost(),
            "master",
            Integer.toString(container.getMappedPort(1433)),
            "sa",
            PASSWORD);
    databaseMeta.addExtraOption("MSSQLNATIVE", "trustServerCertificate", "true");
    for (int i = 0; i < options.length; i += 2) {
      databaseMeta.addExtraOption("MSSQLNATIVE", options[i], options[i + 1]);
    }
    return databaseMeta;
  }

  private static Connection connect(DatabaseMeta databaseMeta) throws Exception {
    Connection connection = driver.connect(databaseMeta.getURL(new Variables()), "sa", PASSWORD);
    DialectContractCheck.runConnectSql(connection, databaseMeta.getConnectSql());
    return connection;
  }

  @Test
  void theDriverMarksTheFailedRows() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta();
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "MS_DEF");
    }
  }

  @Test
  void bulkCopyForBatchInsertsBehavesDifferentlySoNothingIsDeclared() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta("useBulkCopyForBatchInsert", "true");
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.undeclaredForAReason(
          connection,
          databaseMeta.getIDatabase().getBatchFailureContract(),
          BatchFailureContract.MARKS_FAILED_ROWS,
          "MS_BULK");
    }
  }

  @Test
  void xactAbortBehavesDifferentlySoNothingIsDeclared() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta();
    databaseMeta.setConnectSql("SET XACT_ABORT ON");
    try (Connection connection = connect(databaseMeta)) {
      DialectContractCheck.undeclaredForAReason(
          connection,
          databaseMeta.getIDatabase().getBatchFailureContract(),
          BatchFailureContract.MARKS_FAILED_ROWS,
          "MS_XACT");
    }
  }
}
