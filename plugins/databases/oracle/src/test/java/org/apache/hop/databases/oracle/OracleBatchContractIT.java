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

package org.apache.hop.databases.oracle;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.sql.Connection;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
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
 * ojdbc11, at the version Hop downloads, against Oracle Free 23: the driver has to behave as the
 * dialect declares. Runs with {@code -Dhop.it.batchContracts=true} and Docker.
 */
@EnabledIfSystemProperty(named = "hop.it.batchContracts", matches = "true")
class OracleBatchContractIT {

  private static final String PASSWORD = "HopContract1";
  private static GenericContainer<?> container;
  private static DownloadedJdbcDriver driver;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(OracleDatabaseMeta.class);
    container =
        new GenericContainer<>("gvenzl/oracle-free:23-slim-faststart")
            .withEnv("ORACLE_PASSWORD", PASSWORD)
            .withEnv("APP_USER", "hop")
            .withEnv("APP_USER_PASSWORD", PASSWORD)
            .withExposedPorts(1521)
            .waitingFor(Wait.forLogMessage(".*DATABASE IS READY TO USE!.*\\n", 1))
            .withStartupTimeout(java.time.Duration.ofMinutes(5));
    container.start();
    OracleDatabaseMeta dialect = new OracleDatabaseMeta();
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

  @Test
  void theDriverKeepsThePrefix() throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "oracle",
            "ORACLE",
            "Native",
            container.getHost(),
            "/FREEPDB1",
            Integer.toString(container.getMappedPort(1521)),
            "hop",
            PASSWORD);
    try (Connection connection =
        driver.connect(databaseMeta.getURL(new Variables()), "hop", PASSWORD)) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "ORA");
    }
  }
}
