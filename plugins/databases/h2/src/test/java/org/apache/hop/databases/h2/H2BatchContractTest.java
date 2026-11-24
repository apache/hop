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

package org.apache.hop.databases.h2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.batch.BatchContractProbe;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.batch.DialectContractCheck;
import org.apache.hop.junit.batch.RealDriverBatchScenario;
import org.h2.tools.Server;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Measures what the embedded H2 driver does with a failed batch, checks that the dialect declares
 * exactly that, and checks that recovery is correct on the real driver.
 */
class H2BatchContractTest {

  @BeforeAll
  static void registerDialect() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  private static final String URL = "jdbc:h2:mem:batchcontract;DB_CLOSE_DELAY=-1";

  @Test
  void theDriverBehavesAsTheDialectDeclares() throws Exception {
    try (Connection connection = DriverManager.getConnection(URL)) {
      BatchContractProbe.Result result = BatchContractProbe.probe(connection, "PROBE_CONTRACT");
      assertEquals(BatchFailureContract.MARKS_FAILED_ROWS, result.contract(), result.toString());
      BatchFailureContract declared = new H2DatabaseMeta().getBatchFailureContract();
      assertEquals(
          result.contract(),
          declared == null ? BatchFailureContract.UNKNOWN : declared,
          "the dialect must declare what the driver does");
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = BatchFailureContract.class,
      names = {"MARKS_FAILED_ROWS", "UNKNOWN"})
  void recoveryMatchesWhatTheTableHolds(BatchFailureContract contract) throws Exception {
    try (Connection connection = DriverManager.getConnection(URL)) {
      RealDriverBatchScenario.run(connection, "RECOVER_FIRST", contract, 100, 0);
      RealDriverBatchScenario.run(connection, "RECOVER_SPREAD", contract, 100, 3, 37, 38, 99);
      RealDriverBatchScenario.run(connection, "RECOVER_CLEAN", contract, 100);
    }
  }

  /** File mode, with the URL the dialect builds. */
  @Test
  void theDriverBehavesAsDeclaredInFileMode(@TempDir Path folder) throws Exception {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            "h2", "H2", "Native", "", folder.resolve("contract").toString(), "", "sa", "");
    try (Connection connection =
        DriverManager.getConnection(databaseMeta.getURL(new Variables()), "sa", "")) {
      DialectContractCheck.declaredContractHolds(
          connection, databaseMeta.getIDatabase().getBatchFailureContract(), "FILE");
    }
  }

  /** TCP server mode, with the URL the dialect builds when a hostname is set. */
  @Test
  void theDriverBehavesAsDeclaredInTcpMode(@TempDir Path folder) throws Exception {
    Server server =
        Server.createTcpServer("-tcpPort", "0", "-ifNotExists", "-baseDir", folder.toString())
            .start();
    try {
      DatabaseMeta databaseMeta =
          new DatabaseMeta(
              "h2",
              "H2",
              "Native",
              "localhost",
              "contract",
              Integer.toString(server.getPort()),
              "sa",
              "");
      try (Connection connection =
          DriverManager.getConnection(databaseMeta.getURL(new Variables()), "sa", "")) {
        DialectContractCheck.declaredContractHolds(
            connection, databaseMeta.getIDatabase().getBatchFailureContract(), "TCP");
      }
    } finally {
      server.stop();
    }
  }
}
