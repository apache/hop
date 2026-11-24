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

package org.apache.hop.databases.derby;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.hop.core.database.batch.BatchContractProbe;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.junit.batch.RealDriverBatchScenario;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Measures what the embedded Derby driver does with a failed batch, checks that the dialect
 * declares exactly that, and checks that recovery is correct on the real driver.
 */
class DerbyBatchContractTest {

  private static final String URL = "jdbc:derby:memory:batchcontract;create=true";

  @Test
  void theDriverBehavesAsTheDialectDeclares() throws Exception {
    try (Connection connection = DriverManager.getConnection(URL)) {
      BatchContractProbe.Result result = BatchContractProbe.probe(connection, "PROBE_CONTRACT");
      assertEquals(BatchFailureContract.KEEPS_PREFIX, result.contract(), result.toString());
      BatchFailureContract declared = new DerbyDatabaseMeta().getBatchFailureContract();
      assertEquals(
          result.contract(),
          declared == null ? BatchFailureContract.UNKNOWN : declared,
          "the dialect must declare what the driver does");
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = BatchFailureContract.class,
      names = {"KEEPS_PREFIX", "UNKNOWN"})
  void recoveryMatchesWhatTheTableHolds(BatchFailureContract contract) throws Exception {
    try (Connection connection = DriverManager.getConnection(URL)) {
      RealDriverBatchScenario.run(connection, "RECOVER_FIRST", contract, 100, 0);
      RealDriverBatchScenario.run(connection, "RECOVER_SPREAD", contract, 100, 3, 37, 38, 99);
      RealDriverBatchScenario.run(connection, "RECOVER_CLEAN", contract, 100);
    }
  }
}
