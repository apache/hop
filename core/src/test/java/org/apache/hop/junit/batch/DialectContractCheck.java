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
package org.apache.hop.junit.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.Statement;
import org.apache.hop.core.database.batch.BatchContractProbe;
import org.apache.hop.core.database.batch.BatchFailureContract;

/**
 * Checks a dialect's batch failure contract against the real driver and server: the probe has to
 * see exactly what the dialect declares, and a recoverable batch with that contract has to report
 * exactly the rows that end up in the table.
 */
public final class DialectContractCheck {

  private DialectContractCheck() {}

  /**
   * The dialect declares a contract for this connection: the driver must behave that way, and
   * recovery by it must be right for failures at the start, spread out, none, and in a batch large
   * enough for the driver to split it.
   *
   * @param connection an open connection, set up as the dialect would (connection SQL included)
   * @param declared what the dialect declares for this connection
   * @param prefix a short table name prefix, unique per call
   * @return what the probe saw
   */
  public static BatchContractProbe.Result declaredContractHolds(
      Connection connection, BatchFailureContract declared, String prefix) throws Exception {
    BatchContractProbe.Result result = BatchContractProbe.probe(connection, prefix + "_PROBE");
    assertEquals(declared, result.contract(), "the driver behaves as declared: " + result);
    RealDriverBatchScenario.run(connection, prefix + "_FIRST", declared, 100, 0);
    RealDriverBatchScenario.run(connection, prefix + "_SPREAD", declared, 100, 3, 37, 38, 99);
    RealDriverBatchScenario.run(connection, prefix + "_CLEAN", declared, 100);
    RealDriverBatchScenario.run(connection, prefix + "_LARGE", declared, 5000, 1500, 4999);
    return result;
  }

  /**
   * The dialect declares nothing for this connection, because an option changes what the driver
   * does: the probe must indeed see something other than the dialect's default contract.
   *
   * @param connection an open connection, set up as the dialect would
   * @param declared what the dialect declares for this connection, which must be null
   * @param withDefaultOptions what the dialect declares with the default options
   * @param prefix a short table name prefix, unique per call
   * @return what the probe saw
   */
  public static BatchContractProbe.Result undeclaredForAReason(
      Connection connection,
      BatchFailureContract declared,
      BatchFailureContract withDefaultOptions,
      String prefix)
      throws Exception {
    assertNull(declared, "no contract is declared for this connection");
    BatchContractProbe.Result result = BatchContractProbe.probe(connection, prefix + "_PROBE");
    assertNotEquals(
        withDefaultOptions,
        result.contract(),
        "with this option the driver does not behave as with the defaults: " + result);
    return result;
  }

  /** Runs the connection SQL of a dialect, as Hop does after connecting. */
  public static void runConnectSql(Connection connection, String connectSql) throws Exception {
    if (connectSql == null || connectSql.isBlank()) {
      return;
    }
    try (Statement statement = connection.createStatement()) {
      statement.execute(connectSql);
    }
  }
}
