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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.database.batch.BatchFailureContract;
import org.junit.jupiter.api.Test;

/** What this dialect declares about a failed batch, see {@link BatchFailureContract}. */
class MySqlBatchFailureContractTest {

  @Test
  void theDriversDefaultsMarkTheFailedRows() {
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    assertEquals(BatchFailureContract.MARKS_FAILED_ROWS, dialect.getBatchFailureContract());
    dialect.addExtraOption(dialect.getPluginId(), "useServerPrepStmts", "true");
    dialect.addExtraOption(dialect.getPluginId(), "continueBatchOnError", "true");
    dialect.addExtraOption(dialect.getPluginId(), "rewriteBatchedStatements", "false");
    assertEquals(BatchFailureContract.MARKS_FAILED_ROWS, dialect.getBatchFailureContract());
  }

  @Test
  void stoppingAtTheFirstErrorKeepsThePrefix() {
    MySqlDatabaseMeta dialect = new MySqlDatabaseMeta();
    dialect.addExtraOption(dialect.getPluginId(), "continueBatchOnError", "false");
    assertEquals(BatchFailureContract.KEEPS_PREFIX, dialect.getBatchFailureContract());
  }

  @Test
  void rewrittenBatchesAndUnresolvedValuesDeclareNothing() {
    MySqlDatabaseMeta rewrite = new MySqlDatabaseMeta();
    rewrite.addExtraOption(rewrite.getPluginId(), "rewriteBatchedStatements", "true");
    assertNull(rewrite.getBatchFailureContract());

    MySqlDatabaseMeta manualUrl = new MySqlDatabaseMeta();
    manualUrl.setManualUrl("jdbc:mysql://host:3306/db?useSSL=false&rewriteBatchedStatements=true");
    assertNull(manualUrl.getBatchFailureContract());

    MySqlDatabaseMeta variable = new MySqlDatabaseMeta();
    variable.addExtraOption(variable.getPluginId(), "continueBatchOnError", "${CBOE}");
    assertNull(variable.getBatchFailureContract());
  }
}
