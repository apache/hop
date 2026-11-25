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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.junit.jupiter.api.Test;

/** What this dialect declares about a failed batch, see {@link BatchFailureContract}. */
class PostgreSqlBatchFailureContractTest {

  @Test
  void aFailedBatchLosesTheWholeBatchWithEveryMeasuredOption() {
    PostgreSqlDatabaseMeta dialect = new PostgreSqlDatabaseMeta();
    assertEquals(BatchFailureContract.LOSES_WHOLE_BATCH, dialect.getBatchFailureContract());
    dialect.addExtraOption(dialect.getPluginId(), "reWriteBatchedInserts", "true");
    dialect.addExtraOption(dialect.getPluginId(), "autosave", "always");
    assertEquals(BatchFailureContract.LOSES_WHOLE_BATCH, dialect.getBatchFailureContract());
  }

  @Test
  void theFailedEntryIsReadFromTheMessage() {
    PostgreSqlDatabaseMeta dialect = new PostgreSqlDatabaseMeta();
    assertEquals(
        3,
        dialect.getFailedBatchEntry(
            new SQLException(
                "Batch entry 3 INSERT INTO t (id) VALUES (('103'::int4)) was aborted: dup")));
  }

  @Test
  void noFailedEntryForRewrittenStatementsOrAnUnreadableNumber() {
    PostgreSqlDatabaseMeta dialect = new PostgreSqlDatabaseMeta();
    // A VALUES list of several rows: the entry counts statements, not rows.
    assertEquals(
        -1,
        dialect.getFailedBatchEntry(
            new SQLException(
                "Batch entry 0 INSERT INTO t (id) VALUES (('100'::int4)),(('101'::int4)) was"
                    + " aborted")));
    // pgjdbc formats the number by locale.
    assertEquals(
        -1,
        dialect.getFailedBatchEntry(
            new SQLException("Batch entry 1.500 INSERT INTO t (id) VALUES (1) was aborted")));
    assertEquals(-1, dialect.getFailedBatchEntry(new SQLException("something else")));
    assertEquals(-1, dialect.getFailedBatchEntry(null));

    dialect.addExtraOption(dialect.getPluginId(), "reWriteBatchedInserts", "true");
    assertEquals(
        -1,
        dialect.getFailedBatchEntry(
            new SQLException("Batch entry 3 INSERT INTO t (id) VALUES (1) was aborted")));
  }
}
