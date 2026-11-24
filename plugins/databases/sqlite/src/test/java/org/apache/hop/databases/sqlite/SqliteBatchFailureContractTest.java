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
package org.apache.hop.databases.sqlite;

import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hop.core.database.batch.BatchFailureContract;
import org.junit.jupiter.api.Test;

/** What this dialect declares about a failed batch, see {@link BatchFailureContract}. */
class SqliteBatchFailureContractTest {

  /** sqlite-jdbc stops at a failed row but reports no counts, so no contract can be read. */
  @Test
  void declaresNoBatchFailureContract() {
    assertNull(new SqliteDatabaseMeta().getBatchFailureContract());
  }
}
