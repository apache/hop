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

package org.apache.hop.databases.duckdb;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs the declared MERGE and ON CONFLICT against the embedded DuckDB driver. */
class DuckDBMergeTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void theDeclaredMergeWorks() throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(connection, databaseMeta, "MERGE_TEST");
      System.out.println("DuckDB MERGE update counts: " + report);
    }
  }

  @Test
  void theDeclaredOnConflictWorks() throws Exception {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(
              connection, databaseMeta, "MERGE_TEST", UpsertForm.ON_CONFLICT);
      System.out.println("DuckDB ON CONFLICT update counts: " + report);
    }
  }
}
