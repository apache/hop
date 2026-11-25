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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlPlan;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs the declared {@code INSERT ... ON CONFLICT} against the embedded SQLite driver. */
class SqliteMergeTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new SqliteDatabaseMeta());
    return databaseMeta;
  }

  @Test
  void theDeclaredOnConflictWorks() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:")) {
      System.out.println("SQLite " + connection.getMetaData().getDatabaseProductVersion());
      assertNull(DmlSupport.unsupportedReason(databaseMeta(), connection.getMetaData(), null));
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(
              connection, databaseMeta(), "MERGE_TEST", UpsertForm.ON_CONFLICT);
      System.out.println("SQLite ON CONFLICT update counts: " + report);
    }
  }

  /** SQLite has no MERGE, and ON CONFLICT needs a unique key on exactly the key columns. */
  @Test
  void mergeIsRefusedAndOnConflictNeedsAUniqueKey() throws Exception {
    assertNotNull(DmlSupport.unsupportedReason(databaseMeta(), UpsertForm.MERGE));
    try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE NO_KEY (ID INTEGER, LABEL VARCHAR(10))");
      }
      DmlSpec spec =
          DmlSpec.upsert(
              "NO_KEY",
              List.of(DmlColumn.of("ID")),
              List.of(DmlColumn.of("LABEL")),
              List.of(DmlColumn.of("LABEL")),
              false);
      DmlPlan plan =
          DmlSupport.plan(databaseMeta(), connection.getMetaData(), spec, null, "NO_KEY", null);
      assertNotNull(plan.getUnsupportedReason());
      assertTrue(plan.getUnsupportedReason().contains("ID"), plan.getUnsupportedReason());
    }
  }
}
