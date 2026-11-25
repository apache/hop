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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlColumns;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.junit.merge.RealDriverMergeScenario;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Runs the declared MERGE syntax against the embedded H2 driver. */
class H2MergeTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  private static DatabaseMeta databaseMeta() {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new H2DatabaseMeta());
    return databaseMeta;
  }

  @Test
  void theDeclaredMergeWorks() throws Exception {
    try (Connection connection =
        DriverManager.getConnection("jdbc:h2:mem:mergetest;DB_CLOSE_DELAY=-1")) {
      RealDriverMergeScenario.Report report =
          RealDriverMergeScenario.run(connection, databaseMeta(), "MERGE_TEST");
      System.out.println("H2 MERGE update counts: " + report);
    }
  }

  /** "Update only changed rows" with a CLOB or BLOB updates every matched row, and works. */
  @Test
  void largeObjectsAreNotCompared() throws Exception {
    try (Connection connection =
        DriverManager.getConnection("jdbc:h2:mem:mergelob;DB_CLOSE_DELAY=-1")) {
      RealDriverMergeScenario.uncomparableColumn(
          connection, databaseMeta(), "MERGE_LOB", UpsertForm.MERGE, "CLOB", "one", "two");
    }
  }

  /**
   * Without a schema the columns and keys come from the table in the current schema, PUBLIC, not
   * from the table of the same name in another schema; with several and none in the current schema
   * the table is refused.
   */
  @Test
  void withoutASchemaTheCurrentSchemaCounts() throws Exception {
    try (Connection connection =
            DriverManager.getConnection("jdbc:h2:mem:mergeschemas;DB_CLOSE_DELAY=-1");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA ARCHIVE");
      statement.execute(
          "CREATE TABLE ARCHIVE.ORDERS (ID INTEGER PRIMARY KEY, AMOUNT DECIMAL(10,0))");
      statement.execute("CREATE TABLE ORDERS (ID INTEGER, AMOUNT DECIMAL(10,2))");

      List<DmlColumn> columns =
          DmlColumns.fromTable(
              connection.getMetaData(), new H2DatabaseMeta(), null, "orders", List.of("amount"));
      assertEquals(List.of("DECIMAL(10,2)"), columns.stream().map(DmlColumn::nativeType).toList());
      assertEquals(
          DmlSupport.UniqueKey.NONE,
          DmlSupport.uniqueKey(connection.getMetaData(), null, "orders", List.of("ID")));
      assertEquals(
          DmlSupport.UniqueKey.ONLY_THE_KEY,
          DmlSupport.uniqueKey(connection.getMetaData(), "archive", "orders", List.of("ID")));

      statement.execute("SET SCHEMA ARCHIVE");
      assertEquals(
          List.of("DECIMAL(10,0)"),
          DmlColumns.fromTable(
                  connection.getMetaData(), new H2DatabaseMeta(), null, "ORDERS", List.of("AMOUNT"))
              .stream()
              .map(DmlColumn::nativeType)
              .toList());

      statement.execute("CREATE SCHEMA OTHER");
      statement.execute("CREATE TABLE OTHER.ORDERS (ID INTEGER)");
      statement.execute("CREATE SCHEMA EMPTY");
      statement.execute("SET SCHEMA EMPTY");
      HopDatabaseException e =
          assertThrows(
              HopDatabaseException.class,
              () ->
                  DmlColumns.fromTable(
                      connection.getMetaData(),
                      new H2DatabaseMeta(),
                      null,
                      "ORDERS",
                      List.of("ID")));
      assertTrue(e.getMessage().contains("Fill in the schema"), e.getMessage());
    }
  }
}
