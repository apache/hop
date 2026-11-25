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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlColumn;
import org.apache.hop.core.database.dml.DmlSpec;
import org.apache.hop.core.database.dml.DmlStatement;
import org.apache.hop.core.database.dml.DmlStatementBuilder;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.database.dml.UpsertForm;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Pins why Derby declares no MERGE syntax: its MERGE only takes a base table or a table function as
 * the source, so no form of a one-row parameter source is accepted.
 */
class DerbyMergeTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
  }

  @ParameterizedTest
  @EnumSource(DmlSyntax.RowSource.class)
  void noParameterSourceIsAccepted(DmlSyntax.RowSource sourceRow) throws Exception {
    assertNull(new DerbyDatabaseMeta().getDmlSyntax());
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DerbyDatabaseMeta());
    List<DmlColumn> keys = List.of(new DmlColumn("ID", "INTEGER"));
    List<DmlColumn> others = List.of(new DmlColumn("LABEL", "VARCHAR(50)"));
    DmlSyntax.Upsert upsert = DmlSyntax.Upsert.of(UpsertForm.MERGE);
    DmlStatement merge =
        DmlStatementBuilder.upsert(
            databaseMeta,
            DmlSyntax.builder().rowSource(sourceRow).castParameters(true).upsert(upsert).build(),
            upsert,
            DmlSpec.upsert("MERGE_TEST", keys, others, others, false));
    try (Connection connection =
        DriverManager.getConnection("jdbc:derby:memory:mergetest;create=true")) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE MERGE_TEST (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(50))");
      }
      try {
        SQLException e =
            assertThrows(SQLException.class, () -> connection.prepareStatement(merge.getSql()));
        System.out.println("Derby " + sourceRow + ": " + e.getMessage());
        // VALUES and the subqueries are refused as sources; FROM dual has no table to read.
        assertTrue(e.getSQLState().startsWith("42"), e.getSQLState() + " " + e.getMessage());
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE MERGE_TEST");
        }
      }
    }
  }
}
