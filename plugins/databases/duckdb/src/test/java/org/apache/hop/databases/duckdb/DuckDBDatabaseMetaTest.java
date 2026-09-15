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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * The database explorer and every "browse tables" button against the real DuckDB driver. See issue
 * #3743, where neither tables nor schemas showed up.
 */
class DuckDBDatabaseMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  /**
   * A named in-memory database, which is the only in-memory form DuckDB shares between connections
   * within a JVM. The plain {@code jdbc:duckdb:} gives every connection a private one.
   */
  private Database database(String name) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    databaseMeta.setName(name);
    databaseMeta.setDBName(":memory:" + name);
    databaseMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    return new Database(
        new SimpleLoggingObject(name, LoggingObjectType.GENERAL, null),
        new Variables(),
        databaseMeta);
  }

  /**
   * DuckDB JDBC 1.5 renamed the type it reports for an ordinary table from "BASE TABLE" to "TABLE",
   * and Hop asked for "BASE TABLE" only, so every table listing came back empty.
   */
  @Test
  void tablesAreListedWhicheverNameTheDriverGivesTheirType() throws Exception {
    Database db = database("tables");
    db.connect();
    try {
      db.execStatement("CREATE TABLE ORDINARY(a INT)");
      db.execStatement("CREATE TEMP TABLE TEMPORARY_ONE(a INT)");
      db.execStatement("CREATE VIEW A_VIEW AS SELECT * FROM ORDINARY");

      List<String> tables = Arrays.asList(db.getTablenames());
      assertTrue(tables.contains("ORDINARY"), "an ordinary table is a table: " + tables);
      assertTrue(tables.contains("TEMPORARY_ONE"), "a temporary table is a table: " + tables);
      assertTrue(!tables.contains("A_VIEW"), "a view is not a table: " + tables);

      assertTrue(
          db.getTableMap().values().stream().anyMatch(names -> names.contains("ORDINARY")),
          "the table map the explorer builds is populated too: " + db.getTableMap());

      List<String> views = Arrays.asList(db.getViews(false));
      assertTrue(views.contains("A_VIEW"), "a view is still a view: " + views);
    } finally {
      db.disconnect();
    }
  }

  /**
   * "main" exists in every DuckDB catalog, so the plain JDBC schema list is three identical names.
   * Qualifying them is what issue #3666 fixed and what a later refactor dropped again.
   */
  @Test
  void schemasAreQualifiedByTheirCatalog() throws Exception {
    String sql = new DuckDBDatabaseMeta().getSqlListOfSchemas();
    assertNotNull(sql, "DuckDB has to supply its own schema list");

    List<String> schemas = new ArrayList<>();
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        schemas.add(resultSet.getString("name"));
      }
    }

    assertTrue(schemas.contains("memory.main"), "the database's own schema: " + schemas);
    assertTrue(schemas.contains("system.main"), "and the system one beside it: " + schemas);
    assertEquals(
        schemas.size(),
        schemas.stream().distinct().count(),
        "no two schemas share a name once qualified: " + schemas);
  }
}
