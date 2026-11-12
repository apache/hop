/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.actions.propertygraph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.neo4j.actions.propertygraph.ActionCreatePropertyGraph.PlannedStatement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** When a statement fails, the tables this run created are dropped and the others are kept. */
class ActionCreatePropertyGraphTest {

  private static final String CREATE_GRAPH = "CREATE PROPERTY GRAPH g";

  /** Two new tables with a primary key each, then the graph. */
  private static final List<PlannedStatement> STATEMENTS =
      List.of(
          new PlannedStatement("CREATE TABLE a", "a"),
          new PlannedStatement("ALTER TABLE a ADD PRIMARY KEY (id)", null),
          new PlannedStatement("CREATE TABLE b", "b"),
          new PlannedStatement("ALTER TABLE b ADD PRIMARY KEY (id)", null),
          new PlannedStatement(CREATE_GRAPH, null));

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void createdTablesAreDroppedNewestFirst() throws Exception {
    List<String> executed = new ArrayList<>();
    Database database = database(executed, Set.of(CREATE_GRAPH));

    HopException e =
        assertThrows(HopException.class, () -> action().executeStatements(database, STATEMENTS));

    assertTrue(e.getMessage().contains(CREATE_GRAPH + " failed"), e.getMessage());
    assertTrue(!e.getMessage().contains("stay in the database"), e.getMessage());
    assertEquals(
        List.of(
            "CREATE TABLE a",
            "ALTER TABLE a ADD PRIMARY KEY (id)",
            "CREATE TABLE b",
            "ALTER TABLE b ADD PRIMARY KEY (id)",
            CREATE_GRAPH,
            "DROP TABLE b",
            "DROP TABLE a"),
        executed);
  }

  @Test
  void failingCreateTableIsNotDropped() throws Exception {
    List<String> executed = new ArrayList<>();
    Database database = database(executed, Set.of("CREATE TABLE b"));

    assertThrows(HopException.class, () -> action().executeStatements(database, STATEMENTS));

    assertEquals(
        List.of(
            "CREATE TABLE a",
            "ALTER TABLE a ADD PRIMARY KEY (id)",
            "CREATE TABLE b",
            "DROP TABLE a"),
        executed);
  }

  @Test
  void tablesWhichCantBeDroppedAreReported() throws Exception {
    List<String> executed = new ArrayList<>();
    Database database = database(executed, Set.of(CREATE_GRAPH, "DROP TABLE b"));

    HopException e =
        assertThrows(HopException.class, () -> action().executeStatements(database, STATEMENTS));

    assertTrue(e.getMessage().contains("b stay in the database"), e.getMessage());
    assertTrue(e.getMessage().contains("DROP TABLE b failed"), e.getMessage());
    assertTrue(e.getCause().getMessage().contains(CREATE_GRAPH + " failed"));
    assertEquals("DROP TABLE a", executed.get(executed.size() - 1));
  }

  @Test
  void nothingIsDroppedWithoutAFailure() throws Exception {
    List<String> executed = new ArrayList<>();
    action().executeStatements(database(executed, Set.of()), STATEMENTS);

    assertEquals(STATEMENTS.stream().map(PlannedStatement::sql).toList(), executed);
  }

  /** A table used twice is refused before checking or creating any table. */
  @Test
  void duplicateTablesAreRefusedBeforeAnySql() throws Exception {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getStartQuote()).thenReturn("\"");
    when(databaseMeta.getEndQuote()).thenReturn("\"");
    Database database = mock(Database.class);
    when(database.getDatabaseMeta()).thenReturn(databaseMeta);

    ActionCreatePropertyGraph action = action();
    action.setCreatingTables(true);
    action.setNodeTables(List.of(new NodeTableMapping("Company", "Person", null)));

    HopException e =
        assertThrows(
            HopException.class,
            () -> action.getPlannedStatements(database, PropertyGraphGeneratorTest.model()));
    assertTrue(e.getMessage().contains("Table 'Person'"), e.getMessage());
    verify(database, never()).checkTableExists(any(), anyString());
    verify(database, never()).execStatement(anyString());
  }

  private static ActionCreatePropertyGraph action() {
    ActionCreatePropertyGraph action = new ActionCreatePropertyGraph("property graph");
    action.setGraphName("g");
    return action;
  }

  /**
   * A database which records the statements it executes.
   *
   * @param failing The statements which fail
   */
  private static Database database(List<String> executed, Set<String> failing)
      throws HopDatabaseException {
    Database database = mock(Database.class);
    when(database.execStatement(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              executed.add(sql);
              if (failing.contains(sql)) {
                throw new HopDatabaseException(sql + " failed");
              }
              return null;
            });
    return database;
  }
}
