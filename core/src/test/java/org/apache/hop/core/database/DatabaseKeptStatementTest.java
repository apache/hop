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
package org.apache.hop.core.database;

import static org.apache.hop.core.database.DatabaseRegressionFixtures.BOUND;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.database;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.layout;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.metaData;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.resultSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseRegressionFixtures.Column;
import org.apache.hop.core.database.DatabaseRegressionFixtures.PlainDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.StringBindingDialect;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The behaviour the performance change adds. Only meaningful with that change applied: {@link
 * Database#openQueryKeepingStatement} does not exist before it.
 *
 * <ul>
 *   <li>{@code openQueryKeepingStatement}: when it reuses the prepared statement and when it
 *       prepares a new one, and when it closes the one it kept;
 *   <li>the batch flag: a failure after the batch ran is no longer reported as a batch failure;
 *   <li>lookup layouts and value bindings resolved once and reused.
 * </ul>
 */
class DatabaseKeptStatementTest {

  private static final String SQL_A = "SELECT name FROM t WHERE id = ?";
  private static final String SQL_B = "SELECT name FROM u WHERE id = ?";

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    DatabaseTypeRuleRegistry.clearCache();
  }

  /** A connection that prepares a new statement every time, recording them and their results. */
  private static final class Connected {
    final Connection connection = mock(Connection.class);
    final List<PreparedStatement> prepared = new ArrayList<>();
    final List<String> preparedSql = new ArrayList<>();
    final List<ResultSet> resultSets = new ArrayList<>();
    final List<Statement> created = new ArrayList<>();
    ResultSetMetaData metaData;
    final Database db;

    Connected(IDatabase dialect) throws SQLException {
      metaData = metaData(Column.of("name", Types.VARCHAR));
      when(connection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
      when(connection.prepareStatement(anyString(), anyInt(), anyInt()))
          .thenAnswer(
              invocation -> {
                PreparedStatement ps = mock(PreparedStatement.class);
                when(ps.executeQuery())
                    .thenAnswer(
                        execution -> {
                          ResultSet rs =
                              resultSet(metaData, new Object[] {"r" + resultSets.size()});
                          resultSets.add(rs);
                          return rs;
                        });
                prepared.add(ps);
                preparedSql.add(invocation.getArgument(0));
                return ps;
              });
      when(connection.createStatement())
          .thenAnswer(
              invocation -> {
                Statement statement = mock(Statement.class);
                when(statement.executeQuery(anyString()))
                    .thenAnswer(execution -> resultSet(metaData, new Object[] {"plain"}));
                created.add(statement);
                return statement;
              });
      db = database(dialect);
      db.setConnection(connection);
    }

    ResultSet open(String sql, IRowMeta params, Object... values) throws HopDatabaseException {
      return db.openQueryKeepingStatement(sql, params, values, ResultSet.FETCH_FORWARD, false);
    }
  }

  private static IRowMeta idParameter() {
    return layout("id", IValueMeta.TYPE_INTEGER);
  }

  // ---------------------------------------------------------------------------------------------
  // openQueryKeepingStatement
  // ---------------------------------------------------------------------------------------------

  @Test
  void theSameSqlAndParameterLayoutExecutesTheKeptStatementAgain() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    ResultSet first = c.open(SQL_A, params, 1L);
    assertEquals(List.of("name"), List.of(c.db.getReturnRowMeta().getFieldNames()));
    assertEquals("r0", c.db.getRow(first)[0]);
    first.close();

    for (long id = 2; id <= 4; id++) {
      ResultSet rs = c.open(SQL_A, params, id);
      assertEquals(List.of("name"), List.of(c.db.getReturnRowMeta().getFieldNames()));
      assertEquals("r" + (id - 1), c.db.getRow(rs)[0]);
      rs.close();
    }

    assertEquals(1, c.prepared.size());
    PreparedStatement ps = c.prepared.get(0);
    for (long id = 1; id <= 4; id++) {
      verify(ps).setLong(1, id);
    }
    verify(ps, times(4)).executeQuery();
    verify(ps, never()).close();
    // The layout is derived from every result, as openQuery does.
    for (ResultSet rs : c.resultSets) {
      verify(rs).getMetaData();
    }
  }

  @Test
  void otherSqlClosesTheKeptStatementAndPreparesANewOne() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.open(SQL_B, params, 2L);
    c.open(SQL_A, params, 3L);

    assertEquals(List.of(SQL_A, SQL_B, SQL_A), c.preparedSql);
    verify(c.prepared.get(0)).close();
    verify(c.prepared.get(1)).close();
    verify(c.prepared.get(2), never()).close();
  }

  @Test
  void parametersOfOtherTypesForTheSameSqlPrepareANewStatement() throws Exception {
    // The statement is only kept for parameters of the same types: a result whose types follow
    // the parameters is then never read through a statement prepared for other types.
    Connected c = new Connected(new StringBindingDialect());

    c.open(SQL_A, layout("p", IValueMeta.TYPE_STRING), "n");
    c.open(SQL_A, layout("q", IValueMeta.TYPE_STRING), "m");
    c.open(SQL_A, layout("p", IValueMeta.TYPE_INTEGER), 5L);
    c.open(SQL_A, layout("p", IValueMeta.TYPE_STRING), "o");

    assertEquals(3, c.prepared.size());
    verify(c.prepared.get(0)).setObject(1, BOUND + "n", Types.OTHER);
    verify(c.prepared.get(0)).setObject(1, BOUND + "m", Types.OTHER);
    verify(c.prepared.get(0)).close();
    verify(c.prepared.get(1)).setLong(1, 5L);
    verify(c.prepared.get(1)).close();
    verify(c.prepared.get(2)).setObject(1, BOUND + "o", Types.OTHER);
    verify(c.prepared.get(2), never()).close();
  }

  @Test
  void aChangedResultLayoutIsReadWithItsNewLayout() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L).close();
    c.metaData = metaData(Column.of("name", Types.VARCHAR), Column.of("extra", Types.INTEGER));
    c.open(SQL_A, params, 2L);

    assertEquals(1, c.prepared.size());
    assertEquals(List.of("name", "extra"), List.of(c.db.getReturnRowMeta().getFieldNames()));
  }

  @Test
  void anotherLazyConversionSettingPreparesANewStatement() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.db.openQueryKeepingStatement(
        SQL_A, params, new Object[] {1L}, ResultSet.FETCH_FORWARD, false);
    assertFalse(c.db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
    c.db.openQueryKeepingStatement(SQL_A, params, new Object[] {2L}, ResultSet.FETCH_FORWARD, true);
    assertTrue(c.db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());

    assertEquals(2, c.prepared.size());
    verify(c.prepared.get(0)).close();
  }

  @Test
  void anotherFetchModePreparesANewStatement() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.db.openQueryKeepingStatement(
        SQL_A, params, new Object[] {1L}, ResultSet.FETCH_FORWARD, false);
    c.db.openQueryKeepingStatement(
        SQL_A, params, new Object[] {2L}, ResultSet.FETCH_REVERSE, false);

    assertEquals(2, c.prepared.size());
    verify(c.prepared.get(0)).close();
  }

  @Test
  void anotherRowLimitPreparesANewStatementWithThatLimit() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.setQueryLimit(10);
    c.open(SQL_A, params, 2L);
    c.open(SQL_A, params, 3L);

    assertEquals(2, c.prepared.size());
    verify(c.prepared.get(0)).close();
    verify(c.prepared.get(0), never()).setMaxRows(anyInt());
    verify(c.prepared.get(1)).setMaxRows(10);
  }

  @Test
  void anotherQueryTimeoutPreparesANewStatementWithThatTimeout() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.setStatementQueryTimeoutSeconds(30);
    c.open(SQL_A, params, 2L);

    assertEquals(2, c.prepared.size());
    verify(c.prepared.get(0)).close();
    verify(c.prepared.get(1)).setQueryTimeout(30);
  }

  @Test
  void aQueryWithoutParametersClosesTheKeptStatementAndIsNotPrepared() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.openQueryKeepingStatement(
        "SELECT name FROM t", null, null, ResultSet.FETCH_FORWARD, false);
    c.open(SQL_A, params, 2L);

    verify(c.prepared.get(0)).close();
    assertEquals(1, c.created.size());
    assertEquals(2, c.prepared.size());
  }

  @Test
  void closeQueryClosesTheKeptStatementSoTheNextCallPreparesAgain() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    ResultSet rs = c.open(SQL_A, params, 1L);
    c.db.closeQuery(rs);
    verify(rs).close();
    verify(c.prepared.get(0)).close();

    c.open(SQL_A, params, 2L);
    assertEquals(2, c.prepared.size());
    verify(c.prepared.get(1), never()).close();
  }

  @Test
  void closeLookupClosesTheKeptStatementSoTheNextCallPreparesAgain() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.closeLookup();
    c.open(SQL_A, params, 2L);

    verify(c.prepared.get(0)).close();
    assertEquals(2, c.prepared.size());
  }

  @Test
  void disconnectClosesTheKeptStatementAndForgetsIt() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.disconnect();

    verify(c.prepared.get(0)).close();
  }

  @Test
  void aFailingExecutionClosesTheKeptStatement() throws Exception {
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();
    c.open(SQL_A, params, 1L);
    PreparedStatement kept = c.prepared.get(0);
    when(kept.executeQuery()).thenThrow(new SQLException("execution refused"));

    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> c.open(SQL_A, params, 2L));

    assertTrue(e.getMessage().contains("An error occurred executing SQL: "), e.getMessage());
    assertTrue(e.getMessage().contains(SQL_A), e.getMessage());
    verify(kept).close();

    c.open(SQL_A, params, 3L);
    assertEquals(2, c.prepared.size());
  }

  @Test
  void aFailingParameterClosesTheKeptStatement() throws Exception {
    Connected c = new Connected(new PlainDialect());
    c.open(SQL_A, idParameter(), 1L);
    PreparedStatement kept = c.prepared.get(0);

    // Same SQL and parameter types, so the kept statement is used; the value cannot be bound.
    assertThrows(HopDatabaseException.class, () -> c.open(SQL_A, idParameter(), "not a number"));

    verify(kept).close();
    c.open(SQL_A, idParameter(), 3L);
    assertEquals(2, c.prepared.size());
  }

  @Test
  void aPlainOpenQueryInBetweenLeavesTheKeptStatementOpen() throws Exception {
    // openQuery replaces the statement the Database holds without closing it, as it always did.
    // The kept statement is no longer the one held, so it is not closed either: it is left to the
    // driver, like every statement openQuery replaced before it.
    Connected c = new Connected(new PlainDialect());
    IRowMeta params = idParameter();

    c.open(SQL_A, params, 1L);
    c.db.openQuery(SQL_A, params, new Object[] {2L});
    c.open(SQL_A, params, 3L);

    assertEquals(3, c.prepared.size());
    verify(c.prepared.get(0), never()).close();
    verify(c.prepared.get(1), never()).close();
    verify(c.prepared.get(2), never()).close();
  }
}
