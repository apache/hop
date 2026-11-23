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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
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
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Pins two things the change around prepared statements touches:
 *
 * <ul>
 *   <li>{@link Database#openQuery(String, IRowMeta, Object[])} with parameters, called repeatedly,
 *       and what {@link Database#closeQuery(ResultSet)} closes afterwards and in which order;
 *   <li>{@link Database#insertRow(PreparedStatement, boolean, boolean)} and {@link
 *       Database#emptyAndCommit(PreparedStatement, boolean, int)} in batch mode: which exception a
 *       failure of the batch, of the commit after it, or of the clean up after that comes out as.
 * </ul>
 */
class DatabaseQueryCloseAndBatchTest {

  private static final String SQL = "SELECT name FROM t WHERE id = ?";

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    DatabaseTypeRuleRegistry.clearCache();
  }

  // ---------------------------------------------------------------------------------------------
  // openQuery with parameters, repeated, then closeQuery
  // ---------------------------------------------------------------------------------------------

  /** A connection that prepares a new statement for every openQuery, recording them all. */
  private static final class QueryConnection {
    final Connection connection = mock(Connection.class);
    final List<PreparedStatement> prepared = new ArrayList<>();
    final List<ResultSet> resultSets = new ArrayList<>();
    final List<Statement> created = new ArrayList<>();

    QueryConnection(ResultSetMetaData metaData) throws SQLException {
      when(connection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
      when(connection.prepareStatement(anyString(), anyInt(), anyInt()))
          .thenAnswer(
              invocation -> {
                PreparedStatement ps = mock(PreparedStatement.class);
                ResultSet rs =
                    resultSet(metaData, new Object[] {"row of statement " + prepared.size()});
                resultSets.add(rs);
                when(ps.executeQuery()).thenReturn(rs);
                prepared.add(ps);
                return ps;
              });
      when(connection.createStatement())
          .thenAnswer(
              invocation -> {
                Statement statement = mock(Statement.class);
                ResultSet rs = resultSet(metaData, new Object[] {"plain"});
                resultSets.add(rs);
                when(statement.executeQuery(anyString())).thenReturn(rs);
                created.add(statement);
                return statement;
              });
    }
  }

  private static IRowMeta idParameter() {
    return layout("id", IValueMeta.TYPE_INTEGER);
  }

  @Test
  void everyOpenQueryWithParametersPreparesANewStatement() throws Exception {
    QueryConnection qc = new QueryConnection(metaData(Column.of("name", Types.VARCHAR)));
    Database db = database(new PlainDialect());
    db.setConnection(qc.connection);

    for (long id = 1; id <= 3; id++) {
      ResultSet rs = db.openQuery(SQL, idParameter(), new Object[] {id});
      Object[] row = db.getRow(rs);
      assertEquals("row of statement " + (id - 1), row[0]);
    }

    verify(qc.connection, times(3))
        .prepareStatement(SQL, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    for (int i = 0; i < 3; i++) {
      verify(qc.prepared.get(i)).setLong(1, i + 1L);
      verify(qc.prepared.get(i)).executeQuery();
    }
    assertEquals(1, db.getReturnRowMeta().size());
    assertEquals("name", db.getReturnRowMeta().getValueMeta(0).getName());
  }

  @Test
  void closeQueryClosesTheResultSetGivenAndOnlyTheLastStatementResultSetFirst() throws Exception {
    QueryConnection qc = new QueryConnection(metaData(Column.of("name", Types.VARCHAR)));
    Database db = database(new PlainDialect());
    db.setConnection(qc.connection);

    ResultSet last = null;
    for (long id = 1; id <= 3; id++) {
      last = db.openQuery(SQL, idParameter(), new Object[] {id});
    }
    db.closeQuery(last);

    InOrder order = inOrder(last, qc.prepared.get(2));
    order.verify(last).close();
    order.verify(qc.prepared.get(2)).close();
    // The statements and result sets of the earlier queries are never closed: each openQuery
    // replaced the statement the Database holds without closing it.
    verify(qc.prepared.get(0), never()).close();
    verify(qc.prepared.get(1), never()).close();
    verify(qc.resultSets.get(0), never()).close();
    verify(qc.resultSets.get(1), never()).close();
  }

  @Test
  void closeQueryOfAnEarlierResultSetClosesTheLatestStatement() throws Exception {
    QueryConnection qc = new QueryConnection(metaData(Column.of("name", Types.VARCHAR)));
    Database db = database(new PlainDialect());
    db.setConnection(qc.connection);

    ResultSet first = db.openQuery(SQL, idParameter(), new Object[] {1L});
    db.openQuery(SQL, idParameter(), new Object[] {2L});
    db.closeQuery(first);

    verify(first).close();
    verify(qc.prepared.get(1)).close();
    verify(qc.prepared.get(0), never()).close();
    verify(qc.resultSets.get(1), never()).close();

    // Nothing is left to close: a second closeQuery only closes what it is given.
    ResultSet other = mock(ResultSet.class);
    db.closeQuery(other);
    verify(other).close();
    verify(qc.prepared.get(1), times(1)).close();
  }

  @Test
  void closeQueryClosesTheResultSetThenThePlainStatementThenThePreparedOne() throws Exception {
    QueryConnection qc = new QueryConnection(metaData(Column.of("name", Types.VARCHAR)));
    Database db = database(new PlainDialect());
    db.setConnection(qc.connection);

    db.openQuery(SQL, idParameter(), new Object[] {1L});
    ResultSet plain = db.openQuery("SELECT name FROM t");
    db.closeQuery(plain);

    PreparedStatement prepared = qc.prepared.get(0);
    Statement created = qc.created.get(0);
    InOrder order = inOrder(plain, created, prepared);
    order.verify(plain).close();
    order.verify(created).close();
    order.verify(prepared).close();
  }

  @Test
  void openQueryParametersAreBoundForEachCallsOwnLayout() throws Exception {
    QueryConnection qc = new QueryConnection(metaData(Column.of("name", Types.VARCHAR)));
    Database db = database(new StringBindingDialect());
    db.setConnection(qc.connection);
    IRowMeta byName = layout("p", IValueMeta.TYPE_STRING);
    IRowMeta byId = layout("p", IValueMeta.TYPE_INTEGER);
    IRowMeta both = layout("p", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING);

    db.openQuery(SQL, byName, new Object[] {"n"});
    db.openQuery(SQL, byId, new Object[] {5L});
    db.openQuery(SQL, both, new Object[] {6L, "m"});
    db.openQuery(SQL, byName, new Object[] {"o"});

    verify(qc.prepared.get(0)).setObject(1, BOUND + "n", Types.OTHER);
    verify(qc.prepared.get(1)).setLong(1, 5L);
    verify(qc.prepared.get(2)).setLong(1, 6L);
    verify(qc.prepared.get(2)).setObject(2, BOUND + "m", Types.OTHER);
    verify(qc.prepared.get(3)).setObject(1, BOUND + "o", Types.OTHER);
  }

  @Test
  void openQueryOfAPreparedStatementReusesItsLayoutAcrossParameterLayouts() throws Exception {
    Database db = database(new StringBindingDialect());
    ResultSetMetaData metaData = metaData(Column.of("name", Types.VARCHAR));
    PreparedStatement ps = mock(PreparedStatement.class);
    when(ps.executeQuery()).thenAnswer(invocation -> resultSet(metaData, new Object[] {"r"}));
    IRowMeta byName = layout("p", IValueMeta.TYPE_STRING);
    IRowMeta byId = layout("p", IValueMeta.TYPE_INTEGER);

    ResultSet first = db.openQuery(ps, byName, new Object[] {"n"});
    IRowMeta layoutAfterFirst = db.getReturnRowMeta();
    db.openQuery(ps, byId, new Object[] {5L});
    db.openQuery(ps, byName, new Object[] {"o"});

    assertSame(layoutAfterFirst, db.getReturnRowMeta());
    verify(ps).setObject(1, BOUND + "n", Types.OTHER);
    verify(ps).setLong(1, 5L);
    verify(ps).setObject(1, BOUND + "o", Types.OTHER);
    assertEquals(BOUND + "r", db.getRow(first)[0]);
  }

  // ---------------------------------------------------------------------------------------------
  // insertRow in batch mode
  // ---------------------------------------------------------------------------------------------

  /** A database in batch mode with the given commit size, on a connection that supports it. */
  private static final class BatchSetup {
    final Connection connection = mock(Connection.class);
    final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    final PreparedStatement ps = mock(PreparedStatement.class);
    final Database db;

    BatchSetup(IDatabase dialect, int commitSize) throws SQLException {
      when(connection.getMetaData()).thenReturn(metaData);
      when(metaData.supportsBatchUpdates()).thenReturn(true);
      when(metaData.supportsTransactions()).thenReturn(true);
      db = database(dialect);
      db.setConnection(connection);
      db.setCommit(commitSize);
    }
  }

  @Test
  void aBatchIsExecutedCommittedAndClearedEveryCommitSizeRows() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);

    assertFalse(setup.db.insertRow(setup.ps, true));
    assertTrue(setup.db.insertRow(setup.ps, true));
    assertFalse(setup.db.insertRow(setup.ps, true));
    assertTrue(setup.db.insertRow(setup.ps, true));

    InOrder order = inOrder(setup.ps, setup.connection);
    for (int batch = 0; batch < 2; batch++) {
      order.verify(setup.ps, times(2)).addBatch();
      order.verify(setup.ps).executeBatch();
      order.verify(setup.connection).commit();
      order.verify(setup.ps).clearBatch();
    }
    verify(setup.ps, never()).executeUpdate();
  }

  @Test
  void aCommitFailingAfterASuccessfulBatchIsNotABatchFailure() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    doThrow(new SQLException("commit refused")).when(setup.connection).commit();

    setup.db.insertRow(setup.ps, true);
    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> setup.db.insertRow(setup.ps, true));

    assertFalse(e instanceof HopDatabaseBatchException, "not a batch exception");
    assertTrue(
        e.getMessage()
            .contains(
                "Unexpected error inserting/updating row in part [insertRow executeBatch commit]"),
        e.getMessage());
    HopDatabaseException commitFailure = assertInstanceOf(HopDatabaseException.class, e.getCause());
    assertTrue(commitFailure.getMessage().contains("Error comitting connection"));
    SQLException root = assertInstanceOf(SQLException.class, commitFailure.getCause());
    assertEquals("commit refused", root.getMessage());
    verify(setup.ps).executeBatch();
    verify(setup.ps, never()).clearBatch();
  }

  @DatabaseMetaPlugin(type = "REGRESSION_NO_EMPTY_TX", typeDescription = "No empty transactions")
  static class NoEmptyTransactionsDialect extends PlainDialect {
    @Override
    public boolean isSupportsEmptyTransactions() {
      return false;
    }
  }

  @Test
  void aCommitFailingAfterASuccessfulBatchIsIgnoredWithoutEmptyTransactionSupport()
      throws Exception {
    BatchSetup setup = new BatchSetup(new NoEmptyTransactionsDialect(), 2);
    doThrow(new SQLException("commit refused")).when(setup.connection).commit();

    setup.db.insertRow(setup.ps, true);
    assertTrue(setup.db.insertRow(setup.ps, true));

    verify(setup.ps).executeBatch();
    verify(setup.ps).clearBatch();
  }

  @Test
  void aBatchUpdateExceptionIsABatchFailureWithItsCounts() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    BatchUpdateException batchFailure =
        new BatchUpdateException("batch refused", new int[] {1, Statement.EXECUTE_FAILED});
    SQLException next = new SQLException("row 2 refused");
    batchFailure.setNextException(next);
    when(setup.ps.executeBatch()).thenThrow(batchFailure);

    setup.db.insertRow(setup.ps, true);
    HopDatabaseBatchException e =
        assertThrows(HopDatabaseBatchException.class, () -> setup.db.insertRow(setup.ps, true));

    assertTrue(e.getMessage().contains("Error updating batch"), e.getMessage());
    assertSame(batchFailure, e.getCause());
    assertArrayEquals(new int[] {1, Statement.EXECUTE_FAILED}, e.getUpdateCounts());
    assertEquals(List.of(next), e.getExceptionsList());
    verify(setup.connection, never()).commit();
    verify(setup.ps, never()).clearBatch();
  }

  @Test
  void aPlainSqlExceptionFromTheBatchIsABatchFailureWithoutCounts() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    SQLException failure = new SQLException("batch broke");
    when(setup.ps.executeBatch()).thenThrow(failure);

    setup.db.insertRow(setup.ps, true);
    HopDatabaseBatchException e =
        assertThrows(HopDatabaseBatchException.class, () -> setup.db.insertRow(setup.ps, true));

    assertSame(failure, e.getCause());
    assertNull(e.getUpdateCounts());
    verify(setup.connection, never()).commit();
  }

  @Test
  void aFailedBatchLeavesItsRowsCountedForTheFinalEmptyAndCommit() throws Exception {
    // The row counter is only reset after a successful batch, so the final emptyAndCommit that
    // takes its count from the Database executes the batch again.
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    when(setup.ps.executeBatch()).thenThrow(new SQLException("batch broke")).thenReturn(new int[0]);

    setup.db.insertRow(setup.ps, true);
    assertThrows(HopDatabaseBatchException.class, () -> setup.db.insertRow(setup.ps, true));
    setup.db.emptyAndCommit(setup.ps, true);

    verify(setup.ps, times(2)).executeBatch();
    verify(setup.connection).commit();
  }

  @Test
  void afterASuccessfulBatchTheFinalEmptyAndCommitHasNothingToExecute() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);

    setup.db.insertRow(setup.ps, true);
    setup.db.insertRow(setup.ps, true);
    setup.db.emptyAndCommit(setup.ps, true);

    verify(setup.ps, times(1)).executeBatch();
    verify(setup.connection, times(2)).commit();
  }

  /**
   * The cleaning up after a batch that ran and was committed. Upstream still counts it as part of
   * the batch: an SQLException from clearBatch comes out as a batch failure without counts.
   */
  @Test
  void aClearBatchFailureAfterACommittedBatchIsReportedAsABatchFailure() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    SQLException failure = new SQLException("clear refused");
    doThrow(failure).when(setup.ps).clearBatch();

    setup.db.insertRow(setup.ps, true);
    HopDatabaseBatchException e =
        assertThrows(HopDatabaseBatchException.class, () -> setup.db.insertRow(setup.ps, true));

    assertSame(failure, e.getCause());
    assertNull(e.getUpdateCounts());
    verify(setup.connection).commit();
  }

  @Test
  void withoutBatchSupportEveryRowIsExecutedAndACommitFailureNamesTheNormalCommit()
      throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    when(setup.metaData.supportsBatchUpdates()).thenReturn(false);
    doThrow(new SQLException("commit refused")).when(setup.connection).commit();

    assertFalse(setup.db.insertRow(setup.ps, true));
    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> setup.db.insertRow(setup.ps, true));

    assertFalse(e instanceof HopDatabaseBatchException);
    assertTrue(
        e.getMessage()
            .contains("Unexpected error inserting/updating row in part [insertRow normal commit]"),
        e.getMessage());
    verify(setup.ps, times(2)).executeUpdate();
    verify(setup.ps, never()).addBatch();
  }

  @Test
  void aRowFailingWithoutBatchIsNotABatchFailure() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 2);
    when(setup.metaData.supportsBatchUpdates()).thenReturn(false);
    when(setup.ps.executeUpdate()).thenThrow(new SQLException("row refused"));

    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> setup.db.insertRow(setup.ps, true));

    assertFalse(e instanceof HopDatabaseBatchException);
    assertTrue(e.getMessage().contains("Error inserting/updating row"), e.getMessage());
  }

  @Test
  void insertRowLeavingTheCommitToTheCallerNeverExecutesTheBatch() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 1);

    for (int i = 0; i < 3; i++) {
      assertFalse(setup.db.insertRow(setup.ps, true, false));
    }

    verify(setup.ps, times(3)).addBatch();
    verify(setup.ps, never()).executeBatch();
    verify(setup.connection, never()).commit();
  }

  // ---------------------------------------------------------------------------------------------
  // emptyAndCommit in batch mode
  // ---------------------------------------------------------------------------------------------

  @Test
  void emptyAndCommitExecutesCommitsClearsAndCloses() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);

    setup.db.emptyAndCommit(setup.ps, true, 3);

    InOrder order = inOrder(setup.ps, setup.connection);
    order.verify(setup.ps).executeBatch();
    order.verify(setup.connection).commit();
    order.verify(setup.ps).clearBatch();
    order.verify(setup.ps).close();
  }

  @Test
  void emptyAndCommitPassesACommitFailureOnAsItIs() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);
    doThrow(new SQLException("commit refused")).when(setup.connection).commit();

    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> setup.db.emptyAndCommit(setup.ps, true, 3));

    assertFalse(e instanceof HopDatabaseBatchException);
    assertTrue(e.getMessage().contains("Error comitting connection"), e.getMessage());
    verify(setup.ps, never()).clearBatch();
    verify(setup.ps, never()).close();
  }

  @Test
  void emptyAndCommitReportsABatchUpdateExceptionWithItsCounts() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);
    when(setup.ps.executeBatch())
        .thenThrow(new BatchUpdateException("batch refused", new int[] {1, 1, -3}));

    HopDatabaseBatchException e =
        assertThrows(
            HopDatabaseBatchException.class, () -> setup.db.emptyAndCommit(setup.ps, true, 3));

    assertArrayEquals(new int[] {1, 1, -3}, e.getUpdateCounts());
    verify(setup.connection, never()).commit();
  }

  /**
   * As for insertRow: upstream reports a clean up failure after the committed batch as a batch one.
   */
  @Test
  void emptyAndCommitReportsAClearBatchFailureAfterTheCommitAsABatchFailure() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);
    doThrow(new SQLException("clear refused")).when(setup.ps).clearBatch();

    HopDatabaseBatchException e =
        assertThrows(
            HopDatabaseBatchException.class, () -> setup.db.emptyAndCommit(setup.ps, true, 3));

    assertNull(e.getUpdateCounts());
    verify(setup.connection).commit();
  }

  /** And a failure to close the statement after the committed batch as well. */
  @Test
  void emptyAndCommitReportsACloseFailureAfterTheCommitAsABatchFailure() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);
    doThrow(new SQLException("close refused")).when(setup.ps).close();

    HopDatabaseBatchException e =
        assertThrows(
            HopDatabaseBatchException.class, () -> setup.db.emptyAndCommit(setup.ps, true, 3));

    assertNull(e.getUpdateCounts());
    verify(setup.ps).clearBatch();
  }

  @Test
  void emptyAndCommitWithoutRowsOnTheBatchOnlyCommits() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);
    doThrow(new SQLException("close refused")).when(setup.ps).close();

    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> setup.db.emptyAndCommit(setup.ps, true, 0));

    assertFalse(e instanceof HopDatabaseBatchException);
    assertTrue(e.getMessage().contains("Unable to empty ps and commit connection."));
    verify(setup.ps, never()).executeBatch();
    verify(setup.connection).commit();
    verify(setup.ps, never()).clearBatch();
  }

  @Test
  void emptyAndCommitCanLeaveTheStatementOpen() throws Exception {
    BatchSetup setup = new BatchSetup(new PlainDialect(), 10);

    setup.db.emptyAndCommit(setup.ps, true, 2, false);

    verify(setup.ps).executeBatch();
    verify(setup.connection).commit();
    verify(setup.ps).clearBatch();
    verify(setup.ps, never()).close();
  }
}
