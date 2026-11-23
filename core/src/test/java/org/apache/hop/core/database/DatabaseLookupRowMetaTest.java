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

import static org.apache.hop.core.database.DatabaseRegressionFixtures.database;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.metaData;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.resultSet;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.statement;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
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
 * Pins what {@link Database#getLookup} returns, and the layout {@link Database#getReturnRowMeta()}
 * reports after it, when one {@link Database} runs several lookup statements with different result
 * layouts, through each of the overloads.
 *
 * <p>Two upstream behaviours that look like defects are pinned here as they are, and marked so:
 * only {@code getLookup(PreparedStatement)} derives the layout from the statement it executes; the
 * other overloads read with whatever layout the Database last derived, whichever statement that
 * came from.
 */
class DatabaseLookupRowMetaTest {

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    DatabaseTypeRuleRegistry.clearCache();
  }

  /** A statement over fixed rows, recording every result set it hands out. */
  private static final class Lookup {
    final ResultSetMetaData metaData;
    final PreparedStatement statement = mock(PreparedStatement.class);
    final List<ResultSet> resultSets = new ArrayList<>();

    Lookup(Column[] columns, Object[]... rows) throws SQLException {
      this.metaData = metaData(columns);
      when(statement.executeQuery())
          .thenAnswer(
              invocation -> {
                ResultSet rs = resultSet(metaData, rows);
                resultSets.add(rs);
                return rs;
              });
    }
  }

  private static Column[] columns(Column... columns) {
    return columns;
  }

  private static final Timestamp TS = Timestamp.valueOf("2024-02-29 12:34:56");

  /** Two columns: a string and an integer. */
  private static Lookup lookupA() throws SQLException {
    return new Lookup(
        columns(Column.of("name", Types.VARCHAR), Column.of("id", Types.INTEGER)),
        new Object[] {"alpha", 1L});
  }

  /** Three columns of other types. */
  private static Lookup lookupB() throws SQLException {
    return new Lookup(
        columns(
            Column.of("code", Types.VARCHAR),
            Column.of("amount", Types.DOUBLE),
            Column.of("moment", Types.TIMESTAMP)),
        new Object[] {"B1", 2.5, TS});
  }

  private static Object[] firstValues(Object[] row, int count) {
    Object[] values = new Object[count];
    System.arraycopy(row, 0, values, 0, count);
    return values;
  }

  private static void assertLayout(IRowMeta rowMeta, String... names) {
    assertNotNull(rowMeta);
    assertEquals(names.length, rowMeta.size(), String.valueOf(rowMeta));
    for (int i = 0; i < names.length; i++) {
      assertEquals(names[i], rowMeta.getValueMeta(i).getName());
    }
  }

  private static void assertLookupA(Database db, Object[] row) {
    assertArrayEquals(new Object[] {"alpha", 1L}, firstValues(row, 2));
    assertLayout(db.getReturnRowMeta(), "name", "id");
    assertTrue(db.getReturnRowMeta().getValueMeta(0).isString());
    assertTrue(db.getReturnRowMeta().getValueMeta(1).isInteger());
  }

  private static void assertLookupB(Database db, Object[] row) {
    assertArrayEquals(new Object[] {"B1", 2.5, TS}, firstValues(row, 3));
    assertLayout(db.getReturnRowMeta(), "code", "amount", "moment");
    assertTrue(db.getReturnRowMeta().getValueMeta(1).isNumber());
    assertTrue(db.getReturnRowMeta().getValueMeta(2).isDate());
  }

  // ---------------------------------------------------------------------------------------------
  // getLookup(PreparedStatement)
  // ---------------------------------------------------------------------------------------------

  @Test
  void twoStatementsAlternatedEachReturnTheirOwnRowAndLayout() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();
    Lookup b = lookupB();

    for (int round = 0; round < 5; round++) {
      assertLookupA(db, db.getLookup(a.statement));
      assertLookupB(db, db.getLookup(b.statement));
    }
    // Every result set is closed after its lookup.
    for (ResultSet rs : a.resultSets) {
      verify(rs).close();
    }
    for (ResultSet rs : b.resultSets) {
      verify(rs).close();
    }
  }

  /** Statement {@code k} returns {@code k + 1} columns, alternating strings and integers. */
  private static Lookup lookupNumber(int k) throws SQLException {
    int count = k + 1;
    Column[] columns = new Column[count];
    Object[] row = new Object[count];
    for (int j = 0; j < count; j++) {
      boolean string = (j + k) % 2 == 0;
      columns[j] = Column.of("s" + k + "c" + j, string ? Types.VARCHAR : Types.INTEGER);
      row[j] = string ? "v" + k + "-" + j : (long) (k * 100 + j);
    }
    return new Lookup(columns, row);
  }

  private static void assertLookupNumber(Database db, int k, Object[] row) {
    int count = k + 1;
    assertEquals(count, db.getReturnRowMeta().size(), "statement " + k);
    for (int j = 0; j < count; j++) {
      boolean string = (j + k) % 2 == 0;
      assertEquals("s" + k + "c" + j, db.getReturnRowMeta().getValueMeta(j).getName());
      assertEquals(
          string ? IValueMeta.TYPE_STRING : IValueMeta.TYPE_INTEGER,
          db.getReturnRowMeta().getValueMeta(j).getType());
      assertEquals(string ? "v" + k + "-" + j : (long) (k * 100 + j), row[j], "statement " + k);
    }
  }

  @Test
  void tenStatementsCycledEachReturnTheirOwnRowAndLayout() throws Exception {
    Database db = database(new PlainDialect());
    List<Lookup> lookups = new ArrayList<>();
    for (int k = 0; k < 10; k++) {
      lookups.add(lookupNumber(k));
    }

    for (int cycle = 0; cycle < 3; cycle++) {
      for (int k = 0; k < lookups.size(); k++) {
        assertLookupNumber(db, k, db.getLookup(lookups.get(k).statement));
      }
    }
    int[] order = {9, 0, 9, 1, 8, 2, 2, 7, 3, 6, 4, 5, 0, 9};
    for (int k : order) {
      assertLookupNumber(db, k, db.getLookup(lookups.get(k).statement));
    }
  }

  @Test
  void aStatementWhoseResultLayoutChangesIsReadWithItsNewLayout() throws Exception {
    // getLookup(ps) derives the layout from every result set. The same statement can answer a
    // different layout, for example a SELECT * after the table was altered.
    Database db = database(new PlainDialect());
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSetMetaData narrow = metaData(Column.of("id", Types.INTEGER));
    ResultSetMetaData wide =
        metaData(Column.of("id", Types.INTEGER), Column.of("added", Types.VARCHAR));
    when(ps.executeQuery())
        .thenAnswer(invocation -> resultSet(narrow, new Object[] {1L}))
        .thenAnswer(invocation -> resultSet(wide, new Object[] {2L, "new"}));

    Object[] first = db.getLookup(ps);
    assertEquals(1L, first[0]);
    assertLayout(db.getReturnRowMeta(), "id");

    Object[] second = db.getLookup(ps);
    assertArrayEquals(new Object[] {2L, "new"}, firstValues(second, 2));
    assertLayout(db.getReturnRowMeta(), "id", "added");
  }

  @Test
  void aStatementClosedThroughTheDatabaseAndHandedOutAgainIsReadWithItsNewLayout()
      throws Exception {
    // A driver or pool may hand out the same statement object again after it was closed.
    Database db = database(new PlainDialect());
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSetMetaData narrow = metaData(Column.of("id", Types.INTEGER));
    ResultSetMetaData wide =
        metaData(Column.of("code", Types.VARCHAR), Column.of("id", Types.INTEGER));
    when(ps.executeQuery())
        .thenAnswer(invocation -> resultSet(narrow, new Object[] {1L}))
        .thenAnswer(invocation -> resultSet(wide, new Object[] {"c", 2L}));

    db.getLookup(ps);
    db.closePreparedStatement(ps);
    Object[] second = db.getLookup(ps);

    assertArrayEquals(new Object[] {"c", 2L}, firstValues(second, 2));
    assertLayout(db.getReturnRowMeta(), "code", "id");
  }

  @Test
  void noRowFoundReturnsNullButStillDerivesTheLayout() throws Exception {
    Database db = database(new PlainDialect());
    Lookup empty =
        new Lookup(columns(Column.of("name", Types.VARCHAR), Column.of("id", Types.INTEGER)));
    Lookup b = lookupB();

    assertNull(db.getLookup(empty.statement));
    assertLayout(db.getReturnRowMeta(), "name", "id");

    assertLookupB(db, db.getLookup(b.statement));

    assertNull(db.getLookup(empty.statement));
    assertLayout(db.getReturnRowMeta(), "name", "id");
    for (ResultSet rs : empty.resultSets) {
      verify(rs).close();
    }
  }

  @Test
  void getLookupOfAStatementDoesNotCheckForMoreRows() throws Exception {
    Database db = database(new PlainDialect());
    Lookup two =
        new Lookup(
            columns(Column.of("name", Types.VARCHAR)), new Object[] {"first"}, new Object[] {"x"});

    Object[] row = db.getLookup(two.statement);

    assertEquals("first", row[0]);
    verify(two.resultSets.get(0), times(1)).next();
  }

  @Test
  void aFailingLookupLeavesNoReturnLayout() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();
    assertLookupA(db, db.getLookup(a.statement));
    PreparedStatement failing = mock(PreparedStatement.class);
    when(failing.executeQuery()).thenThrow(new SQLException("lookup failed"));

    HopDatabaseException e = assertThrows(HopDatabaseException.class, () -> db.getLookup(failing));

    assertTrue(e.getMessage().contains("Error looking up row in database"), e.getMessage());
    assertNull(db.getReturnRowMeta());
  }

  @Test
  void lookupsReadThroughTheDialectsBinding() throws Exception {
    Database db = database(new StringBindingDialect());
    Lookup a = lookupA();
    Lookup b = lookupB();

    for (int round = 0; round < 3; round++) {
      Object[] rowA = db.getLookup(a.statement);
      assertArrayEquals(
          new Object[] {DatabaseRegressionFixtures.BOUND + "alpha", 1L}, firstValues(rowA, 2));
      Object[] rowB = db.getLookup(b.statement);
      assertArrayEquals(
          new Object[] {DatabaseRegressionFixtures.BOUND + "B1", 2.5, TS}, firstValues(rowB, 3));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // failOnMultipleResults
  // ---------------------------------------------------------------------------------------------

  @Test
  void failOnMultipleResultsRejectsASecondRowAndClosesTheResultSet() throws Exception {
    Database db = database(new PlainDialect());
    Lookup two =
        new Lookup(
            columns(Column.of("name", Types.VARCHAR)), new Object[] {"a"}, new Object[] {"b"});

    HopDatabaseException e =
        assertThrows(HopDatabaseException.class, () -> db.getLookup(two.statement, true));

    assertTrue(
        e.getMessage()
            .contains(
                "Only 1 row was expected as a result of a lookup, and at least 2 were found!"),
        e.getMessage());
    verify(two.resultSets.get(0)).close();
  }

  @Test
  void failOnMultipleResultsAcceptsASingleRow() throws Exception {
    Database db = database(new PlainDialect());
    Lookup one = new Lookup(columns(Column.of("name", Types.VARCHAR)), new Object[] {"a"});

    assertEquals("a", db.getLookup(one.statement, true)[0]);
    verify(one.resultSets.get(0), times(2)).next();
  }

  @Test
  void failOnMultipleResultsDoesNotLookFurtherWhenNothingWasFound() throws Exception {
    Database db = database(new PlainDialect());
    Lookup none = new Lookup(columns(Column.of("name", Types.VARCHAR)));

    assertNull(db.getLookup(none.statement, true));
    verify(none.resultSets.get(0), times(1)).next();
  }

  // ---------------------------------------------------------------------------------------------
  // The other overloads
  // ---------------------------------------------------------------------------------------------

  @Test
  void theFirstLookupThroughAnyOverloadDerivesTheLayoutOfItsStatement() throws Exception {
    Database first = database(new PlainDialect());
    assertLookupA(first, first.getLookup(lookupA().statement, false));
    Database second = database(new PlainDialect());
    assertLookupB(second, second.getLookup(lookupB().statement, false, false));
  }

  /**
   * Pins an upstream defect: getLookup(ps, boolean) derives the layout only when the Database has
   * none yet, so a second statement is read with the first statement's layout.
   */
  @Test
  void upstreamDefectGetLookupWithFailFlagReadsASecondStatementWithTheFirstLayout()
      throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();
    Lookup c =
        new Lookup(
            columns(
                Column.of("c1", Types.VARCHAR),
                Column.of("c2", Types.INTEGER),
                Column.of("c3", Types.VARCHAR)),
            new Object[] {"see", 3L, "extra"});

    assertLookupA(db, db.getLookup(a.statement, false));

    Object[] row = db.getLookup(c.statement, false);
    assertArrayEquals(new Object[] {"see", 3L, null}, firstValues(row, 3));
    assertLayout(db.getReturnRowMeta(), "name", "id");

    assertLookupA(db, db.getLookup(a.statement, false, false));
  }

  /**
   * Pins an upstream defect: a getLookup(ps) on one statement leaves its layout behind for the
   * overloads that do not derive one, including the ones on the Database's own lookup statement.
   */
  @Test
  void upstreamDefectTheOtherOverloadsReuseTheLayoutOfTheLastGetLookupOfAStatement()
      throws Exception {
    Database db = database(new PlainDialect());
    Lookup own =
        new Lookup(
            columns(
                Column.of("l1", Types.VARCHAR),
                Column.of("l2", Types.INTEGER),
                Column.of("l3", Types.VARCHAR)),
            new Object[] {"ell", 3L, "more"});
    Connection connection = mock(Connection.class);
    when(connection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));
    when(connection.prepareStatement(anyString())).thenReturn(own.statement);
    db.setConnection(connection);
    db.setLookup("t", new String[] {"k"}, new String[] {"="}, new String[] {"l1"}, null, null);

    // The Database's own lookup statement, first: its layout is derived.
    Object[] first = db.getLookup();
    assertArrayEquals(new Object[] {"ell", 3L, "more"}, firstValues(first, 3));
    assertLayout(db.getReturnRowMeta(), "l1", "l2", "l3");

    // Another statement: getLookup(ps) derives its layout.
    Lookup a = lookupA();
    assertLookupA(db, db.getLookup(a.statement));

    // Back to the own statement through each overload: read with the other statement's layout.
    Object[] viaNoArgs = db.getLookup();
    assertArrayEquals(new Object[] {"ell", 3L}, firstValues(viaNoArgs, 2));
    assertNull(viaNoArgs[2]);
    assertLayout(db.getReturnRowMeta(), "name", "id");

    Object[] viaFail = db.getLookup(false);
    assertArrayEquals(new Object[] {"ell", 3L, null}, firstValues(viaFail, 3));

    Object[] viaFailLazy = db.getLookup(false, false);
    assertArrayEquals(new Object[] {"ell", 3L, null}, firstValues(viaFailLazy, 3));
    assertLayout(db.getReturnRowMeta(), "name", "id");
  }

  @Test
  void lazyConversionOnTheFirstLookupGivesBinaryStrings() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();

    Object[] row = db.getLookup(a.statement, false, true);

    assertArrayEquals("alpha".getBytes(StandardCharsets.UTF_8), (byte[]) row[0]);
    assertEquals(1L, row[1]);
    assertTrue(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
  }

  /**
   * Pins an upstream defect: the layout derived by the first lookup is kept by the overloads that
   * take a lazy conversion flag, so asking for no lazy conversion afterwards still gets bytes.
   */
  @Test
  void upstreamDefectTheLazyConversionOfTheFirstLookupSticks() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();

    db.getLookup(a.statement, false, true);
    Object[] notLazy = db.getLookup(a.statement, false, false);

    assertInstanceOf(byte[].class, notLazy[0]);
    assertTrue(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
  }

  @Test
  void getLookupOfAStatementIgnoresTheLazyConversionOfAnEarlierLookup() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();

    db.getLookup(a.statement, false, true);
    Object[] row = db.getLookup(a.statement);

    assertEquals("alpha", row[0]);
    assertFalse(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
  }

  @Test
  void getLookupOfAStatementDerivesAgainAfterAnyOtherOverload() throws Exception {
    Database db = database(new PlainDialect());
    Lookup a = lookupA();
    Lookup b = lookupB();

    assertLookupA(db, db.getLookup(a.statement, false));
    assertLookupB(db, db.getLookup(b.statement));
    assertLookupA(db, db.getLookup(a.statement));
    assertLookupB(db, db.getLookup(b.statement));
  }
}
