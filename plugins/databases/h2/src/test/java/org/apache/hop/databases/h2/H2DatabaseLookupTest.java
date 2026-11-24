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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins what {@link Database#getLookup} and the queries around it return on a real JDBC driver (an
 * in-memory H2 database): the values and the layout {@link Database#getReturnRowMeta()} reports,
 * for each overload, with and without lazy conversion, and when one Database mixes lookups with
 * other statements.
 *
 * <p>Every lookup overload reads a result with the layout of the statement it executes, with the
 * lazy conversion it asks for, whatever the Database ran before (#9679).
 */
class H2DatabaseLookupTest {

  private static final Timestamp TS = Timestamp.valueOf("2024-02-29 12:34:56.789");

  private DatabaseMeta databaseMeta;
  private Database db;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  @BeforeEach
  void setUp() throws Exception {
    databaseMeta =
        new DatabaseMeta(
            "h2",
            "H2",
            "Native",
            "",
            "mem:lookup" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1",
            "",
            "sa",
            "");
    db = new Database(new LoggingObject("lookup-test"), new Variables(), databaseMeta);
    db.connect();
    db.execStatement(
        "CREATE TABLE typed (id BIGINT PRIMARY KEY, i INTEGER, s VARCHAR(20), n DOUBLE,"
            + " bn DECIMAL(12,3), d DATE, ts TIMESTAMP, b BOOLEAN, bin VARBINARY(10))");
    db.execStatement(
        "INSERT INTO typed VALUES (1, 10, 'one', 1.5, 12.345, DATE '2024-02-29',"
            + " TIMESTAMP '2024-02-29 12:34:56.789', TRUE, X'0102')");
    db.execStatement(
        "INSERT INTO typed VALUES (2, 20, 'two', 2.5, 0.001, DATE '2000-01-01',"
            + " TIMESTAMP '2000-01-01 00:00:00', FALSE, X'FF')");
    db.execStatement(
        "INSERT INTO typed VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
    db.execStatement("CREATE TABLE other (code VARCHAR(10), amount DOUBLE, moment TIMESTAMP)");
    db.execStatement("INSERT INTO other VALUES ('B1', 2.5, TIMESTAMP '2024-02-29 12:34:56.789')");
    db.execStatement("INSERT INTO other VALUES ('B2', 3.5, TIMESTAMP '2001-01-01 00:00:00')");
    db.execStatement("INSERT INTO other VALUES ('B2', 4.5, TIMESTAMP '2002-01-01 00:00:00')");
    db.execStatement("CREATE SCHEMA lookups");
    db.execStatement("CREATE TABLE lookups.coded (k INTEGER, label VARCHAR(10))");
    db.execStatement("INSERT INTO lookups.coded VALUES (7, 'seven')");
    db.commit(true);
  }

  @AfterEach
  void tearDown() {
    if (db != null) {
      db.disconnect();
    }
  }

  private PreparedStatement prepare(String sql) throws Exception {
    return db.getConnection().prepareStatement(sql);
  }

  private static IRowMeta keyMeta(String name) {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger(name));
    return rowMeta;
  }

  private static void assertLayout(IRowMeta rowMeta, String... namesAndTypes) {
    assertNotNull(rowMeta);
    assertEquals(namesAndTypes.length / 2, rowMeta.size(), String.valueOf(rowMeta));
    for (int i = 0; i < rowMeta.size(); i++) {
      IValueMeta valueMeta = rowMeta.getValueMeta(i);
      assertEquals(namesAndTypes[2 * i], valueMeta.getName(), "name of column " + i);
      assertEquals(namesAndTypes[2 * i + 1], valueMeta.getTypeDesc(), "type of " + i);
    }
  }

  /**
   * The layout of {@link #TYPED_SQL}: Hop maps H2's DOUBLE to a BigNumber, DECIMAL(12,3) to a
   * Number.
   */
  private static final String[] TYPED_LAYOUT = {
    "I", "Integer",
    "S", "String",
    "N", "BigNumber",
    "BN", "Number",
    "D", "Date",
    "TS", "Timestamp",
    "B", "Boolean",
    "BIN", "Binary"
  };

  private static final String TYPED_SQL =
      "SELECT i, s, n, bn, d, ts, b, bin FROM typed WHERE id = ?";

  private Object[] lookupTyped(PreparedStatement ps, long id) throws Exception {
    db.setValues(keyMeta("id"), new Object[] {id}, ps);
    return db.getLookup(ps);
  }

  private static void assertTypedRowOne(Object[] row) {
    assertEquals(10L, row[0]);
    assertEquals("one", row[1]);
    assertEquals(new BigDecimal("1.5"), row[2]);
    assertEquals(12.345, row[3]);
    assertEquals(
        java.sql.Date.valueOf("2024-02-29").getTime(), ((java.util.Date) row[4]).getTime());
    assertEquals(TS, row[5]);
    assertEquals(Boolean.TRUE, row[6]);
    assertArrayEquals(new byte[] {1, 2}, (byte[]) row[7]);
  }

  private static Object[] firstValues(Object[] row, int count) {
    Object[] values = new Object[count];
    System.arraycopy(row, 0, values, 0, count);
    return values;
  }

  // ---------------------------------------------------------------------------------------------
  // getLookup(PreparedStatement): values and layout of every common type
  // ---------------------------------------------------------------------------------------------

  @Test
  void getLookupOfAStatementReadsEveryCommonType() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);

    assertTypedRowOne(lookupTyped(ps, 1));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);

    Object[] two = lookupTyped(ps, 2);
    assertEquals(20L, two[0]);
    assertEquals("two", two[1]);
    assertEquals(0.001, two[3]);
    assertEquals(Boolean.FALSE, two[6]);
    assertArrayEquals(new byte[] {(byte) 0xFF}, (byte[]) two[7]);
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);

    Object[] nulls = lookupTyped(ps, 3);
    for (int i = 0; i < 8; i++) {
      assertNull(nulls[i], "column " + i);
    }
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    ps.close();
  }

  @Test
  void getLookupReturnsARowAtLeastAsWideAsTheLayout() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    Object[] row = lookupTyped(ps, 1);
    assertTrue(row.length >= 8, "row of " + row.length);
    ps.close();
  }

  @Test
  void theLayoutCarriesTheColumnLengthsAndPrecisions() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    lookupTyped(ps, 1);
    IRowMeta rowMeta = db.getReturnRowMeta();
    assertEquals(20, rowMeta.getValueMeta(1).getLength());
    assertEquals(12, rowMeta.getValueMeta(3).getLength());
    assertEquals(3, rowMeta.getValueMeta(3).getPrecision());
    ps.close();
  }

  @Test
  void noRowFoundReturnsNullAndStillReportsTheLayout() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    assertNull(lookupTyped(ps, 99));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    ps.close();
  }

  @Test
  void twoStatementsAlternatedEachReturnTheirOwnRowAndLayout() throws Exception {
    PreparedStatement typed = prepare(TYPED_SQL);
    PreparedStatement other = prepare("SELECT code, amount, moment FROM other WHERE code = 'B1'");

    for (int round = 0; round < 4; round++) {
      assertTypedRowOne(lookupTyped(typed, 1));
      assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
      Object[] row = db.getLookup(other);
      assertArrayEquals(new Object[] {"B1", new BigDecimal("2.5"), TS}, firstValues(row, 3));
      assertLayout(
          db.getReturnRowMeta(), "CODE", "String", "AMOUNT", "BigNumber", "MOMENT", "Timestamp");
    }
    typed.close();
    other.close();
  }

  @Test
  void aStatementRunAfterItsTableGainedAColumnIsReadWithTheNewLayout() throws Exception {
    PreparedStatement ps = prepare("SELECT * FROM lookups.coded WHERE k = 7");
    assertArrayEquals(new Object[] {7L, "seven"}, firstValues(db.getLookup(ps), 2));
    assertLayout(db.getReturnRowMeta(), "K", "Integer", "LABEL", "String");

    db.execStatement("ALTER TABLE lookups.coded ADD COLUMN extra VARCHAR(5) DEFAULT 'x'");

    assertArrayEquals(new Object[] {7L, "seven", "x"}, firstValues(db.getLookup(ps), 3));
    assertLayout(db.getReturnRowMeta(), "K", "Integer", "LABEL", "String", "EXTRA", "String");
    ps.close();
  }

  @Test
  void aFailingLookupOfAStatementLeavesNoReturnLayout() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    lookupTyped(ps, 1);
    PreparedStatement failing = prepare("SELECT 1/i FROM typed WHERE id = 1");
    db.execStatement("UPDATE typed SET i = 0 WHERE id = 1");

    assertThrows(HopDatabaseException.class, () -> db.getLookup(failing));
    assertNull(db.getReturnRowMeta());
    ps.close();
    failing.close();
  }

  // ---------------------------------------------------------------------------------------------
  // Lazy conversion
  // ---------------------------------------------------------------------------------------------

  @Test
  void lazyConversionOnTheFirstLookupGivesBinaryStrings() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    db.setValues(keyMeta("id"), new Object[] {1L}, ps);

    Object[] row = db.getLookup(ps, false, true);

    assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), (byte[]) row[1]);
    assertEquals(10L, row[0]);
    assertEquals(new BigDecimal("1.5"), row[2]);
    assertTrue(db.getReturnRowMeta().getValueMeta(1).isStorageBinaryString());
    assertFalse(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
    assertEquals("one", db.getReturnRowMeta().getString(row, 1));
    ps.close();
  }

  @Test
  void getLookupOfAStatementIgnoresTheLazyConversionOfAnEarlierLookup() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    db.setValues(keyMeta("id"), new Object[] {1L}, ps);
    db.getLookup(ps, false, true);

    Object[] row = lookupTyped(ps, 1);

    assertEquals("one", row[1]);
    assertFalse(db.getReturnRowMeta().getValueMeta(1).isStorageBinaryString());
    ps.close();
  }

  /** A lookup without lazy conversion after a lazy one gets strings again (#9679). */
  @Test
  void theLazyConversionOfAnEarlierLookupDoesNotStick() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    db.setValues(keyMeta("id"), new Object[] {1L}, ps);
    db.getLookup(ps, false, true);

    db.setValues(keyMeta("id"), new Object[] {1L}, ps);
    Object[] notLazy = db.getLookup(ps, false, false);

    assertEquals("one", notLazy[1]);
    assertFalse(db.getReturnRowMeta().getValueMeta(1).isStorageBinaryString());

    db.setValues(keyMeta("id"), new Object[] {1L}, ps);
    assertInstanceOf(byte[].class, db.getLookup(ps, false, true)[1]);
    assertTrue(db.getReturnRowMeta().getValueMeta(1).isStorageBinaryString());
    ps.close();
  }

  // ---------------------------------------------------------------------------------------------
  // failOnMultipleResults
  // ---------------------------------------------------------------------------------------------

  @Test
  void failOnMultipleResultsRejectsASecondRow() throws Exception {
    PreparedStatement ps = prepare("SELECT amount FROM other WHERE code = 'B2'");
    HopDatabaseException e = assertThrows(HopDatabaseException.class, () -> db.getLookup(ps, true));
    assertTrue(
        e.getMessage()
            .contains(
                "Only 1 row was expected as a result of a lookup, and at least 2 were found!"),
        e.getMessage());
    ps.close();
  }

  @Test
  void failOnMultipleResultsAcceptsASingleRowAndNoRow() throws Exception {
    PreparedStatement one = prepare("SELECT amount FROM other WHERE code = 'B1'");
    assertEquals(new BigDecimal("2.5"), db.getLookup(one, true)[0]);
    one.close();
    Database fresh = new Database(new LoggingObject("fresh"), new Variables(), databaseMeta);
    fresh.connect();
    try {
      PreparedStatement none =
          fresh.getConnection().prepareStatement("SELECT amount FROM other WHERE code = 'none'");
      assertNull(fresh.getLookup(none, true));
      assertLayout(fresh.getReturnRowMeta(), "AMOUNT", "BigNumber");
      none.close();
    } finally {
      fresh.disconnect();
    }
  }

  @Test
  void withoutFailOnMultipleResultsTheFirstRowIsReturned() throws Exception {
    PreparedStatement ps =
        prepare("SELECT amount FROM other WHERE code = 'B2' ORDER BY amount DESC");
    assertEquals(new BigDecimal("4.5"), db.getLookup(ps)[0]);
    ps.close();
  }

  // ---------------------------------------------------------------------------------------------
  // setLookup and the overloads on the Database's own lookup statement
  // ---------------------------------------------------------------------------------------------

  @Test
  void setLookupWithoutSchemaReadsTheRequestedColumns() throws Exception {
    db.setLookup(
        "typed",
        new String[] {"id"},
        new String[] {"="},
        new String[] {"s", "bn", "ts"},
        null,
        null);
    db.setValuesLookup(keyMeta("id"), new Object[] {1L});

    Object[] row = db.getLookup();

    assertArrayEquals(new Object[] {"one", 12.345, TS}, firstValues(row, 3));
    assertLayout(db.getReturnRowMeta(), "S", "String", "BN", "Number", "TS", "Timestamp");
  }

  @Test
  void setLookupWithSchemaAndRename() throws Exception {
    db.setLookup(
        "lookups",
        "coded",
        new String[] {"k"},
        new String[] {"="},
        new String[] {"label"},
        new String[] {"renamed"},
        null);
    db.setValuesLookup(keyMeta("k"), new Object[] {7L});

    assertEquals("seven", db.getLookup(false)[0]);
    // The layout takes the column name, not the alias.
    assertLayout(db.getReturnRowMeta(), "LABEL", "String");

    db.setValuesLookup(keyMeta("k"), new Object[] {8L});
    assertNull(db.getLookup(false, false));
  }

  @Test
  void setLookupWithOrderByReturnsTheFirstOrderedRow() throws Exception {
    IRowMeta codeMeta = new RowMeta();
    codeMeta.addValueMeta(new ValueMetaString("code"));
    db.setLookup(
        "other",
        new String[] {"code"},
        new String[] {"="},
        new String[] {"amount", "moment"},
        null,
        "amount DESC");
    db.setValuesLookup(codeMeta, new Object[] {"B2"});
    assertEquals(new BigDecimal("4.5"), db.getLookup()[0]);

    db.setLookup(
        "other",
        new String[] {"code"},
        new String[] {"="},
        new String[] {"amount", "moment"},
        null,
        "amount ASC");
    db.setValuesLookup(codeMeta, new Object[] {"B2"});
    assertEquals(new BigDecimal("3.5"), db.getLookup()[0]);
    assertLayout(db.getReturnRowMeta(), "AMOUNT", "BigNumber", "MOMENT", "Timestamp");
  }

  @Test
  void setLookupCheckingForMultipleResultsRejectsTwoRows() throws Exception {
    IRowMeta codeMeta = new RowMeta();
    codeMeta.addValueMeta(new ValueMetaString("code"));
    db.setLookup(
        null,
        "other",
        new String[] {"code"},
        new String[] {"="},
        new String[] {"amount"},
        null,
        null,
        true);
    db.setValuesLookup(codeMeta, new Object[] {"B2"});
    assertThrows(HopDatabaseException.class, () -> db.getLookup(true));
    db.setValuesLookup(codeMeta, new Object[] {"B1"});
    assertEquals(new BigDecimal("2.5"), db.getLookup(true)[0]);
  }

  @Test
  void setLookupWithBetweenAndIsNullConditions() throws Exception {
    IRowMeta range = new RowMeta();
    range.addValueMeta(new ValueMetaInteger("low"));
    range.addValueMeta(new ValueMetaInteger("high"));
    db.setLookup(
        "typed",
        new String[] {"i", "s"},
        new String[] {"BETWEEN", "IS NOT NULL"},
        new String[] {"id"},
        null,
        "id");
    db.setValuesLookup(range, new Object[] {15L, 25L});
    assertEquals(2L, db.getLookup()[0]);
    assertLayout(db.getReturnRowMeta(), "ID", "Integer");
  }

  @Test
  void theFirstLookupThroughTheOwnStatementDerivesItsLayout() throws Exception {
    db.setLookup(
        "typed",
        new String[] {"id"},
        new String[] {"="},
        new String[] {"i", "s", "n", "bn", "d", "ts", "b", "bin"},
        null,
        null);
    db.setValuesLookup(keyMeta("id"), new Object[] {1L});
    assertTypedRowOne(db.getLookup(false, false));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    db.setValuesLookup(keyMeta("id"), new Object[] {2L});
    assertEquals("two", db.getLookup(false, false)[1]);
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
  }

  @Test
  void getLookupOfAStatementDerivesAgainAfterTheOwnStatement() throws Exception {
    db.setLookup(
        "other", new String[] {"code"}, new String[] {"="}, new String[] {"amount"}, null, null);
    IRowMeta codeMeta = new RowMeta();
    codeMeta.addValueMeta(new ValueMetaString("code"));
    db.setValuesLookup(codeMeta, new Object[] {"B1"});
    assertEquals(new BigDecimal("2.5"), db.getLookup()[0]);

    PreparedStatement typed = prepare(TYPED_SQL);
    assertTypedRowOne(lookupTyped(typed, 1));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    typed.close();
  }

  /** getLookup(ps, boolean) reads each statement with its own layout (#9679). */
  @Test
  void getLookupWithFailFlagReadsEachStatementWithItsOwnLayout() throws Exception {
    PreparedStatement first = prepare("SELECT s, i FROM typed WHERE id = 1");
    PreparedStatement second = prepare("SELECT code, amount, moment FROM other WHERE code = 'B1'");

    assertArrayEquals(new Object[] {"one", 10L}, firstValues(db.getLookup(first, false), 2));
    assertLayout(db.getReturnRowMeta(), "S", "String", "I", "Integer");

    Object[] row = db.getLookup(second, false);
    assertArrayEquals(new Object[] {"B1", new BigDecimal("2.5"), TS}, firstValues(row, 3));
    assertLayout(
        db.getReturnRowMeta(), "CODE", "String", "AMOUNT", "BigNumber", "MOMENT", "Timestamp");

    assertArrayEquals(new Object[] {"one", 10L}, firstValues(db.getLookup(first, false), 2));
    assertLayout(db.getReturnRowMeta(), "S", "String", "I", "Integer");
    first.close();
    second.close();
  }

  /**
   * A lookup through the Database's own lookup statement after a query is read with its own layout,
   * not the query's: it used to fail converting its text to the query's integer (#9679).
   */
  @Test
  void aLookupAfterGetRowsIsReadWithItsOwnLayout() throws Exception {
    List<Object[]> rows = db.getRows("SELECT id, i FROM typed ORDER BY id", -1);
    assertEquals(3, rows.size());
    assertLayout(db.getReturnRowMeta(), "ID", "Integer", "I", "Integer");

    db.setLookup(
        "typed", new String[] {"id"}, new String[] {"="}, new String[] {"s", "d"}, null, null);
    db.setValuesLookup(keyMeta("id"), new Object[] {1L});

    Object[] row = db.getLookup(false);
    assertEquals("one", row[0]);
    assertEquals(
        java.sql.Date.valueOf("2024-02-29").getTime(), ((java.util.Date) row[1]).getTime());
    assertLayout(db.getReturnRowMeta(), "S", "String", "D", "Date");
  }

  // ---------------------------------------------------------------------------------------------
  // Lookups mixed with insert, update and delete through the Database's own statements
  // ---------------------------------------------------------------------------------------------

  @Test
  void lookupsBetweenInsertUpdateAndDeleteSeeTheChanges() throws Exception {
    db.execStatement("CREATE TABLE dim (id BIGINT, name VARCHAR(10))");
    db.setLookup("dim", new String[] {"id"}, new String[] {"="}, new String[] {"name"}, null, null);

    IRowMeta insertMeta = new RowMeta();
    insertMeta.addValueMeta(new ValueMetaInteger("ID"));
    insertMeta.addValueMeta(new ValueMetaString("NAME"));
    db.prepareInsert(insertMeta, "dim");
    db.setValuesInsert(insertMeta, new Object[] {1L, "first"});
    db.insertRow();

    db.setValuesLookup(keyMeta("id"), new Object[] {1L});
    assertEquals("first", db.getLookup()[0]);
    assertLayout(db.getReturnRowMeta(), "NAME", "String");

    assertTrue(
        db.prepareUpdate("dim", new String[] {"id"}, new String[] {"="}, new String[] {"name"}));
    IRowMeta updateMeta = new RowMeta();
    updateMeta.addValueMeta(new ValueMetaString("name"));
    updateMeta.addValueMeta(new ValueMetaInteger("id"));
    db.setValuesUpdate(updateMeta, new Object[] {"changed", 1L});
    db.updateRow();

    db.setValuesLookup(keyMeta("id"), new Object[] {1L});
    assertEquals("changed", db.getLookup(false)[0]);
    assertLayout(db.getReturnRowMeta(), "NAME", "String");

    assertTrue(db.prepareDelete("dim", new String[] {"id"}, new String[] {"="}));
    db.setValuesUpdate(keyMeta("id"), new Object[] {1L});
    db.updateRow();

    db.setValuesLookup(keyMeta("id"), new Object[] {1L});
    assertNull(db.getLookup(false, false));
    assertLayout(db.getReturnRowMeta(), "NAME", "String");
    db.closeInsert();
    db.closeUpdate();
  }

  // ---------------------------------------------------------------------------------------------
  // Queries: openQuery / getRow / getRows / getOneRow
  // ---------------------------------------------------------------------------------------------

  @Test
  void openQueryAndGetRowReadEveryCommonType() throws Exception {
    ResultSet rs = db.openQuery("SELECT i, s, n, bn, d, ts, b, bin FROM typed ORDER BY id");
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    assertTypedRowOne(db.getRow(rs));
    assertEquals("two", db.getRow(rs)[1]);
    assertNull(db.getRow(rs)[1]);
    assertNull(db.getRow(rs));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    db.closeQuery(rs);
  }

  @Test
  void openQueryWithLazyConversionGivesBinaryStrings() throws Exception {
    ResultSet rs =
        db.openQuery(
            "SELECT s, i FROM typed WHERE id = 1", null, null, ResultSet.FETCH_FORWARD, true);
    IRowMeta rowMeta = db.getReturnRowMeta();
    assertTrue(rowMeta.getValueMeta(0).isStorageBinaryString());
    Object[] row = db.getRow(rs, true);
    assertArrayEquals("one".getBytes(StandardCharsets.UTF_8), (byte[]) row[0]);
    assertEquals(10L, row[1]);
    db.closeQuery(rs);
  }

  @Test
  void openQueryOfAPreparedStatementAndGetRow() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    ResultSet rs = db.openQuery(ps, keyMeta("id"), new Object[] {1L});
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    assertTypedRowOne(db.getRow(rs));
    rs.close();
    ps.close();
  }

  @Test
  void getRowsReportsTheLayoutOfItsQuery() throws Exception {
    List<Object[]> rows = db.getRows("SELECT code, amount FROM other ORDER BY amount", -1);
    assertEquals(3, rows.size());
    assertArrayEquals(new Object[] {"B1", new BigDecimal("2.5")}, firstValues(rows.get(0), 2));
    assertLayout(db.getReturnRowMeta(), "CODE", "String", "AMOUNT", "BigNumber");
  }

  @Test
  void getOneRowReportsTheLayoutOfItsQuery() throws Exception {
    RowMetaAndData one = db.getOneRow("SELECT s, bn FROM typed WHERE id = 1");
    assertEquals("one", one.getData()[0]);
    assertEquals(12.345, one.getData()[1]);
    assertLayout(one.getRowMeta(), "S", "String", "BN", "Number");
  }

  @Test
  void aQueryAfterALookupReportsItsOwnLayout() throws Exception {
    PreparedStatement ps = prepare(TYPED_SQL);
    lookupTyped(ps, 1);
    db.getRows("SELECT code, amount FROM other", -1);
    assertLayout(db.getReturnRowMeta(), "CODE", "String", "AMOUNT", "BigNumber");
    assertTypedRowOne(lookupTyped(ps, 1));
    assertLayout(db.getReturnRowMeta(), TYPED_LAYOUT);
    ps.close();
  }
}
