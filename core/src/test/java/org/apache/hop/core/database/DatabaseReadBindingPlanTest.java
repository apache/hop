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
import static org.apache.hop.core.database.DatabaseRegressionFixtures.INT_BOUND;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.READ_REFUSED;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.database;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.databaseMeta;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.layout;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.metaData;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.resultSet;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.sampleRow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseRegressionFixtures.Column;
import org.apache.hop.core.database.DatabaseRegressionFixtures.DecliningBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.FailingBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.InheritedOverrideDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.IntegerBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.NationalStringBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.OverridingReadDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.PlainDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.ReadOnlyBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.RetypeableValueMeta;
import org.apache.hop.core.database.DatabaseRegressionFixtures.StringBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.SwitchableBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.WriteOnlyBindingDialect;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaBinary;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaInternetAddress;
import org.apache.hop.core.row.value.ValueMetaJson;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins how {@link Database#getRow} reads a row from a result set: which getter each value type
 * uses, when a binding the dialect declared takes over, that a dialect which reads values itself is
 * asked for every value of every row, and that every call reads by the row layout and dialect as
 * they are at that call.
 */
class DatabaseReadBindingPlanTest {

  private static final ResultSetMetaData NO_METADATA = null;

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    // Binding rules are cached per dialect class.
    DatabaseTypeRuleRegistry.clearCache();
  }

  private static ResultSet oneRow(Object... values) {
    return resultSet(null, new Object[][] {values});
  }

  // ---------------------------------------------------------------------------------------------
  // Every common value type, no bindings
  // ---------------------------------------------------------------------------------------------

  private static IRowMeta everyCommonType() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaNumber("number"));
    rowMeta.addValueMeta(new ValueMetaInteger("integer"));
    rowMeta.addValueMeta(new ValueMetaString("string"));
    rowMeta.addValueMeta(new ValueMetaDate("date"));
    rowMeta.addValueMeta(new ValueMetaTimestamp("timestamp"));
    rowMeta.addValueMeta(new ValueMetaBigNumber("bignumber"));
    rowMeta.addValueMeta(new ValueMetaBinary("binary"));
    rowMeta.addValueMeta(new ValueMetaBoolean("boolean"));
    rowMeta.addValueMeta(new ValueMetaInternetAddress("inet"));
    return rowMeta;
  }

  private static final Timestamp DATE = Timestamp.valueOf("2024-02-29 00:00:00");
  private static final Timestamp TIMESTAMP = Timestamp.valueOf("2024-02-29 12:34:56.123456789");
  private static final BigDecimal BIG = new BigDecimal("12345678901234567890.0123");
  private static final byte[] BYTES = {1, 2, 3};

  @Test
  void everyCommonTypeIsReadWithItsOwnGetterWithoutBindings() throws Exception {
    Database db = database(new PlainDialect());
    IRowMeta rowMeta = everyCommonType();
    ResultSet rs = oneRow(3.5, 42L, "abc", DATE, TIMESTAMP, BIG, BYTES, true, "192.168.10.20");

    Object[] row = db.getRow(rs, NO_METADATA, rowMeta);

    assertEquals(3.5, row[0]);
    assertEquals(42L, row[1]);
    assertEquals("abc", row[2]);
    assertEquals(DATE, row[3]);
    assertEquals(TIMESTAMP, row[4]);
    assertEquals(BIG, row[5]);
    assertArrayEquals(BYTES, (byte[]) row[6]);
    assertEquals(true, row[7]);
    assertEquals(InetAddress.getByName("192.168.10.20"), row[8]);

    verify(rs).getDouble(1);
    verify(rs).getLong(2);
    verify(rs).getString(3);
    // A date is read as a timestamp: precision -1 and a dialect that converts timestamps to dates.
    verify(rs).getTimestamp(4);
    verify(rs).getTimestamp(5);
    verify(rs).getBigDecimal(6);
    verify(rs).getBytes(7);
    verify(rs).getBoolean(8);
    verify(rs).getString(9);
  }

  @Test
  void everyCommonTypeReadsNullWhenTheDriverSaysSo() throws Exception {
    Database db = database(new PlainDialect());
    IRowMeta rowMeta = everyCommonType();

    Object[] row = db.getRow(oneRow(new Object[rowMeta.size()]), NO_METADATA, rowMeta);

    for (int i = 0; i < rowMeta.size(); i++) {
      assertNull(row[i], rowMeta.getValueMeta(i).getName());
    }
  }

  @Test
  void jsonIsReadFromTheBinaryStream() throws Exception {
    Database db = database(new PlainDialect());
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaJson("json"));
    ResultSet rs = oneRow(new ByteArrayInputStream("{\"a\":1}".getBytes(StandardCharsets.UTF_8)));

    Object[] row = db.getRow(rs, NO_METADATA, rowMeta);

    assertEquals(new ObjectMapper().readTree("{\"a\":1}"), row[0]);
    verify(rs).getBinaryStream(1);
  }

  @Test
  void aDateWithPrecisionOneIsReadAsADate() throws Exception {
    Database db = database(new PlainDialect());
    ValueMetaDate date = new ValueMetaDate("d");
    date.setPrecision(1);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(date);
    java.sql.Date value = java.sql.Date.valueOf("2024-02-29");
    ResultSet rs = oneRow(value);

    Object[] row = db.getRow(rs, NO_METADATA, rowMeta);

    assertEquals(value, row[0]);
    verify(rs).getDate(1);
  }

  @Test
  void rowsAreReadUntilTheResultSetIsExhausted() throws Exception {
    Database db = database(new PlainDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    ResultSet rs = resultSet(null, new Object[] {"a", 1L}, new Object[] {null, 2L});

    assertArrayEquals(new Object[] {"a", 1L}, trim(db.getRow(rs, NO_METADATA, rowMeta), 2));
    assertArrayEquals(new Object[] {null, 2L}, trim(db.getRow(rs, NO_METADATA, rowMeta), 2));
    assertNull(db.getRow(rs, NO_METADATA, rowMeta));
  }

  /** The row data array may be longer than the row; only its first {@code size} values count. */
  private static Object[] trim(Object[] row, int size) {
    Object[] trimmed = new Object[size];
    System.arraycopy(row, 0, trimmed, 0, size);
    return trimmed;
  }

  // ---------------------------------------------------------------------------------------------
  // Declared bindings
  // ---------------------------------------------------------------------------------------------

  @Test
  void aReadBindingForOneTypeIsUsedForThatTypeOnly() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta =
        layout("c", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING, IValueMeta.TYPE_STRING);
    ResultSet rs = resultSet(null, new Object[] {7L, "x", null}, new Object[] {8L, null, "y"});

    assertArrayEquals(
        new Object[] {7L, BOUND + "x", null}, trim(db.getRow(rs, NO_METADATA, rowMeta), 3));
    assertArrayEquals(
        new Object[] {8L, null, BOUND + "y"}, trim(db.getRow(rs, NO_METADATA, rowMeta), 3));

    verify(rs, times(2)).getLong(1);
    verify(rs, times(2)).getObject(2);
    verify(rs, times(2)).getObject(3);
  }

  @Test
  void aWriteOnlyBindingLeavesReadingToTheValueTypeForEveryRow() throws Exception {
    Database db = database(new WriteOnlyBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);
    ResultSet rs = resultSet(null, new Object[] {"a"}, new Object[] {null}, new Object[] {"c"});

    assertEquals("a", db.getRow(rs, NO_METADATA, rowMeta)[0]);
    assertNull(db.getRow(rs, NO_METADATA, rowMeta)[0]);
    assertEquals("c", db.getRow(rs, NO_METADATA, rowMeta)[0]);
    verify(rs, times(3)).getString(1);
  }

  @Test
  void aReadOnlyBindingReads() throws Exception {
    Database db = database(new ReadOnlyBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    assertEquals("readonly:r", db.getRow(oneRow("r"), NO_METADATA, rowMeta)[0]);
  }

  @Test
  void aFailingReadBindingFailsTheRowWithTheValueNamed() throws Exception {
    Database db = database(new FailingBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING);

    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class, () -> db.getRow(oneRow(1L, "x"), NO_METADATA, rowMeta));

    assertTrue(e.getMessage().contains("Couldn't get row from result set"), e.getMessage());
    HopDatabaseException cause = assertInstanceOf(HopDatabaseException.class, e.getCause());
    assertTrue(
        cause.getMessage().contains("Unable to read value 'c1' from the result set"),
        cause.getMessage());
    SQLException root = assertInstanceOf(SQLException.class, cause.getCause());
    assertEquals(READ_REFUSED, root.getMessage());
  }

  @Test
  void aBindingThatDeclinesSomeValuesIsStillAskedForTheNextOnes() throws Exception {
    Database db = database(new DecliningBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);
    ResultSet rs = resultSet(null, new Object[] {null}, new Object[] {"x"});

    assertNull(db.getRow(rs, NO_METADATA, rowMeta)[0]);
    assertEquals(BOUND + "x", db.getRow(rs, NO_METADATA, rowMeta)[0]);
  }

  // ---------------------------------------------------------------------------------------------
  // A dialect that reads values itself
  // ---------------------------------------------------------------------------------------------

  private static void assertDialectReadsEveryValue(OverridingReadDialect dialect) throws Exception {
    Database db = database(dialect);
    IRowMeta rowMeta =
        layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER);
    ResultSet rs =
        resultSet(
            null,
            new Object[] {"a", 1L, 1.5},
            new Object[] {"b", 2L, 2.5},
            new Object[] {"c", 3L, 3.5});

    for (int round = 0; round < 3; round++) {
      Object[] row = db.getRow(rs, NO_METADATA, rowMeta);
      String letter = String.valueOf((char) ('a' + round));
      assertEquals("dialect:" + BOUND + letter, row[0]);
      assertEquals("dialect:" + (round + 1), row[1]);
      assertEquals("dialect:" + (round + 1.5), row[2]);
    }
    assertNull(db.getRow(rs, NO_METADATA, rowMeta));

    List<String> expected = new ArrayList<>();
    for (int round = 0; round < 3; round++) {
      expected.add("c0@0");
      expected.add("c1@1");
      expected.add("c2@2");
    }
    assertEquals(expected, dialect.asked);
  }

  @Test
  void aDialectOverridingGetValueFromResultSetIsAskedForEveryValueOfEveryRow() throws Exception {
    assertDialectReadsEveryValue(new OverridingReadDialect());
  }

  @Test
  void aDialectInheritingSuchAnOverrideIsAskedForEveryValueOfEveryRow() throws Exception {
    assertDialectReadsEveryValue(new InheritedOverrideDialect());
  }

  @Test
  void aDatabaseMetaSubclassIsAskedForEveryValueOfEveryRow() throws Exception {
    DatabaseMeta databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getIDatabase()).thenReturn(new StringBindingDialect());
    when(databaseMeta.getValueFromResultSet(any(ResultSet.class), any(IValueMeta.class), anyInt()))
        .thenAnswer(invocation -> "meta:" + invocation.getArgument(2));
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    Database db = new Database(log, new Variables(), databaseMeta);
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    ResultSet rs = resultSet(null, new Object[] {"a", 1L}, new Object[] {"b", 2L});

    assertArrayEquals(
        new Object[] {"meta:0", "meta:1"}, trim(db.getRow(rs, NO_METADATA, rowMeta), 2));
    assertArrayEquals(
        new Object[] {"meta:0", "meta:1"}, trim(db.getRow(rs, NO_METADATA, rowMeta), 2));
    verify(databaseMeta, times(4)).getValueFromResultSet(any(), any(), anyInt());
  }

  // ---------------------------------------------------------------------------------------------
  // Layout changes between calls on one Database
  // ---------------------------------------------------------------------------------------------

  private static final Set<Integer> STRINGS = Set.of(IValueMeta.TYPE_STRING);
  private static final Set<Integer> INTEGERS = Set.of(IValueMeta.TYPE_INTEGER);
  private static final Set<Integer> NOTHING = Set.of();

  /** What reading {@code stored} gives with the given Hop types bound. */
  private static Object expectedRead(int type, Object stored, Set<Integer> boundTypes) {
    if (boundTypes.contains(type)) {
      return (type == IValueMeta.TYPE_INTEGER ? INT_BOUND : BOUND) + stored;
    }
    return stored;
  }

  private static void readAndVerify(
      Database db, IRowMeta rowMeta, int round, Set<Integer> boundTypes) throws Exception {
    Object[] stored = sampleRow(rowMeta, round);
    Object[] row = db.getRow(oneRow(stored), NO_METADATA, rowMeta);
    for (int i = 0; i < rowMeta.size(); i++) {
      assertEquals(
          expectedRead(rowMeta.getValueMeta(i).getType(), stored[i], boundTypes),
          row[i],
          "column " + i + " of " + rowMeta + " in round " + round);
    }
  }

  @Test
  void twoLayoutsAlternated() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta a = layout("a", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    IRowMeta b = layout("b", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING);

    for (int round = 0; round < 6; round++) {
      readAndVerify(db, a, round, STRINGS);
      readAndVerify(db, b, round, STRINGS);
    }
  }

  @Test
  void threeLayoutsOfDifferentSizesRoundRobin() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta a = layout("a", IValueMeta.TYPE_STRING);
    IRowMeta b =
        layout("b", IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_STRING, IValueMeta.TYPE_BOOLEAN);
    IRowMeta c = layout("c", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_BIGNUMBER);

    for (int round = 0; round < 5; round++) {
      readAndVerify(db, a, round, STRINGS);
      readAndVerify(db, b, round, STRINGS);
      readAndVerify(db, c, round, STRINGS);
    }
    for (int round = 5; round < 8; round++) {
      readAndVerify(db, c, round, STRINGS);
      readAndVerify(db, b, round, STRINGS);
      readAndVerify(db, a, round, STRINGS);
    }
  }

  private static List<IRowMeta> sevenLayouts() {
    int s = IValueMeta.TYPE_STRING;
    int i = IValueMeta.TYPE_INTEGER;
    int n = IValueMeta.TYPE_NUMBER;
    int b = IValueMeta.TYPE_BOOLEAN;
    int g = IValueMeta.TYPE_BIGNUMBER;
    List<IRowMeta> layouts = new ArrayList<>();
    layouts.add(layout("l0", s, i));
    layouts.add(layout("l1", i, s));
    layouts.add(layout("l2", s, s, s));
    layouts.add(layout("l3", n, n, n));
    layouts.add(layout("l4", s, b, g, i));
    layouts.add(layout("l5", g, i, s, n, s));
    layouts.add(layout("l6", i));
    return layouts;
  }

  @Test
  void moreLayoutsThanAnyCacheWouldHoldCycledRepeatedly() throws Exception {
    Database db = database(new StringBindingDialect());
    List<IRowMeta> layouts = sevenLayouts();

    int round = 0;
    for (int cycle = 0; cycle < 4; cycle++) {
      for (IRowMeta rowMeta : layouts) {
        readAndVerify(db, rowMeta, round++, STRINGS);
      }
    }
    int[] order = {6, 0, 0, 5, 1, 6, 3, 2, 4, 4, 1, 5, 0, 3};
    for (int index : order) {
      readAndVerify(db, layouts.get(index), round++, STRINGS);
    }
  }

  @Test
  void aValueMetaReplacedAppendedOrRemovedInTheSameRowMetaIsRead() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    readAndVerify(db, rowMeta, 0, STRINGS);

    rowMeta.setValueMeta(0, new ValueMetaInteger("c0"));
    readAndVerify(db, rowMeta, 1, STRINGS);

    rowMeta.addValueMeta(new ValueMetaString("c2"));
    readAndVerify(db, rowMeta, 2, STRINGS);

    rowMeta.removeValueMeta(0);
    readAndVerify(db, rowMeta, 3, STRINGS);

    rowMeta.setValueMeta(1, new ValueMetaNumber("c2"));
    rowMeta.setValueMeta(0, new ValueMetaString("c1"));
    readAndVerify(db, rowMeta, 4, STRINGS);
  }

  @Test
  void anOriginalColumnTypeChangedInPlaceIsEvaluatedAtEachCall() throws Exception {
    // The binding applies to NVARCHAR columns only, the way the DuckDB time binding looks at the
    // original column type. It is changed on the very value meta in the row.
    Database db = database(new NationalStringBindingDialect());
    ValueMetaString string = new ValueMetaString("s");
    string.setOriginalColumnType(Types.VARCHAR);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(string);

    assertEquals("plain", db.getRow(oneRow("plain"), NO_METADATA, rowMeta)[0]);

    string.setOriginalColumnType(Types.NVARCHAR);
    assertEquals(BOUND + "national", db.getRow(oneRow("national"), NO_METADATA, rowMeta)[0]);

    string.setOriginalColumnType(Types.VARCHAR);
    assertEquals("plain", db.getRow(oneRow("plain"), NO_METADATA, rowMeta)[0]);
  }

  @Test
  void aValueMetaRetypedInPlaceIsReadAsItsNewType() throws Exception {
    Database db = database(new StringBindingDialect());
    RetypeableValueMeta value = new RetypeableValueMeta("v", IValueMeta.TYPE_STRING);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(value);

    assertEquals(BOUND + "x", db.getRow(oneRow("x"), NO_METADATA, rowMeta)[0]);

    value.retype(IValueMeta.TYPE_INTEGER);
    assertEquals(7L, db.getRow(oneRow(7L), NO_METADATA, rowMeta)[0]);
  }

  @Test
  void equalRowMetasThatBindDifferentlyAreEachReadTheirOwnWay() throws Exception {
    Database db = database(new NationalStringBindingDialect());
    ValueMetaString national = new ValueMetaString("s");
    national.setOriginalColumnType(Types.NVARCHAR);
    ValueMetaString plain = new ValueMetaString("s");
    plain.setOriginalColumnType(Types.VARCHAR);
    IRowMeta nationalRow = new RowMeta();
    nationalRow.addValueMeta(national);
    IRowMeta plainRow = new RowMeta();
    plainRow.addValueMeta(plain);
    assertEquals(nationalRow, plainRow);
    assertNotSame(nationalRow, plainRow);

    for (int round = 0; round < 3; round++) {
      assertEquals(
          BOUND + "n" + round, db.getRow(oneRow("n" + round), NO_METADATA, nationalRow)[0]);
      assertEquals("p" + round, db.getRow(oneRow("p" + round), NO_METADATA, plainRow)[0]);
    }
  }

  @Test
  void theDialectSwitchedOnTheSameDatabaseMetaReadsByTheNewDialect() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(new StringBindingDialect());
    Database db = database(databaseMeta);
    IRowMeta rowMeta =
        layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER);

    readAndVerify(db, rowMeta, 0, STRINGS);

    databaseMeta.setIDatabase(new IntegerBindingDialect());
    readAndVerify(db, rowMeta, 1, INTEGERS);

    databaseMeta.setIDatabase(new PlainDialect());
    readAndVerify(db, rowMeta, 2, NOTHING);

    databaseMeta.setIDatabase(new StringBindingDialect());
    readAndVerify(db, rowMeta, 3, STRINGS);
  }

  @Test
  void switchingToAndFromADialectThatReadsItselfFollowsTheDialect() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(new StringBindingDialect());
    Database db = database(databaseMeta);
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    readAndVerify(db, rowMeta, 0, STRINGS);

    OverridingReadDialect overriding = new OverridingReadDialect();
    databaseMeta.setIDatabase(overriding);
    Object[] row = db.getRow(oneRow("x", 5L), NO_METADATA, rowMeta);
    assertEquals("dialect:" + BOUND + "x", row[0]);
    assertEquals("dialect:5", row[1]);
    assertEquals(List.of("c0@0", "c1@1"), overriding.asked);

    databaseMeta.setIDatabase(new StringBindingDialect());
    readAndVerify(db, rowMeta, 1, STRINGS);

    databaseMeta.setIDatabase(overriding);
    db.getRow(oneRow("y", 6L), NO_METADATA, rowMeta);
    assertEquals(4, overriding.asked.size());
  }

  @Test
  void aDialectSettingChangedInPlaceIsEvaluatedAtEachCall() throws Exception {
    SwitchableBindingDialect dialect = new SwitchableBindingDialect();
    Database db = database(dialect);
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    readAndVerify(db, rowMeta, 0, NOTHING);

    dialect.bindStrings = true;
    readAndVerify(db, rowMeta, 1, STRINGS);

    dialect.bindStrings = false;
    readAndVerify(db, rowMeta, 2, NOTHING);
  }

  // ---------------------------------------------------------------------------------------------
  // Lazy conversion, with the layout derived by getRow(ResultSet, boolean)
  // ---------------------------------------------------------------------------------------------

  private static ResultSetMetaData nameAndId() throws SQLException {
    return metaData(Column.of("name", Types.VARCHAR), Column.of("id", Types.INTEGER));
  }

  @Test
  void withoutLazyConversionAStringIsReadAsAString() throws Exception {
    Database db = database(new PlainDialect());
    ResultSet rs = resultSet(nameAndId(), new Object[] {"abc", 1L});

    Object[] row = db.getRow(rs, false);

    assertEquals("abc", row[0]);
    assertEquals(1L, row[1]);
    assertFalse(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
    verify(rs).getString(1);
  }

  @Test
  void withLazyConversionAStringIsReadAsItsBytes() throws Exception {
    Database db = database(new PlainDialect());
    byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
    ResultSet rs = resultSet(nameAndId(), new Object[] {bytes, 1L});

    Object[] row = db.getRow(rs, true);

    assertArrayEquals(bytes, (byte[]) row[0]);
    assertEquals(1L, row[1]);
    IValueMeta name = db.getReturnRowMeta().getValueMeta(0);
    assertTrue(name.isStorageBinaryString());
    assertEquals("abc", name.getString(row[0]));
    verify(rs).getBytes(1);
  }

  @Test
  void withLazyConversionAStringBindingStillHandsBackItsOwnValue() throws Exception {
    // The layout says binary string storage, the binding returns what it reads: a String.
    Database db = database(new StringBindingDialect());
    ResultSet rs = resultSet(nameAndId(), new Object[] {"abc", 1L});

    Object[] row = db.getRow(rs, true);

    assertEquals(BOUND + "abc", row[0]);
    assertTrue(db.getReturnRowMeta().getValueMeta(0).isStorageBinaryString());
  }

  @Test
  void theLayoutDerivedByTheFirstGetRowIsKeptWhateverLazyConversionLaterCallsAskFor()
      throws Exception {
    Database db = database(new PlainDialect());
    byte[] bytes = "abc".getBytes(StandardCharsets.UTF_8);
    ResultSet rs =
        resultSet(nameAndId(), new Object[] {bytes, 1L}, new Object[] {"def".getBytes(), 2L});

    db.getRow(rs, true);
    IRowMeta derived = db.getReturnRowMeta();
    Object[] second = db.getRow(rs, false);

    assertSame(derived, db.getReturnRowMeta(), "the first layout is kept");
    assertArrayEquals("def".getBytes(), (byte[]) second[0]);
  }

  @Test
  void anExplicitBinaryStringLayoutIsReadAsBytesAndANormalOneAsStrings() throws Exception {
    Database db = database(new PlainDialect());
    ValueMetaString lazy = new ValueMetaString("lazy");
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazy.setStorageMetadata(new ValueMetaString("lazy"));
    IRowMeta lazyRow = new RowMeta();
    lazyRow.addValueMeta(lazy);
    IRowMeta normalRow = layout("normal", IValueMeta.TYPE_STRING);
    byte[] bytes = "x".getBytes(StandardCharsets.UTF_8);

    for (int round = 0; round < 3; round++) {
      ResultSet lazyRs = oneRow((Object) bytes);
      assertArrayEquals(bytes, (byte[]) db.getRow(lazyRs, NO_METADATA, lazyRow)[0]);
      verify(lazyRs).getBytes(1);

      ResultSet normalRs = oneRow("y");
      assertEquals("y", db.getRow(normalRs, NO_METADATA, normalRow)[0]);
      verify(normalRs).getString(1);
    }
  }
}
