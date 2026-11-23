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
import static org.apache.hop.core.database.DatabaseRegressionFixtures.WRITE_REFUSED;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.database;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.databaseMeta;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.layout;
import static org.apache.hop.core.database.DatabaseRegressionFixtures.sampleRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.DatabaseRegressionFixtures.DecliningBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.FailingBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.IntegerBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.LongStringBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.NationalStringBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.PlainDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.ReadOnlyBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.RetypeableValueMeta;
import org.apache.hop.core.database.DatabaseRegressionFixtures.StringBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.SwitchableBindingDialect;
import org.apache.hop.core.database.DatabaseRegressionFixtures.WriteOnlyBindingDialect;
import org.apache.hop.core.database.types.DatabaseTypeRuleRegistry;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Pins how {@link Database#setValues} writes a row into a prepared statement: which setter each
 * value type ends up on, when a binding the dialect declared takes over, and that every call uses
 * the binding for the row layout and dialect as they are at that call, however the layout changes
 * between calls on one {@link Database}.
 *
 * <p>Every test uses a fresh statement mock per call, verifies the exact setter calls and then that
 * nothing else was set.
 */
class DatabaseValueBindingPlanTest {

  @BeforeAll
  static void setUpClass() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    // Binding rules are cached per dialect class.
    DatabaseTypeRuleRegistry.clearCache();
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
    rowMeta.addValueMeta(new ValueMetaJson("json"));
    return rowMeta;
  }

  private static final Timestamp TIMESTAMP = Timestamp.valueOf("2024-02-29 12:34:56.123456789");
  private static final BigDecimal BIG = new BigDecimal("12345678901234567890.0123");
  private static final byte[] BYTES = {1, 2, 3};

  private static Object[] everyCommonTypeValues() throws Exception {
    JsonNode json = new ObjectMapper().readTree("{\"a\":1}");
    return new Object[] {
      3.5,
      42L,
      "abc",
      new Date(86_400_000L),
      TIMESTAMP,
      BIG,
      BYTES,
      true,
      InetAddress.getByName("192.168.10.20"),
      json
    };
  }

  private static void verifyEveryCommonTypeWritten(PreparedStatement ps) throws SQLException {
    verify(ps).setDouble(1, 3.5);
    verify(ps).setLong(2, 42L);
    verify(ps).setString(3, "abc");
    // A date goes in as a timestamp (precision -1, a dialect that converts timestamps to dates),
    // through the Calendar overload with the value type's date format time zone.
    verify(ps).setTimestamp(eq(4), eq(new Timestamp(86_400_000L)), any(Calendar.class));
    verify(ps).setTimestamp(5, TIMESTAMP);
    verify(ps).setBigDecimal(6, BIG);
    verify(ps).setBytes(7, BYTES);
    // The dialect does not claim a boolean data type, so a boolean is written as Y/N.
    verify(ps).setString(8, "Y");
    verify(ps).setString(9, "192.168.10.20");
    // JSON is written pretty printed.
    verify(ps).setString(eq(10), argThat(json -> json.replaceAll("\\s", "").equals("{\"a\":1}")));
    verifyNoMoreInteractions(ps);
  }

  @Test
  void everyCommonTypeGoesToItsOwnSetterWithoutBindings() throws Exception {
    Database db = database(new PlainDialect());
    PreparedStatement ps = mock(PreparedStatement.class);

    db.setValues(everyCommonType(), everyCommonTypeValues(), ps);

    verifyEveryCommonTypeWritten(ps);
  }

  @Test
  void rowMetaAndDataIsWrittenTheSameWay() throws Exception {
    Database db = database(new PlainDialect());
    PreparedStatement ps = mock(PreparedStatement.class);

    db.setValues(new RowMetaAndData(everyCommonType(), everyCommonTypeValues()), ps);

    verifyEveryCommonTypeWritten(ps);
  }

  @Test
  void everyCommonTypeWritesANullOfItsOwnSqlType() throws Exception {
    Database db = database(new PlainDialect());
    PreparedStatement ps = mock(PreparedStatement.class);

    db.setValues(everyCommonType(), new Object[10], ps);

    verify(ps).setNull(1, Types.DOUBLE);
    verify(ps).setNull(2, Types.INTEGER);
    verify(ps).setNull(3, Types.VARCHAR);
    verify(ps).setNull(4, Types.TIMESTAMP);
    verify(ps).setNull(5, Types.TIMESTAMP);
    verify(ps).setNull(6, Types.DECIMAL);
    verify(ps).setNull(7, Types.BINARY);
    verify(ps).setNull(8, Types.CHAR);
    verify(ps).setNull(9, Types.VARCHAR);
    verify(ps).setNull(10, Types.VARCHAR);
    verifyNoMoreInteractions(ps);
  }

  @Test
  void aBooleanIsABooleanWhenTheConnectionSupportsIt() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(new PlainDialect());
    databaseMeta.setSupportsBooleanDataType(true);
    Database db = database(databaseMeta);
    IRowMeta rowMeta = layout("b", IValueMeta.TYPE_BOOLEAN, IValueMeta.TYPE_BOOLEAN);

    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {false, null}, ps);

    verify(ps).setBoolean(1, false);
    verify(ps).setNull(2, Types.BOOLEAN);
    verifyNoMoreInteractions(ps);
  }

  @Test
  void aDateWithPrecisionOneIsWrittenAsADate() throws Exception {
    Database db = database(new PlainDialect());
    ValueMetaDate date = new ValueMetaDate("d");
    date.setPrecision(1);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(date);

    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {new Date(86_400_000L)}, ps);
    PreparedStatement nulls = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {null}, nulls);

    verify(ps).setDate(eq(1), eq(new java.sql.Date(86_400_000L)), any(Calendar.class));
    verifyNoMoreInteractions(ps);
    verify(nulls).setNull(1, Types.DATE);
    verifyNoMoreInteractions(nulls);
  }

  // ---------------------------------------------------------------------------------------------
  // Declared bindings
  // ---------------------------------------------------------------------------------------------

  @Test
  void aBindingForOneTypeIsUsedForThatTypeOnly() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta =
        layout(
            "c",
            IValueMeta.TYPE_INTEGER,
            IValueMeta.TYPE_STRING,
            IValueMeta.TYPE_NUMBER,
            IValueMeta.TYPE_STRING);

    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {7L, "x", 1.25, null}, ps);

    verify(ps).setLong(1, 7L);
    verify(ps).setObject(2, BOUND + "x", Types.OTHER);
    verify(ps).setDouble(3, 1.25);
    // The binding gets the null too.
    verify(ps).setObject(4, null, Types.OTHER);
    verifyNoMoreInteractions(ps);
  }

  @Test
  void aBindingIsUsedForEveryRowOfTheSameLayout() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    for (int round = 0; round < 5; round++) {
      PreparedStatement ps = mock(PreparedStatement.class);
      db.setValues(rowMeta, new Object[] {"v" + round, (long) round}, ps);

      verify(ps).setObject(1, BOUND + "v" + round, Types.OTHER);
      verify(ps).setLong(2, round);
      verifyNoMoreInteractions(ps);
    }
  }

  @Test
  void aReadOnlyBindingLeavesWritingToTheValueTypeForEveryRow() throws Exception {
    Database db = database(new ReadOnlyBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    for (int round = 0; round < 3; round++) {
      PreparedStatement ps = mock(PreparedStatement.class);
      Object value = round == 1 ? null : "v" + round;
      db.setValues(rowMeta, new Object[] {value, (long) round}, ps);

      if (value == null) {
        verify(ps).setNull(1, Types.VARCHAR);
      } else {
        verify(ps).setString(1, (String) value);
      }
      verify(ps).setLong(2, round);
      verifyNoMoreInteractions(ps);
    }
  }

  @Test
  void aWriteOnlyBindingWrites() throws Exception {
    Database db = database(new WriteOnlyBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"w"}, ps);

    verify(ps).setObject(1, "writeonly:w", Types.OTHER);
    verifyNoMoreInteractions(ps);
  }

  @Test
  void aFailingBindingFailsTheRowWithTheValueNamed() throws Exception {
    Database db = database(new FailingBindingDialect());
    IRowMeta rowMeta =
        layout("c", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING, IValueMeta.TYPE_NUMBER);
    PreparedStatement ps = mock(PreparedStatement.class);

    HopDatabaseException e =
        assertThrows(
            HopDatabaseException.class,
            () -> db.setValues(rowMeta, new Object[] {1L, "x", 2.0}, ps));

    assertTrue(e.getMessage().contains("offending row : "), e.getMessage());
    HopDatabaseException cause = assertInstanceOf(HopDatabaseException.class, e.getCause());
    assertTrue(
        cause.getMessage().contains("Unable to write value 'c1' to the prepared statement"),
        cause.getMessage());
    SQLException root = assertInstanceOf(SQLException.class, cause.getCause());
    assertEquals(WRITE_REFUSED, root.getMessage());

    // Written up to the failing value, nothing after it.
    verify(ps).setLong(1, 1L);
    verify(ps, never()).setDouble(anyInt(), anyDouble());
    verifyNoMoreInteractions(ps);
  }

  @Test
  void aFailingBindingFailsEveryRowNotJustTheFirst() throws Exception {
    Database db = database(new FailingBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    for (int round = 0; round < 3; round++) {
      PreparedStatement ps = mock(PreparedStatement.class);
      Object[] row = {"x" + round};
      HopDatabaseException e =
          assertThrows(HopDatabaseException.class, () -> db.setValues(rowMeta, row, ps));
      assertInstanceOf(SQLException.class, e.getCause().getCause());
      verifyNoMoreInteractions(ps);
    }
  }

  @Test
  void aBindingThatDeclinesSomeValuesIsStillAskedForTheNextOnes() throws Exception {
    // UnsupportedOperationException from write means "the value type writes this one". The
    // binding contract does not say that applies to every later value as well.
    Database db = database(new DecliningBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    PreparedStatement first = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {null}, first);
    verify(first).setNull(1, Types.VARCHAR);
    verifyNoMoreInteractions(first);

    PreparedStatement second = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"x"}, second);
    verify(second).setObject(1, BOUND + "x", Types.OTHER);
    verifyNoMoreInteractions(second);
  }

  @Test
  void ignoringAValueShiftsTheParametersButKeepsEachValuesBinding() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta =
        layout(
            "c",
            IValueMeta.TYPE_INTEGER,
            IValueMeta.TYPE_STRING,
            IValueMeta.TYPE_STRING,
            IValueMeta.TYPE_NUMBER);
    Object[] row = {1L, "b", "c", 4.0};

    PreparedStatement skipSecond = mock(PreparedStatement.class);
    db.setValues(rowMeta, row, skipSecond, 1);
    verify(skipSecond).setLong(1, 1L);
    verify(skipSecond).setObject(2, BOUND + "c", Types.OTHER);
    verify(skipSecond).setDouble(3, 4.0);
    verifyNoMoreInteractions(skipSecond);

    PreparedStatement skipFirst = mock(PreparedStatement.class);
    db.setValues(rowMeta, row, skipFirst, 0);
    verify(skipFirst).setObject(1, BOUND + "b", Types.OTHER);
    verify(skipFirst).setObject(2, BOUND + "c", Types.OTHER);
    verify(skipFirst).setDouble(3, 4.0);
    verifyNoMoreInteractions(skipFirst);

    PreparedStatement skipNone = mock(PreparedStatement.class);
    db.setValues(rowMeta, row, skipNone, -1);
    verify(skipNone).setLong(1, 1L);
    verify(skipNone).setObject(2, BOUND + "b", Types.OTHER);
    verify(skipNone).setObject(3, BOUND + "c", Types.OTHER);
    verify(skipNone).setDouble(4, 4.0);
    verifyNoMoreInteractions(skipNone);
  }

  @Test
  void theSingleValueSetterUsesTheBindingToo() throws Exception {
    Database db = database(new StringBindingDialect());

    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValue(ps, new ValueMetaString("s"), "one", 3);
    db.setValue(ps, new ValueMetaInteger("i"), 9L, 4);

    verify(ps).setObject(3, BOUND + "one", Types.OTHER);
    verify(ps).setLong(4, 9L);
    verifyNoMoreInteractions(ps);
  }

  // ---------------------------------------------------------------------------------------------
  // Layout changes between calls on one Database
  // ---------------------------------------------------------------------------------------------

  /**
   * Verifies the setters for one row written with the given Hop types bound: strings through {@link
   * DatabaseRegressionFixtures#STRING_BINDING}, integers through {@link
   * DatabaseRegressionFixtures#INTEGER_BINDING}, everything else the value type's own way.
   */
  private static void verifyWritten(
      PreparedStatement ps, IRowMeta rowMeta, Object[] row, Set<Integer> boundTypes)
      throws SQLException {
    for (int i = 0; i < rowMeta.size(); i++) {
      int position = i + 1;
      Object value = row[i];
      int type = rowMeta.getValueMeta(i).getType();
      boolean bound = boundTypes.contains(type);
      switch (type) {
        case IValueMeta.TYPE_STRING -> {
          if (bound) {
            verify(ps).setObject(position, BOUND + value, Types.OTHER);
          } else {
            verify(ps).setString(position, (String) value);
          }
        }
        case IValueMeta.TYPE_INTEGER -> {
          if (bound) {
            verify(ps).setObject(position, INT_BOUND + value, Types.BIGINT);
          } else {
            verify(ps).setLong(position, (Long) value);
          }
        }
        case IValueMeta.TYPE_NUMBER -> verify(ps).setDouble(position, (Double) value);
        case IValueMeta.TYPE_BIGNUMBER -> verify(ps).setBigDecimal(position, (BigDecimal) value);
        case IValueMeta.TYPE_BOOLEAN ->
            verify(ps).setString(position, Boolean.TRUE.equals(value) ? "Y" : "N");
        default -> throw new IllegalArgumentException("No test value type " + type);
      }
    }
    verifyNoMoreInteractions(ps);
  }

  private static void writeAndVerify(
      Database db, IRowMeta rowMeta, int round, Set<Integer> boundTypes) throws Exception {
    Object[] row = sampleRow(rowMeta, round);
    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, row, ps);
    verifyWritten(ps, rowMeta, row, boundTypes);
  }

  private static final Set<Integer> STRINGS = Set.of(IValueMeta.TYPE_STRING);
  private static final Set<Integer> INTEGERS = Set.of(IValueMeta.TYPE_INTEGER);
  private static final Set<Integer> NOTHING = Set.of();

  @Test
  void twoLayoutsAlternated() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta a = layout("a", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    IRowMeta b = layout("b", IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_STRING);

    for (int round = 0; round < 6; round++) {
      writeAndVerify(db, a, round, STRINGS);
      writeAndVerify(db, b, round, STRINGS);
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
      writeAndVerify(db, a, round, STRINGS);
      writeAndVerify(db, b, round, STRINGS);
      writeAndVerify(db, c, round, STRINGS);
    }
    // And backwards.
    for (int round = 5; round < 8; round++) {
      writeAndVerify(db, c, round, STRINGS);
      writeAndVerify(db, b, round, STRINGS);
      writeAndVerify(db, a, round, STRINGS);
    }
  }

  /** Seven layouts where the same position holds a string in some and something else in others. */
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
        writeAndVerify(db, rowMeta, round++, STRINGS);
      }
    }
    // An irregular order as well, including the same layout twice in a row.
    int[] order = {6, 0, 0, 5, 1, 6, 3, 2, 4, 4, 1, 5, 0, 3};
    for (int index : order) {
      writeAndVerify(db, layouts.get(index), round++, STRINGS);
    }
  }

  @Test
  void aValueMetaReplacedInTheSameRowMetaIsWrittenAsTheNewType() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    writeAndVerify(db, rowMeta, 0, STRINGS);

    rowMeta.setValueMeta(0, new ValueMetaInteger("c0"));
    writeAndVerify(db, rowMeta, 1, STRINGS);

    rowMeta.setValueMeta(1, new ValueMetaString("c1"));
    writeAndVerify(db, rowMeta, 2, STRINGS);

    rowMeta.setValueMeta(0, new ValueMetaString("c0"));
    writeAndVerify(db, rowMeta, 3, STRINGS);
  }

  @Test
  void aValueMetaAppendedToOrRemovedFromTheSameRowMetaIsWritten() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_INTEGER);

    writeAndVerify(db, rowMeta, 0, STRINGS);

    rowMeta.addValueMeta(new ValueMetaString("c1"));
    writeAndVerify(db, rowMeta, 1, STRINGS);

    rowMeta.addValueMeta(new ValueMetaNumber("c2"));
    writeAndVerify(db, rowMeta, 2, STRINGS);

    rowMeta.removeValueMeta(1);
    writeAndVerify(db, rowMeta, 3, STRINGS);

    // Same size as two calls ago, other types at the same positions.
    rowMeta.addValueMeta(0, new ValueMetaString("first"));
    rowMeta.removeValueMeta(2);
    assertEquals(2, rowMeta.size());
    writeAndVerify(db, rowMeta, 4, STRINGS);
  }

  @Test
  void aRowMetaGivenANewListIsWrittenAsTheNewList() throws Exception {
    Database db = database(new StringBindingDialect());
    RowMeta rowMeta = (RowMeta) layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    writeAndVerify(db, rowMeta, 0, STRINGS);

    List<IValueMeta> swapped = new ArrayList<>();
    swapped.add(new ValueMetaInteger("c0"));
    swapped.add(new ValueMetaString("c1"));
    rowMeta.setValueMetaList(swapped);
    writeAndVerify(db, rowMeta, 1, STRINGS);

    rowMeta.clear();
    rowMeta.addValueMeta(new ValueMetaString("only"));
    writeAndVerify(db, rowMeta, 2, STRINGS);
  }

  @Test
  void aConditionOnALengthChangedInPlaceIsEvaluatedForTheLengthAtEachCall() throws Exception {
    // The binding applies to strings longer than 100 only, the way the MySQL and CrateDB vector
    // bindings look at the length. The length is changed on the very value meta in the row.
    Database db = database(new LongStringBindingDialect());
    ValueMetaString string = new ValueMetaString("s");
    string.setLength(10);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(string);

    PreparedStatement shortString = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"short"}, shortString);
    verify(shortString).setString(1, "short");
    verifyNoMoreInteractions(shortString);

    string.setLength(500);
    PreparedStatement longString = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"long"}, longString);
    verify(longString).setObject(1, BOUND + "long", Types.OTHER);
    verifyNoMoreInteractions(longString);

    string.setLength(10);
    PreparedStatement shortAgain = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"short"}, shortAgain);
    verify(shortAgain).setString(1, "short");
    verifyNoMoreInteractions(shortAgain);
  }

  @Test
  void aValueMetaRetypedInPlaceIsWrittenAsItsNewType() throws Exception {
    // Only reachable through a ValueMetaBase subclass: the type field is protected.
    Database db = database(new StringBindingDialect());
    RetypeableValueMeta value = new RetypeableValueMeta("v", IValueMeta.TYPE_STRING);
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(value);

    PreparedStatement asString = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"x"}, asString);
    verify(asString).setObject(1, BOUND + "x", Types.OTHER);
    verifyNoMoreInteractions(asString);

    value.retype(IValueMeta.TYPE_INTEGER);
    PreparedStatement asInteger = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {7L}, asInteger);
    verify(asInteger).setLong(1, 7L);
    verifyNoMoreInteractions(asInteger);
  }

  @Test
  void equalRowMetasThatBindDifferentlyAreEachWrittenTheirOwnWay() throws Exception {
    // ValueMetaBase.equals ignores the original column type; the binding condition looks at it.
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
      PreparedStatement nationalPs = mock(PreparedStatement.class);
      db.setValues(nationalRow, new Object[] {"n" + round}, nationalPs);
      verify(nationalPs).setObject(1, BOUND + "n" + round, Types.OTHER);
      verifyNoMoreInteractions(nationalPs);

      PreparedStatement plainPs = mock(PreparedStatement.class);
      db.setValues(plainRow, new Object[] {"p" + round}, plainPs);
      verify(plainPs).setString(1, "p" + round);
      verifyNoMoreInteractions(plainPs);
    }
  }

  @Test
  void equalRowMetasWithTheSameBindingAreWrittenTheSameWay() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta first = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    IRowMeta second = first.clone();
    assertEquals(first, second);

    for (int round = 0; round < 3; round++) {
      writeAndVerify(db, first, round, STRINGS);
      writeAndVerify(db, second, round, STRINGS);
    }
  }

  @Test
  void theDialectSwitchedOnTheSameDatabaseMetaBindsByTheNewDialect() throws Exception {
    DatabaseMeta databaseMeta = databaseMeta(new StringBindingDialect());
    Database db = database(databaseMeta);
    IRowMeta rowMeta =
        layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER);

    writeAndVerify(db, rowMeta, 0, STRINGS);

    databaseMeta.setIDatabase(new IntegerBindingDialect());
    writeAndVerify(db, rowMeta, 1, INTEGERS);

    databaseMeta.setIDatabase(new PlainDialect());
    writeAndVerify(db, rowMeta, 2, NOTHING);

    databaseMeta.setIDatabase(new StringBindingDialect());
    writeAndVerify(db, rowMeta, 3, STRINGS);

    databaseMeta.setIDatabase(new IntegerBindingDialect());
    writeAndVerify(db, rowMeta, 4, INTEGERS);
  }

  @Test
  void aDialectSettingChangedInPlaceIsEvaluatedAtEachCall() throws Exception {
    // The binding applies while the dialect instance says so, like the Netezza date binding.
    SwitchableBindingDialect dialect = new SwitchableBindingDialect();
    Database db = database(dialect);
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING);

    writeAndVerify(db, rowMeta, 0, NOTHING);

    dialect.bindStrings = true;
    writeAndVerify(db, rowMeta, 1, STRINGS);

    dialect.bindStrings = false;
    writeAndVerify(db, rowMeta, 2, NOTHING);
  }

  @Test
  void theSameLayoutOnTwoDatabasesWithDifferentDialects() throws Exception {
    Database strings = database(new StringBindingDialect());
    Database integers = database(new IntegerBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);

    for (int round = 0; round < 3; round++) {
      writeAndVerify(strings, rowMeta, round, STRINGS);
      writeAndVerify(integers, rowMeta, round, INTEGERS);
    }
  }

  @Test
  void writingThroughTheBindingDoesNotChangeTheRowMeta() throws Exception {
    Database db = database(new StringBindingDialect());
    IRowMeta rowMeta = layout("c", IValueMeta.TYPE_STRING, IValueMeta.TYPE_INTEGER);
    IValueMeta first = rowMeta.getValueMeta(0);

    writeAndVerify(db, rowMeta, 0, STRINGS);

    assertEquals(2, rowMeta.size());
    assertSame(first, rowMeta.getValueMeta(0));
    // Nothing was asked to set a long on any statement while verifying: a sanity check that the
    // verification helper really saw the bound path.
    PreparedStatement ps = mock(PreparedStatement.class);
    db.setValues(rowMeta, new Object[] {"z", 1L}, ps);
    verify(ps, never()).setString(anyInt(), anyString());
    verify(ps).setLong(anyInt(), anyLong());
  }
}
