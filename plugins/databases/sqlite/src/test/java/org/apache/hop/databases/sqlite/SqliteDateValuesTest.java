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

package org.apache.hop.databases.sqlite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Reading and writing the date and time formats SQLite accepts. Issue #3910. */
class SqliteDateValuesTest {

  /**
   * The formats listed at https://www.sqlite.org/lang_datefunc.html. Only the third of these is one
   * the SQLite JDBC driver parses by itself; the others used to fail with "Error parsing time
   * stamp".
   */
  @ParameterizedTest(name = "{0} is {1}")
  @CsvSource({
    "2024-05-16,                  2024-05-16 00:00:00.0",
    "2024-05-16 10:11,            2024-05-16 10:11:00.0",
    "2024-05-16 10:11:12,         2024-05-16 10:11:12.0",
    "2024-05-16 10:11:12.123,     2024-05-16 10:11:12.123",
    "2024-05-16T10:11,            2024-05-16 10:11:00.0",
    "2024-05-16T10:11:12,         2024-05-16 10:11:12.0",
    "2024-05-16T10:11:12.123,     2024-05-16 10:11:12.123",
  })
  void parsesEveryDateFormatSqliteAccepts(String stored, String expected) {
    assertEquals(Timestamp.valueOf(expected), SqliteDateValues.parse(stored));
  }

  /** SQLite dates a value that carries only a time to 2000-01-01. */
  @ParameterizedTest(name = "{0} is {1}")
  @CsvSource({
    "10:11,        2000-01-01 10:11:00.0",
    "10:11:12,     2000-01-01 10:11:12.0",
    "10:11:12.123, 2000-01-01 10:11:12.123",
  })
  void datesATimeOfDayToTheDateSqliteGivesIt(String stored, String expected) {
    assertEquals(Timestamp.valueOf(expected), SqliteDateValues.parse(stored));
  }

  @Test
  void readsATrailingZoneAsTheZoneOfTheValue() {
    Timestamp utc =
        Timestamp.from(OffsetDateTime.of(2024, 5, 16, 10, 11, 12, 0, ZoneOffset.UTC).toInstant());

    assertEquals(utc, SqliteDateValues.parse("2024-05-16T10:11:12Z"));
    assertEquals(utc, SqliteDateValues.parse("2024-05-16 10:11:12+00:00"));
    assertEquals(
        Timestamp.from(
            OffsetDateTime.of(2024, 5, 16, 12, 11, 12, 0, ZoneOffset.ofHours(2)).toInstant()),
        SqliteDateValues.parse("2024-05-16 12:11:12+02:00"));
  }

  /** A value with no zone is local time, which is how the driver reads the format it does parse. */
  @Test
  void readsAValueWithoutAZoneAsLocalTime() {
    assertEquals(
        Timestamp.valueOf("2024-05-16 10:11:12.0"), SqliteDateValues.parse("2024-05-16 10:11:12"));
  }

  @Test
  void keepsPrecisionBeyondMilliseconds() {
    assertEquals(
        Timestamp.valueOf("2024-05-16 10:11:12.123456789"),
        SqliteDateValues.parse("2024-05-16 10:11:12.123456789"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        "   ",
        "not a date",
        "2024-05-16X10:11:12",
        "2024-13-45",
        "16/05/2024",
        "2460446.9",
      })
  void refusesWhatIsNotADateSqliteWouldRead(String stored) {
    assertNull(SqliteDateValues.parse(stored));
  }

  @Test
  void readsTheStoredTextRatherThanAskingTheDriverToParseIt() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject(1)).thenReturn("2024-05-16");

    assertEquals(Timestamp.valueOf("2024-05-16 00:00:00.0"), SqliteDateValues.read(resultSet, 1));
    verify(resultSet, never()).getTimestamp(anyInt());
  }

  /** A number keeps the driver's reading: that is what Hop's own writes used to produce. */
  @Test
  void leavesANumberToTheDriver() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject(1)).thenReturn(1715854272000L);
    when(resultSet.getTimestamp(1)).thenReturn(Timestamp.valueOf("2024-05-16 12:11:12.0"));

    assertEquals(Timestamp.valueOf("2024-05-16 12:11:12.0"), SqliteDateValues.read(resultSet, 1));
  }

  @Test
  void readsNullAsNull() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject(1)).thenReturn(null);

    assertNull(SqliteDateValues.read(resultSet, 1));
  }

  @Test
  void saysWhichValueItCouldNotRead() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject(1)).thenReturn("16/05/2024");

    SQLException e = assertThrows(SQLException.class, () -> SqliteDateValues.read(resultSet, 1));
    assertTrue(e.getMessage().contains("16/05/2024"), e.getMessage());
  }

  @Test
  void writesADateAsTextSqliteCanRead() throws Exception {
    assertEquals(
        "2024-05-16 10:11:12.000",
        written(new ValueMetaDate("d"), Timestamp.valueOf("2024-05-16 10:11:12.0")));
  }

  @Test
  void writesTheNanosecondsATimestampCarries() throws Exception {
    assertEquals(
        "2024-05-16 10:11:12.123456789",
        written(new ValueMetaTimestamp("ts"), Timestamp.valueOf("2024-05-16 10:11:12.123456789")));
  }

  /** Precision 1 is how the rest of Hop's JDBC date handling spells "no time of day". */
  @Test
  void writesADateWithoutATimeOfDayAsADate() throws Exception {
    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setPrecision(1);

    assertEquals("2024-05-16", written(valueMeta, Timestamp.valueOf("2024-05-16 10:11:12.0")));
  }

  @Test
  void writesNullAsNull() throws Exception {
    PreparedStatement statement = mock(PreparedStatement.class);

    SqliteDateValues.write(dialect(), new ValueMetaDate("d"), statement, 1, null);

    verify(statement).setNull(1, Types.VARCHAR);
  }

  private static String written(IValueMeta valueMeta, Object value)
      throws SQLException, HopValueException {
    PreparedStatement statement = mock(PreparedStatement.class);
    SqliteDateValues.write(dialect(), valueMeta, statement, 1, value);

    ArgumentCaptor<String> text = ArgumentCaptor.forClass(String.class);
    verify(statement).setString(eq(1), text.capture());
    return text.getValue();
  }

  private static IDatabase dialect() {
    return new SqliteDatabaseMeta();
  }
}
