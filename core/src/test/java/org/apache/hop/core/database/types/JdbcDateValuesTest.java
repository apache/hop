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
package org.apache.hop.core.database.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class JdbcDateValuesTest {

  private static final long INSTANT = 1_795_000_000_123L;

  private IDatabase database;
  private PreparedStatement statement;

  @BeforeEach
  void setUp() {
    database = mock(IDatabase.class);
    when(database.isSupportsTimeStampToDateConversion()).thenReturn(true);
    statement = mock(PreparedStatement.class);
  }

  private static ValueMetaDate date(String zone, int precision) {
    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setPrecision(precision);
    valueMeta.setDateFormatTimeZone(zone == null ? null : TimeZone.getTimeZone(zone));
    return valueMeta;
  }

  private List<Calendar> writeTimestamps(ValueMetaDate valueMeta, int count) throws Exception {
    for (int i = 0; i < count; i++) {
      JdbcDateValues.write(database, valueMeta, statement, 1, new Date(INSTANT), true);
    }
    ArgumentCaptor<Calendar> calendars = ArgumentCaptor.forClass(Calendar.class);
    verify(statement, times(count))
        .setTimestamp(eq(1), eq(new Timestamp(INSTANT)), calendars.capture());
    return calendars.getAllValues();
  }

  @Test
  void theSameZoneOnOneThreadGetsTheSameCalendar() {
    TimeZone zone = TimeZone.getTimeZone("Europe/Brussels");
    Calendar first = JdbcDateValues.calendarFor(zone);
    assertSame(first, JdbcDateValues.calendarFor(zone));
    // An equal zone that is another object is still the same zone.
    assertSame(first, JdbcDateValues.calendarFor(TimeZone.getTimeZone("Europe/Brussels")));
    assertEquals(zone, first.getTimeZone());
  }

  @Test
  void anotherZoneGetsACalendarInThatZone() {
    TimeZone brussels = TimeZone.getTimeZone("Europe/Brussels");
    TimeZone kathmandu = TimeZone.getTimeZone("Asia/Kathmandu");
    Calendar first = JdbcDateValues.calendarFor(brussels);
    Calendar second = JdbcDateValues.calendarFor(kathmandu);
    assertNotSame(first, second);
    assertEquals(kathmandu, second.getTimeZone());
    // Switching back gives a calendar in the first zone again.
    assertEquals(brussels, JdbcDateValues.calendarFor(brussels).getTimeZone());
  }

  @Test
  void eachThreadHasItsOwnCalendar() throws Exception {
    TimeZone zone = TimeZone.getTimeZone("UTC");
    Calendar here = JdbcDateValues.calendarFor(zone);
    AtomicReference<Calendar> there = new AtomicReference<>();
    Thread thread = new Thread(() -> there.set(JdbcDateValues.calendarFor(zone)));
    thread.start();
    thread.join();
    assertNotSame(here, there.get());
    assertEquals(zone, there.get().getTimeZone());
  }

  @Test
  void rowsInOneZoneShareTheCalendarHandedToTheDriver() throws Exception {
    List<Calendar> calendars = writeTimestamps(date("America/New_York", 0), 3);
    assertSame(calendars.get(0), calendars.get(1));
    assertSame(calendars.get(0), calendars.get(2));
    assertEquals(TimeZone.getTimeZone("America/New_York"), calendars.get(0).getTimeZone());
  }

  @Test
  void columnsInDifferentZonesEachGetACalendarInTheirOwnZone() throws Exception {
    List<TimeZone> seen = new ArrayList<>();
    for (String zone : new String[] {"UTC", "Asia/Kathmandu", "UTC", "Australia/Lord_Howe"}) {
      PreparedStatement ps = mock(PreparedStatement.class);
      JdbcDateValues.write(database, date(zone, 0), ps, 1, new Date(INSTANT), true);
      ArgumentCaptor<Calendar> calendar = ArgumentCaptor.forClass(Calendar.class);
      verify(ps).setTimestamp(eq(1), any(Timestamp.class), calendar.capture());
      seen.add(calendar.getValue().getTimeZone());
    }
    assertEquals(
        List.of(
            TimeZone.getTimeZone("UTC"),
            TimeZone.getTimeZone("Asia/Kathmandu"),
            TimeZone.getTimeZone("UTC"),
            TimeZone.getTimeZone("Australia/Lord_Howe")),
        seen);
  }

  @Test
  void aDateWithPrecisionOneUsesTheZoneCalendar() throws Exception {
    JdbcDateValues.write(
        database, date("Asia/Kathmandu", 1), statement, 2, new Date(INSTANT), true);
    ArgumentCaptor<Calendar> calendar = ArgumentCaptor.forClass(Calendar.class);
    verify(statement).setDate(eq(2), eq(new java.sql.Date(INSTANT)), calendar.capture());
    assertEquals(TimeZone.getTimeZone("Asia/Kathmandu"), calendar.getValue().getTimeZone());
  }

  @Test
  void withoutCalendarOverloadsNoCalendarIsPassed() throws Exception {
    JdbcDateValues.write(database, date("UTC", 0), statement, 1, new Date(INSTANT), false);
    JdbcDateValues.write(database, date("UTC", 1), statement, 2, new Date(INSTANT), false);
    verify(statement).setTimestamp(1, new Timestamp(INSTANT));
    verify(statement).setDate(2, new java.sql.Date(INSTANT));
    verify(statement, never()).setTimestamp(anyInt(), any(Timestamp.class), any(Calendar.class));
    verify(statement, never()).setDate(anyInt(), any(java.sql.Date.class), any(Calendar.class));
  }

  @Test
  void aNullValueIsBoundAsNullWithoutACalendar() throws Exception {
    JdbcDateValues.write(database, date("UTC", 0), statement, 1, null, true);
    JdbcDateValues.write(database, date("UTC", 1), statement, 2, null, true);
    verify(statement).setNull(1, java.sql.Types.TIMESTAMP);
    verify(statement).setNull(2, java.sql.Types.DATE);
  }
}
