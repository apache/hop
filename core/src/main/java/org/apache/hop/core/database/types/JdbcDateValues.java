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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/**
 * Writing a date or timestamp through JDBC.
 *
 * <p>Held in one place because dialects differ in only one respect: some JDBC drivers have never
 * implemented the {@link Calendar} overloads of setDate and setTimestamp, and a dialect whose
 * driver is one of those should not have to restate the rest of the handling to say so.
 */
public final class JdbcDateValues {

  /** The calendar a thread last handed to a driver, with the time zone it was made for. */
  private record ZoneCalendar(TimeZone timeZone, Calendar calendar) {}

  private static final ThreadLocal<ZoneCalendar> CALENDARS = new ThreadLocal<>();

  private JdbcDateValues() {
    // Utility class.
  }

  /**
   * The calendar to hand to the driver for a time zone. Creating a calendar per value costs more
   * than the rest of binding a date, so each thread keeps the one it last used and makes a new one
   * only when the time zone differs. The driver only borrows the calendar for the duration of the
   * setDate or setTimestamp call, and a thread binds one value at a time.
   */
  static Calendar calendarFor(TimeZone timeZone) {
    ZoneCalendar cached = CALENDARS.get();
    if (cached != null && (cached.timeZone() == timeZone || cached.timeZone().equals(timeZone))) {
      return cached.calendar();
    }
    Calendar calendar = Calendar.getInstance(timeZone);
    CALENDARS.set(new ZoneCalendar(timeZone, calendar));
    return calendar;
  }

  /**
   * Writes a date value.
   *
   * @param calendarOverloadsSupported whether the driver implements the Calendar overloads of
   *     setDate and setTimestamp. When it does not, a configured time zone cannot be honoured.
   */
  public static void write(
      IDatabase database,
      IValueMeta valueMeta,
      PreparedStatement preparedStatement,
      int index,
      Object value,
      boolean calendarOverloadsSupported)
      throws SQLException, HopValueException {

    boolean asDate =
        valueMeta.getPrecision() == 1 || !database.isSupportsTimeStampToDateConversion();

    if (valueMeta.isNull(value)) {
      preparedStatement.setNull(index, asDate ? Types.DATE : Types.TIMESTAMP);
      return;
    }

    Calendar calendar =
        calendarOverloadsSupported && valueMeta.getDateFormatTimeZone() != null
            ? calendarFor(valueMeta.getDateFormatTimeZone())
            : null;

    if (asDate) {
      java.sql.Date date = new java.sql.Date(valueMeta.getInteger(value));
      if (calendar == null) {
        preparedStatement.setDate(index, date);
      } else {
        preparedStatement.setDate(index, date, calendar);
      }
      return;
    }

    // Preserve nanosecond precision when the value already carries it.
    Timestamp timestamp =
        value instanceof Timestamp existing ? existing : new Timestamp(valueMeta.getInteger(value));
    if (calendar == null) {
      preparedStatement.setTimestamp(index, timestamp);
    } else {
      preparedStatement.setTimestamp(index, timestamp, calendar);
    }
  }

  /** A binding for drivers that have not implemented the Calendar overloads. */
  public static final IValueBinding WITHOUT_CALENDAR_OVERLOADS =
      new IValueBinding() {
        @Override
        public Object read(
            IDatabase database, IValueMeta valueMeta, java.sql.ResultSet resultSet, int index) {
          throw new UnsupportedOperationException("This binding only writes values");
        }

        @Override
        public void write(
            IDatabase database,
            IValueMeta valueMeta,
            PreparedStatement preparedStatement,
            int index,
            Object value)
            throws SQLException, HopValueException {
          JdbcDateValues.write(database, valueMeta, preparedStatement, index, value, false);
        }
      };
}
