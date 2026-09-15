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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/**
 * Reading and writing a date on SQLite.
 *
 * <p>SQLite has no date or time storage class. A date is text, a number, or a Julian day, held in
 * whichever column it was put, and the declared type of that column only decides its affinity (<a
 * href="https://www.sqlite.org/datatype3.html">datatype3</a>). The JDBC driver hides that behind
 * getDate and getTimestamp, which parse exactly one text layout, {@code yyyy-MM-dd HH:mm:ss.SSS},
 * and answer "Error parsing time stamp" for every other layout SQLite documents as a date/time
 * value (<a href="https://www.sqlite.org/lang_datefunc.html">lang_datefunc</a>) - including the
 * plain {@code YYYY-MM-DD} that SQLite's own DATE() returns and that every other client displays as
 * a date. So this reads the stored value and parses it here.
 *
 * <p>Writing goes back out as text for the same reason. Left to the driver, a date is written as a
 * count of milliseconds since the epoch: SQLite's own DATE() and DATETIME() return null on it, and
 * other clients show a large integer where a date should be.
 *
 * <p>See issue #3910.
 */
public final class SqliteDateValues {

  /** The date SQLite gives a value that carries only a time. */
  private static final LocalDate TIME_ONLY_DATE = LocalDate.of(2000, 1, 1);

  /** The length of the {@code YYYY-MM-DD} that opens every SQLite date/time value that has one. */
  private static final int DATE_LENGTH = 10;

  private static final Pattern DATE_START = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}");

  /** The {@code Z} or {@code (+-)HH:MM} a SQLite date/time value may end with. */
  private static final Pattern ZONE_END = Pattern.compile("(?:Z|[+-]\\d{2}:\\d{2})$");

  /** {@code HH:MM}, then optionally seconds, then optionally a fraction of a second. */
  private static final DateTimeFormatter TIME =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .optionalStart()
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
          .optionalEnd()
          .optionalEnd()
          .toFormatter(Locale.ROOT);

  /**
   * What Hop writes: the layout SQLite's date functions read, carrying milliseconds always and the
   * rest of a nanosecond only when the value has one.
   */
  private static final DateTimeFormatter TEXT_DATE_TIME =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendLiteral(' ')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
          .toFormatter(Locale.ROOT);

  private SqliteDateValues() {
    // Utility class.
  }

  /**
   * Reads a date or timestamp column.
   *
   * @param resultSet the result set to read from
   * @param index the 1-based column index
   * @return the value, or null
   */
  public static Object read(ResultSet resultSet, int index) throws SQLException {
    Object stored = resultSet.getObject(index);
    if (stored == null) {
      return null;
    }
    if (stored instanceof CharSequence text) {
      Timestamp timestamp = parse(text.toString());
      if (timestamp == null) {
        throw new SQLException(
            "SQLite value '"
                + text
                + "' is not one of the date and time formats SQLite accepts, see "
                + "https://www.sqlite.org/lang_datefunc.html");
      }
      return timestamp;
    }
    // A number is left to the driver, which reads an integer as milliseconds since the epoch and a
    // real as a Julian day. That is what Hop has always done with them, and what Hop's own writes
    // used to produce, so those keep reading back as they did.
    return resultSet.getTimestamp(index);
  }

  /**
   * Writes a date or timestamp value as the text SQLite reads.
   *
   * @param database the dialect being written to
   * @param valueMeta the value metadata describing the column
   * @param preparedStatement the statement to write into
   * @param index the 1-based parameter index
   * @param value the value to write, possibly null
   */
  public static void write(
      IDatabase database,
      IValueMeta valueMeta,
      PreparedStatement preparedStatement,
      int index,
      Object value)
      throws SQLException, HopValueException {

    if (valueMeta.isNull(value)) {
      preparedStatement.setNull(index, Types.VARCHAR);
      return;
    }

    LocalDateTime dateTime = asLocalDateTime(valueMeta.getDate(value));

    // The same reading of precision 1 the rest of Hop's JDBC date handling makes: a date without a
    // time of day.
    boolean dateOnly =
        valueMeta.getPrecision() == 1 || !database.isSupportsTimeStampToDateConversion();

    preparedStatement.setString(
        index,
        dateOnly
            ? dateTime.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE)
            : dateTime.format(TEXT_DATE_TIME));
  }

  /** Keeps the nanoseconds a timestamp carries, which going through the epoch would round away. */
  private static LocalDateTime asLocalDateTime(Date date) {
    if (date instanceof Timestamp timestamp) {
      return timestamp.toLocalDateTime();
    }
    return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
  }

  /**
   * Parses one of the date and time formats SQLite documents.
   *
   * <p>Those are {@code YYYY-MM-DD}, a date followed by {@code HH:MM}, {@code HH:MM:SS} or {@code
   * HH:MM:SS.SSS} after a space or a {@code T}, and any of the three times on their own, which
   * SQLite dates to 2000-01-01. Any of them may carry a trailing {@code Z} or {@code (+-)HH:MM}.
   *
   * <p>A value with no zone is read as local time, which is how the SQLite JDBC driver reads the
   * one format it does parse; keeping that reading means the values that worked before still come
   * back as the same instant.
   *
   * @return the timestamp, or null when the text is not a SQLite date/time value
   */
  static Timestamp parse(String text) {
    if (text == null) {
      return null;
    }
    String value = text.trim();
    if (value.isEmpty()) {
      return null;
    }

    ZoneOffset offset = null;
    Matcher zone = ZONE_END.matcher(value);
    if (zone.find()) {
      offset = ZoneOffset.of(value.substring(zone.start()).toUpperCase(Locale.ROOT));
      value = value.substring(0, zone.start()).trim();
    }

    LocalDateTime dateTime = parseDateTime(value);
    if (dateTime == null) {
      return null;
    }
    return offset == null
        ? Timestamp.valueOf(dateTime)
        : Timestamp.from(OffsetDateTime.of(dateTime, offset).toInstant());
  }

  private static LocalDateTime parseDateTime(String value) {
    try {
      if (!DATE_START.matcher(value).find()) {
        // A time on its own, which SQLite reads as a time on 2000-01-01.
        return LocalDateTime.of(TIME_ONLY_DATE, LocalTime.parse(value, TIME));
      }
      LocalDate date = LocalDate.parse(value.substring(0, DATE_LENGTH));
      String rest = value.substring(DATE_LENGTH);
      if (rest.isEmpty()) {
        return date.atStartOfDay();
      }
      char separator = rest.charAt(0);
      if (separator != ' ' && separator != 'T' && separator != 't') {
        return null;
      }
      return LocalDateTime.of(date, LocalTime.parse(rest.substring(1).trim(), TIME));
    } catch (DateTimeParseException e) {
      return null;
    }
  }
}
