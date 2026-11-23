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
package org.apache.hop.junit.dates;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.row.value.ValueMetaInteger;

/**
 * Writes Date values through {@link Database#setValues} in batches and checks that the database
 * stores exactly what the driver stores when every value gets a fresh {@link Calendar} in its time
 * zone. The values cover several time zones (each row switches zone for every column), both 2026
 * daylight saving changes of three of them, dates before 1900 and the year 9999, as timestamps and
 * as dates (precision 1).
 */
public final class DateWriteScenario {

  /** The zones, one timestamp and one date column each. */
  public static final String[] ZONES = {
    "UTC", "Europe/Brussels", "America/New_York", "Asia/Kathmandu", "Australia/Lord_Howe"
  };

  private DateWriteScenario() {}

  /** The instants written, one row each. */
  public static List<Long> instants() {
    List<Long> list = new ArrayList<>();
    for (String fixed :
        new String[] {
          "1970-01-01T00:00:00Z",
          "1850-06-01T12:00:00Z",
          "1899-12-31T23:59:59.999Z",
          "1970-01-01T00:30:00Z",
          "2026-11-23T10:11:12.345Z",
          "9999-12-31T12:00:00Z"
        }) {
      list.add(Instant.parse(fixed).toEpochMilli());
    }
    for (String start :
        new String[] {
          "2026-03-28T23:00:00Z",
          "2026-10-24T23:00:00Z",
          "2026-03-08T05:00:00Z",
          "2026-11-01T04:00:00Z",
          "2026-04-04T14:00:00Z",
          "2026-10-03T14:00:00Z"
        }) {
      long from = Instant.parse(start).toEpochMilli();
      for (int i = 0; i < 16; i++) {
        list.add(from + i * 15 * 60_000L);
      }
    }
    return list;
  }

  /**
   * @param db a connected database, auto-commit off
   * @param timestampType the column type for a timestamp, for example {@code TIMESTAMP(3)}
   * @param dateType the column type for a date, for example {@code DATE}
   * @param prefix a short table name prefix, unique per call
   */
  public static void run(Database db, String timestampType, String dateType, String prefix)
      throws Exception {
    String viaHop = prefix + "_HOP";
    String viaJdbc = prefix + "_JDBC";
    StringBuilder columns = new StringBuilder("ID INTEGER");
    StringBuilder placeholders = new StringBuilder("?");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("ID"));
    for (int z = 0; z < ZONES.length; z++) {
      columns.append(", TS").append(z).append(' ').append(timestampType);
      columns.append(", D").append(z).append(' ').append(dateType);
      placeholders.append(", ?, ?");
      ValueMetaDate timestamp = new ValueMetaDate("TS" + z);
      timestamp.setDateFormatTimeZone(TimeZone.getTimeZone(ZONES[z]));
      rowMeta.addValueMeta(timestamp);
      ValueMetaDate date = new ValueMetaDate("D" + z);
      date.setPrecision(1);
      date.setDateFormatTimeZone(TimeZone.getTimeZone(ZONES[z]));
      rowMeta.addValueMeta(date);
    }
    Connection connection = db.getConnection();
    try (Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE " + viaHop + " (" + columns + ")");
      statement.execute("CREATE TABLE " + viaJdbc + " (" + columns + ")");
    }
    connection.commit();
    try {
      List<Long> instants = instants();
      try (PreparedStatement hop =
              connection.prepareStatement(
                  "INSERT INTO " + viaHop + " VALUES (" + placeholders + ")");
          PreparedStatement jdbc =
              connection.prepareStatement(
                  "INSERT INTO " + viaJdbc + " VALUES (" + placeholders + ")")) {
        for (int id = 0; id < instants.size(); id++) {
          long instant = instants.get(id);
          Object[] row = new Object[rowMeta.size()];
          row[0] = (long) id;
          jdbc.setLong(1, id);
          for (int z = 0; z < ZONES.length; z++) {
            row[1 + 2 * z] = new java.util.Date(instant);
            row[2 + 2 * z] = new java.util.Date(instant);
            TimeZone zone = TimeZone.getTimeZone(ZONES[z]);
            jdbc.setTimestamp(2 + 2 * z, new Timestamp(instant), Calendar.getInstance(zone));
            jdbc.setDate(3 + 2 * z, new java.sql.Date(instant), Calendar.getInstance(zone));
          }
          db.setValues(rowMeta, row, hop);
          hop.addBatch();
          jdbc.addBatch();
          if ((id + 1) % 50 == 0) {
            hop.executeBatch();
            jdbc.executeBatch();
          }
        }
        hop.executeBatch();
        jdbc.executeBatch();
      }
      connection.commit();
      List<String> fresh = read(connection, viaJdbc);
      assertEquals(instants().size(), fresh.size());
      assertEquals(fresh, read(connection, viaHop), "Hop stores what a fresh calendar stores");
    } finally {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE " + viaHop);
        statement.execute("DROP TABLE " + viaJdbc);
      }
      connection.commit();
    }
  }

  private static List<String> read(Connection connection, String table) throws Exception {
    List<String> rows = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM " + table + " ORDER BY ID")) {
      int columns = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        StringBuilder line = new StringBuilder();
        for (int c = 1; c <= columns; c++) {
          line.append(rs.getString(c)).append('|');
        }
        rows.add(line.toString());
      }
    }
    return rows;
  }
}
