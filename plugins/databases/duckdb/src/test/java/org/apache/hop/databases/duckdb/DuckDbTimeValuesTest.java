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

package org.apache.hop.databases.duckdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Reading DuckDB temporal columns through the engine. See issue #3744, where a TIME column could
 * not be read at all because the driver cannot make a timestamp out of it.
 */
class DuckDbTimeValuesTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  private Database database(String name) {
    DatabaseMeta databaseMeta = new DatabaseMeta();
    databaseMeta.setIDatabase(new DuckDBDatabaseMeta());
    databaseMeta.setName(name);
    databaseMeta.setDBName(":memory:" + name);
    databaseMeta.setAccessType(DatabaseMeta.TYPE_ACCESS_NATIVE);
    return new Database(
        new SimpleLoggingObject(name, LoggingObjectType.GENERAL, null),
        new Variables(),
        databaseMeta);
  }

  private static Date onTheEpochDay(LocalTime time) {
    return Date.from(time.atDate(LocalDate.EPOCH).atZone(ZoneId.systemDefault()).toInstant());
  }

  private static Date at(LocalDateTime dateTime) {
    return Date.from(dateTime.atZone(ZoneId.systemDefault()).toInstant());
  }

  /** The report: reading a TIME column failed outright. */
  @Test
  void aTimeColumnIsReadAsATimeOnTheEpochDay() throws Exception {
    Database db = database("time");
    db.connect();
    try {
      db.execStatement("CREATE TABLE run_time AS SELECT CAST('11:22:33.456' AS TIME) AS t");

      RowMetaAndData row = db.getOneRow("SELECT t FROM run_time");
      assertEquals(IValueMeta.TYPE_DATE, row.getRowMeta().getValueMeta(0).getType());
      assertEquals(
          onTheEpochDay(LocalTime.of(11, 22, 33, 456_000_000)),
          row.getDate(0, null),
          "the time, milliseconds and all, on the day JDBC puts a time-only value");
    } finally {
      db.disconnect();
    }
  }

  /** The binding is chosen per column, so the ordinary date types must keep their own handling. */
  @Test
  void datesAndTimestampsAreUnaffected() throws Exception {
    Database db = database("dates");
    db.connect();
    try {
      db.execStatement(
          "CREATE TABLE moments AS SELECT CAST('2024-03-04 05:06:07.891' AS TIMESTAMP) AS ts,"
              + " CAST('2024-03-04' AS DATE) AS d");

      RowMetaAndData row = db.getOneRow("SELECT ts, d FROM moments");
      assertEquals(at(LocalDateTime.of(2024, 3, 4, 5, 6, 7, 891_000_000)), row.getDate(0, null));
      assertEquals(at(LocalDate.of(2024, 3, 4).atStartOfDay()), row.getDate(1, null));
    } finally {
      db.disconnect();
    }
  }

  @Test
  void aNullTimeStaysNull() throws Exception {
    Database db = database("nulltime");
    db.connect();
    try {
      db.execStatement("CREATE TABLE absent AS SELECT CAST(NULL AS TIME) AS t");
      assertNull(db.getOneRow("SELECT t FROM absent").getDate(0, null));
    } finally {
      db.disconnect();
    }
  }

  @Test
  void onlyATimeColumnTakesTheTimeBinding() {
    IValueMeta fromTime = new ValueMetaDate("t");
    fromTime.setOriginalColumnType(Types.TIME);
    assertTrue(DuckDbTimeValues.isTimeColumn(null, fromTime));

    IValueMeta fromTimestamp = new ValueMetaDate("ts");
    fromTimestamp.setOriginalColumnType(Types.TIMESTAMP);
    assertFalse(DuckDbTimeValues.isTimeColumn(null, fromTimestamp));

    IValueMeta fromDate = new ValueMetaDate("d");
    fromDate.setOriginalColumnType(Types.DATE);
    assertFalse(DuckDbTimeValues.isTimeColumn(null, fromDate));
  }
}
