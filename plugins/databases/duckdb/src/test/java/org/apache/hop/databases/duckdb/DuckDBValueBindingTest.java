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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.value.ValueMetaDate;
import org.apache.hop.core.variables.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The DuckDB driver has never implemented the Calendar overloads of setDate and setTimestamp, so a
 * configured time zone must not be passed to it. This used to be an isDuckDbVariant() check inside
 * ValueMetaBase.
 */
class DuckDBValueBindingTest {

  private DatabaseMeta databaseMeta;
  private PreparedStatement preparedStatement;
  private Database database;

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    databaseMeta = mock(DatabaseMeta.class);
    when(databaseMeta.getIDatabase()).thenReturn(new DuckDBDatabaseMeta());
    preparedStatement = mock(PreparedStatement.class);
    ILoggingObject log = mock(ILoggingObject.class);
    when(log.getLogLevel()).thenReturn(LogLevel.NOTHING);
    database = new Database(log, new Variables(), databaseMeta);
  }

  @Test
  void aDateIsWrittenWithoutACalendarEvenWhenATimeZoneIsSet() throws Exception {
    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setPrecision(1);
    valueMeta.setDateFormatTimeZone(TimeZone.getTimeZone("Europe/Brussels"));

    // Through the engine, which is where a binding is chosen.
    database.setValue(preparedStatement, valueMeta, new Date(System.currentTimeMillis()), 1);

    verify(preparedStatement).setDate(eq(1), any(java.sql.Date.class));
    verify(preparedStatement, never())
        .setDate(eq(1), any(java.sql.Date.class), any(Calendar.class));
  }

  @Test
  void aTimestampIsWrittenWithoutACalendarEvenWhenATimeZoneIsSet() throws Exception {
    ValueMetaDate valueMeta = new ValueMetaDate("d");
    valueMeta.setDateFormatTimeZone(TimeZone.getTimeZone("Europe/Brussels"));

    database.setValue(preparedStatement, valueMeta, new Timestamp(System.currentTimeMillis()), 1);

    verify(preparedStatement).setTimestamp(eq(1), any(Timestamp.class));
    verify(preparedStatement, never())
        .setTimestamp(eq(1), any(Timestamp.class), any(Calendar.class));
  }
}
