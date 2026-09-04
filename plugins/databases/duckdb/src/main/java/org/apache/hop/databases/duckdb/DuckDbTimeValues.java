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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.util.Date;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.types.IValueBinding;
import org.apache.hop.core.database.types.JdbcDateValues;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;

/**
 * Reading a DuckDB {@code TIME} column.
 *
 * <p>The DuckDB driver cannot produce a {@code Timestamp} for a column that carries only a time:
 * {@code getTimestamp} fails on it, which is how Hop reads every date column. So such a column is
 * read through {@code getObject}, which hands out the {@code LocalTime} the driver already holds,
 * and the time is placed on the epoch day the way {@link java.sql.Time} does.
 *
 * <p>DuckDB's {@code TIMETZ} has the same problem, but it is reported as {@link
 * java.sql.Types#TIME_WITH_TIMEZONE}, which no Hop mapping claims, so it never reaches a date in
 * the first place. Whoever maps that type has to widen {@link #isTimeColumn} along with it, or the
 * column lands on the default read and fails the way this one used to. The {@code OffsetTime}
 * branch below is already here for that.
 *
 * <p>See issue #3744.
 */
public final class DuckDbTimeValues {

  /** The date JDBC gives a value that carries only a time. */
  private static final LocalDate EPOCH = LocalDate.EPOCH;

  private DuckDbTimeValues() {
    // Utility class.
  }

  /** Whether this value was read from a column that carries a time and no date. */
  public static boolean isTimeColumn(IDatabase database, IValueMeta valueMeta) {
    return valueMeta.getOriginalColumnType() == Types.TIME;
  }

  /** Reads and writes a time-only column: the read is DuckDB's, the write is the ordinary one. */
  public static final IValueBinding BINDING =
      new IValueBinding() {
        @Override
        public Object read(IDatabase database, IValueMeta valueMeta, ResultSet resultSet, int index)
            throws SQLException {
          Object stored = resultSet.getObject(index);
          if (stored == null || resultSet.wasNull()) {
            return null;
          }
          if (stored instanceof OffsetTime offsetTime) {
            return Date.from(offsetTime.atDate(EPOCH).toInstant());
          }
          if (stored instanceof LocalTime localTime) {
            return Date.from(localTime.atDate(EPOCH).atZone(ZoneId.systemDefault()).toInstant());
          }
          if (stored instanceof Date date) {
            return date;
          }
          // Whatever else the driver decides to hand out still has to become a date.
          return resultSet.getTime(index);
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
