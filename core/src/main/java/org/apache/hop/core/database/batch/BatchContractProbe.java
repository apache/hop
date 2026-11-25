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

package org.apache.hop.core.database.batch;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

/**
 * Measures what a JDBC driver does with a failed batch, so a dialect can declare its {@link
 * BatchFailureContract} from evidence instead of a guess.
 *
 * <p>It creates a scratch table, commits two rows into it, then sends a batch of ten rows of which
 * two repeat those primary keys (at positions 3 and 6), commits, and looks at the update counts and
 * at which rows survived. A duplicate primary key is used because every engine enforces it; a value
 * that is too long, for example, is silently accepted by SQLite. The table is dropped afterwards.
 */
public final class BatchContractProbe {

  /** Positions in the ten-row batch that repeat an existing key. */
  static final int[] BAD_POSITIONS = {3, 6};

  static final int BATCH_SIZE = 10;

  private static final int FIRST_KEY = 100;

  /**
   * What the probe saw.
   *
   * @param contract the contract the observations fit, or {@link BatchFailureContract#UNKNOWN}
   * @param updateCounts the counts of the BatchUpdateException, or null if there was none
   * @param exceptionType the class of the exception the batch threw, or null if it didn't fail
   * @param survivingRows how many of the eight valid rows were in the table after the commit
   */
  public record Result(
      BatchFailureContract contract, int[] updateCounts, String exceptionType, int survivingRows) {

    @Override
    public String toString() {
      return contract
          + " (counts="
          + (updateCounts == null ? "none" : Arrays.toString(updateCounts))
          + ", exception="
          + exceptionType
          + ", surviving valid rows="
          + survivingRows
          + " of "
          + (BATCH_SIZE - BAD_POSITIONS.length)
          + ")";
    }
  }

  private BatchContractProbe() {}

  /**
   * Runs the probe on an open connection. Auto-commit is switched off for the duration and restored
   * afterwards.
   *
   * @param connection the connection to probe
   * @param tableName a table name that does not exist yet; it is created and dropped
   */
  public static Result probe(Connection connection, String tableName) throws SQLException {
    boolean autoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try {
      execute(connection, "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY)");
      connection.commit();
      try {
        return run(connection, tableName);
      } finally {
        safeRollback(connection);
        execute(connection, "DROP TABLE " + tableName);
        connection.commit();
      }
    } finally {
      connection.setAutoCommit(autoCommit);
    }
  }

  private static Result run(Connection connection, String tableName) throws SQLException {
    try (PreparedStatement seed =
        connection.prepareStatement("INSERT INTO " + tableName + " (ID) VALUES (?)")) {
      for (int position : BAD_POSITIONS) {
        seed.setInt(1, FIRST_KEY + position);
        seed.executeUpdate();
      }
    }
    connection.commit();

    int[] counts = null;
    String exceptionType = null;
    try (PreparedStatement insert =
        connection.prepareStatement("INSERT INTO " + tableName + " (ID) VALUES (?)")) {
      for (int i = 0; i < BATCH_SIZE; i++) {
        insert.setInt(1, FIRST_KEY + i);
        insert.addBatch();
      }
      try {
        insert.executeBatch();
      } catch (BatchUpdateException e) {
        counts = e.getUpdateCounts();
        exceptionType = e.getClass().getName();
      } catch (SQLException e) {
        exceptionType = e.getClass().getName();
      }
    }
    try {
      connection.commit();
    } catch (SQLException e) {
      // A driver that aborted the transaction may refuse the commit; the count below tells.
      safeRollback(connection);
    }

    int surviving = countValidRows(connection, tableName);
    return new Result(classify(counts, exceptionType, surviving), counts, exceptionType, surviving);
  }

  /** Fits the observations to a contract; anything that fits none is {@code UNKNOWN}. */
  static BatchFailureContract classify(int[] counts, String exceptionType, int surviving) {
    int valid = BATCH_SIZE - BAD_POSITIONS.length;
    if (exceptionType == null) {
      // No failure at all means the probe proved nothing.
      return BatchFailureContract.UNKNOWN;
    }
    if (counts == null) {
      // Without counts the failure can only be read when nothing survived it. A partial write
      // without counts (SQLite) can't be attributed to rows.
      return surviving == 0 ? BatchFailureContract.LOSES_WHOLE_BATCH : BatchFailureContract.UNKNOWN;
    }
    if (counts.length == BAD_POSITIONS[0] && surviving == BAD_POSITIONS[0]) {
      return BatchFailureContract.KEEPS_PREFIX;
    }
    if (counts.length == BATCH_SIZE && surviving == 0) {
      return BatchFailureContract.LOSES_WHOLE_BATCH;
    }
    if (counts.length == BATCH_SIZE && surviving == valid && marksExactlyTheBadRows(counts)) {
      return BatchFailureContract.MARKS_FAILED_ROWS;
    }
    return BatchFailureContract.UNKNOWN;
  }

  private static boolean marksExactlyTheBadRows(int[] counts) {
    for (int i = 0; i < counts.length; i++) {
      int index = i;
      boolean bad = Arrays.stream(BAD_POSITIONS).anyMatch(p -> p == index);
      if (bad != (counts[i] == Statement.EXECUTE_FAILED)) {
        return false;
      }
    }
    return true;
  }

  private static int countValidRows(Connection connection, String tableName) throws SQLException {
    int count = 0;
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT ID FROM " + tableName)) {
      while (resultSet.next()) {
        int position = resultSet.getInt(1) - FIRST_KEY;
        if (Arrays.stream(BAD_POSITIONS).noneMatch(p -> p == position)) {
          count++;
        }
      }
    }
    connection.commit();
    return count;
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void safeRollback(Connection connection) {
    try {
      connection.rollback();
    } catch (SQLException e) {
      // Nothing to roll back.
    }
  }
}
