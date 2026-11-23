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

package org.apache.hop.junit.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.core.database.batch.BatchFailureContract;
import org.apache.hop.core.database.batch.IBatchRowListener;
import org.apache.hop.core.database.batch.RecoverableBatch;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * Runs a {@link RecoverableBatch} against a real JDBC driver and checks that the rows it reports as
 * written are exactly the rows the table holds afterwards. Database plugins use it to prove that
 * their declared contract recovers correctly on their own driver.
 */
public final class RealDriverBatchScenario {

  private RealDriverBatchScenario() {}

  /** The outcome of one run. */
  public record Outcome(Set<Integer> written, Set<Integer> rejected, Set<Integer> inTable) {}

  /**
   * Inserts keys {@code 0..rows-1} in one batch into a fresh table in which {@code duplicates}
   * already exist, recovers with the given contract, and asserts the outcome.
   *
   * @param connection an open connection; auto-commit is switched off
   * @param tableName a table name that does not exist yet; it is created and dropped
   * @param contract the contract to recover by
   * @param rows how many rows to insert
   * @param duplicates keys that already exist, so their inserts fail
   * @return what happened, already checked
   */
  public static Outcome run(
      Connection connection,
      String tableName,
      BatchFailureContract contract,
      int rows,
      int... duplicates)
      throws Exception {
    connection.setAutoCommit(false);
    execute(connection, "CREATE TABLE " + tableName + " (ID INTEGER NOT NULL PRIMARY KEY)");
    connection.commit();
    try {
      try (PreparedStatement seed =
          connection.prepareStatement("INSERT INTO " + tableName + " (ID) VALUES (?)")) {
        for (int key : duplicates) {
          seed.setInt(1, key);
          seed.executeUpdate();
        }
      }
      connection.commit();

      Set<Integer> written = new TreeSet<>();
      Set<Integer> rejected = new TreeSet<>();
      List<Integer> order = new ArrayList<>();
      try (PreparedStatement insert =
          connection.prepareStatement("INSERT INTO " + tableName + " (ID) VALUES (?)")) {
        RecoverableBatch batch =
            new RecoverableBatch(
                    connection,
                    insert,
                    (statement, row) -> {
                      try {
                        statement.setInt(1, (Integer) row[0]);
                      } catch (SQLException e) {
                        throw new HopDatabaseException(e);
                      }
                    },
                    contract,
                    connection.getMetaData().supportsTransactions())
                .withStatementFactory(
                    () ->
                        connection.prepareStatement(
                            "INSERT INTO " + tableName + " (ID) VALUES (?)"));
        for (int i = 0; i < rows; i++) {
          batch.add(new Object[] {i}, new Object[] {i});
        }
        batch.flush(
            new IBatchRowListener() {
              @Override
              public void rowWritten(Object[] outputRow) {
                written.add((Integer) outputRow[0]);
                order.add((Integer) outputRow[0]);
              }

              @Override
              public void rowRejected(Object[] outputRow, String message, boolean attempted) {
                rejected.add((Integer) outputRow[0]);
                order.add((Integer) outputRow[0]);
              }
            });
        if (batch.getStatement() != insert) {
          batch.getStatement().close();
        }
      }

      Set<Integer> inTable = new TreeSet<>();
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT ID FROM " + tableName)) {
        while (resultSet.next()) {
          inTable.add(resultSet.getInt(1));
        }
      }
      connection.commit();

      Set<Integer> expectedRejected = new TreeSet<>();
      for (int key : duplicates) {
        if (key < rows) {
          expectedRejected.add(key);
        }
      }
      Set<Integer> insertedByBatch = new TreeSet<>(inTable);
      for (int key : duplicates) {
        insertedByBatch.remove(key);
      }
      assertEquals(insertedByBatch, written, "rows reported as written must be the rows inserted");
      assertEquals(expectedRejected, rejected, "exactly the duplicates must be rejected");
      assertEquals(rows, order.size(), "every row must be reported exactly once");
      return new Outcome(written, rejected, inTable);
    } finally {
      try {
        connection.rollback();
      } catch (SQLException e) {
        // nothing pending
      }
      execute(connection, "DROP TABLE " + tableName);
      connection.commit();
    }
  }

  private static void execute(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }
}
