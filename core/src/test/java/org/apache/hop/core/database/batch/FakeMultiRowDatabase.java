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

import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hop.core.exception.HopDatabaseException;

/**
 * An in-memory connection whose statements each carry a number of rows of (key, value), applied
 * atomically: a statement with one bad row changes nothing. It fails batches the way real drivers
 * do, so {@link MultiRowBatch} can be checked against a ledger of what is really committed.
 *
 * <p>A row inserts its value under its key: a key that is already there fails, as a primary key
 * does, and so does a key in {@link #failOn}, as a check constraint would.
 */
final class FakeMultiRowDatabase {

  private final BatchFailureContract behaviour;
  private final boolean transactional;

  private Map<Object, Object> committed = new LinkedHashMap<>();
  private Map<Object, Object> working = new LinkedHashMap<>();

  /** The working table when the last savepoint was set; null without one. */
  private Map<Object, Object> savepointState;

  private final Set<Object> failing = new HashSet<>();
  private boolean aborted;
  private int commitFailures;
  private boolean closeOnFailure;
  private boolean foldCounts;

  int executeBatchCalls;
  int executeUpdateCalls;
  int commits;
  int rollbacks;
  final List<Integer> preparedSizes = new ArrayList<>();

  /** The number of rows of every statement that succeeded, in order. */
  final List<Integer> statementSizes = new ArrayList<>();

  FakeMultiRowDatabase(BatchFailureContract behaviour, boolean transactional) {
    this.behaviour = behaviour;
    this.transactional = transactional;
  }

  FakeMultiRowDatabase failOn(Object... keys) {
    failing.addAll(Arrays.asList(keys));
    return this;
  }

  FakeMultiRowDatabase seed(Object key, Object value) {
    committed.put(key, value);
    working.put(key, value);
    return this;
  }

  /** The next commits fail with a serialization failure, and roll the transaction back. */
  FakeMultiRowDatabase failCommits(int times) {
    commitFailures = times;
    return this;
  }

  /** Close a statement when it fails, as DuckDB does. */
  FakeMultiRowDatabase closeStatementsOnFailure() {
    closeOnFailure = true;
    return this;
  }

  /**
   * A successful batch of several statements reports one count for all of them, as a driver that
   * folds the statements of a batch into fewer ones may (pgjdbc's reWriteBatchedInserts merges
   * INSERTs).
   */
  FakeMultiRowDatabase foldBatchCounts() {
    foldCounts = true;
    return this;
  }

  Map<Object, Object> committed() {
    return committed;
  }

  Set<Integer> preparedSizeSet() {
    return new TreeSet<>(preparedSizes);
  }

  /** Applies one statement's rows to the working table, all or nothing. */
  private int apply(List<Object[]> rows) throws SQLException {
    if (aborted) {
      throw new SQLException("current transaction is aborted", "25P02");
    }
    Map<Object, Object> next = new LinkedHashMap<>(working);
    for (Object[] row : rows) {
      Object key = row[0];
      if (failing.contains(key) || key == null) {
        throw fail("invalid key " + key);
      }
      if (next.containsKey(key)) {
        throw fail("duplicate key " + key);
      }
      next.put(key, row[1]);
    }
    working = next;
    statementSizes.add(rows.size());
    if (!transactional) {
      committed = new LinkedHashMap<>(working);
    }
    return rows.size();
  }

  private SQLException fail(String message) {
    if (behaviour == BatchFailureContract.LOSES_WHOLE_BATCH) {
      aborted = true;
    }
    return new SQLException(message, "23505");
  }

  private int[] executeBatch(List<List<Object[]>> entries) throws SQLException {
    executeBatchCalls++;
    int[] counts = new int[entries.size()];
    SQLException first = null;
    for (int i = 0; i < entries.size(); i++) {
      try {
        counts[i] = apply(entries.get(i));
      } catch (SQLException e) {
        switch (behaviour) {
          case MARKS_FAILED_ROWS -> {
            counts[i] = Statement.EXECUTE_FAILED;
            if (first == null) {
              first = e;
            }
          }
          case KEEPS_PREFIX ->
              throw new BatchUpdateException(
                  e.getMessage(), e.getSQLState(), Arrays.copyOf(counts, i), e);
          case LOSES_WHOLE_BATCH -> {
            int[] all = new int[entries.size()];
            Arrays.fill(all, Statement.EXECUTE_FAILED);
            throw new BatchUpdateException(
                "Batch entry " + i + " was aborted: " + e.getMessage(), e.getSQLState(), all, e);
          }
          default -> throw new SQLException(e.getMessage(), e.getSQLState(), e);
        }
      }
    }
    if (first != null) {
      throw new BatchUpdateException(first.getMessage(), first.getSQLState(), counts, first);
    }
    if (foldCounts && counts.length > 1) {
      return new int[] {Arrays.stream(counts).sum()};
    }
    return counts;
  }

  Connection connection() {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "commit" -> {
                    commits++;
                    savepointState = null;
                    if (commitFailures > 0) {
                      commitFailures--;
                      working = new LinkedHashMap<>(committed);
                      aborted = false;
                      throw new SQLException("could not serialize access", "40001");
                    }
                    if (!aborted) {
                      committed = new LinkedHashMap<>(working);
                    }
                    working = new LinkedHashMap<>(committed);
                    aborted = false;
                    yield null;
                  }
                  case "rollback" -> {
                    rollbacks++;
                    if (args != null && args.length == 1 && savepointState != null) {
                      working = new LinkedHashMap<>(savepointState);
                    } else {
                      working = new LinkedHashMap<>(committed);
                      savepointState = null;
                    }
                    aborted = false;
                    yield null;
                  }
                  case "setSavepoint" -> {
                    savepointState = new LinkedHashMap<>(working);
                    yield java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {java.sql.Savepoint.class},
                        (savepointProxy, savepointMethod, savepointArgs) -> 0);
                  }
                  case "releaseSavepoint" -> null;
                  case "getAutoCommit", "isClosed" -> false;
                  case "getMetaData" ->
                      java.lang.reflect.Proxy.newProxyInstance(
                          getClass().getClassLoader(),
                          new Class<?>[] {java.sql.DatabaseMetaData.class},
                          (metaProxy, metaMethod, metaArgs) -> {
                            if ("supportsSavepoints".equals(metaMethod.getName())) {
                              return true;
                            }
                            throw new UnsupportedOperationException(metaMethod.getName());
                          });
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  /** A statement for this many rows of (key, value). */
  PreparedStatement prepare(int rows) {
    preparedSizes.add(rows);
    Object[] parameters = new Object[rows * 2];
    List<List<Object[]>> batch = new ArrayList<>();
    boolean[] closed = {false};
    return (PreparedStatement)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (proxy, method, args) -> {
              String name = method.getName();
              if (name.equals("setObject")) {
                parameters[(Integer) args[0] - 1] = args[1];
                return null;
              }
              return switch (name) {
                case "addBatch" -> {
                  batch.add(rows(parameters));
                  yield null;
                }
                case "clearBatch" -> {
                  batch.clear();
                  yield null;
                }
                case "executeBatch" -> {
                  List<List<Object[]>> entries = new ArrayList<>(batch);
                  batch.clear();
                  try {
                    yield executeBatch(entries);
                  } catch (SQLException e) {
                    closed[0] = closeOnFailure;
                    throw e;
                  }
                }
                case "executeUpdate" -> {
                  executeUpdateCalls++;
                  try {
                    yield apply(rows(parameters));
                  } catch (SQLException e) {
                    closed[0] = closeOnFailure;
                    throw e;
                  }
                }
                case "isClosed" -> closed[0];
                case "close" -> {
                  closed[0] = true;
                  yield null;
                }
                default -> throw new UnsupportedOperationException(name);
              };
            });
  }

  private static List<Object[]> rows(Object[] parameters) {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < parameters.length; i += 2) {
      rows.add(new Object[] {parameters[i], parameters[i + 1]});
    }
    return rows;
  }

  /** The statements of a {@link MultiRowBatch} on this fake: bind rows are {key, value}. */
  MultiRowBatch.IStatements statements(int rowsPerStatement) {
    return statements(rowsPerStatement, 0);
  }

  /**
   * The statements of a {@link MultiRowBatch} on this fake, with a size limit: a row takes as many
   * bytes as its value has characters.
   */
  MultiRowBatch.IStatements statements(int rowsPerStatement, long maxStatementBytes) {
    return new MultiRowBatch.IStatements() {
      @Override
      public int getRowsPerStatement() {
        return rowsPerStatement;
      }

      @Override
      public long getMaxStatementBytes() {
        return maxStatementBytes;
      }

      @Override
      public long estimateBytes(Object[] bindRow) {
        return String.valueOf(bindRow[1]).length();
      }

      @Override
      public PreparedStatement prepare(int rows) {
        return FakeMultiRowDatabase.this.prepare(rows);
      }

      @Override
      public void bind(PreparedStatement statement, int rows, int row, Object[] bindRow)
          throws HopDatabaseException {
        if ("unbindable".equals(bindRow[1])) {
          throw new HopDatabaseException("can't convert value");
        }
        try {
          statement.setObject(row * 2 + 1, bindRow[0]);
          statement.setObject(row * 2 + 2, bindRow[1]);
        } catch (SQLException e) {
          throw new HopDatabaseException(e);
        }
      }
    };
  }
}
