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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An in-memory JDBC connection with a single table whose first statement parameter is a unique key,
 * that fails batches the way real drivers do. It keeps a ledger of the keys that are really
 * committed, so a test can check what a transform reported against what the database holds.
 *
 * <p>Every statement prepared on it inserts into that table: parameter 1 is the key, the other
 * parameters are ignored. A row fails when its key is already present (committed or pending in the
 * current transaction) or is listed in {@link #failOn(Object...)}.
 *
 * <p>Usage: {@code database.setConnection(fake.connection())} on a {@code Database}, or use the
 * connection directly.
 */
public class FakeBatchDatabase {

  /** How the fake driver reacts to a failing row in a batch, each one measured on a real driver. */
  public enum Behaviour {
    /** SQL Server, MySQL, H2: run every row, mark the failures with EXECUTE_FAILED. */
    MARKS_FAILED_ROWS,
    /** Oracle, Derby: stop at the first failure, return the counts of the rows before it. */
    KEEPS_PREFIX,
    /** PostgreSQL, DuckDB: abort the transaction, nothing survives until it is rolled back. */
    LOSES_WHOLE_BATCH,
    /** SQLite: write the rows before the failure, then throw without any counts. */
    PARTIAL_WITHOUT_COUNTS
  }

  private final Behaviour behaviour;
  private final boolean transactional;

  private final Set<Object> committed = new LinkedHashSet<>();
  private final List<Object> pending = new ArrayList<>();
  private final Set<Object> alwaysFailing = new HashSet<>();
  private final Set<Object> deadlocking = new HashSet<>();
  private final Map<Object, Integer> updateCounts = new HashMap<>();
  private final Map<Object, Object> parents = new HashMap<>();
  private boolean batchCountsWithoutInfo;
  private boolean aborted;

  private int executeBatchCalls;
  private int executeUpdateCalls;
  private int commits;
  private int rollbacks;

  private final Connection connection;
  private final DatabaseMetaData metaData;

  public FakeBatchDatabase(Behaviour behaviour) {
    this(behaviour, true);
  }

  public FakeBatchDatabase(Behaviour behaviour, boolean transactional) {
    this.behaviour = behaviour;
    this.transactional = transactional;
    this.metaData = proxy(DatabaseMetaData.class, this::metaData);
    this.connection = proxy(Connection.class, this::connectionCall);
  }

  /** Puts keys in the table as already committed, so inserting them again fails. */
  public FakeBatchDatabase seed(Object... keys) {
    committed.addAll(Arrays.asList(keys));
    return this;
  }

  /** Makes every insert of these keys fail, as a check constraint would. */
  public FakeBatchDatabase failOn(Object... keys) {
    alwaysFailing.addAll(Arrays.asList(keys));
    return this;
  }

  /**
   * Makes a single-row execution of these keys fail as a deadlock victim does on every database:
   * SQLState 40001, and the whole transaction is rolled back, whatever the behaviour.
   */
  public FakeBatchDatabase deadlockOn(Object... keys) {
    deadlocking.addAll(Arrays.asList(keys));
    return this;
  }

  /**
   * Makes the key fail unless its parent key is already there, committed or pending in the current
   * transaction, as a foreign key does.
   */
  public FakeBatchDatabase requires(Object child, Object parent) {
    parents.put(child, parent);
    return this;
  }

  /**
   * Makes a successful execution of this key report the given update count instead of 1, as an
   * UPDATE whose key matches nothing reports 0. The row still counts as written in the ledger: the
   * statement succeeded.
   */
  public FakeBatchDatabase updateCount(Object key, int count) {
    updateCounts.put(key, count);
    return this;
  }

  /**
   * Makes a successful batch report {@link Statement#SUCCESS_NO_INFO} for every row, as Oracle and
   * drivers that rewrite batches do. Single-row executions still report their count.
   */
  public FakeBatchDatabase batchCountsWithoutInfo() {
    batchCountsWithoutInfo = true;
    return this;
  }

  /** The update count a successful execution of this key reports. */
  public int countOf(Object key) {
    return updateCounts.getOrDefault(key, 1);
  }

  public Connection connection() {
    return connection;
  }

  /** The keys that are committed, in commit order, including any seeded ones. */
  public Set<Object> committed() {
    return Collections.unmodifiableSet(committed);
  }

  public int getExecuteBatchCalls() {
    return executeBatchCalls;
  }

  public int getExecuteUpdateCalls() {
    return executeUpdateCalls;
  }

  /** Round trips that wrote rows: batch executions plus single-row executions. */
  public int getExecutions() {
    return executeBatchCalls + executeUpdateCalls;
  }

  public int getCommits() {
    return commits;
  }

  public int getRollbacks() {
    return rollbacks;
  }

  private boolean violates(Object key) {
    Object parent = parents.get(key);
    return key == null
        || (parent != null && !committed.contains(parent) && !pending.contains(parent))
        || alwaysFailing.contains(key)
        || committed.contains(key)
        || pending.contains(key);
  }

  private SQLException rowError(Object key) {
    return new SQLException("duplicate or invalid key " + key, "23505");
  }

  private int[] executeBatch(List<Object> keys) throws SQLException {
    executeBatchCalls++;
    try {
      return runBatch(keys);
    } finally {
      if (!transactional) {
        // Without transactions every applied row is permanent at once.
        committed.addAll(pending);
        pending.clear();
      }
    }
  }

  private int[] runBatch(List<Object> keys) throws SQLException {
    if (aborted) {
      int[] counts = new int[keys.size()];
      Arrays.fill(counts, Statement.EXECUTE_FAILED);
      throw new BatchUpdateException("current transaction is aborted", "25P02", counts);
    }
    int[] counts = new int[keys.size()];
    SQLException first = null;
    for (int i = 0; i < keys.size(); i++) {
      Object key = keys.get(i);
      if (!violates(key)) {
        pending.add(key);
        counts[i] = batchCountsWithoutInfo ? Statement.SUCCESS_NO_INFO : countOf(key);
        continue;
      }
      SQLException error = rowError(key);
      switch (behaviour) {
        case MARKS_FAILED_ROWS -> {
          counts[i] = Statement.EXECUTE_FAILED;
          if (first == null) {
            first = error;
          } else {
            first.setNextException(error);
          }
        }
        case KEEPS_PREFIX -> {
          BatchUpdateException bue =
              new BatchUpdateException(
                  error.getMessage(), error.getSQLState(), Arrays.copyOf(counts, i));
          bue.setNextException(error);
          throw bue;
        }
        case LOSES_WHOLE_BATCH -> {
          aborted = true;
          int[] all = new int[keys.size()];
          Arrays.fill(all, Statement.EXECUTE_FAILED);
          // Worded like pgjdbc, which names the entry that aborted the batch.
          BatchUpdateException bue =
              new BatchUpdateException(
                  "Batch entry " + i + " was aborted: " + error.getMessage(),
                  error.getSQLState(),
                  all);
          bue.setNextException(error);
          throw bue;
        }
        case PARTIAL_WITHOUT_COUNTS -> throw error;
      }
    }
    if (first != null) {
      BatchUpdateException bue =
          new BatchUpdateException(first.getMessage(), first.getSQLState(), counts);
      bue.setNextException(first);
      throw bue;
    }
    return counts;
  }

  private int executeUpdate(Object key) throws SQLException {
    executeUpdateCalls++;
    if (aborted) {
      throw new SQLException("current transaction is aborted", "25P02");
    }
    if (deadlocking.remove(key)) {
      pending.clear();
      throw new SQLException("deadlock detected, transaction rolled back", "40001");
    }
    if (violates(key)) {
      if (behaviour == Behaviour.LOSES_WHOLE_BATCH) {
        aborted = true;
      }
      throw rowError(key);
    }
    if (transactional) {
      pending.add(key);
    } else {
      committed.add(key);
    }
    return countOf(key);
  }

  private void commit() {
    commits++;
    if (!aborted) {
      committed.addAll(pending);
    }
    // Committing an aborted transaction rolls it back, as PostgreSQL does.
    pending.clear();
    aborted = false;
  }

  private void rollback() {
    rollbacks++;
    pending.clear();
    aborted = false;
  }

  /** Remembers how many rows were pending when it was set. */
  private record FakeSavepoint(int pendingRows) implements Savepoint {
    @Override
    public int getSavepointId() {
      return pendingRows;
    }

    @Override
    public String getSavepointName() {
      return "sp" + pendingRows;
    }
  }

  /** Undoes the rows written after the savepoint, and clears an aborted transaction. */
  private void rollbackTo(FakeSavepoint savepoint) {
    while (pending.size() > savepoint.pendingRows()) {
      pending.remove(pending.size() - 1);
    }
    aborted = false;
  }

  private Object connectionCall(Method method, Object[] args) throws SQLException {
    switch (method.getName()) {
      case "prepareStatement":
        return new FakeStatement().proxy;
      case "commit":
        commit();
        return null;
      case "rollback":
        if (args == null || args.length == 0) {
          rollback();
        } else {
          if (args[0] instanceof FakeSavepoint savepoint) {
            rollbackTo(savepoint);
          }
        }
        return null;
      case "setSavepoint":
        return new FakeSavepoint(pending.size());
      case "releaseSavepoint":
        return null;
      case "getMetaData":
        return metaData;
      case "getAutoCommit":
      case "isClosed":
        return false;
      case "isValid":
        return true;
      default:
        return defaultValue(method);
    }
  }

  private Object metaData(Method method, Object[] args) {
    return switch (method.getName()) {
      case "supportsTransactions" -> transactional;
      case "supportsBatchUpdates" -> true;
      case "getConnection" -> connection;
      default -> defaultValue(method);
    };
  }

  private class FakeStatement {
    private final List<Object> batch = new ArrayList<>();
    private Object key;
    private final PreparedStatement proxy = proxy(PreparedStatement.class, this::call);

    private Object call(Method method, Object[] args) throws SQLException {
      String name = method.getName();
      if (name.startsWith("set")
          && args != null
          && args.length >= 2
          && args[0] instanceof Integer index) {
        if (index == 1) {
          key = args[1];
        }
        return null;
      }
      return switch (name) {
        case "addBatch" -> {
          batch.add(key);
          yield null;
        }
        case "clearBatch" -> {
          batch.clear();
          yield null;
        }
        case "executeBatch" -> {
          List<Object> keys = new ArrayList<>(batch);
          batch.clear();
          yield executeBatch(keys);
        }
        case "executeUpdate" -> executeUpdate(key);
        case "getConnection" -> connection;
        default -> defaultValue(method);
      };
    }
  }

  @FunctionalInterface
  private interface Handler {
    Object handle(Method method, Object[] args) throws SQLException;
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<T> type, Handler handler) {
    InvocationHandler invocationHandler =
        (proxy, method, args) -> {
          if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
              case "hashCode" -> System.identityHashCode(proxy);
              case "equals" -> proxy == args[0];
              default -> "Fake" + type.getSimpleName();
            };
          }
          return handler.handle(method, args);
        };
    return (T)
        Proxy.newProxyInstance(
            FakeBatchDatabase.class.getClassLoader(), new Class<?>[] {type}, invocationHandler);
  }

  private static Object defaultValue(Method method) {
    Class<?> type = method.getReturnType();
    if (type == boolean.class) {
      return false;
    }
    if (type == int.class || type == short.class || type == byte.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == double.class) {
      return 0.0d;
    }
    if (type == float.class) {
      return 0.0f;
    }
    return null;
  }
}
