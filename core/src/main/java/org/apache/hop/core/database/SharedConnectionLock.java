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

package org.apache.hop.core.database;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Keeps the savepoints of the transforms that share one connection (a transactional pipeline) from
 * running into each other.
 *
 * <p>A savepoint belongs to the transaction, not to the transform that set it. On PostgreSQL,
 * releasing a savepoint also releases every savepoint set after it, and rolling back to a savepoint
 * also undoes every statement run after it. When two transforms write on the same connection at the
 * same time, one transform's release takes away the other's savepoint, and one transform's rollback
 * undoes rows the other already passed on as written.
 *
 * <p>A savepoint section ({@link Database#runInSavepoint(ISavepointWork)}) holds this lock from the
 * savepoint to its release or rollback. The shared connection is wrapped, so every statement run on
 * it, and every commit, rollback and savepoint, takes the same lock first: nothing another
 * transform runs can land inside the section. The lock is re-entrant, so the statements of the
 * section itself pass.
 *
 * <p>What is not run through a statement, such as {@link java.sql.DatabaseMetaData} queries or
 * fetching more rows of a result set that is already open, does not wait. Those only read.
 */
public final class SharedConnectionLock {

  /** Statement methods that send something to the database. */
  private static final Set<String> STATEMENT_EXECUTE =
      Set.of(
          "execute",
          "executeQuery",
          "executeUpdate",
          "executeLargeUpdate",
          "executeBatch",
          "executeLargeBatch");

  /** Connection methods that change or end the transaction. */
  private static final Set<String> CONNECTION_TRANSACTION =
      Set.of("commit", "rollback", "setSavepoint", "releaseSavepoint", "setAutoCommit", "close");

  /** Connection methods that create a statement, which is wrapped in turn. */
  private static final Set<String> CONNECTION_STATEMENT =
      Set.of("createStatement", "prepareStatement", "prepareCall");

  private final ReentrantLock lock = new ReentrantLock();
  private final Connection target;
  private final Connection connection;

  private SharedConnectionLock(Connection target) {
    this.target = target;
    this.connection =
        (Connection)
            Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                new ConnectionHandler());
  }

  /**
   * Wraps a connection that several transforms are going to share.
   *
   * @param target the real connection
   * @return the lock, with {@link #getConnection()} the connection to share
   */
  public static SharedConnectionLock wrap(Connection target) {
    return new SharedConnectionLock(target);
  }

  /** The wrapped connection, to share instead of the real one. */
  public Connection getConnection() {
    return connection;
  }

  /** The real connection. Statements run on it directly do not wait for the lock. */
  public Connection getTarget() {
    return target;
  }

  /** Waits until no other thread is in a savepoint section or a statement on this connection. */
  public void lock() {
    lock.lock();
  }

  public void unlock() {
    lock.unlock();
  }

  /** Whether the calling thread holds the lock, for tests and assertions. */
  public boolean isHeldByCurrentThread() {
    return lock.isHeldByCurrentThread();
  }

  /** Whether any thread holds the lock, for tests. */
  public boolean isLocked() {
    return lock.isLocked();
  }

  private Object locked(Object target, Method method, Object[] args) throws Throwable {
    lock.lock();
    try {
      return invoke(target, method, args);
    } finally {
      lock.unlock();
    }
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /**
   * The methods of {@link Object} and {@link java.sql.Wrapper} that a proxy answers itself.
   *
   * @return true if handled, with the answer in {@code result[0]}
   */
  private static boolean handleCommon(
      Object proxy, Object target, Method method, Object[] args, Object[] result) throws Throwable {
    switch (method.getName()) {
      case "equals":
        if (method.getParameterCount() == 1) {
          result[0] = proxy == args[0];
          return true;
        }
        return false;
      case "hashCode":
        if (method.getParameterCount() == 0) {
          result[0] = System.identityHashCode(proxy);
          return true;
        }
        return false;
      case "toString":
        if (method.getParameterCount() == 0) {
          result[0] = "SharedConnectionLock[" + target + "]";
          return true;
        }
        return false;
      case "unwrap":
        Class<?> unwrapTo = (Class<?>) args[0];
        result[0] = unwrapTo.isInstance(proxy) ? proxy : invoke(target, method, args);
        return true;
      case "isWrapperFor":
        Class<?> wrapperFor = (Class<?>) args[0];
        result[0] = wrapperFor.isInstance(proxy) || (Boolean) invoke(target, method, args);
        return true;
      default:
        return false;
    }
  }

  private final class ConnectionHandler implements InvocationHandler {
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object[] result = new Object[1];
      if (handleCommon(proxy, target, method, args, result)) {
        return result[0];
      }
      String name = method.getName();
      if (CONNECTION_TRANSACTION.contains(name)) {
        return locked(target, method, args);
      }
      Object value = SharedConnectionLock.invoke(target, method, args);
      if (CONNECTION_STATEMENT.contains(name) && value instanceof Statement statement) {
        return wrapStatement(statement);
      }
      return value;
    }
  }

  private Statement wrapStatement(Statement statement) {
    Class<?> type;
    if (statement instanceof CallableStatement) {
      type = CallableStatement.class;
    } else if (statement instanceof PreparedStatement) {
      type = PreparedStatement.class;
    } else {
      type = Statement.class;
    }
    return (Statement)
        Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            new Class<?>[] {type},
            new StatementHandler(statement));
  }

  private final class StatementHandler implements InvocationHandler {
    private final Statement statement;

    private StatementHandler(Statement statement) {
      this.statement = statement;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object[] result = new Object[1];
      if (handleCommon(proxy, statement, method, args, result)) {
        return result[0];
      }
      String name = method.getName();
      if ("getConnection".equals(name) && method.getParameterCount() == 0) {
        return connection;
      }
      if (STATEMENT_EXECUTE.contains(name)) {
        return locked(statement, method, args);
      }
      return SharedConnectionLock.invoke(statement, method, args);
    }
  }
}
