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

package org.apache.hop.pipeline.transforms.update;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Puts a JDBC driver that handles a failed batch differently in front of a real H2 connection. The
 * rows really run on H2, one by one, so the table shows what such a driver leaves behind; only what
 * {@code executeBatch()} reports changes. This gives the shapes of update counts that H2 itself
 * never returns, with a table that matches them.
 *
 * <p>Only the UPDATE statements prepared on the connection are affected; everything else goes to H2
 * as it is.
 */
final class SimulatedBatchDriver {

  /** What the simulated driver does when a row of a batch fails. */
  enum Shape {
    /**
     * Stops at the failed row and returns the counts of the rows before it, which stay applied. The
     * rows after it are never sent. Oracle and embedded Derby do this.
     */
    KEEPS_PREFIX,

    /**
     * Stops at the failed row, keeps the rows before it, and throws an SQLException without any
     * update counts. SQLite does this.
     */
    PREFIX_WITHOUT_COUNTS
  }

  private SimulatedBatchDriver() {}

  static Connection wrap(Connection connection, Shape shape) {
    InvocationHandler handler =
        (proxy, method, args) -> {
          Object result = invoke(connection, method, args);
          if (method.getName().equals("prepareStatement")
              && args != null
              && args[0] instanceof String sql
              && sql.trim().toUpperCase(Locale.ROOT).startsWith("UPDATE")) {
            return statement((PreparedStatement) result, shape);
          }
          return result;
        };
    return (Connection)
        Proxy.newProxyInstance(
            SimulatedBatchDriver.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            handler);
  }

  private static PreparedStatement statement(PreparedStatement real, Shape shape) {
    BatchStatement batch = new BatchStatement(real, shape);
    return (PreparedStatement)
        Proxy.newProxyInstance(
            SimulatedBatchDriver.class.getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            batch::invoke);
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /** A parameter setter call: setLong(1, 5), setNull(2, Types.VARCHAR), ... */
  private record Call(Method method, Object[] args) {}

  /** Keeps the batch itself and runs its rows one by one on the real statement. */
  private static final class BatchStatement {
    private final PreparedStatement real;
    private final Shape shape;
    private List<Call> parameters = new ArrayList<>();
    private final List<List<Call>> batch = new ArrayList<>();

    private BatchStatement(PreparedStatement real, Shape shape) {
      this.real = real;
      this.shape = shape;
    }

    private Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (isParameterSetter(method, args)) {
        parameters.add(new Call(method, args));
        return SimulatedBatchDriver.invoke(real, method, args);
      }
      switch (name) {
        case "clearParameters":
          parameters = new ArrayList<>();
          return SimulatedBatchDriver.invoke(real, method, args);
        case "addBatch":
          if (args == null || args.length == 0) {
            batch.add(new ArrayList<>(parameters));
            return null;
          }
          return SimulatedBatchDriver.invoke(real, method, args);
        case "clearBatch":
          batch.clear();
          return null;
        case "executeBatch":
          return executeBatch();
        default:
          return SimulatedBatchDriver.invoke(real, method, args);
      }
    }

    private static boolean isParameterSetter(Method method, Object[] args) {
      return method.getName().startsWith("set")
          && args != null
          && args.length >= 2
          && args[0] instanceof Integer;
    }

    private int[] executeBatch() throws Throwable {
      List<List<Call>> rows = new ArrayList<>(batch);
      batch.clear();
      int[] counts = new int[rows.size()];
      for (int i = 0; i < rows.size(); i++) {
        for (Call call : rows.get(i)) {
          SimulatedBatchDriver.invoke(real, call.method(), call.args());
        }
        try {
          counts[i] = real.executeUpdate();
        } catch (SQLException e) {
          if (shape == Shape.KEEPS_PREFIX) {
            throw new BatchUpdateException(
                e.getMessage(), e.getSQLState(), e.getErrorCode(), Arrays.copyOf(counts, i), e);
          }
          throw new SQLException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
        }
      }
      return counts;
    }
  }
}
