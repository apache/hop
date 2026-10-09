/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.neo4j.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphIndex;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.neo4j.shared.NeoConnectionUtils;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionContext;

/**
 * An open Bolt connection: a driver with one session. Closing it closes both. Notifications of the
 * statements are logged, never treated as errors.
 */
@Getter
public class BoltGraphConnection implements IGraphConnection {
  private final Driver driver;
  private final Session session;
  private final BoltGraphDialect graphDialect;
  private final ILogChannel log;

  public BoltGraphConnection(
      Driver driver, Session session, BoltGraphDialect graphDialect, ILogChannel log) {
    this.driver = driver;
    this.session = session;
    this.graphDialect = graphDialect == null ? Neo4jGraphDialect.INSTANCE : graphDialect;
    this.log = log;
  }

  /**
   * Consume a result: the rows as maps of plain values and graph values, logging the notifications.
   */
  private List<Map<String, Object>> consume(Result result) {
    List<Map<String, Object>> rows = new ArrayList<>();
    while (result.hasNext()) {
      rows.add(BoltValues.toRow(result.next().asMap()));
    }
    if (log != null) {
      NeoConnectionUtils.logNotifications(log, result.consume());
    } else {
      result.consume();
    }
    return rows;
  }

  @Override
  public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
      throws HopException {
    try {
      return consume(session.run(statement, parameters == null ? Map.of() : parameters));
    } catch (Exception e) {
      throw new HopException("Error executing statement: " + statement, e);
    }
  }

  @Override
  public IGraphTransaction beginTransaction() throws HopException {
    try {
      return new BoltTransaction(session.beginTransaction());
    } catch (Exception e) {
      throw new HopException("Error starting a transaction", e);
    }
  }

  @Override
  public <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException {
    try {
      return session.executeWrite(
          tx -> {
            try {
              return work.execute(new BoltTransactionContext(tx));
            } catch (HopException e) {
              throw new WorkException(e);
            }
          });
    } catch (WorkException e) {
      throw e.getHopException();
    } catch (Exception e) {
      throw new HopException("Error executing a write transaction", e);
    }
  }

  @Override
  public <T> T executeRead(IGraphTransactionWork<T> work) throws HopException {
    try {
      return session.executeRead(
          tx -> {
            try {
              return work.execute(new BoltTransactionContext(tx));
            } catch (HopException e) {
              throw new WorkException(e);
            }
          });
    } catch (WorkException e) {
      throw e.getHopException();
    } catch (Exception e) {
      throw new HopException("Error executing a read transaction", e);
    }
  }

  @Override
  public List<GraphIndex> getIndexes() throws HopException {
    return graphDialect.getIndexes(this);
  }

  @Override
  public void close() throws HopException {
    try {
      session.close();
    } finally {
      driver.close();
    }
  }

  /** Carries a HopException out of the driver's transaction callback. */
  private static class WorkException extends RuntimeException {
    WorkException(HopException cause) {
      super(cause);
    }

    HopException getHopException() {
      return (HopException) getCause();
    }
  }

  /** The statements of {@link #executeWrite}: committed by the driver, not by the work. */
  private class BoltTransactionContext implements IGraphTransaction {
    private final TransactionContext context;

    BoltTransactionContext(TransactionContext context) {
      this.context = context;
    }

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters) {
      return consume(context.run(statement, parameters == null ? Map.of() : parameters));
    }

    @Override
    public void commit() {
      // Committed when the work returns
    }

    @Override
    public void rollback() throws HopException {
      throw new HopException("Throw an exception from the work to roll back its transaction");
    }

    @Override
    public void close() {
      // Closed by the driver
    }
  }

  /** An explicit transaction. */
  private class BoltTransaction implements IGraphTransaction {
    private final Transaction transaction;

    BoltTransaction(Transaction transaction) {
      this.transaction = transaction;
    }

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
        throws HopException {
      try {
        return consume(transaction.run(statement, parameters == null ? Map.of() : parameters));
      } catch (Exception e) {
        throw new HopException("Error executing statement: " + statement, e);
      }
    }

    @Override
    public void commit() throws HopException {
      try {
        transaction.commit();
      } catch (Exception e) {
        throw new HopException("Error committing a transaction", e);
      }
    }

    @Override
    public void rollback() throws HopException {
      try {
        transaction.rollback();
      } catch (Exception e) {
        throw new HopException("Error rolling back a transaction", e);
      }
    }

    @Override
    public void close() throws HopException {
      try {
        transaction.close();
      } catch (Exception e) {
        throw new HopException("Error closing a transaction", e);
      }
    }
  }
}
