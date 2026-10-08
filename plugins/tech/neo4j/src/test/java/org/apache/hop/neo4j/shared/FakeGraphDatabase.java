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

package org.apache.hop.neo4j.shared;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;

/**
 * A graph database for tests with the given dialect: its connections record the statements and fail
 * every one with the given message, if any.
 */
public class FakeGraphDatabase extends BaseGraphDatabase {
  private final IGraphDialect dialect;
  private final String errorMessage;
  public final List<String> executed = new ArrayList<>();

  public FakeGraphDatabase(IGraphDialect dialect, String errorMessage) {
    this.dialect = dialect;
    this.errorMessage = errorMessage;
  }

  @Override
  public IGraphDialect getGraphDialect() {
    return dialect;
  }

  /** When set, connecting fails with this message. */
  public String connectErrorMessage;

  @Override
  public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName)
      throws HopException {
    if (connectErrorMessage != null) {
      throw new HopException(connectErrorMessage);
    }
    return new FakeConnection();
  }

  @Override
  public String test(IVariables variables, String connectionName) {
    return "fake";
  }

  public IGraphConnection newConnection() {
    return new FakeConnection();
  }

  private class FakeConnection implements IGraphConnection {
    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
        throws HopException {
      executed.add(statement);
      if (errorMessage != null) {
        throw new HopException(errorMessage);
      }
      return List.of();
    }

    @Override
    public IGraphTransaction beginTransaction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException {
      return work.execute(
          new IGraphTransaction() {
            @Override
            public List<Map<String, Object>> execute(
                String statement, Map<String, Object> parameters) throws HopException {
              return FakeConnection.this.execute(statement, parameters);
            }

            @Override
            public void commit() {}

            @Override
            public void rollback() {}

            @Override
            public void close() {}
          });
    }

    @Override
    public IGraphDialect getGraphDialect() {
      return dialect;
    }

    @Override
    public void close() {}
  }
}
