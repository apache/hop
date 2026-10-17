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

package org.apache.hop.neo4j.actions.cypherscript;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.BaseGraphDatabase;
import org.apache.hop.core.graph.CypherGraphDialect;
import org.apache.hop.core.graph.GraphDatabaseMeta;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphDialect;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A script that fails halfway is rolled back as a whole, and the action reports the error. */
class CypherScriptTransactionTest {

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @Test
  void failedStatementRollsBackTheScript() throws Exception {
    TransactionalDatabase database = new TransactionalDatabase();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("tx", database));

    CypherScript action = new CypherScript("script");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("tx");
    action.setScript("CREATE (:A)\n;\nFAIL\n;\nCREATE (:B)");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(List.of("CREATE (:A)", "FAIL"), database.executed);
    assertTrue(database.rolledBack, "The transaction was not rolled back");
    assertFalse(database.committed, "A failed script was committed");
  }

  @Test
  void successfulScriptIsCommitted() throws Exception {
    TransactionalDatabase database = new TransactionalDatabase();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("tx", database));

    CypherScript action = new CypherScript("script");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("tx");
    action.setScript("CREATE (:A)\n;\nCREATE (:B)");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(List.of("CREATE (:A)", "CREATE (:B)"), database.executed);
    assertTrue(database.committed);
  }

  @Test
  void autoCommitStatementRunsOnItsOwn() throws Exception {
    TransactionalDatabase database = new TransactionalDatabase(new AutoCommitDialect());
    CypherScript action = action(database);
    action.setScript("CREATE (:A)\n;\nCREATE (:B)\n;\nAUTO\n;\nCREATE (:C)");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(List.of("CREATE (:A)", "CREATE (:B)", "AUTO", "CREATE (:C)"), database.executed);
    assertEquals(List.of("AUTO"), database.autoCommitted);
    // The statements before and after the auto-commit statement: one transaction each
    assertEquals(2, database.commits);
  }

  @Test
  void failureAfterAutoCommitKeepsWhatRanBefore() throws Exception {
    TransactionalDatabase database = new TransactionalDatabase(new AutoCommitDialect());
    CypherScript action = action(database);
    action.setScript("CREATE (:A)\n;\nAUTO\n;\nFAIL\n;\nCREATE (:B)");

    Result result = action.execute(new Result(), 0);

    assertFalse(result.getResult());
    assertEquals(1, result.getNrErrors());
    assertEquals(List.of("CREATE (:A)", "AUTO", "FAIL"), database.executed);
    assertEquals(List.of("AUTO"), database.autoCommitted);
    assertEquals(1, database.commits);
    assertTrue(
        database.rolledBack, "The transaction with the failed statement was not rolled back");
  }

  @Test
  void scriptWithoutAutoCommitStatementsKeepsOneTransaction() throws Exception {
    TransactionalDatabase database = new TransactionalDatabase(new AutoCommitDialect());
    CypherScript action = action(database);
    action.setScript("CREATE (:A)\n;\nCREATE (:B)");

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(List.of(), database.autoCommitted);
    assertEquals(1, database.commits);
  }

  private static CypherScript action(TransactionalDatabase database) throws HopException {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider
        .getSerializer(GraphDatabaseMeta.class)
        .save(new GraphDatabaseMeta("tx", database));
    CypherScript action = new CypherScript("script");
    action.setMetadataProvider(metadataProvider);
    action.setConnectionName("tx");
    return action;
  }

  /** Statements starting with AUTO can't run in an explicit transaction. */
  private static class AutoCommitDialect extends CypherGraphDialect {
    AutoCommitDialect() {
      super("AUTO_COMMIT");
    }

    @Override
    public boolean isRequiringAutoCommit(String statement) {
      return statement.startsWith("AUTO");
    }
  }

  /** Runs write work in a transaction that commits on return and rolls back on an exception. */
  private static class TransactionalDatabase extends BaseGraphDatabase {
    final List<String> executed = new ArrayList<>();
    final List<String> autoCommitted = new ArrayList<>();
    private final IGraphDialect dialect;
    boolean committed;
    boolean rolledBack;
    int commits;
    boolean inTransaction;

    TransactionalDatabase() {
      this(CypherGraphDialect.DEFAULT);
    }

    TransactionalDatabase(IGraphDialect dialect) {
      this.dialect = dialect;
    }

    @Override
    public IGraphDialect getGraphDialect() {
      return dialect;
    }

    @Override
    public IGraphConnection connect(ILogChannel log, IVariables variables, String connectionName) {
      return new IGraphConnection() {
        @Override
        public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters)
            throws HopException {
          executed.add(statement);
          if (!inTransaction) {
            autoCommitted.add(statement);
          }
          if (statement.startsWith("FAIL")) {
            throw new HopException("Syntax error in " + statement);
          }
          return List.of();
        }

        @Override
        public IGraphTransaction beginTransaction() {
          throw new UnsupportedOperationException();
        }

        @Override
        public <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException {
          IGraphTransaction transaction =
              new IGraphTransaction() {
                @Override
                public List<Map<String, Object>> execute(
                    String statement, Map<String, Object> parameters) throws HopException {
                  return connect(log, variables, connectionName).execute(statement, parameters);
                }

                @Override
                public void commit() {
                  // Done below
                }

                @Override
                public void rollback() {
                  // Done below
                }

                @Override
                public void close() {
                  // Nothing to close
                }
              };
          inTransaction = true;
          try {
            T value = work.execute(transaction);
            committed = true;
            commits++;
            return value;
          } catch (HopException | RuntimeException e) {
            rolledBack = true;
            throw e;
          } finally {
            inTransaction = false;
          }
        }

        @Override
        public IGraphDialect getGraphDialect() {
          return dialect;
        }

        @Override
        public boolean isSupportingTransactions() {
          return true;
        }

        @Override
        public void close() {
          // Nothing to close
        }
      };
    }

    @Override
    public String test(IVariables variables, String connectionName) {
      return "transactional";
    }
  }
}
