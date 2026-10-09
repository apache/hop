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

package org.apache.hop.neo4j.transforms.cypher;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.graph.IGraphConnection;
import org.apache.hop.core.graph.IGraphTransaction;
import org.apache.hop.core.graph.IGraphTransactionWork;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.neo4j.bolt.Neo4jGraphDialect;
import org.apache.hop.neo4j.shared.NeoConnection;
import org.apache.hop.pipeline.transforms.mock.TransformMockHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;
import org.neo4j.driver.summary.ResultSummary;

/**
 * A retried batch of statements passes on the output rows of the attempt which succeeded only.
 * Without retries the rows stream: they are passed on while the statements execute.
 */
class CypherRetryTest {

  private TransformMockHelper<CypherMeta, CypherData> helper;
  private CypherMeta meta;
  private CypherData data;
  private RecordingCypher cypher;

  @BeforeAll
  static void beforeAll() throws HopException {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() {
    helper = new TransformMockHelper<>("Cypher", CypherMeta.class, CypherData.class);
    when(helper.logChannelFactory.create(any(), any(ILoggingObject.class)))
        .thenReturn(helper.iLogChannel);
    meta = new CypherMeta();
    data = new CypherData();
    cypher =
        new RecordingCypher(
            helper.transformMeta, meta, data, 0, helper.pipelineMeta, helper.pipeline);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    data.outputRowMeta = rowMeta;
    data.hasInput = true;
    data.cypherStatements = new ArrayList<>();
    data.cypherStatements.add(new CypherStatement(new Object[] {"a"}, "CREATE (:A)", Map.of()));
    data.cypherStatements.add(new CypherStatement(new Object[] {"b"}, "CREATE (:B)", Map.of()));
  }

  @AfterEach
  void tearDown() {
    helper.cleanUp();
  }

  @Test
  void retriedBatchOutputsEachRowOnce() throws Exception {
    // The second statement of the first attempt fails, after the first one returned its row
    data.graphConnection = new FailingConnection(1);
    data.attempts = 2;

    cypher.runGenericStatementsBatch();

    assertEquals(2, cypher.rows.size());
    assertArrayEquals(new Object[] {"a"}, cypher.rows.get(0));
    assertArrayEquals(new Object[] {"b"}, cypher.rows.get(1));
    assertTrue(data.cypherStatements.isEmpty());
  }

  @Test
  void failedBatchOutputsNoRows() {
    data.graphConnection = new FailingConnection(1);
    data.attempts = 1;

    assertThrows(HopException.class, () -> cypher.runGenericStatementsBatch());
    assertTrue(cypher.rows.isEmpty());
    assertTrue(data.attemptRows.isEmpty());
  }

  @Test
  void noRetriesOfChangesWithoutTransactions() {
    assertEquals(1, Cypher.getAttempts(3, false, false));
    assertEquals(3, Cypher.getAttempts(3, true, false));
    assertEquals(3, Cypher.getAttempts(3, false, true));
    assertEquals(1, Cypher.getAttempts(1, false, false));
  }

  @Test
  void streamingRule() {
    // Retries configured: rows are kept until the attempt succeeded
    assertFalse(Cypher.isStreamingRows(2, true, false));
    assertFalse(Cypher.isStreamingRows(2, false, false));
    // No retries: rows stream for reads and for the one statement without input
    assertTrue(Cypher.isStreamingRows(1, true, true));
    assertTrue(Cypher.isStreamingRows(1, false, false));
    // Writes from input rows: kept per batch so that the driver can retry them
    assertFalse(Cypher.isStreamingRows(1, false, true));
  }

  @Test
  void genericRowsStreamWithoutRetries() throws Exception {
    meta.setReadOnly(true);
    data.attempts = 1;
    RecordingConnection connection = new RecordingConnection(false);
    data.graphConnection = connection;

    cypher.runGenericStatementsBatch();

    // The row of the first statement was passed on before the second statement executed
    assertEquals(List.of(0, 1), connection.rowsBeforeStatement);
    assertEquals(2, cypher.rows.size());
    assertTrue(data.attemptRows.isEmpty());
  }

  @Test
  void genericStreamedRowsAreNotExecutedAgain() {
    meta.setReadOnly(true);
    data.attempts = 1;
    data.graphConnection = new RecordingConnection(true);

    // The connection executes the work again after its rows were passed on: that fails
    // instead of passing the row on twice.
    assertThrows(HopRuntimeException.class, () -> cypher.runGenericStatementsBatch());
    assertEquals(2, cypher.rows.size());
  }

  @Test
  void boltReadRowsStreamWithoutRetries() throws Exception {
    meta.setReadOnly(true);
    data.attempts = 1;
    int[] rowsBeforeSecondStatement = {-1};
    setUpBolt(
        tx -> {
          when(tx.run(eq("CREATE (:B)"), anyMap()))
              .thenAnswer(
                  invocation -> {
                    rowsBeforeSecondStatement[0] = cypher.rows.size();
                    return emptyResult();
                  });
        },
        1);

    cypher.batchComplete();

    assertEquals(1, rowsBeforeSecondStatement[0]);
    assertEquals(2, cypher.rows.size());
  }

  @Test
  void boltStreamedRowsAreNotRetriedByTheDriver() {
    meta.setReadOnly(true);
    data.attempts = 1;
    // The driver calls the work again, after its rows were passed on
    setUpBolt(tx -> {}, 2);

    assertThrows(HopException.class, () -> cypher.batchComplete());
    assertEquals(2, cypher.rows.size());
  }

  @Test
  void boltWritesFromInputAreBufferedForDriverRetries() throws Exception {
    data.attempts = 1;
    // The driver calls the work again: the rows of the first call are dropped
    setUpBolt(tx -> {}, 2);

    cypher.batchComplete();

    assertEquals(2, cypher.rows.size());
    assertArrayEquals(new Object[] {"a"}, cypher.rows.get(0));
    assertArrayEquals(new Object[] {"b"}, cypher.rows.get(1));
  }

  /**
   * A Bolt session whose managed transactions call the work the given number of times, as the
   * driver does when it retries a transient error.
   */
  private void setUpBolt(java.util.function.Consumer<TransactionContext> configure, int calls) {
    NeoConnection neoConnection = mock(NeoConnection.class);
    when(neoConnection.getDialect()).thenReturn(Neo4jGraphDialect.INSTANCE);
    data.neoConnection = neoConnection;
    TransactionContext tx = mock(TransactionContext.class);
    when(tx.run(any(String.class), anyMap())).thenAnswer(invocation -> emptyResult());
    configure.accept(tx);
    Session session = mock(Session.class);
    org.mockito.stubbing.Answer<Object> work =
        invocation -> {
          TransactionCallback<?> callback = invocation.getArgument(0);
          Object result = null;
          for (int i = 0; i < calls; i++) {
            result = callback.execute(tx);
          }
          return result;
        };
    when(session.executeRead(any())).thenAnswer(work);
    when(session.executeWrite(any())).thenAnswer(work);
    data.session = session;
  }

  private static Result emptyResult() {
    Result result = mock(Result.class);
    when(result.consume()).thenReturn(mock(ResultSummary.class));
    return result;
  }

  /** Records the rows passed on to the next transforms. */
  private static class RecordingCypher extends Cypher {
    final List<Object[]> rows = new ArrayList<>();

    RecordingCypher(
        org.apache.hop.pipeline.transform.TransformMeta transformMeta,
        CypherMeta meta,
        CypherData data,
        int copyNr,
        org.apache.hop.pipeline.PipelineMeta pipelineMeta,
        org.apache.hop.pipeline.Pipeline pipeline) {
      super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
    }

    @Override
    public void putRow(IRowMeta rowMeta, Object[] row) {
      rows.add(row);
    }
  }

  /** A connection whose transactions fail on the second statement the given number of times. */
  private static class FailingConnection implements IGraphConnection {
    private int failuresLeft;

    FailingConnection(int failures) {
      this.failuresLeft = failures;
    }

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters) {
      return List.of();
    }

    @Override
    public IGraphTransaction beginTransaction() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T executeWrite(IGraphTransactionWork<T> work) throws HopException {
      int[] statements = {0};
      return work.execute(
          new IGraphTransaction() {
            @Override
            public List<Map<String, Object>> execute(
                String statement, Map<String, Object> parameters) throws HopException {
              statements[0]++;
              if (statements[0] == 2 && failuresLeft > 0) {
                failuresLeft--;
                throw new HopException("Transient failure");
              }
              return List.of();
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
    public void close() {}
  }

  /**
   * A connection which records the number of rows passed on before each statement. Optionally it
   * executes the work a second time, as a connection retrying a transient error.
   */
  private class RecordingConnection implements IGraphConnection {
    final List<Integer> rowsBeforeStatement = new ArrayList<>();
    private final boolean executeTwice;

    RecordingConnection(boolean executeTwice) {
      this.executeTwice = executeTwice;
    }

    @Override
    public List<Map<String, Object>> execute(String statement, Map<String, Object> parameters) {
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
                String statement, Map<String, Object> parameters) {
              rowsBeforeStatement.add(cypher.rows.size());
              return List.of();
            }

            @Override
            public void commit() {}

            @Override
            public void rollback() {}

            @Override
            public void close() {}
          };
      T result = work.execute(transaction);
      if (executeTwice) {
        result = work.execute(transaction);
      }
      return result;
    }

    @Override
    public void close() {}
  }
}
