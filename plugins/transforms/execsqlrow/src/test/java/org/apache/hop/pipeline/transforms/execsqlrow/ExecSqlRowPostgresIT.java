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

package org.apache.hop.pipeline.transforms.execsqlrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Execute row SQL script with an error hop on PostgreSQL, where a failed statement aborts the
 * transaction: only the failing statement may go to the error hop, the rows after it must still
 * run. Also in a transactional pipeline, where several transforms share one connection.
 */
class ExecSqlRowPostgresIT {

  private static final String CONNECTION = "pg";
  private static final int ROWS = 2_000;
  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    SharedConnectionRun.registerConnectionGroupExtensionPoint();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  private static DatabaseMeta databaseMeta() {
    return new DatabaseMeta(
        CONNECTION,
        "POSTGRESQL",
        "Native",
        container.getHost(),
        container.getDatabaseName(),
        Integer.toString(container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)),
        container.getUsername(),
        container.getPassword());
  }

  private static List<Object[]> query(String sql) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta())) {
      db.connect();
      return db.getRows(sql, -1);
    }
  }

  private static long count(String sql) throws Exception {
    return ((Number) query(sql).get(0)[0]).longValue();
  }

  private static void execute(String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }

  private static PipelineMeta pipelineMeta() throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(metadataProvider);
    return pipelineMeta;
  }

  /**
   * Gives the transform an error hop. After the transform is created: its constructor looks at the
   * hops of the pipeline.
   */
  private static void addErrorHop(PipelineMeta pipelineMeta, TransformMeta transformMeta) {
    TransformMeta target =
        new TransformMeta(transformMeta.getName() + " errors", transformMeta.getTransform());
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
    TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
    errorMeta.setEnabled(true);
    transformMeta.setTransformErrorMeta(errorMeta);
  }

  private static void runAlone(BaseTransform<?, ?> transform) throws Exception {
    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
  }

  private static List<Object[]> syncList() {
    return Collections.synchronizedList(new ArrayList<>());
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaString("sql"));
    return inputRowMeta;
  }

  /**
   * An INSERT of ids 1..rows, except that every badEvery-th row inserts id 1 again, a duplicate
   * key.
   */
  private static Queue<Object[]> inserts(String table, int rows, int badEvery) {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= rows; id++) {
      long value = id % badEvery == 0 ? 1 : id;
      input.add(new Object[] {"INSERT INTO " + table + " VALUES (" + value + ")"});
    }
    return input;
  }

  private static void createTarget(String table) throws Exception {
    execute("DROP TABLE IF EXISTS " + table, "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY)");
  }

  /** An Execute row SQL script with an error hop, fed from input, that collects its rows. */
  private static ExecSqlRow execSqlRow(
      String name,
      int commitSize,
      Queue<Object[]> input,
      List<Object[]> output,
      List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = pipelineMeta();

    ExecSqlRowMeta meta = new ExecSqlRowMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSqlFieldName("sql");
    meta.setSendOneStatement(true);
    meta.setCommitSize(commitSize);
    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);

    ExecSqlRow transform =
        new ExecSqlRow(
            transformMeta, meta, new ExecSqlRowData(), 0, pipelineMeta, new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            return input.poll();
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            output.add(row);
          }

          @Override
          public void putError(
              IRowMeta rowMeta,
              Object[] row,
              long nrErrors,
              String errorDescriptions,
              String fieldNames,
              String errorCodes) {
            errors.add(row);
          }
        };
    addErrorHop(pipelineMeta, transformMeta);
    return transform;
  }

  /**
   * @param commitSize 1 commits after every row, 100 only at the end
   */
  @ParameterizedTest
  @ValueSource(ints = {100, 1})
  void onlyTheFailingStatementGoesToTheErrorHop(int commitSize) throws Exception {
    createTarget("exec_row_target");
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (String id : new String[] {"1", "2", "2", "3", "4", "5"}) {
      input.add(new Object[] {"INSERT INTO exec_row_target VALUES (" + id + ")"});
    }

    runAlone(execSqlRow("exec row sql", commitSize, input, output, errors));

    assertEquals(5, output.size());
    assertEquals(1, errors.size());
    assertEquals("INSERT INTO exec_row_target VALUES (2)", errors.get(0)[0]);
    List<Object[]> rows = query("SELECT id FROM exec_row_target ORDER BY id");
    assertEquals(5, rows.size());
    assertEquals(5L, ((Number) rows.get(4)[0]).longValue());
  }

  /**
   * Two Execute row SQL scripts with error hops in a transactional pipeline share one connection,
   * each setting and releasing a savepoint per row at the same time as the other.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void twoExecSqlRowsWithErrorHopsInATransactionalPipeline(int commitSize) throws Exception {
    createTarget("exec_row_tx_a");
    createTarget("exec_row_tx_b");
    List<Object[]> outputA = syncList();
    List<Object[]> errorsA = syncList();
    List<Object[]> outputB = syncList();
    List<Object[]> errorsB = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        execSqlRow("exec A", commitSize, inserts("exec_row_tx_a", ROWS, 97), outputA, errorsA));
    run.join(
        execSqlRow("exec B", commitSize, inserts("exec_row_tx_b", ROWS, 89), outputB, errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 97, errorsA.size());
    assertEquals(ROWS - ROWS / 97, outputA.size());
    assertEquals(ROWS / 89, errorsB.size());
    assertEquals(ROWS - ROWS / 89, outputB.size());
    assertEquals(ROWS - ROWS / 97, count("SELECT count(*) FROM exec_row_tx_a"));
    assertEquals(0, count("SELECT count(*) FROM exec_row_tx_a WHERE id % 97 = 0"));
    assertEquals(ROWS - ROWS / 89, count("SELECT count(*) FROM exec_row_tx_b"));
    assertEquals(0, count("SELECT count(*) FROM exec_row_tx_b WHERE id % 89 = 0"));
  }

  /**
   * An Execute row SQL script without an error hop shares the connection with one that has an error
   * hop: none of its rows may be undone by the other transform's rollbacks.
   */
  @Test
  void anExecSqlRowWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createTarget("exec_row_tx_err");
    createTarget("exec_row_tx_plain");
    List<Object[]> errors = syncList();
    List<Object[]> plainOutput = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        execSqlRow(
            "with error hop", 100, inserts("exec_row_tx_err", ROWS, 13), syncList(), errors));
    ExecSqlRow plain =
        execSqlRow(
            "without error hop",
            100,
            inserts("exec_row_tx_plain", ROWS, Integer.MAX_VALUE),
            plainOutput,
            syncList());
    plain.getTransformMeta().setTransformErrorMeta(null);
    run.join(plain);
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 13, errors.size());
    assertEquals(ROWS, plainOutput.size());
    assertEquals(ROWS, count("SELECT count(*) FROM exec_row_tx_plain"));
    assertEquals(ROWS - ROWS / 13, count("SELECT count(*) FROM exec_row_tx_err"));
  }
}
