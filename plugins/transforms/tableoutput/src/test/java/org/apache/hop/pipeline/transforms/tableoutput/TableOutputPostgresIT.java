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

package org.apache.hop.pipeline.transforms.tableoutput;

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
import org.apache.hop.core.row.value.ValueMetaInteger;
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
 * Table output with an error hop on PostgreSQL, where a failed statement aborts the transaction:
 * only the failing row may go to the error hop, the rows after it must still be written. Also in a
 * transactional pipeline, where several transforms share one connection.
 */
class TableOutputPostgresIT {

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
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    return inputRowMeta;
  }

  /** Rows 1..rows named name, except every badEvery-th row, whose name is too long. */
  private static Queue<Object[]> rows(int rows, String name, int badEvery) {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= rows; id++) {
      input.add(new Object[] {id, id % badEvery == 0 ? "toolong" : name});
    }
    return input;
  }

  private static void createTarget(String table) throws Exception {
    execute(
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(5))");
  }

  /** A Table output with an error hop, fed from input, that collects its output and errors. */
  private static TableOutput tableOutput(
      String name,
      String table,
      int commitSize,
      boolean batch,
      Queue<Object[]> input,
      List<Object[]> output,
      List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = pipelineMeta();

    TableOutputMeta meta = new TableOutputMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSchemaName("");
    meta.setTableName(table);
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);

    TableOutput transform =
        new TableOutput(
            transformMeta,
            meta,
            new TableOutputData(),
            0,
            pipelineMeta,
            new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            Object[] row = input.poll();
            return row == null ? null : row.clone();
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
  void onlyTheFailingRowsGoToTheErrorHop(int commitSize) throws Exception {
    createTarget("table_output_target");
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    // Row 3 is too long for the column.
    input.add(new Object[] {1L, "a"});
    input.add(new Object[] {2L, "b"});
    input.add(new Object[] {3L, "toolong"});
    input.add(new Object[] {4L, "d"});
    input.add(new Object[] {5L, "e"});
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    runAlone(
        tableOutput(
            "table output", "table_output_target", commitSize, false, input, output, errors));

    assertEquals(4, output.size());
    assertEquals(1, errors.size());
    assertEquals(3L, errors.get(0)[0]);
    assertEquals(
        List.of("1:a", "2:b", "4:d", "5:e"),
        query("SELECT id, name FROM table_output_target ORDER BY id").stream()
            .map(r -> r[0] + ":" + r[1])
            .toList());
  }

  /**
   * "Use batch update" with an error hop on PostgreSQL: savepoints take over and batching is off,
   * so only the failing rows go to the error hop.
   */
  @Test
  void batchModeIsSwitchedOffWithAnErrorHop() throws Exception {
    createTarget("table_output_batch");
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    runAlone(
        tableOutput(
            "table output", "table_output_batch", 10, true, rows(50, "a", 7), output, errors));

    assertEquals(7, errors.size());
    assertEquals(43, output.size());
    assertEquals(43, count("SELECT count(*) FROM table_output_batch"));
  }

  /**
   * Two Table outputs with error hops in a transactional pipeline share one connection, each
   * setting and releasing a savepoint per row at the same time as the other. On main this already
   * used savepoints on the shared connection.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void twoTableOutputsWithErrorHopsInATransactionalPipeline(int commitSize) throws Exception {
    createTarget("table_output_tx_a");
    createTarget("table_output_tx_b");
    List<Object[]> outputA = syncList();
    List<Object[]> errorsA = syncList();
    List<Object[]> outputB = syncList();
    List<Object[]> errorsB = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        tableOutput(
            "table output A",
            "table_output_tx_a",
            commitSize,
            false,
            rows(ROWS, "a", 97),
            outputA,
            errorsA));
    run.join(
        tableOutput(
            "table output B",
            "table_output_tx_b",
            commitSize,
            false,
            rows(ROWS, "b", 89),
            outputB,
            errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 97, errorsA.size());
    assertEquals(ROWS - ROWS / 97, outputA.size());
    assertEquals(ROWS / 89, errorsB.size());
    assertEquals(ROWS - ROWS / 89, outputB.size());
    assertEquals(
        ROWS - ROWS / 97, count("SELECT count(*) FROM table_output_tx_a WHERE name = 'a'"));
    assertEquals(ROWS - ROWS / 97, count("SELECT count(*) FROM table_output_tx_a"));
    assertEquals(
        ROWS - ROWS / 89, count("SELECT count(*) FROM table_output_tx_b WHERE name = 'b'"));
    assertEquals(ROWS - ROWS / 89, count("SELECT count(*) FROM table_output_tx_b"));
  }

  /**
   * A Table output without an error hop shares the connection with one that has an error hop: none
   * of its rows may be undone by the other transform's rollbacks.
   */
  @Test
  void aTableOutputWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createTarget("table_output_tx_err");
    createTarget("table_output_tx_plain");
    List<Object[]> errors = syncList();
    List<Object[]> plainOutput = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        tableOutput(
            "with error hop",
            "table_output_tx_err",
            100,
            false,
            rows(ROWS, "a", 13),
            syncList(),
            errors));
    TableOutput plain =
        tableOutput(
            "without error hop",
            "table_output_tx_plain",
            100,
            false,
            rows(ROWS, "p", Integer.MAX_VALUE),
            plainOutput,
            syncList());
    plain.getTransformMeta().setTransformErrorMeta(null);
    run.join(plain);
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 13, errors.size());
    assertEquals(ROWS, plainOutput.size());
    assertEquals(ROWS, count("SELECT count(*) FROM table_output_tx_plain WHERE name = 'p'"));
    assertEquals(ROWS - ROWS / 13, count("SELECT count(*) FROM table_output_tx_err"));
  }
}
