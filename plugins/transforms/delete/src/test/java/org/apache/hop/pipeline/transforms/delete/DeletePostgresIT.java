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

package org.apache.hop.pipeline.transforms.delete;

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
 * Delete with an error hop on PostgreSQL, where a failed statement aborts the transaction: only the
 * failing row may go to the error hop, the rows after it must still be deleted. Also in a
 * transactional pipeline, where several transforms share one connection.
 */
class DeletePostgresIT {

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
    return (Long) query(sql).get(0)[0];
  }

  private static void execute(String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    return inputRowMeta;
  }

  /** Rows 1..rows to delete. */
  private static Queue<Object[]> rows(int rows) {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= rows; id++) {
      input.add(new Object[] {id, "x"});
    }
    return input;
  }

  /**
   * A table of ids 1..rows, with a child table that still references every badEvery-th id, so
   * deleting those fails.
   */
  private static void createTarget(String table, int rows, int badEvery) throws Exception {
    execute(
        "DROP TABLE IF EXISTS " + table + "_child",
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(5))",
        "INSERT INTO " + table + " SELECT g, 'x' FROM generate_series(1, " + rows + ") g",
        "CREATE TABLE " + table + "_child (parent_id BIGINT REFERENCES " + table + " (id))",
        "INSERT INTO "
            + table
            + "_child SELECT g FROM generate_series(1, "
            + rows
            + ") g WHERE g % "
            + badEvery
            + " = 0");
  }

  /** A Delete with an error hop, fed from input, that collects its output and error rows. */
  private static Delete delete(
      String name,
      String table,
      int commitSize,
      boolean batch,
      Queue<Object[]> input,
      List<Object[]> output,
      List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(metadataProvider);

    DeleteMeta meta = new DeleteMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    meta.getLookup().setSchemaName("");
    meta.getLookup().setTableName(table);
    DeleteKeyField key = new DeleteKeyField();
    key.setKeyStream("id");
    key.setKeyLookup("id");
    key.setKeyCondition("=");
    meta.getLookup().getFields().add(key);
    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);

    Delete delete =
        new Delete(
            transformMeta, meta, new DeleteData(), 0, pipelineMeta, new LocalPipelineEngine()) {
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

    // After the transform: its constructor looks at the hops of the pipeline.
    TransformMeta target = new TransformMeta(name + " errors", new DeleteMeta());
    pipelineMeta.addTransform(target);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
    TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
    errorMeta.setEnabled(true);
    transformMeta.setTransformErrorMeta(errorMeta);
    return delete;
  }

  private static void runAlone(Delete transform) throws Exception {
    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
  }

  /**
   * @param commitSize 1 commits after every row, 100 only at the end
   */
  @ParameterizedTest
  @ValueSource(ints = {100, 1})
  void onlyTheFailingRowsGoToTheErrorHop(int commitSize) throws Exception {
    // Row 3 is still referenced, so deleting it fails.
    createTarget("delete_target", 5, 3);
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    runAlone(delete("delete", "delete_target", commitSize, false, rows(5), output, errors));

    assertEquals(4, output.size());
    assertEquals(1, errors.size());
    assertEquals(3L, errors.get(0)[0]);
    assertEquals(
        List.of("3:x"),
        query("SELECT id, name FROM delete_target ORDER BY id").stream()
            .map(r -> r[0] + ":" + r[1])
            .toList());
  }

  /**
   * "Use batch update" with an error hop: batching is switched off for the run, so only the failing
   * row goes to the error hop, not every row of its batch.
   */
  @Test
  void batchModeIsSwitchedOffWithAnErrorHop() throws Exception {
    createTarget("delete_batch", 50, 7);
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    runAlone(delete("delete", "delete_batch", 10, true, rows(50), output, errors));

    assertEquals(7, errors.size());
    assertEquals(43, output.size());
    assertEquals(7, count("SELECT count(*) FROM delete_batch"));
  }

  /**
   * Two Deletes with error hops in a transactional pipeline share one connection, each setting and
   * releasing a savepoint per row at the same time as the other.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void twoDeletesWithErrorHopsInATransactionalPipeline(int commitSize) throws Exception {
    createTarget("delete_tx_a", ROWS, 97);
    createTarget("delete_tx_b", ROWS, 89);
    List<Object[]> outputA = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> errorsA = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> outputB = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> errorsB = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(delete("delete A", "delete_tx_a", commitSize, false, rows(ROWS), outputA, errorsA));
    run.join(delete("delete B", "delete_tx_b", commitSize, false, rows(ROWS), outputB, errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 97, errorsA.size());
    assertEquals(ROWS - ROWS / 97, outputA.size());
    assertEquals(ROWS / 89, errorsB.size());
    assertEquals(ROWS - ROWS / 89, outputB.size());
    // Only the referenced rows are left.
    assertEquals(ROWS / 97, count("SELECT count(*) FROM delete_tx_a"));
    assertEquals(0, count("SELECT count(*) FROM delete_tx_a WHERE id % 97 <> 0"));
    assertEquals(ROWS / 89, count("SELECT count(*) FROM delete_tx_b"));
    assertEquals(0, count("SELECT count(*) FROM delete_tx_b WHERE id % 89 <> 0"));
  }

  /**
   * A Delete without an error hop shares the connection with one that has an error hop: none of its
   * deletes may be undone by the other transform's rollbacks.
   */
  @Test
  void aDeleteWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createTarget("delete_tx_err", ROWS, 13);
    createTarget("delete_tx_plain", ROWS, Integer.MAX_VALUE);
    List<Object[]> errors = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> plainOutput = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        delete(
            "with error hop",
            "delete_tx_err",
            100,
            false,
            rows(ROWS),
            Collections.synchronizedList(new ArrayList<>()),
            errors));
    Delete plain =
        delete(
            "without error hop",
            "delete_tx_plain",
            100,
            false,
            rows(ROWS),
            plainOutput,
            Collections.synchronizedList(new ArrayList<>()));
    plain.getTransformMeta().setTransformErrorMeta(null);
    run.join(plain);
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 13, errors.size());
    assertEquals(ROWS, plainOutput.size());
    assertEquals(0, count("SELECT count(*) FROM delete_tx_plain"));
    assertEquals(ROWS / 13, count("SELECT count(*) FROM delete_tx_err"));
  }
}
