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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
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
 * Synchronize after merge with an error hop on PostgreSQL, where a failed statement aborts the
 * transaction: only the failing rows may go to the error hop, the inserts, updates and deletes
 * around them must still be written. Also in a transactional pipeline, where several transforms
 * share one connection.
 *
 * <p>The input has an operation per row: ids in the first half of the table are updated, every
 * fifth of them deleted, ids in the second half inserted. Every {@code badEvery}-th row has a name
 * too long for its column, so its insert or update fails; a delete doesn't write the name and never
 * fails.
 */
class SynchronizeAfterMergePostgresIT {

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

  private static void execute(String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }

  /** The first half of the ids exists, named 'x'. */
  private static void createTarget(String table, int rows) throws Exception {
    execute(
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(5))",
        "INSERT INTO " + table + " SELECT g, 'x' FROM generate_series(1, " + rows / 2 + ") g");
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("op"));
    return rowMeta;
  }

  private static String operation(long id, int rows) {
    if (id > rows / 2) {
      return "insert";
    }
    return id % 5 == 0 ? "delete" : "update";
  }

  private static boolean bad(long id, int badEvery) {
    return id % badEvery == 0;
  }

  private static Queue<Object[]> rows(int rows, String name, int badEvery) {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= rows; id++) {
      input.add(new Object[] {id, bad(id, badEvery) ? "toolong" : name, operation(id, rows)});
    }
    return input;
  }

  /** The rows that must fail: an insert or update with a name that is too long. */
  private static int expectedErrors(int rows, int badEvery) {
    int errors = 0;
    for (long id = 1; id <= rows; id++) {
      if (bad(id, badEvery) && !"delete".equals(operation(id, rows))) {
        errors++;
      }
    }
    return errors;
  }

  /** The table after the run, as id:name in id order. */
  private static String expectedTable(int rows, String name, int badEvery) {
    StringJoiner table = new StringJoiner(",");
    for (long id = 1; id <= rows; id++) {
      String operation = operation(id, rows);
      boolean failed = bad(id, badEvery);
      if ("delete".equals(operation)) {
        continue;
      }
      if ("update".equals(operation)) {
        table.add(id + ":" + (failed ? "x" : name));
      } else if (!failed) {
        table.add(id + ":" + name);
      }
    }
    return table.toString();
  }

  private static String table(String table) throws Exception {
    StringJoiner content = new StringJoiner(",");
    for (Object[] row : query("SELECT id, name FROM " + table + " ORDER BY id")) {
      content.add(row[0] + ":" + row[1]);
    }
    return content.toString();
  }

  /**
   * Every input row leaves the transform once: on the output or on the error hop, never on both.
   */
  private static void assertEveryRowLeavesOnce(
      int rows, List<Object[]> output, List<Object[]> errors) {
    Set<Long> seen = new TreeSet<>();
    List<Object[]> all = new ArrayList<>(output);
    if (errors != null) {
      all.addAll(errors);
    }
    for (Object[] row : all) {
      assertTrue(seen.add((Long) row[0]), "row " + row[0] + " left the transform twice");
    }
    assertEquals(rows, seen.size(), "rows left on neither stream");
  }

  /** A Synchronize after merge, with an error hop when errors isn't null. */
  private static SynchronizeAfterMerge synchronize(
      String name,
      String table,
      int commitSize,
      Queue<Object[]> input,
      List<Object[]> output,
      List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(metadataProvider);

    SynchronizeAfterMergeMeta meta = new SynchronizeAfterMergeMeta();
    meta.setConnection(CONNECTION);
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setOperationOrderField("op");
    meta.setOrderInsert("insert");
    meta.setOrderUpdate("update");
    meta.setOrderDelete("delete");
    meta.setPerformingLookup(false);
    meta.setUsingBatchUpdates(false);
    meta.getLookup().setSchemaName("");
    meta.getLookup().setTableName(table);
    SynchronizeAfterMergeMeta.KeyCondition key = new SynchronizeAfterMergeMeta.KeyCondition();
    key.setColumnName("id");
    key.setCondition("=");
    key.setFieldName("id");
    key.setFieldName2("");
    meta.getLookup().getKeyConditions().add(key);
    SynchronizeAfterMergeMeta.ValueUpdate id = new SynchronizeAfterMergeMeta.ValueUpdate();
    id.setColumnName("id");
    id.setFieldName("id");
    id.setUpdate(false);
    SynchronizeAfterMergeMeta.ValueUpdate value = new SynchronizeAfterMergeMeta.ValueUpdate();
    value.setColumnName("name");
    value.setFieldName("name");
    value.setUpdate(true);
    meta.getLookup().getValueUpdates().add(id);
    meta.getLookup().getValueUpdates().add(value);

    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);

    SynchronizeAfterMerge transform =
        new SynchronizeAfterMerge(
            transformMeta,
            meta,
            new SynchronizeAfterMergeData(),
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

    if (errors != null) {
      // After the transform: its constructor looks at the hops of the pipeline.
      TransformMeta target = new TransformMeta(name + " errors", new SynchronizeAfterMergeMeta());
      pipelineMeta.addTransform(target);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
      TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
      errorMeta.setEnabled(true);
      transformMeta.setTransformErrorMeta(errorMeta);
    }
    return transform;
  }

  /**
   * @param commitSize 1 commits after every row, 100 every hundred rows
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void onlyTheFailingRowsGoToTheErrorHop(int commitSize) throws Exception {
    createTarget("sync_target", ROWS);
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();
    SynchronizeAfterMerge transform =
        synchronize("sync", "sync_target", commitSize, rows(ROWS, "a", 97), output, errors);

    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }

    assertEquals(0, transform.getErrors(), "the transform failed");
    assertEquals(expectedErrors(ROWS, 97), errors.size());
    assertEveryRowLeavesOnce(ROWS, output, errors);
    assertEquals(expectedTable(ROWS, "a", 97), table("sync_target"));
  }

  /**
   * Two of them with error hops in a transactional pipeline: one connection, a savepoint per row
   * each, at the same time.
   */
  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void twoSynchronizesWithErrorHopsInATransactionalPipeline(int commitSize) throws Exception {
    createTarget("sync_tx_a", ROWS);
    createTarget("sync_tx_b", ROWS);
    List<Object[]> errorsA = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> errorsB = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> outputA = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> outputB = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(synchronize("sync A", "sync_tx_a", commitSize, rows(ROWS, "a", 97), outputA, errorsA));
    run.join(synchronize("sync B", "sync_tx_b", commitSize, rows(ROWS, "b", 89), outputB, errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(expectedErrors(ROWS, 97), errorsA.size());
    assertEquals(expectedErrors(ROWS, 89), errorsB.size());
    assertEveryRowLeavesOnce(ROWS, outputA, errorsA);
    assertEveryRowLeavesOnce(ROWS, outputB, errorsB);
    assertEquals(expectedTable(ROWS, "a", 97), table("sync_tx_a"));
    assertEquals(expectedTable(ROWS, "b", 89), table("sync_tx_b"));
  }

  /** One without an error hop next to one with: none of its rows may be undone. */
  @Test
  void aSynchronizeWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createTarget("sync_tx_err", ROWS);
    createTarget("sync_tx_plain", ROWS);
    List<Object[]> errors = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> output = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> plainOutput = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        synchronize("with error hop", "sync_tx_err", 100, rows(ROWS, "a", 13), output, errors));
    run.join(
        synchronize(
            "without error hop",
            "sync_tx_plain",
            100,
            rows(ROWS, "p", Integer.MAX_VALUE),
            plainOutput,
            null));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(expectedErrors(ROWS, 13), errors.size());
    assertEveryRowLeavesOnce(ROWS, output, errors);
    assertEveryRowLeavesOnce(ROWS, plainOutput, null);
    assertEquals(expectedTable(ROWS, "a", 13), table("sync_tx_err"));
    assertEquals(expectedTable(ROWS, "p", Integer.MAX_VALUE), table("sync_tx_plain"));
  }
}
