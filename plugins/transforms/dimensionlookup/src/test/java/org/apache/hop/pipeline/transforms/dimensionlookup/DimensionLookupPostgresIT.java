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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import org.apache.hop.core.row.value.ValueMetaDate;
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
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLField;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLKey;
import org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Dimension lookup/update with an error hop on PostgreSQL, where a failed statement aborts the
 * transaction: only the failing row may go to the error hop, the rows after it must still be
 * written. Also in a transactional pipeline, where several transforms share one connection.
 */
class DimensionLookupPostgresIT {

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
    inputRowMeta.addValueMeta(new ValueMetaInteger("cust_id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));
    inputRowMeta.addValueMeta(new ValueMetaDate("updated"));
    return inputRowMeta;
  }

  /** New customers 1..rows named name, except every badEvery-th, whose name is too long. */
  private static Queue<Object[]> customers(int rows, String name, int badEvery) {
    Date updated = new Date();
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= rows; id++) {
      input.add(new Object[] {id, id % badEvery == 0 ? "toolong" : name, updated});
    }
    return input;
  }

  private static void createDimension(String table) throws Exception {
    execute(
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE "
            + table
            + " (tk BIGINT PRIMARY KEY, version INTEGER, date_from TIMESTAMP,"
            + " date_to TIMESTAMP, cust_id BIGINT, name VARCHAR(5))");
  }

  /** A Dimension lookup/update with an error hop, fed from input, that collects its rows. */
  private static DimensionLookup dimension(
      String name,
      String table,
      int cacheSize,
      int commitSize,
      Queue<Object[]> input,
      List<Object[]> output,
      List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = pipelineMeta();

    DimensionLookupMeta meta = new DimensionLookupMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setSchemaName("");
    meta.setTableName(table);
    meta.setUpdate(true);
    meta.setCommitSize(commitSize);
    meta.setCacheSize(cacheSize);
    DLKey key = new DLKey();
    key.setName("cust_id");
    key.setLookup("cust_id");
    meta.getFields().getKeys().add(key);
    DLField field = new DLField();
    field.setName("name");
    field.setLookup("name");
    field.setUpdate(DimensionLookupMeta.DimensionUpdateType.INSERT.getCode());
    meta.getFields().getFields().add(field);
    meta.getFields().getDate().setName("updated");
    meta.getFields().getReturns().setKeyField("tk");
    meta.getFields().getReturns().setCreationMethod(TechnicalKeyCreationMethod.TABLE_MAXIMUM);
    meta.getFields().getReturns().setVersionField("version");

    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);
    DimensionLookup transform =
        new DimensionLookup(
            transformMeta,
            meta,
            new DimensionLookupData(),
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
   * @param cacheSize 0 caches every row, -1 caches nothing
   * @param commitSize 1 commits inside every row, 100 only at the end
   */
  @ParameterizedTest
  @CsvSource({"0, 100", "-1, 100", "0, 1", "-1, 1"})
  void onlyTheFailingRowsGoToTheErrorHop(int cacheSize, int commitSize) throws Exception {
    createDimension("d_customer");

    // Rows 3 and 5 are too long for the column: row 3 is a new customer, row 5 a new version of
    // customer 1, which first closes the current version and then fails to insert the new one.
    String[][] rows = {
      {"1", "a"}, {"2", "b"}, {"3", "toolong"}, {"4", "d"}, {"1", "toolong"}, {"5", "e"}, {"1", "z"}
    };
    Date updated = new Date();
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (String[] row : rows) {
      input.add(new Object[] {Long.valueOf(row[0]), row[1], updated});
    }
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();

    runAlone(dimension("dimension", "d_customer", cacheSize, commitSize, input, output, errors));

    assertEquals(5, output.size());
    assertEquals(2, errors.size());
    assertEquals(3L, errors.get(0)[0]);
    assertEquals(1L, errors.get(1)[0]);
    assertEquals(
        List.of("1:1:a:closed", "1:2:z:open", "2:1:b:open", "4:1:d:open", "5:1:e:open"),
        query(
                "SELECT cust_id, version, name, date_to < '2100-01-01' FROM d_customer"
                    + " WHERE tk > 0 ORDER BY cust_id, version")
            .stream()
            .map(
                r ->
                    r[0]
                        + ":"
                        + r[1]
                        + ":"
                        + r[2]
                        + ":"
                        + (Boolean.TRUE.equals(r[3]) ? "closed" : "open"))
            .toList());
  }

  /**
   * Two Dimension lookup/updates with error hops in a transactional pipeline share one connection,
   * each on its own dimension, setting and releasing a savepoint per row at the same time.
   */
  @ParameterizedTest(name = "cache size {0}, commit size {1}")
  @CsvSource({"0, 100", "-1, 100", "0, 1"})
  void twoDimensionsWithErrorHopsInATransactionalPipeline(int cacheSize, int commitSize)
      throws Exception {
    createDimension("d_tx_a");
    createDimension("d_tx_b");
    List<Object[]> outputA = syncList();
    List<Object[]> errorsA = syncList();
    List<Object[]> outputB = syncList();
    List<Object[]> errorsB = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        dimension(
            "dimension A",
            "d_tx_a",
            cacheSize,
            commitSize,
            customers(ROWS, "a", 97),
            outputA,
            errorsA));
    run.join(
        dimension(
            "dimension B",
            "d_tx_b",
            cacheSize,
            commitSize,
            customers(ROWS, "b", 89),
            outputB,
            errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 97, errorsA.size());
    assertEquals(ROWS - ROWS / 97, outputA.size());
    assertEquals(ROWS / 89, errorsB.size());
    assertEquals(ROWS - ROWS / 89, outputB.size());
    assertDimension("d_tx_a", "a", 97);
    assertDimension("d_tx_b", "b", 89);
  }

  /** One version per good customer, with a technical key of its own; no bad customer. */
  private static void assertDimension(String table, String name, int badEvery) throws Exception {
    assertEquals(
        ROWS - ROWS / badEvery,
        count("SELECT count(*) FROM " + table + " WHERE tk > 0 AND name = '" + name + "'"));
    assertEquals(
        ROWS - ROWS / badEvery, count("SELECT count(DISTINCT tk) FROM " + table + " WHERE tk > 0"));
    assertEquals(
        0, count("SELECT count(*) FROM " + table + " WHERE cust_id % " + badEvery + " = 0"));
  }

  /**
   * A Dimension lookup/update without an error hop shares the connection with one that has an error
   * hop: none of its rows may be undone by the other transform's rollbacks.
   */
  @Test
  void aDimensionWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createDimension("d_tx_err");
    createDimension("d_tx_plain");
    List<Object[]> errors = syncList();
    List<Object[]> plainOutput = syncList();

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(
        dimension(
            "with error hop", "d_tx_err", 0, 100, customers(ROWS, "a", 13), syncList(), errors));
    DimensionLookup plain =
        dimension(
            "without error hop",
            "d_tx_plain",
            0,
            100,
            customers(ROWS, "p", Integer.MAX_VALUE),
            plainOutput,
            syncList());
    plain.getTransformMeta().setTransformErrorMeta(null);
    run.join(plain);
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 13, errors.size());
    assertEquals(ROWS, plainOutput.size());
    assertEquals(ROWS, count("SELECT count(*) FROM d_tx_plain WHERE tk > 0 AND name = 'p'"));
    assertDimension("d_tx_err", "a", 13);
  }
}
