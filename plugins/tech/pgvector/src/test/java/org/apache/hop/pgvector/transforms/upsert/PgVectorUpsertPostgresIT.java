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

package org.apache.hop.pgvector.transforms.upsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
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
import org.apache.hop.metadata.api.IHopMetadataProvider;
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
import org.testcontainers.utility.DockerImageName;

/**
 * PgVector upsert with an error hop on PostgreSQL, where a failed statement aborts the transaction:
 * only the rows whose embedding has the wrong number of dimensions may go to the error hop, the
 * chunks around them must still be stored. Also in a transactional pipeline, where several
 * transforms share one connection.
 */
class PgVectorUpsertPostgresIT {

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
    container =
        new PostgreSQLContainer<>(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"));
    container.start();
    execute("CREATE EXTENSION IF NOT EXISTS vector");
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

  private static void execute(String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta())) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }

  private static long count(String sql) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta())) {
      db.connect();
      return (Long) db.getRows(sql, -1).get(0)[0];
    }
  }

  /**
   * Created here rather than by the transform: its schema options switch the connection to
   * autocommit for the DDL, which is not what these tests are about.
   */
  private static void createTable(String table) throws Exception {
    execute(
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE "
            + table
            + " (id TEXT PRIMARY KEY, document_id TEXT, chunk_index INTEGER,"
            + " content TEXT NOT NULL, embedding vector(2))");
  }

  private static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("document_id"));
    rowMeta.addValueMeta(new ValueMetaInteger("chunk_index"));
    rowMeta.addValueMeta(new ValueMetaString("chunk_text"));
    rowMeta.addValueMeta(new ValueMetaString("embedding"));
    return rowMeta;
  }

  /** Chunks 1..rows of one document; every badEvery-th has three dimensions instead of two. */
  private static Queue<Object[]> rows(int rows, String text, int badEvery) {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long chunk = 1; chunk <= rows; chunk++) {
      String embedding = chunk % badEvery == 0 ? "[1,2,3]" : "[1,2]";
      input.add(new Object[] {"doc", chunk, text, embedding});
    }
    return input;
  }

  /** An upsert into table, with an error hop when errors isn't null. */
  private static PgVectorUpsert upsert(
      String name, String table, int commitSize, Queue<Object[]> input, List<Object[]> errors)
      throws Exception {
    return upsert(name, table, commitSize, input::poll, errors, false);
  }

  /**
   * An upsert into table, with an error hop when errors isn't null, that creates the table and an
   * HNSW index when createSchema is set.
   */
  private static PgVectorUpsert upsert(
      String name,
      String table,
      int commitSize,
      Supplier<Object[]> input,
      List<Object[]> errors,
      boolean createSchema)
      throws Exception {
    IRowMeta inputRowMeta = inputRowMeta();
    PipelineMeta pipelineMeta = new PipelineMeta();
    // Not named metadataProvider: inside the transform that name is BaseTransform's own field.
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta());
    pipelineMeta.setMetadataProvider(provider);

    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setConnection(CONNECTION);
    meta.setSchemaName("public");
    meta.setTableName(table);
    meta.setEmbeddingDimensions("2");
    meta.setCreateTableIfMissing(createSchema);
    meta.setCreateHnswIndex(createSchema);
    meta.setCommitSize(Integer.toString(commitSize));
    TransformMeta transformMeta = new TransformMeta(name, meta);
    pipelineMeta.addTransform(transformMeta);

    PgVectorUpsert transform =
        new PgVectorUpsert(
            transformMeta,
            meta,
            new PgVectorUpsertData(),
            0,
            pipelineMeta,
            new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public IHopMetadataProvider getMetadataProvider() {
            return provider;
          }

          @Override
          public Object[] getRow() {
            Object[] row = input.get();
            return row == null ? null : row.clone();
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            // passed on, not needed here
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
      TransformMeta target = new TransformMeta(name + " errors", new PgVectorUpsertMeta());
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
    createTable("vec_single");
    List<Object[]> errors = new ArrayList<>();
    PgVectorUpsert transform =
        upsert("upsert", "vec_single", commitSize, rows(ROWS, "a", 97), errors);

    assertTrue(transform.init());
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }

    assertEquals(0, transform.getErrors(), "the transform failed");
    assertEquals(ROWS / 97, errors.size());
    assertEquals(ROWS - ROWS / 97, count("SELECT count(*) FROM vec_single"));
  }

  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {100, 1})
  void twoUpsertsWithErrorHopsInATransactionalPipeline(int commitSize) throws Exception {
    createTable("vec_tx_a");
    createTable("vec_tx_b");
    List<Object[]> errorsA = Collections.synchronizedList(new ArrayList<>());
    List<Object[]> errorsB = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(upsert("upsert A", "vec_tx_a", commitSize, rows(ROWS, "a", 97), errorsA));
    run.join(upsert("upsert B", "vec_tx_b", commitSize, rows(ROWS, "b", 89), errorsB));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 97, errorsA.size());
    assertEquals(ROWS / 89, errorsB.size());
    assertEquals(ROWS - ROWS / 97, count("SELECT count(*) FROM vec_tx_a WHERE content = 'a'"));
    assertEquals(ROWS - ROWS / 89, count("SELECT count(*) FROM vec_tx_b WHERE content = 'b'"));
  }

  /** One without an error hop (it batches) next to one with: none of its chunks may be undone. */
  @Test
  void anUpsertWithoutErrorHopNextToOneWithAnErrorHop() throws Exception {
    createTable("vec_tx_err");
    createTable("vec_tx_plain");
    List<Object[]> errors = Collections.synchronizedList(new ArrayList<>());

    SharedConnectionRun run = new SharedConnectionRun();
    run.join(upsert("with error hop", "vec_tx_err", 100, rows(ROWS, "a", 13), errors));
    run.join(
        upsert("without error hop", "vec_tx_plain", 100, rows(ROWS, "p", Integer.MAX_VALUE), null));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(ROWS / 13, errors.size());
    assertEquals(ROWS - ROWS / 13, count("SELECT count(*) FROM vec_tx_err"));
    assertEquals(ROWS, count("SELECT count(*) FROM vec_tx_plain"));
  }

  /**
   * In a transactional pipeline the schema DDL runs in the pipeline's transaction. It used to
   * switch the shared connection to autocommit, which commits the open transaction: the rows the
   * other transforms had written so far were committed halfway through the run, so a later rollback
   * of the pipeline could no longer undo them.
   */
  @Test
  void theSchemaDdlDoesNotCommitTheTransactionOfAPipeline() throws Exception {
    createTable("vec_tx_writer");
    execute("DROP TABLE IF EXISTS vec_tx_ddl");
    CountDownLatch writerWrote = new CountDownLatch(1);
    CountDownLatch schemaCreated = new CountDownLatch(1);
    AtomicLong committedHalfway = new AtomicLong(-1);

    // The writer batches 20 rows into the transaction (commit size 10), then waits for the schema.
    AtomicInteger written = new AtomicInteger();
    Supplier<Object[]> writerInput =
        () -> {
          int chunk = written.incrementAndGet();
          if (chunk <= 20) {
            return new Object[] {"doc", (long) chunk, "w", "[1,2]"};
          }
          if (chunk == 21) {
            writerWrote.countDown();
            await(schemaCreated);
            try {
              committedHalfway.set(count("SELECT count(*) FROM vec_tx_writer"));
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          }
          return null;
        };

    // Its first row creates the table and index; asking for the second means that is done.
    AtomicInteger asked = new AtomicInteger();
    Supplier<Object[]> schemaInput =
        () -> {
          if (asked.incrementAndGet() == 1) {
            await(writerWrote);
            return new Object[] {"doc", 1L, "s", "[1,2]"};
          }
          schemaCreated.countDown();
          return null;
        };

    SharedConnectionRun run = new SharedConnectionRun().timeoutSeconds(60);
    run.join(upsert("writer", "vec_tx_writer", 10, writerInput, null, false));
    run.join(upsert("with schema", "vec_tx_ddl", 10, schemaInput, null, true));
    assertTrue(run.run(), "the transaction was rolled back");

    assertEquals(0, committedHalfway.get(), "the rows of the writer were committed halfway");
    assertEquals(20, count("SELECT count(*) FROM vec_tx_writer"));
    assertEquals(1, count("SELECT count(*) FROM vec_tx_ddl"));
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
