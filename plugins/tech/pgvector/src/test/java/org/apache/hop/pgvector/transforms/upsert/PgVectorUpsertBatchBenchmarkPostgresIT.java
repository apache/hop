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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pgvector.transforms.upsert.PgVectorUpsertRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Times the pgvector upsert on PostgreSQL with an error hop and a few bad rows: recovered batches
 * (the default since #9768) against a savepoint per row (HOP_DATABASE_BATCH_RECOVERY=LEGACY), with
 * plain batches without an error hop and without bad rows as the reference. Only runs with {@code
 * -Dhop.batch.benchmark=true}; the size is set with {@code hop.batch.benchmark.rows}, {@code
 * hop.batch.benchmark.commitSize} and {@code hop.batch.benchmark.dimensions}.
 */
@EnabledIfSystemProperty(named = "hop.batch.benchmark", matches = "true")
class PgVectorUpsertBatchBenchmarkPostgresIT {

  private static final String TABLE = "chunks_benchmark";
  private static final int ROWS = Integer.getInteger("hop.batch.benchmark.rows", 100_000);
  private static final int COMMIT_SIZE =
      Integer.getInteger("hop.batch.benchmark.commitSize", 1_000);
  private static final int DIMENSIONS = Integer.getInteger("hop.batch.benchmark.dimensions", 32);
  private static final int BAD_ROWS = 10;

  private static PostgreSQLContainer<?> container;
  private static DatabaseMeta db;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = PgVectorUpsertBatchBehaviourPostgresIT.startPgvector();
    db = PgVectorUpsertBatchBehaviourPostgresIT.databaseMeta(container);
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  @Test
  void recoveredBatchesAgainstASavepointPerRow() throws Exception {
    List<Object[]> good = rows(ROWS, false);
    List<Object[]> withBadRows = rows(ROWS, true);

    // Warm up the JVM and the database on a tenth of the rows
    List<Object[]> warmUp = rows(ROWS / 10, true);
    run("LEGACY", true, warmUp);
    run(null, true, warmUp);
    run(null, false, rows(ROWS / 10, false));

    long savepoints = run("LEGACY", true, withBadRows);
    long recovered = run(null, true, withBadRows);
    long plain = run(null, false, good);

    System.out.printf(
        "pgvector upsert, %d rows of %d dimensions, commit size %d, %d bad rows:%n"
            + "  error hop, a savepoint per row (LEGACY): %6d ms, %8.0f rows/s%n"
            + "  error hop, recovered batches (default):  %6d ms, %8.0f rows/s%n"
            + "  no error hop, no bad rows, batches:       %6d ms, %8.0f rows/s%n",
        ROWS,
        DIMENSIONS,
        COMMIT_SIZE,
        BAD_ROWS,
        savepoints,
        ROWS * 1000.0 / savepoints,
        recovered,
        ROWS * 1000.0 / recovered,
        plain,
        ROWS * 1000.0 / plain);
  }

  /**
   * @return how long the run took, in milliseconds
   */
  private static long run(String recovery, boolean errorHop, List<Object[]> rows) throws Exception {
    PgVectorUpsertRun.execute(
        db,
        "DROP TABLE IF EXISTS " + TABLE,
        "CREATE TABLE "
            + TABLE
            + " (id TEXT PRIMARY KEY, document_id TEXT, chunk_index INTEGER, content TEXT NOT NULL,"
            + " embedding vector("
            + DIMENSIONS
            + "))");
    long start = System.nanoTime();
    Result r =
        PgVectorUpsertRun.on(db, TABLE)
            .recovery(recovery)
            .errorHop(errorHop)
            .commitSize(COMMIT_SIZE)
            .discardRows()
            .run(rows);
    long elapsed = (System.nanoTime() - start) / 1_000_000;

    int bad = errorHop ? badRows(rows.size()) : 0;
    assertEquals(0, r.errorCount);
    assertEquals(rows.size() - bad, r.linesWritten);
    assertEquals(bad, r.linesRejected);
    return elapsed;
  }

  /** How many of the given number of rows {@link #rows} makes bad. */
  private static int badRows(int count) {
    return Math.min(BAD_ROWS, count);
  }

  /**
   * Chunks 0..count-1 of 100 rows per document, BAD_ROWS of them spread over the run with a vector
   * of one dimension too many.
   */
  private static List<Object[]> rows(int count, boolean withBadRows) {
    StringBuilder good = new StringBuilder("[");
    for (int i = 0; i < DIMENSIONS; i++) {
      good.append(i == 0 ? "" : ",").append(0.001f * (i + 1));
    }
    String embedding = good + "]";
    String wrong = good + ",1]";
    int every = count / badRows(count);
    List<Object[]> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      boolean bad = withBadRows && i % every == every / 2;
      rows.add(
          PgVectorUpsertRun.row(
              "doc-" + (i / 100), i % 100, "chunk " + i, bad ? wrong : embedding));
    }
    return rows;
  }
}
