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

import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Delete with an error hop on PostgreSQL, timed: 100,000 rows at commit size 1,000 with a few rows
 * the foreign key refuses, once with recovered batches (the default since #9768) and once with a
 * savepoint per row (HOP_DATABASE_BATCH_RECOVERY=LEGACY, as before). Both must route the rows the
 * same; the times are printed.
 *
 * <p>Not part of a normal build: run it with {@code -Dhop.batch.benchmark=true}, for example {@code
 * ./mvnw -o -pl plugins/transforms/delete verify -Dhop.batch.benchmark=true
 * -Dit.test=DeleteBatchBenchmarkPostgresIT}.
 */
@EnabledIfSystemProperty(named = "hop.batch.benchmark", matches = "true")
class DeleteBatchBenchmarkPostgresIT {

  private static final int ROWS = 100_000;
  private static final int COMMIT_SIZE = 1_000;
  private static final long[] BAD = {17, 1_000, 12_345, 33_333, 50_001, 77_777, 99_999, 100_000};

  private static PostgreSQLContainer<?> container;

  @BeforeAll
  static void setUpOnce() throws Exception {
    assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is not available");
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(PostgreSqlDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
    container = new PostgreSQLContainer<>("postgres:16-alpine");
    container.start();
  }

  @AfterAll
  static void tearDownOnce() {
    if (container != null) {
      container.stop();
    }
  }

  /** One timed run: a fresh table, then the transform, with the given recovery setting. */
  private static long run(String recovery, int n, long... bad) throws Exception {
    DatabaseMeta db = DeleteRecoveredBatchPostgresIT.target(container, "POSTGRESQL", null, n, bad);
    long start = System.nanoTime();
    Result r =
        DeleteRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(true)
            .commitSize(COMMIT_SIZE)
            .run(rows(n));
    long millis = (System.nanoTime() - start) / 1_000_000;

    assertEquals(n - bad.length, r.output.size());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(ids(bad), remaining(db));
    assertEquals(n - bad.length, r.linesUpdated);
    assertEquals(recovery == null, r.recoveredBatches);
    return millis;
  }

  @Test
  void recoveredBatchesAgainstASavepointPerRow() throws Exception {
    // Warm up the JVM, the driver and the server on both paths
    run(null, 10_000, 17, 5_000);
    run("LEGACY", 10_000, 17, 5_000);

    List<String> lines = new ArrayList<>();
    for (int round = 1; round <= 2; round++) {
      long recovered = run(null, ROWS, BAD);
      long savepoints = run("LEGACY", ROWS, BAD);
      lines.add(
          String.format(
              Locale.ROOT,
              "round %d: recovered batches %,d ms (%,.0f rows/s), savepoint per row %,d ms"
                  + " (%,.0f rows/s), %.1fx",
              round,
              recovered,
              ROWS * 1000.0 / recovered,
              savepoints,
              ROWS * 1000.0 / savepoints,
              (double) savepoints / recovered));
    }
    System.out.println(
        "Delete on PostgreSQL with an error hop, "
            + ROWS
            + " rows, commit size "
            + COMMIT_SIZE
            + ", "
            + BAD.length
            + " refused rows");
    lines.forEach(System.out::println);
  }
}
