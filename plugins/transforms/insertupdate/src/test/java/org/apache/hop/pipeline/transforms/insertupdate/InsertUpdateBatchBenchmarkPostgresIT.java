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

package org.apache.hop.pipeline.transforms.insertupdate;

import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRecoveredBatchH2Test.countersFor;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.ids;
import static org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.rows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.postgresql.PostgreSqlDatabaseMeta;
import org.apache.hop.pipeline.transforms.insertupdate.InsertUpdateRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Insert/Update on PostgreSQL with an error hop: today's savepoint per row against the recovered
 * batches of "Use batch updates". 100,000 rows, half of them in the table and updated, the other
 * half inserted, commit size 1,000, a few rows the table refuses among both. Prints the times;
 * asserts that both give the same streams, counters and table.
 *
 * <p>Not part of the build: run it with {@code -Dhop.batch.benchmark=true}, for example {@code
 * -Dit.test=InsertUpdateBatchBenchmarkPostgresIT -Dhop.batch.benchmark=true verify}.
 */
@EnabledIfSystemProperty(named = "hop.batch.benchmark", matches = "true")
class InsertUpdateBatchBenchmarkPostgresIT {

  private static final int ROWS = 100_000;
  private static final int COMMIT_SIZE = 1_000;
  private static final long[] BAD = {1, 2, 999, 1_000, 1_001, 33_333, 50_000, 77_777, 99_999};

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

  private static long run(String label, boolean batch, List<List<String>> tables, Result[] result)
      throws Exception {
    DatabaseMeta db = InsertUpdateBatchBehaviourPostgresIT.target(container, "POSTGRESQL", ROWS);
    long start = System.nanoTime();
    Result r =
        InsertUpdateRun.on(db)
            .batch(batch)
            .errorHop(true)
            .commitSize(COMMIT_SIZE)
            .run(rows(ROWS, BAD));
    long millis = (System.nanoTime() - start) / 1_000_000;
    System.out.printf(
        Locale.ROOT,
        "Insert/Update benchmark, %s: %,d rows (%,d inserts, %,d updates, %d refused), commit size"
            + " %,d: %,d ms, %,.0f rows/s%n",
        label,
        ROWS,
        r.linesOutput,
        r.linesUpdated,
        r.linesRejected,
        COMMIT_SIZE,
        millis,
        ROWS * 1000.0 / Math.max(1, millis));
    tables.add(InsertUpdateRun.table(db));
    result[0] = r;
    return millis;
  }

  @Test
  void recoveredBatchesAgainstASavepointPerRow() throws Exception {
    List<List<String>> tables = new ArrayList<>();
    Result[] rowByRow = new Result[1];
    Result[] batches = new Result[1];

    long rowByRowMillis = run("savepoint per row", false, tables, rowByRow);
    long batchMillis = run("recovered batches", true, tables, batches);

    assertTrue(rowByRow[0].useSavepoints);
    assertFalse(rowByRow[0].recoveredBatches);
    assertTrue(batches[0].recoveredBatches);
    assertFalse(batches[0].useSavepoints);
    for (Result r : new Result[] {rowByRow[0], batches[0]}) {
      assertEquals(countersFor(ROWS, BAD), r.counters());
      assertEquals(ids(BAD), r.errorKeys());
      assertEquals(ROWS - BAD.length, r.output.size());
    }
    assertEquals(rowByRow[0].outputKeys(), batches[0].outputKeys());
    assertEquals(tables.get(0), tables.get(1));
    System.out.printf(
        Locale.ROOT,
        "Insert/Update benchmark: recovered batches take %.1f%% of the time of a savepoint per"
            + " row (%.1fx faster)%n",
        100.0 * batchMillis / Math.max(1, rowByRowMillis),
        (double) rowByRowMillis / Math.max(1, batchMillis));
  }
}
