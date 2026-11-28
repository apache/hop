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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Locale;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * How long Synchronize after merge takes on PostgreSQL with an error hop and a few bad rows:
 * recovered batches, by PostgreSQL's validated contract, against the savepoint per row that
 * HOP_DATABASE_BATCH_RECOVERY=LEGACY gives (with batch updates on: the statements run at once,
 * behind their savepoint, and the rows wait for the commit as if they were batched). 100,000 rows,
 * a third each updates, inserts and deletes, commit size 1,000, ten bad rows spread over the three
 * statements.
 *
 * <p>Not part of a build: run it with {@code -Dhop.batch.benchmark=true}, for example {@code
 * -Dtest=SynchronizeAfterMergeBatchBenchmarkPostgresIT -Dhop.batch.benchmark=true}.
 */
@EnabledIfSystemProperty(named = "hop.batch.benchmark", matches = "true")
class SynchronizeAfterMergeBatchBenchmarkPostgresIT {

  private static final int ROWS = 100_000;
  private static final int COMMIT_SIZE = 1_000;
  private static final long[] BAD = {
    1, 2, 3, 25_000, 25_001, 50_000, 75_001, 99_998, 99_999, 100_000
  };

  @BeforeAll
  static void setUpOnce() throws Exception {
    SynchronizeAfterMergeBatchBehaviourPostgresIT.setUpOnce();
  }

  @AfterAll
  static void tearDownOnce() {
    SynchronizeAfterMergeBatchBehaviourPostgresIT.tearDownOnce();
  }

  /**
   * The table of SynchronizeAfterMergeRecoveredBatchH2Test.cycle(ROWS, BAD), created anew with set
   * based SQL: the updates, the deletes and the bad inserts are in it, the bad deletes have a
   * child.
   */
  static DatabaseMeta target() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourPostgresIT.databaseMeta("POSTGRESQL");
    StringBuilder badInserts = new StringBuilder("-1");
    StringBuilder badDeletes = new StringBuilder("-1");
    for (long id : BAD) {
      if (id % 3 == 2) {
        badInserts.append(", ").append(id);
      } else if (id % 3 == 0) {
        badDeletes.append(", ").append(id);
      }
    }
    SynchronizeAfterMergeRun.execute(
        db,
        "DROP TABLE IF EXISTS target_child",
        "DROP TABLE IF EXISTS target",
        SynchronizeAfterMergeRun.createTable("target"),
        SynchronizeAfterMergeRun.createChildTable("target"),
        "INSERT INTO target (id, name) SELECT g, 'x' FROM generate_series(1, "
            + ROWS
            + ") g WHERE g % 3 <> 2 OR g IN ("
            + badInserts
            + ")",
        "INSERT INTO target_child (id, parent) SELECT g, g FROM generate_series(1, "
            + ROWS
            + ") g WHERE g IN ("
            + badDeletes
            + ")");
    return db;
  }

  private static long run(String recovery, boolean batch) throws Exception {
    DatabaseMeta db = target();
    long start = System.nanoTime();
    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(batch)
            .errorHop(true)
            .commitSize(COMMIT_SIZE)
            .run(SynchronizeAfterMergeRecoveredBatchH2Test.cycle(ROWS, BAD));
    long elapsed = (System.nanoTime() - start) / 1_000_000;
    System.out.printf(Locale.ROOT, "recovery %s, batch %s: %,d ms%n", recovery, batch, elapsed);

    assertEquals(0, r.errorCount);
    assertFalse(r.stopped);
    assertEquals(BAD.length, r.errors.size());
    assertEquals(ROWS - BAD.length, r.output.size());
    assertEquals(recovery == null && batch, r.recoveredBatches);
    List<String> count = SynchronizeAfterMergeRun.query(db, "SELECT COUNT(*) FROM target");
    assertEquals(
        List.of(
            Integer.toString(
                SynchronizeAfterMergeRecoveredBatchH2Test.cycleTable(ROWS, BAD).size())),
        count);
    return elapsed;
  }

  @Test
  void recoveredBatchesAgainstASavepointPerRow() throws Exception {
    // Warm up the JIT and the container
    run(null, true);

    long recovered = run(null, true);
    long savepoints = run("LEGACY", true);

    System.out.printf(
        Locale.ROOT,
        "Synchronize after merge on PostgreSQL, %,d rows, commit size %,d, %d bad rows, error hop:%n"
            + "  recovered batches:                 %,9d ms  %,9.0f rows/s%n"
            + "  LEGACY, a savepoint per row:       %,9d ms  %,9.0f rows/s%n",
        ROWS,
        COMMIT_SIZE,
        BAD.length,
        recovered,
        ROWS * 1000.0 / recovered,
        savepoints,
        ROWS * 1000.0 / savepoints);
    assertTrue(recovered > 0);
  }
}
