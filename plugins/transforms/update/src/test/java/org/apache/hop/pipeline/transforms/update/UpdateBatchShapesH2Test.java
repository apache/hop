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

package org.apache.hop.pipeline.transforms.update;

import static org.apache.hop.pipeline.transforms.update.UpdateRun.BATCH;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.DRIVER;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaPlugin;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.update.SimulatedBatchDriver.Shape;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Pins how Update works out the rows of a failed batch from the shape of the update counts, the
 * same way for every driver: one count per row with {@code EXECUTE_FAILED} at the failed rows (H2
 * itself), fewer counts than rows (a driver that stops at the failed row), and no counts at all.
 *
 * <p>The last two shapes come from {@link SimulatedBatchDriver} in front of H2, which really runs
 * the rows one by one up to the failed one, so the table holds what such a driver leaves behind.
 *
 * <p>The dialect is a subclass of H2. It declares no batch failure contract of its own, as no
 * dialect does until its driver has been measured, so a failed batch is handled the way it always
 * was.
 */
class UpdateBatchShapesH2Test {

  /** H2 as a dialect whose driver has not been measured: it declares no batch failure contract. */
  @DatabaseMetaPlugin(type = "H2_UPDATE_UNVALIDATED", typeDescription = "H2, not measured")
  public static class UnvalidatedH2DatabaseMeta extends H2DatabaseMeta {}

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(UnvalidatedH2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  private static DatabaseMeta target(int n) throws Exception {
    return UpdateBatchBehaviourH2Test.target("H2_UPDATE_UNVALIDATED", n);
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  /** One count per row, EXECUTE_FAILED at the failed rows: only they are rejected. */
  @Test
  void marksFailedRows_onlyTheFailedRowsAreRejected() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertTrue(r.batchMode);
    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD002", "UPD002"), r.errorCodes());
    assertEquals(List.of(DRIVER, DRIVER), r.errorKinds());
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
  }

  /**
   * Fewer counts than rows: the row after the counted ones is rejected and the rows after it, which
   * the driver never ran, are sent again as a new batch.
   */
  @Test
  void keepsPrefix_theRowsAfterTheFailedOneAreSentAgain() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .driver(Shape.KEEPS_PREFIX)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertEquals(idsWithout(10, 3, 7), r.outputKeys());
    assertEquals(ids(3, 7), r.errorKeys());
    assertEquals(List.of("UPD002", "UPD002"), r.errorCodes());
    assertEquals(List.of(BATCH, BATCH), r.errorKinds());
    assertEquals(List.of(0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 8), r.outputSizeAtRead);
    assertEquals(tableUpdatedExcept(10, 3, 7), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(10, 8, 0, 8, 2), r.counters());
  }

  /** The new batch fails too, twice: each failure is sorted out the same way. */
  @Test
  void keepsPrefix_aRetryThatFailsIsSortedOutTheSameWay() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(10)
            .driver(Shape.KEEPS_PREFIX)
            .run(rows(10, 2, 5, 9));

    assertEquals(idsWithout(10, 2, 5, 9), r.outputKeys());
    assertEquals(ids(2, 5, 9), r.errorKeys());
    assertEquals(tableUpdatedExcept(10, 2, 5, 9), UpdateRun.table(db));
    assertEquals(counters(10, 7, 0, 7, 3), r.counters());
  }

  /** A failure on the first row gives a zero-length counts array. */
  @Test
  void keepsPrefix_aFailureOnTheFirstRow() throws Exception {
    DatabaseMeta db = target(5);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .driver(Shape.KEEPS_PREFIX)
            .run(rows(5, 1));

    assertEquals(ids(2, 3, 4, 5), r.outputKeys());
    assertEquals(ids(1), r.errorKeys());
    assertEquals(tableUpdatedExcept(5, 1), UpdateRun.table(db));
  }

  @Test
  void keepsPrefix_everyRowFails() throws Exception {
    DatabaseMeta db = target(5);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .driver(Shape.KEEPS_PREFIX)
            .run(rows(5, 1, 2, 3, 4, 5));

    assertEquals(ids(), r.outputKeys());
    assertEquals(ids(1, 2, 3, 4, 5), r.errorKeys());
    assertEquals(tableWith(5, id -> false), UpdateRun.table(db));
    assertEquals(counters(5, 0, 0, 0, 5), r.counters());
  }

  /**
   * Buggy, see #9722: a driver that gives no counts. Nothing says which rows were applied, so every
   * row of the batch goes to the error hop. But the driver applied the rows before the failed one,
   * 1, 2 and 6, and the transform commits them: they are in the table, and on the error hop. The
   * rows after the failed one (4, 5, 8, 9, 10) were never sent, and are fine rows that are not
   * updated either.
   */
  @Test
  void noCounts_Buggy_everyRowGoesToTheErrorHopAlthoughSomeAreCommitted() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(5)
            .driver(Shape.PREFIX_WITHOUT_COUNTS)
            .watchCommits()
            .run(rows(10, 3, 7));

    assertEquals(ids(), r.outputKeys());
    assertEquals(idsWithout(10), r.errorKeys());
    assertEquals(Collections.nCopies(10, "UPD002"), r.errorCodes());
    assertEquals(Collections.nCopies(10, BATCH), r.errorKinds());
    assertEquals(tableWith(10, id -> id == 1 || id == 2 || id == 6), UpdateRun.table(db));
    // What the table held for each row as it went to the error hop
    assertEquals(List.of("r1", "r2", "x", "x", "x", "r6", "x", "x", "x", "x"), r.committedAtError);
    assertEquals(counters(10, 0, 0, 0, 10), r.counters());
  }

  /** A batch without failures is the same for every shape. */
  @ParameterizedTest
  @EnumSource(Shape.class)
  void aBatchWithoutFailuresPassesEveryRowOn(Shape shape) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(4)
            .driver(shape)
            .watchCommits()
            .run(rows(10));

    assertEquals(idsWithout(10), r.outputKeys());
    assertEquals(ids(), r.errorKeys());
    assertEquals(tableUpdatedExcept(10), UpdateRun.table(db));
    assertEquals(ids(), r.passedOnBeforeTheirCommit());
    assertEquals(counters(10, 10, 0, 10, 0), r.counters());
  }

  /**
   * Without an error hop the shape doesn't matter: the failed batch is rolled back, with the rows
   * the driver applied before the failed one, and the transform stops.
   */
  @ParameterizedTest
  @EnumSource(Shape.class)
  void noErrorHop_aFailedBatchIsRolledBackWhateverTheShape(Shape shape) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).driver(shape).run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertFalse(r.errorLog.isEmpty());
  }
}
