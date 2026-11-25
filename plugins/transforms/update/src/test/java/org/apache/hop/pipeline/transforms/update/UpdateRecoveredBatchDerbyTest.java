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

import static org.apache.hop.pipeline.transforms.update.UpdateRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.update.UpdateRecoveredBatchH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.ids;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.idsWithout;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.rows;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableUpdatedExcept;
import static org.apache.hop.pipeline.transforms.update.UpdateRun.tableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.derby.DerbyDatabaseMeta;
import org.apache.hop.pipeline.transforms.update.UpdateRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Update on embedded Derby, whose contract KEEPS_PREFIX is validated: the driver stops at the
 * failed row of a batch, keeps the rows before it and never runs the rows after it.
 */
class UpdateRecoveredBatchDerbyTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DerbyDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta target(int n) throws Exception {
    String name = "memory:update" + UUID.randomUUID().toString().replace("-", "") + ";create=true";
    DatabaseMeta databaseMeta = new DatabaseMeta("derby", "DERBY", "Native", "", name, "", "", "");
    UpdateRun.createTarget(databaseMeta, n);
    return databaseMeta;
  }

  private static List<Long> counters(
      long input, long updated, long skipped, long written, long rejected) {
    return UpdateBatchBehaviourH2Test.counters(input, updated, skipped, written, rejected);
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    long[][] badSets = {{}, {1}, {5}, {6}, {10}, {3, 4}, {2, 7}, {1, 2, 3, 4, 5}, {1, 5, 6, 10}};
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : badSets) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10, bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "UPD002"), r.errorCodes());
    assertEquals(tableUpdatedExcept(10, bad), UpdateRun.table(db));
    assertEquals(counters(10, 10 - bad.length, 0, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @Test
  void rowsOutsideTheBatchKeepTheirPlace() throws Exception {
    DatabaseMeta db = target(10);

    Result r =
        UpdateRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(UpdateBatchBehaviourH2Test.mixedRows());

    assertEquals(ids(1, 2, 3, 4, 6), r.outputKeys());
    assertEquals(ids(11, 12, 5), r.errorKeys());
    assertEquals(tableWith(10, id -> id == 1 || id == 3 || id == 4), UpdateRun.table(db));
    assertEquals(counters(8, 3, 2, 5, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10);

    Result r = UpdateRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10, 7));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(tableWith(10, id -> id <= 4), UpdateRun.table(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.recoveredBatches);
  }
}
