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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_DONE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_FAILED;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.MIXED_WRITTEN;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.mixedRows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.updates;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.createCycleTarget;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycle;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleCounters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleLabels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRecoveredBatchH2Test.cycleTable;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.labels;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.range;
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
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Synchronize after merge on embedded Derby, whose contract KEEPS_PREFIX is validated: the driver
 * stops at the failed row of a batch, keeps the rows before it and never runs the rows after it.
 */
class SynchronizeAfterMergeRecoveredBatchDerbyTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DerbyDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta database() {
    String name = "memory:sam" + UUID.randomUUID().toString().replace("-", "") + ";create=true";
    return new DatabaseMeta("derby", "DERBY", "Native", "", name, "", "", "");
  }

  static Stream<Arguments> failures() {
    List<Arguments> arguments = new ArrayList<>();
    for (int commitSize : new int[] {1, 3, 5, 100}) {
      for (long[] bad : SynchronizeAfterMergeRecoveredBatchH2Test.BAD_SETS) {
        arguments.add(Arguments.of(commitSize, bad));
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "commit size {0}, bad rows {1}")
  @MethodSource("failures")
  void everyRowReachesOneStreamInInputOrder(int commitSize, long[] bad) throws Exception {
    DatabaseMeta db = database();
    createCycleTarget(db, bad);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(cycle(bad));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(cycleLabels(false, bad), r.outputLabels());
    assertEquals(cycleLabels(true, bad), r.errorLabels());
    assertEquals(Collections.nCopies(bad.length, "SUYNC002"), r.errorCodes());
    assertEquals(cycleTable(bad), SynchronizeAfterMergeRun.table(db));
    assertEquals(cycleCounters(bad), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @ParameterizedTest(name = "commit size {0}")
  @ValueSource(ints = {1, 3, 100})
  void skippedRowsKeepTheirPlace(int commitSize) throws Exception {
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 6));
    SynchronizeAfterMergeRun.refer(db, SynchronizeAfterMergeRun.TARGET, 5);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(mixedRows());

    assertEquals(MIXED_WRITTEN, r.outputLabels());
    assertEquals(MIXED_FAILED, r.errorLabels());
    assertEquals(MIXED_DONE, SynchronizeAfterMergeRun.table(db));
    assertEquals(counters(0, 3, 3, 1, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = database();
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, range(1, 10));

    Result r =
        SynchronizeAfterMergeRun.on(db).batch(true).errorHop(false).commitSize(4).run(updates(9));

    assertEquals(labels("u1", "u2", "u3", "u4", "u5", "u6", "u7", "u8"), r.outputLabels());
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.recoveredBatches);
  }
}
