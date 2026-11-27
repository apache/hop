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

import static org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.assertPassedOnAfterTheCommit;
import static org.apache.hop.pipeline.transforms.delete.DeleteBatchBehaviourH2Test.assertRecovered;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.counters;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.ids;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.idsWithout;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.remaining;
import static org.apache.hop.pipeline.transforms.delete.DeleteRun.rows;
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
import org.apache.hop.pipeline.transforms.delete.DeleteRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Delete on embedded Derby, whose contract KEEPS_PREFIX is validated: the driver stops at the
 * failed row of a batch, keeps the rows before it and never runs the rows after it.
 */
class DeleteRecoveredBatchDerbyTest {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(DerbyDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta target(int n, long... referenced) throws Exception {
    String name = "memory:delete" + UUID.randomUUID().toString().replace("-", "") + ";create=true";
    DatabaseMeta databaseMeta = new DatabaseMeta("derby", "DERBY", "Native", "", name, "", "", "");
    DeleteRun.createTarget(databaseMeta, n, referenced);
    return databaseMeta;
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
    DatabaseMeta db = target(10, bad);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .watchCommits()
            .run(rows(10));

    assertFalse(r.stopped);
    assertEquals(0, r.errorCount);
    assertEquals(idsWithout(10, bad), r.outputKeys());
    assertEquals(ids(bad), r.errorKeys());
    assertEquals(Collections.nCopies(bad.length, "DEL002"), r.errorCodes());
    assertEquals(ids(bad), remaining(db));
    assertEquals(counters(10, 10 - bad.length, 10 - bad.length, bad.length), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @Test
  void aKeyThatCannotBeBoundKeepsItsPlace() throws Exception {
    DatabaseMeta db = target(10, 2, 7);

    Result r =
        DeleteRun.on(db)
            .batch(true)
            .errorHop(true)
            .commitSize(3)
            .watchCommits()
            .run(DeleteRun.rowsWithUnboundKeys(10, 4));

    assertEquals(idsWithout(10, 2, 4, 7), r.outputKeys());
    assertEquals(List.of(2L, "no number 4", 7L), r.errorKeys());
    assertEquals(List.of("DEL002", "DEL001", "DEL002"), r.errorCodes());
    assertEquals(ids(2, 4, 7), remaining(db));
    assertEquals(counters(10, 7, 7, 3), r.counters());
    assertPassedOnAfterTheCommit(r);
    assertRecovered(r);
  }

  @Test
  void withoutAnErrorHopAFailedBatchIsRolledBackAndStops() throws Exception {
    DatabaseMeta db = target(10, 7);

    Result r = DeleteRun.on(db).batch(true).errorHop(false).commitSize(4).run(rows(10));

    assertEquals(ids(1, 2, 3, 4), r.outputKeys());
    assertEquals(ids(5, 6, 7, 8, 9, 10), remaining(db));
    assertEquals(1, r.errorCount);
    assertTrue(r.stopped);
    assertTrue(r.recoveredBatches);
  }
}
