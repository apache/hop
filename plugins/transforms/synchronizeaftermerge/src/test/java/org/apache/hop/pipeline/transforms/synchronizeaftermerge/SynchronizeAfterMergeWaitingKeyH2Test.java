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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.counters;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.DELETE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.INSERT;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.UPDATE;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.delete;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.in;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.insert;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.named;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.rows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBigNumber;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * On recovered batches, a row whose key waits in a batch of another statement of the same table
 * makes the pending batches run first, so that the statements of one key run in input order: the
 * key compared the way a database might (see PendingKeys), the key an INSERT writes, the key an
 * UPDATE gives a row, and a key condition that is not an equality, which can match any row.
 *
 * <p>The table {@code target} runs its batches update, delete, insert at the end of the input, so
 * without the waiting key a delete of a key that an insert before it writes runs first.
 */
class SynchronizeAfterMergeWaitingKeyH2Test {

  static final String WAITS = "waits in a batch";

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  static DatabaseMeta target(long... ids) throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.createTarget(db, SynchronizeAfterMergeRun.TARGET, ids);
    return db;
  }

  static SynchronizeAfterMergeRun recovered(DatabaseMeta db) {
    return SynchronizeAfterMergeRun.on(db).batch(true).errorHop(true).commitSize(100).debug();
  }

  /** The delete of 2 waits in the delete batch: the insert of 2 lets it run first. */
  @Test
  void aKeyWaitingInAnotherBatchMakesTheBatchesRunFirst() throws Exception {
    DatabaseMeta db = target(1, 2);

    Result r = recovered(db).run(rows(delete(2), named(insert(2), "a"), named(update(1), "b")));

    assertTrue(r.recoveredBatches);
    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of("1:b", "2:a"), SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errors.size());
  }

  /** Repeats within one statement need not wait: a batch runs in the order its rows were added. */
  @Test
  void repeatsWithinOneStatementDoNotWait() throws Exception {
    DatabaseMeta db = target(1, 2, 3);

    Result r =
        recovered(db)
            .run(
                rows(
                    named(update(2), "a"),
                    named(update(2), "b"),
                    delete(3),
                    delete(3),
                    named(insert(4), "a"),
                    named(insert(4), "b"),
                    named(update(1), "c")));

    assertEquals(0, r.debugLines(WAITS));
    assertEquals(List.of("1:c", "2:b", "4:a"), SynchronizeAfterMergeRun.table(db));
    // The second insert of 4 is refused, as row by row
    assertEquals(List.of("i4"), r.errorLabels());
  }

  /** id (Integer), name, op, tbl (unused), and one more field of the given type. */
  static IRowMeta rowMeta(IValueMeta extra) {
    IRowMeta rowMeta = SynchronizeAfterMergeRun.idNameOpTableRowMeta();
    rowMeta.addValueMeta(extra);
    return rowMeta;
  }

  /**
   * The INSERT writes the key column from a BigNumber field, the DELETE compares an Integer one:
   * 1.00 and 1 are the same key.
   */
  @Test
  void keysOfDifferentTypesAreTheSameKey() throws Exception {
    DatabaseMeta db = target(2);

    Result r =
        recovered(db)
            .configure(meta -> meta.getLookup().getValueUpdates().get(0).setFieldName("number"))
            .run(
                rowMeta(new ValueMetaBigNumber("number")),
                List.of(
                    new Object[] {1L, "a", INSERT, null, new BigDecimal("1.00")},
                    new Object[] {1L, "r1", DELETE, null, new BigDecimal("1")}));

    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db));
  }

  /**
   * On a column that ignores case the delete of AB deletes the insert of ab: differently cased text
   * is the same key.
   */
  @Test
  void differentlyCasedTextOnACaseInsensitiveColumn() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.execute(
        db,
        "CREATE TABLE target (id VARCHAR_IGNORECASE(20) NOT NULL PRIMARY KEY, name VARCHAR(20))");
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("op"));

    Result r =
        recovered(db)
            .run(
                rowMeta,
                List.of(new Object[] {"ab", "a", INSERT}, new Object[] {"AB", "-", DELETE}));

    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of(), SynchronizeAfterMergeRun.table(db));
    assertEquals(0, r.errors.size());
  }

  /** The same key in another table makes nothing wait; in the same table it does. */
  @Test
  void theSameKeyInAnotherTableDoesNotWait() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.createTarget(db, "sam_a", 1);
    SynchronizeAfterMergeRun.createTarget(db, "sam_b");

    Result r =
        recovered(db)
            .tableNameInField(true)
            .run(
                rows(
                    in("sam_a", delete(1)),
                    in("sam_b", named(insert(1), "b")),
                    in("sam_a", named(insert(1), "a"))));

    assertEquals(1, r.debugLines(WAITS));
    assertTrue(
        r.debugLog.stream()
            .filter(line -> line.contains(WAITS))
            .findFirst()
            .orElseThrow()
            .contains("sam_a"));
    assertEquals(List.of("1:a"), SynchronizeAfterMergeRun.table(db, "sam_a"));
    assertEquals(List.of("1:b"), SynchronizeAfterMergeRun.table(db, "sam_b"));
  }

  /**
   * A key condition that is not an equality can match any row a batch waits to write: the delete of
   * every id from 3 on waits for the insert of 5 before it, as row by row, and the update of every
   * id from 1 on waits for the delete.
   */
  @Test
  void aConditionThatIsNoEqualityMakesEveryOtherBatchRunFirst() throws Exception {
    DatabaseMeta db = target(1, 2, 3, 4);

    Result r =
        recovered(db)
            .configure(meta -> meta.getLookup().getKeyConditions().get(0).setCondition(">="))
            .run(rows(named(insert(5), "a"), delete(3), named(update(1), "b")));

    assertEquals(2, r.debugLines(WAITS));
    // The update of every id from 1 on, after the delete
    assertEquals(List.of("1:b", "2:b"), SynchronizeAfterMergeRun.table(db));
  }

  /**
   * An INSERT that leaves the key to the database has a key the transform can't tell: every later
   * row of another statement waits for it.
   */
  @Test
  void anInsertThatLeavesTheKeyToTheDatabase() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.execute(
        db,
        "CREATE TABLE target (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 100) PRIMARY"
            + " KEY, name VARCHAR(20))",
        "INSERT INTO target (id, name) VALUES (1, 'x'), (2, 'x')");

    Result r =
        recovered(db)
            .configure(meta -> meta.getLookup().getValueUpdates().remove(0))
            .run(rows(named(insert(7), "a"), named(insert(8), "b"), named(update(2), "c")));

    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of("1:x", "2:c", "100:a", "101:b"), SynchronizeAfterMergeRun.table(db));
  }

  /**
   * An UPDATE that sets the key column gives the row another key: here 1 becomes 5, after the
   * delete of 5 has run, as row by row.
   */
  @Test
  void anUpdateThatChangesTheKey() throws Exception {
    DatabaseMeta db = target(1, 2);

    Result r =
        recovered(db)
            .configure(
                meta -> {
                  SynchronizeAfterMergeMeta.ValueUpdate id =
                      meta.getLookup().getValueUpdates().get(0);
                  id.setFieldName("new_id");
                  id.setUpdate(true);
                })
            .run(
                rowMeta(new ValueMetaInteger("new_id")),
                List.of(
                    new Object[] {5L, "r5", DELETE, null, 5L},
                    new Object[] {1L, "b", UPDATE, null, 5L}));

    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of("2:x", "5:b"), SynchronizeAfterMergeRun.table(db));
  }

  /**
   * With "Perform lookup" a row is looked up first, and the lookup can't see the rows in the
   * batches: an update of a key that waits in the update batch makes it run first, so that the
   * lookup compares with the name the first update set, as row by row.
   */
  @Test
  void aLookedUpRowWaitsForItsOwnStatementToo() throws Exception {
    DatabaseMeta db = target(1, 2);

    Result r =
        recovered(db)
            .lookup(true)
            .run(rows(named(update(2), "a"), named(update(2), SynchronizeAfterMergeRun.SEED)));

    assertEquals(1, r.debugLines(WAITS));
    assertEquals(List.of("1:x", "2:x"), SynchronizeAfterMergeRun.table(db));
    // Both are updated; looked up against the table before the first ran, the second would be
    // skipped and 2 would keep the name a
    assertEquals(counters(2, 0, 2, 0, 2, 0), r.counters());
  }

  /**
   * HOP_DATABASE_BATCH_RECOVERY=LEGACY and "As before" on the connection keep the order of the
   * batches as it was: the delete of 1 runs before its insert, and 1 stays.
   */
  @ParameterizedTest(name = "recovery {0}, contract on the connection {1}")
  @CsvSource(value = {"LEGACY, ", " , UNKNOWN"})
  void withoutAValidatedContractTheOrderOfTheBatchesIsAsBefore(String recovery, String contract)
      throws Exception {
    DatabaseMeta db = target(2);
    if (contract != null) {
      db.setBatchFailureContractOverride(contract);
    }

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .debug()
            .run(rows(named(insert(1), "a"), delete(1)));

    assertFalse(r.recoveredBatches);
    assertEquals(0, r.debugLines(WAITS));
    assertEquals(List.of("1:a", "2:x"), SynchronizeAfterMergeRun.table(db));
  }
}
