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

import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.delete;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.in;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.insert;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.named;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.rows;
import static org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeBatchBehaviourH2Test.H2OracleFlagsDatabaseMeta;
import org.apache.hop.pipeline.transforms.synchronizeaftermerge.SynchronizeAfterMergeRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins the order in which Synchronize after merge runs its INSERT, UPDATE and DELETE batches today,
 * with inputs where that order decides what the table holds: two rows of one key within one commit
 * window. Key 1 is not in the table, key 2 is, named x; the rows name it a and b.
 *
 * <p>Buggy: in batch mode the order is not the input order and is not fixed either. Within a commit
 * window the batches run at the end of the input in the order in which a Hashtable lists "table
 * name + statement", which depends on the name of the table: for {@code target} that is update,
 * delete, insert; for {@code t} insert, delete, update. And when the database counts {@code commit
 * size} rows added to the batches, it runs the batch of the row that makes the count before the
 * others: with commit size 2 that is the batch of the second row.
 *
 * <p>With an error hop H2 switches batching off, and the rows run one by one in input order.
 *
 * <p>Since #9768 H2's validated batch failure contract recovers failed batches, and keeps batching
 * on with an error hop. Before a row's statement joins a batch, the batches run first when its key
 * waits in a batch of another statement of the same table: so on the default path the statements of
 * one key run in input order, with and without an error hop, and a row the database refuses goes to
 * the error hop while the others are written. HOP_DATABASE_BATCH_RECOVERY=LEGACY keeps the order of
 * the batches as it was.
 */
class SynchronizeAfterMergeStatementOrderH2Test {

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(H2OracleFlagsDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** Two rows of one key, by name. */
  static final Map<String, List<Object[]>> CASES =
      Map.of(
          "insert then delete", rows(named(insert(1), "a"), delete(1)),
          "delete then insert", rows(delete(2), named(insert(2), "a")),
          "insert then update", rows(named(insert(1), "a"), named(update(1), "b")),
          "update then delete", rows(named(update(2), "b"), delete(2)),
          "update twice", rows(named(update(2), "a"), named(update(2), "b")),
          "insert twice", rows(named(insert(1), "a"), named(insert(1), "b")),
          "delete twice", rows(delete(2), delete(2)),
          "update then insert", rows(named(update(1), "b"), named(insert(1), "a")),
          "insert, update, delete", rows(named(insert(1), "a"), named(update(1), "b"), delete(1)));

  /** What the table holds when the rows run in input order, one by one. */
  static final Map<String, List<String>> IN_INPUT_ORDER =
      Map.of(
          "insert then delete", List.of("2:x"),
          "delete then insert", List.of("2:a"),
          "insert then update", List.of("1:b", "2:x"),
          "update then delete", List.of(),
          "update twice", List.of("2:b"),
          // The second insert is refused
          "insert twice", List.of("1:a", "2:x"),
          "delete twice", List.of(),
          // The update finds no row yet
          "update then insert", List.of("1:a", "2:x"),
          "insert, update, delete", List.of("2:x"));

  /** Marks a run that fails on a duplicate key: the table is left as it was. */
  static final List<String> FAILS = List.of("fails");

  /**
   * What the table holds after a batched run without an error hop as it was, by table and commit
   * size. The batches of {@code target} run update, delete, insert; those of {@code t} insert,
   * delete, update; with commit size 2 the batch of the second row runs first.
   */
  static List<String> batched(String table, int commitSize, String name) {
    if (commitSize == 2) {
      return switch (name) {
        case "insert then delete", "insert then update" -> List.of("1:a", "2:x");
        case "delete then insert", "insert twice" -> FAILS;
        case "update twice" -> List.of("2:b");
        case "update then insert" -> List.of("1:b", "2:x");
        case "insert, update, delete" ->
            table.equals("target") ? List.of("1:a", "2:x") : List.of("2:x");
        default -> List.of();
      };
    }
    if (table.equals("target")) {
      return switch (name) {
        case "insert then delete",
                "insert then update",
                "update then insert",
                "insert, update, delete" ->
            List.of("1:a", "2:x");
        case "delete then insert" -> List.of("2:a");
        case "insert twice" -> FAILS;
        case "update twice" -> List.of("2:b");
        default -> List.of();
      };
    }
    return switch (name) {
      case "insert then delete", "insert, update, delete" -> List.of("2:x");
      case "insert then update", "update then insert" -> List.of("1:b", "2:x");
      case "delete then insert", "insert twice" -> FAILS;
      case "update twice" -> List.of("2:b");
      default -> List.of();
    };
  }

  /**
   * What the table holds on recovered batches: the statements of one key run in input order. With
   * an error hop the second insert of a key is refused and goes to the error hop; without one it
   * fails the batch, and the transform.
   */
  static List<String> inInputOrder(String name, boolean errorHop) {
    if (!errorHop && name.equals("insert twice")) {
      return FAILS;
    }
    return IN_INPUT_ORDER.get(name);
  }

  static Stream<Arguments> batchedCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (String table : new String[] {"target", "t"}) {
        for (int commitSize : new int[] {100, 2}) {
          for (String name : CASES.keySet().stream().sorted().toList()) {
            arguments.add(
                Arguments.of(recovery, table, commitSize, name, batched(table, commitSize, name)));
          }
        }
      }
    }
    return arguments.stream();
  }

  static DatabaseMeta target(String type, String table) throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database(type);
    SynchronizeAfterMergeRun.createTarget(db, table, 2);
    return db;
  }

  static Stream<Arguments> legacyBatchedCases() {
    return batchedCases().filter(arguments -> "LEGACY".equals(arguments.get()[0]));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: batched, without an error hop,
   * the statements of one key run in the order of their batches, which depends on the table name
   * and the commit size, not on the input.
   */
  @ParameterizedTest(name = "recovery {0}, table {1}, commit size {2}: {3}")
  @MethodSource("legacyBatchedCases")
  void batchOn_noErrorHop_legacy_Buggy_theBatchesRunInAnOrderOfTheirOwn(
      String recovery, String table, int commitSize, String name, List<String> expected)
      throws Exception {
    DatabaseMeta db = target("H2", table);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .table(table)
            .batch(true)
            .errorHop(false)
            .commitSize(commitSize)
            .run(CASES.get(name));

    assertTrue(r.batchMode);
    if (expected == FAILS) {
      assertEquals(1, r.errorCount);
      assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, table));
    } else {
      assertEquals(0, r.errorCount);
      assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
    }
  }

  static Stream<Arguments> batchedCasesWithoutFailures() {
    return batchedCases().filter(arguments -> arguments.get()[4] != FAILS);
  }

  /**
   * Buggy, the same with an error hop where batching stays on with one (the flags of Oracle and
   * PostgreSQL): the batches run in the same order.
   */
  @ParameterizedTest(name = "recovery {0}, table {1}, commit size {2}: {3}")
  @MethodSource("batchedCasesWithoutFailures")
  void oracleFlags_batchOn_errorHop_Buggy_theBatchesRunInTheSameOrder(
      String recovery, String table, int commitSize, String name, List<String> expected)
      throws Exception {
    DatabaseMeta db = target(SynchronizeAfterMergeBatchBehaviourH2Test.ORACLE_FLAGS, table);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .table(table)
            .batch(true)
            .errorHop(true)
            .commitSize(commitSize)
            .run(CASES.get(name));

    assertTrue(r.batchMode);
    assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
  }

  static Stream<Arguments> rowByRowCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (String recovery : new String[] {null, "LEGACY"}) {
      for (String table : new String[] {"target", "t"}) {
        for (String name : CASES.keySet().stream().sorted().toList()) {
          arguments.add(Arguments.of(recovery, table, name, IN_INPUT_ORDER.get(name)));
        }
      }
    }
    return arguments.stream();
  }

  /** Batch updates off: the rows run one by one, in input order. */
  @ParameterizedTest(name = "recovery {0}, table {1}: {2}")
  @MethodSource("rowByRowCases")
  void batchOff_errorHop_theRowsRunInInputOrder(
      String recovery, String table, String name, List<String> expected) throws Exception {
    DatabaseMeta db = target("H2", table);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .table(table)
            .batch(false)
            .errorHop(true)
            .commitSize(100)
            .run(CASES.get(name));

    assertFalse(r.batchMode);
    assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
  }

  static Stream<Arguments> legacyRowByRowCases() {
    return rowByRowCases().filter(arguments -> "LEGACY".equals(arguments.get()[0]));
  }

  /**
   * With HOP_DATABASE_BATCH_RECOVERY=LEGACY and an error hop H2 switches batching off (see
   * SynchronizeAfterMergeBatchBehaviourH2Test), so the rows run in input order.
   */
  @ParameterizedTest(name = "recovery {0}, table {1}: {2}")
  @MethodSource("legacyRowByRowCases")
  void batchOn_errorHop_legacy_batchingIsOffAndTheRowsRunInInputOrder(
      String recovery, String table, String name, List<String> expected) throws Exception {
    DatabaseMeta db = target("H2", table);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery(recovery)
            .table(table)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(CASES.get(name));

    assertFalse(r.batchMode);
    assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
  }

  static Stream<Arguments> recoveredCases() {
    List<Arguments> arguments = new ArrayList<>();
    for (boolean errorHop : new boolean[] {true, false}) {
      for (String table : new String[] {"target", "t"}) {
        for (int commitSize : new int[] {100, 2}) {
          for (String name : CASES.keySet().stream().sorted().toList()) {
            arguments.add(
                Arguments.of(errorHop, table, commitSize, name, inInputOrder(name, errorHop)));
          }
        }
      }
    }
    return arguments.stream();
  }

  /**
   * Since #9768, on the default path: the batches are recovered, and a row whose key waits in a
   * batch of another statement makes the batches run first. So the statements of one key run in
   * input order and the table holds what row by row gives, with and without an error hop; with one
   * the second insert of a key goes to the error hop, without one it fails the transform.
   */
  @ParameterizedTest(name = "error hop {0}, table {1}, commit size {2}: {3}")
  @MethodSource("recoveredCases")
  void batchOn_operationsOnOneKeyRunInInputOrder(
      boolean errorHop, String table, int commitSize, String name, List<String> expected)
      throws Exception {
    DatabaseMeta db = target("H2", table);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .table(table)
            .batch(true)
            .errorHop(errorHop)
            .commitSize(commitSize)
            .run(CASES.get(name));

    assertTrue(r.recoveredBatches);
    if (expected == FAILS) {
      assertEquals(1, r.errorCount);
      assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, table));
      return;
    }
    assertEquals(0, r.errorCount);
    assertEquals(expected, SynchronizeAfterMergeRun.table(db, table));
    assertEquals(name.equals("insert twice") ? 1 : 0, r.errors.size());
    assertEquals(CASES.get(name).size(), r.output.size() + r.errors.size());
  }

  /**
   * Since #9768, the two tables with an error hop: in each table the statements of key 1 run in
   * input order; the same key in the other table makes nothing wait.
   */
  @Test
  void batchOn_errorHop_twoTablesFromAField() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.createTarget(db, "sam_a", 2);
    SynchronizeAfterMergeRun.createTarget(db, "sam_b", 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .tableNameInField(true)
            .batch(true)
            .errorHop(true)
            .commitSize(100)
            .run(twoTables());

    assertTrue(r.recoveredBatches);
    assertEquals(labels("u1@sam_a", "i1@sam_a", "i1@sam_b", "d1@sam_b"), r.outputLabels());
    assertEquals(List.of("1:a", "2:x"), SynchronizeAfterMergeRun.table(db, "sam_a"));
    assertEquals(List.of("2:x"), SynchronizeAfterMergeRun.table(db, "sam_b"));
  }

  /**
   * Buggy, as it still is with HOP_DATABASE_BATCH_RECOVERY=LEGACY: two tables, the table name in a
   * field. Every table has batches of its own, and the batches of both run at the end in the order
   * of one Hashtable: sam_b delete, sam_a insert, sam_a update, sam_b insert. So in sam_a the
   * update of 1 runs after its insert, and in sam_b the delete of 1 before it, the other way round
   * from the input.
   */
  @Test
  void batchOn_noErrorHop_legacy_Buggy_twoTablesFromAField() throws Exception {
    DatabaseMeta db = SynchronizeAfterMergeBatchBehaviourH2Test.database("H2");
    SynchronizeAfterMergeRun.createTarget(db, "sam_a", 2);
    SynchronizeAfterMergeRun.createTarget(db, "sam_b", 2);

    Result r =
        SynchronizeAfterMergeRun.on(db)
            .recovery("LEGACY")
            .tableNameInField(true)
            .batch(true)
            .errorHop(false)
            .commitSize(100)
            .run(twoTables());

    assertEquals(0, r.errorCount);
    assertEquals(labels("u1@sam_a", "i1@sam_a", "i1@sam_b", "d1@sam_b"), r.outputLabels());
    assertEquals(TWO_TABLES_A, SynchronizeAfterMergeRun.table(db, "sam_a"));
    assertEquals(TWO_TABLES_B, SynchronizeAfterMergeRun.table(db, "sam_b"));
  }

  static List<String> labels(String... labels) {
    return List.of(labels);
  }

  /** Update 1 then insert 1 in sam_a, insert 1 then delete 1 in sam_b. */
  static List<Object[]> twoTables() {
    return rows(
        in("sam_a", named(update(1), "b")),
        in("sam_a", named(insert(1), "a")),
        in("sam_b", named(insert(1), "a")),
        in("sam_b", delete(1)));
  }

  /** In input order sam_a would hold 1:a and sam_b no 1. */
  static final List<String> TWO_TABLES_A = List.of("1:b", "2:x");

  static final List<String> TWO_TABLES_B = List.of("1:a", "2:x");
}
