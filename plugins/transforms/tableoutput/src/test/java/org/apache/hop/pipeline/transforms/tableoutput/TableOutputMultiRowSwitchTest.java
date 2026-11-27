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

package org.apache.hop.pipeline.transforms.tableoutput;

import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRowMeta;
import static org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.idNameRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.batch.MultiRowBatch;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The switches that turn statements of several rows off: the variable HOP_DATABASE_MULTI_ROW_DML,
 * the connection option "Write one row per statement", and HOP_DATABASE_BATCH_RECOVERY=LEGACY.
 *
 * <p>With a switch on, a Table output that asks for 100 rows per statement must do exactly what one
 * that asks for 1 does: the same table, the same rows on the main output and the error hop, in the
 * same order and at the same moments, the same error fields and the same counters. A switch on its
 * own must not change a run of one row per statement either. Each run is compared with its twin, so
 * this pins the one-row path as it is, without restating it.
 */
class TableOutputMultiRowSwitchTest {

  private static final String CREATE_TARGET =
      "CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(20))";
  private static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";

  @TempDir static Path folder;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  /** Both databases declare a multi-row INSERT. */
  enum Db {
    H2,
    DUCKDB
  }

  /** How statements of several rows are switched off, or not. */
  enum Switch {
    NONE,
    VARIABLE,
    CONNECTION,
    LEGACY
  }

  /** What the transform is asked to do. */
  enum Scenario {
    /** 23 rows, commit size 10: two full commits and a remainder. */
    CLEAN(23, 10, true, false),
    /** Seeded duplicate keys go to the error hop, one of them in the last, partial commit. */
    ERROR_HOP(23, 10, true, true, 3, 8, 9, 17, 20),
    /** A duplicate key in the second commit without an error hop stops the transform. */
    FAILURE_WITHOUT_ERROR_HOP(25, 10, true, false, 13),
    /** Batch updates off, with an error hop: rows per statement never applies. */
    NO_BATCH(12, 5, false, true, 4, 9);

    final int rows;
    final int commitSize;
    final boolean batch;
    final boolean errorHop;
    final long[] seeded;

    Scenario(int rows, int commitSize, boolean batch, boolean errorHop, long... seeded) {
      this.rows = rows;
      this.commitSize = commitSize;
      this.batch = batch;
      this.errorHop = errorHop;
      this.seeded = seeded;
    }
  }

  /** Everything observable about a run, to compare two runs with. */
  record Outcome(
      List<String> table,
      List<Object> outputKeys,
      List<Object> errorKeys,
      List<String> errorCodes,
      List<String> errorDescriptions,
      List<Integer> outputSizeAtRead,
      List<Integer> errorSizeAtRead,
      List<Long> counters,
      boolean stopped,
      int rowsPerStatement,
      List<String> batchClasses) {}

  private static DatabaseMeta database(Db db, boolean optionDisabled, long... seeded)
      throws Exception {
    DatabaseMeta databaseMeta =
        db == Db.H2
            ? new DatabaseMeta(
                "switch",
                "H2",
                "Native",
                "",
                "mem:switch" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1",
                "",
                "sa",
                "")
            : new DatabaseMeta(
                "switch",
                "DuckDB",
                "Native",
                "",
                folder.resolve(UUID.randomUUID() + ".duckdb").toString(),
                "",
                "",
                "");
    databaseMeta.setMultiRowDmlDisabled(optionDisabled);
    TableOutputRun.execute(databaseMeta, CREATE_TARGET);
    for (long id : seeded) {
      TableOutputRun.execute(databaseMeta, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    return databaseMeta;
  }

  private static Outcome run(Db db, Scenario scenario, Switch off, int rowsPerStatement)
      throws Exception {
    DatabaseMeta databaseMeta = database(db, off == Switch.CONNECTION, scenario.seeded);
    TableOutputRun run =
        TableOutputRun.on(databaseMeta)
            .recovery(off == Switch.LEGACY ? "LEGACY" : null)
            .batch(scenario.batch)
            .errorHop(scenario.errorHop)
            .commitSize(scenario.commitSize)
            .configure(meta -> meta.setRowsPerStatement(Integer.toString(rowsPerStatement)));
    if (off == Switch.VARIABLE) {
      run.variable(Const.HOP_DATABASE_MULTI_ROW_DML, "DISABLED");
    }
    Result r = run.run(idNameRowMeta(), idNameRows(scenario.rows));
    assertTrue(r.initialized);
    return new Outcome(
        TableOutputRun.query(databaseMeta, SELECT_TARGET),
        r.outputKeys(),
        r.errorKeys(),
        scenario.errorHop ? r.errorCodes() : List.of(),
        scenario.errorHop ? r.errorDescriptions() : List.of(),
        r.outputSizeAtRead,
        r.errorSizeAtRead,
        List.of(r.linesOutput, r.linesWritten, r.linesRejected, r.errorCount),
        r.stopped,
        r.rowsPerStatement,
        r.batchClasses);
  }

  static Stream<Arguments> switchedOff() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : Db.values()) {
      for (Scenario scenario : Scenario.values()) {
        for (Switch off : new Switch[] {Switch.VARIABLE, Switch.CONNECTION, Switch.LEGACY}) {
          arguments.add(Arguments.of(db, scenario, off));
        }
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("switchedOff")
  void aHundredRowsPerStatementSwitchedOffRunsAsOne(Db db, Scenario scenario, Switch off)
      throws Exception {
    Outcome hundred = run(db, scenario, off, 100);
    Outcome one = run(db, scenario, off, 1);

    assertEquals(1, hundred.rowsPerStatement());
    assertFalse(hundred.batchClasses().contains(MultiRowBatch.class.getSimpleName()));
    assertEquals(one, hundred);
  }

  static Stream<Arguments> switchesOfThisOption() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : Db.values()) {
      for (Scenario scenario : Scenario.values()) {
        for (Switch off : new Switch[] {Switch.VARIABLE, Switch.CONNECTION}) {
          arguments.add(Arguments.of(db, scenario, off));
        }
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("switchesOfThisOption")
  void theSwitchChangesNothingAtOneRowPerStatement(Db db, Scenario scenario, Switch off)
      throws Exception {
    assertEquals(run(db, scenario, Switch.NONE, 1), run(db, scenario, off, 1));
  }

  static Stream<Arguments> batchedScenarios() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : Db.values()) {
      for (Scenario scenario : Scenario.values()) {
        if (scenario.batch) {
          arguments.add(Arguments.of(db, scenario));
        }
      }
    }
    return arguments.stream();
  }

  /**
   * Without a switch the same runs do write several rows per statement: the tests above can fail.
   */
  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("batchedScenarios")
  void withoutASwitchSeveralRowsPerStatementAreWritten(Db db, Scenario scenario) throws Exception {
    Outcome hundred = run(db, scenario, Switch.NONE, 100);
    Outcome one = run(db, scenario, Switch.NONE, 1);

    assertTrue(hundred.rowsPerStatement() > 1);
    assertTrue(hundred.batchClasses().contains(MultiRowBatch.class.getSimpleName()));
    assertNotEquals(one, hundred);
    // The rows themselves end up the same.
    assertEquals(one.table(), hundred.table());
    assertEquals(one.outputKeys(), hundred.outputKeys());
    assertEquals(one.errorKeys(), hundred.errorKeys());
  }
}
