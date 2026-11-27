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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.pipeline.transforms.tableoutput.TableOutputRun.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Pins what Table output does today with one INSERT per row, on H2 and DuckDB, on the two paths a
 * new way of writing rows must leave alone:
 *
 * <ul>
 *   <li>{@link Mode#LEGACY}: a new transform with {@code HOP_DATABASE_BATCH_RECOVERY=LEGACY}, the
 *       batch handling Table output always had;
 *   <li>{@link Mode#SAVED}: a transform loaded as an older Hop saved it, on a connection whose
 *       batch failure contract is validated, so failed batches are recovered.
 * </ul>
 *
 * <p>Each run is reduced to one line that holds everything that can be observed: the table, the
 * rows on the main output and on the error hop in their order, the error codes and kinds, how many
 * rows had left on each stream every time a row was read, the counters and whether the transform
 * stopped. The lines are what the runs give today, including behaviour that may not be what one
 * wants (DuckDB without batch updates, for one): they are here to show that it does not change.
 */
class TableOutputOneRowPathTest {

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

  enum Db {
    H2,
    DUCKDB
  }

  enum Mode {
    LEGACY,
    SAVED
  }

  /** What the transform is asked to do; seeded ids are in the table before it runs. */
  enum Scenario {
    /** 23 rows, commit size 10: two full commits and a remainder. */
    CLEAN(23, 10, true, false),
    /** Seeded duplicate keys go to the error hop, one of them in the last, partial commit. */
    ERROR_HOP(23, 10, true, true, 3, 8, 9, 17, 20),
    /** A duplicate key in the second commit without an error hop stops the transform. */
    FAILURE_WITHOUT_ERROR_HOP(25, 10, true, false, 13),
    /** Batch updates off, with an error hop. */
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

  /** What every run gives today, by database, mode and scenario. */
  private static final Map<String, String> EXPECTED =
      Map.ofEntries(
          Map.entry(
              "H2 LEGACY CLEAN",
              "table=1-23 seeds= out=1-23 err= codes= kinds= outAtRead=0x10,10x10,20x4 errAtRead=0x24 output/written/rejected/errors=23/23/0/0 stopped=false"),
          Map.entry(
              "H2 LEGACY ERROR_HOP",
              "table=1-23 seeds=3,8-9,17,20 out=1-2,4-7,10-16,18-19,21-23 err=3,8-9,17,20 codes=TOP0002x5 kinds=driverx5 outAtRead=0x10,7x10,15x4 errAtRead=0x10,3x10,5x4 output/written/rejected/errors=18/18/5/0 stopped=false"),
          Map.entry(
              "H2 LEGACY FAILURE_WITHOUT_ERROR_HOP",
              "table=1-10,13 seeds=13 out=1-10 err= codes= kinds= outAtRead=0x10,10x10 errAtRead=0x20 output/written/rejected/errors=19/10/0/1 stopped=true"),
          Map.entry(
              "H2 LEGACY NO_BATCH",
              "table=1-12 seeds=4,9 out=1-3,5-8,10-12 err=4,9 codes=TOP001x2 kinds=rowx2 outAtRead=0x1,1x1,2x1,3x2,4x1,5x1,6x1,7x2,8x1,9x1,10x1 errAtRead=0x4,1x5,2x4 output/written/rejected/errors=10/10/2/0 stopped=false"),
          Map.entry(
              "H2 SAVED CLEAN",
              "table=1-23 seeds= out=1-23 err= codes= kinds= outAtRead=0x10,10x10,20x4 errAtRead=0x24 output/written/rejected/errors=23/23/0/0 stopped=false"),
          Map.entry(
              "H2 SAVED ERROR_HOP",
              "table=1-23 seeds=3,8-9,17,20 out=1-2,4-7,10-16,18-19,21-23 err=3,8-9,17,20 codes=TOP0002x5 kinds=driverx5 outAtRead=0x10,7x10,15x4 errAtRead=0x10,3x10,5x4 output/written/rejected/errors=18/18/5/0 stopped=false"),
          Map.entry(
              "H2 SAVED FAILURE_WITHOUT_ERROR_HOP",
              "table=1-10,13 seeds=13 out=1-10 err= codes= kinds= outAtRead=0x10,10x10 errAtRead=0x20 output/written/rejected/errors=10/10/0/1 stopped=true"),
          Map.entry(
              "H2 SAVED NO_BATCH",
              "table=1-12 seeds=4,9 out=1-3,5-8,10-12 err=4,9 codes=TOP001x2 kinds=rowx2 outAtRead=0x1,1x1,2x1,3x2,4x1,5x1,6x1,7x2,8x1,9x1,10x1 errAtRead=0x4,1x5,2x4 output/written/rejected/errors=10/10/2/0 stopped=false"),
          Map.entry(
              "DUCKDB LEGACY CLEAN",
              "table=1-23 seeds= out=1-23 err= codes= kinds= outAtRead=0x10,10x10,20x4 errAtRead=0x24 output/written/rejected/errors=23/23/0/0 stopped=false"),
          Map.entry(
              "DUCKDB LEGACY ERROR_HOP",
              "table=3,8-9,17,20 seeds=3,8-9,17,20 out= err=1-9 codes=TOP0003x9 kinds=batchx9 outAtRead=0x10 errAtRead=0x10 output/written/rejected/errors=0/0/9/1 stopped=true"),
          Map.entry(
              "DUCKDB LEGACY FAILURE_WITHOUT_ERROR_HOP",
              "table=1-10,13 seeds=13 out=1-10 err= codes= kinds= outAtRead=0x10,10x10 errAtRead=0x20 output/written/rejected/errors=10/10/0/1 stopped=true"),
          Map.entry(
              "DUCKDB LEGACY NO_BATCH",
              "table=4,9 seeds=4,9 out=1-3 err=4-12 codes=TOP001x9 kinds=rowx1,statement-closedx8 outAtRead=0x1,1x1,2x1,3x10 errAtRead=0x4,1x1,2x1,3x1,4x1,5x1,6x1,7x1,8x1,9x1 output/written/rejected/errors=3/3/9/0 stopped=false"),
          Map.entry(
              "DUCKDB SAVED CLEAN",
              "table=1-23 seeds= out=1-23 err= codes= kinds= outAtRead=0x10,10x10,20x4 errAtRead=0x24 output/written/rejected/errors=23/23/0/0 stopped=false"),
          Map.entry(
              "DUCKDB SAVED ERROR_HOP",
              "table=1-23 seeds=3,8-9,17,20 out=1-2,4-7,10-16,18-19,21-23 err=3,8-9,17,20 codes=TOP0002x5 kinds=driverx5 outAtRead=0x10,7x10,15x4 errAtRead=0x10,3x10,5x4 output/written/rejected/errors=18/18/5/0 stopped=false"),
          Map.entry(
              "DUCKDB SAVED FAILURE_WITHOUT_ERROR_HOP",
              "table=1-10,13 seeds=13 out=1-10 err= codes= kinds= outAtRead=0x10,10x10 errAtRead=0x20 output/written/rejected/errors=10/10/0/1 stopped=true"),
          Map.entry(
              "DUCKDB SAVED NO_BATCH",
              "table=4,9 seeds=4,9 out=1-3 err=4-12 codes=TOP001x9 kinds=rowx1,statement-closedx8 outAtRead=0x1,1x1,2x1,3x10 errAtRead=0x4,1x1,2x1,3x1,4x1,5x1,6x1,7x1,8x1,9x1 output/written/rejected/errors=3/3/9/0 stopped=false"));

  static Stream<Arguments> runs() {
    List<Arguments> arguments = new ArrayList<>();
    for (Db db : Db.values()) {
      for (Mode mode : Mode.values()) {
        for (Scenario scenario : Scenario.values()) {
          arguments.add(Arguments.of(db, mode, scenario));
        }
      }
    }
    return arguments.stream();
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("runs")
  void runsAsItAlwaysDid(Db db, Mode mode, Scenario scenario) throws Exception {
    String key = db + " " + mode + " " + scenario;
    String outcome = run(db, mode, scenario);
    assertEquals(EXPECTED.get(key), outcome, key);
  }

  private static String run(Db db, Mode mode, Scenario scenario) throws Exception {
    DatabaseMeta databaseMeta =
        db == Db.H2
            ? new DatabaseMeta(
                "pin",
                "H2",
                "Native",
                "",
                "mem:pin" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1",
                "",
                "sa",
                "")
            : new DatabaseMeta(
                "pin",
                "DuckDB",
                "Native",
                "",
                folder.resolve(UUID.randomUUID() + ".duckdb").toString(),
                "",
                "",
                "");
    TableOutputRun.execute(databaseMeta, CREATE_TARGET);
    for (long id : scenario.seeded) {
      TableOutputRun.execute(databaseMeta, "INSERT INTO target VALUES (" + id + ", 'seed')");
    }
    TableOutputRun run =
        TableOutputRun.on(databaseMeta)
            .batch(scenario.batch)
            .errorHop(scenario.errorHop)
            .commitSize(scenario.commitSize);
    if (mode == Mode.LEGACY) {
      run.recovery("LEGACY");
    } else {
      run.recovery(null).asSaved();
    }
    Result r = run.run(idNameRowMeta(), idNameRows(scenario.rows));
    assertTrue(r.initialized);
    return describe(TableOutputRun.query(databaseMeta, SELECT_TARGET), r, scenario.errorHop);
  }

  /** One line with everything observable about a run. */
  static String describe(List<String> table, Result r, boolean errorHop) {
    List<Object> tableIds = new ArrayList<>();
    List<Object> seeds = new ArrayList<>();
    for (String line : table) {
      long id = Long.parseLong(line.substring(0, line.indexOf(':')));
      tableIds.add(id);
      if (line.endsWith(":seed")) {
        seeds.add(id);
      }
    }
    return "table="
        + ranges(tableIds)
        + " seeds="
        + ranges(seeds)
        + " out="
        + ranges(r.outputKeys())
        + " err="
        + ranges(errorHop ? r.errorKeys() : List.of())
        + " codes="
        + (errorHop ? runLengths(r.errorCodes()) : "")
        + " kinds="
        + (errorHop ? runLengths(kinds(r.errorKinds())) : "")
        + " outAtRead="
        + runLengths(r.outputSizeAtRead)
        + " errAtRead="
        + runLengths(r.errorSizeAtRead)
        + " output/written/rejected/errors="
        + r.linesOutput
        + "/"
        + r.linesWritten
        + "/"
        + r.linesRejected
        + "/"
        + r.errorCount
        + " stopped="
        + r.stopped;
  }

  /** The error kinds, with the DuckDB driver's exceptions as {@code driver} like H2's. */
  private static List<String> kinds(List<String> kinds) {
    List<String> mapped = new ArrayList<>();
    for (String kind : kinds) {
      mapped.add(
          kind == null
              ? null
              : kind.startsWith("other: java.sql.SQLException: ")
                  ? TableOutputRun.DRIVER
                  : kind.contains("Statement was closed") ? "statement-closed" : kind);
    }
    return mapped;
  }

  /** Ids in order as ranges: 1-3,5,7-9; an id out of order starts a new range. */
  static String ranges(List<Object> ids) {
    StringBuilder text = new StringBuilder();
    int i = 0;
    while (i < ids.size()) {
      long start = ((Number) ids.get(i)).longValue();
      long end = start;
      int j = i + 1;
      while (j < ids.size() && ((Number) ids.get(j)).longValue() == end + 1) {
        end++;
        j++;
      }
      text.append(text.length() == 0 ? "" : ",").append(start);
      if (end > start) {
        text.append('-').append(end);
      }
      i = j;
    }
    return text.toString();
  }

  /** Values in order with their repeat counts: 0x11,10x10,20x3. */
  static String runLengths(List<?> values) {
    StringBuilder text = new StringBuilder();
    int i = 0;
    while (i < values.size()) {
      Object value = values.get(i);
      int j = i + 1;
      while (j < values.size() && String.valueOf(values.get(j)).equals(String.valueOf(value))) {
        j++;
      }
      text.append(text.length() == 0 ? "" : ",").append(value).append('x').append(j - i);
      i = j;
    }
    return text.toString();
  }
}
