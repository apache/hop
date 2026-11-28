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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.engines.local.transaction.SharedConnectionRun;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs one Insert/Update end to end (init, processRow until done, dispose) against a real database
 * and records everything it can be observed doing: the rows on the main output and on the error hop
 * (with the error fields), the transform's counters and its basic and error log lines.
 *
 * <p>The main output and the error hop are real row sets, so the engine's own {@code putRow} and
 * {@code putError} run and update the counters as they would in a pipeline. The loop is the one of
 * the engine's run thread: processRow until it returns false or the transform is stopped, then
 * dispose; an exception out of processRow sets the errors and stops the transform, as there.
 *
 * <p>By default the transform looks up table {@code target} by {@code id = id}, inserts a missing
 * id with its name and updates the name of an id it finds, from input rows (id, name).
 */
final class InsertUpdateRun {

  static final String TRANSFORM = "insert update";
  static final String ERROR_TARGET = "insert update errors";
  static final String NR_ERRORS = "nr_errors";
  static final String ERROR_DESCRIPTIONS = "error_descriptions";
  static final String ERROR_FIELDS = "error_fields";
  static final String ERROR_CODES = "error_codes";

  /** The value of {@code name} that the table refuses, by a check constraint. */
  static final String BAD = "bad";

  /** The value of {@code name} every row of the table starts with. */
  static final String SEED = "x";

  /**
   * A table that refuses {@link #BAD} as a name, on an insert and an update alike, on H2,
   * PostgreSQL, DuckDB and Derby.
   */
  static final String CREATE_TARGET =
      "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL"
          + " CHECK (name <> '"
          + BAD
          + "'))";

  static final String SELECT_TARGET = "SELECT id, name FROM target ORDER BY id";

  /** Something to do after the transform processed a row. */
  @FunctionalInterface
  interface IAfterRow {
    /**
     * @param processed how many times processRow returned true so far
     */
    void after(int processed, InsertUpdate transform) throws Exception;
  }

  /** Something to do with the transform after init, before the first row. */
  @FunctionalInterface
  interface IAfterInit {
    void after(InsertUpdate transform, InsertUpdateData data) throws Exception;
  }

  private final DatabaseMeta databaseMeta;
  private int commitSize = 100;
  private boolean batch;
  private boolean errorHop;
  private String recovery;
  private SimulatedBatchDriver.Shape shape;
  private Consumer<InsertUpdateMeta> configure = meta -> {};
  private IAfterRow afterRow = (processed, transform) -> {};
  private IAfterInit afterInit = (transform, data) -> {};
  private boolean watchCommits;
  private String connectionGroup;

  private InsertUpdateRun(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  static InsertUpdateRun on(DatabaseMeta databaseMeta) {
    return new InsertUpdateRun(databaseMeta);
  }

  InsertUpdateRun commitSize(int commitSize) {
    this.commitSize = commitSize;
    return this;
  }

  /** Sets "Use batch updates"; off by default, as in a new transform. */
  InsertUpdateRun batch(boolean batch) {
    this.batch = batch;
    return this;
  }

  /** Puts a driver that handles a failed batch differently in front of the H2 connection. */
  InsertUpdateRun driver(SimulatedBatchDriver.Shape shape) {
    this.shape = shape;
    return this;
  }

  InsertUpdateRun errorHop(boolean errorHop) {
    this.errorHop = errorHop;
    return this;
  }

  /** Sets the variable HOP_DATABASE_BATCH_RECOVERY for the run; null (the default) leaves it. */
  InsertUpdateRun recovery(String recovery) {
    this.recovery = recovery;
    return this;
  }

  InsertUpdateRun configure(Consumer<InsertUpdateMeta> configure) {
    this.configure = this.configure.andThen(configure);
    return this;
  }

  InsertUpdateRun afterRow(IAfterRow afterRow) {
    this.afterRow = afterRow;
    return this;
  }

  InsertUpdateRun afterInit(IAfterInit afterInit) {
    this.afterInit = afterInit;
    return this;
  }

  /**
   * Looks up, over a connection of its own, what the table holds for every row the moment it is
   * passed on: {@link Result#committedAtOutput} and {@link Result#committedAtError}.
   */
  InsertUpdateRun watchCommits() {
    this.watchCommits = true;
    return this;
  }

  /**
   * Runs the transform the way a transactional pipeline does: its database joins this connection
   * group, whose transaction is committed (or rolled back when the transform failed) after the
   * transform is disposed. Register the extension point of {@link
   * SharedConnectionRun#registerConnectionGroupExtensionPoint()} first.
   */
  InsertUpdateRun connectionGroup(String connectionGroup) {
    this.connectionGroup = connectionGroup;
    return this;
  }

  /** What one run did. */
  static final class Result {
    boolean initialized;
    boolean recoveredBatches;
    boolean useSavepoints;

    final List<Object[]> output = new ArrayList<>();
    IRowMeta outputRowMeta;
    final List<Object[]> errors = new ArrayList<>();
    IRowMeta errorRowMeta;

    /**
     * How many rows had gone to the main output, and to the error hop, each time the transform read
     * a row (and once more when it found the end of the input). This shows when rows leave: one by
     * one, or a batch at a time.
     */
    final List<Integer> outputSizeAtRead = new ArrayList<>();

    final List<Integer> errorSizeAtRead = new ArrayList<>();

    final List<String> basicLog = new ArrayList<>();
    final List<String> detailedLog = new ArrayList<>();
    final List<String> errorLog = new ArrayList<>();

    long linesInput;
    long linesOutput;
    long linesWritten;
    long linesUpdated;
    long linesSkipped;
    long linesRejected;
    long errorCount;
    boolean stopped;

    /** What processRow threw, if anything. */
    Throwable thrown;

    /**
     * With {@link InsertUpdateRun#watchCommits()}: the committed name of each main output row's id
     * the moment the row was passed on, as another connection sees it; null when the id is not in
     * the table.
     */
    final List<String> committedAtOutput = new ArrayList<>();

    /** The same for the rows on the error hop. */
    final List<String> committedAtError = new ArrayList<>();

    /**
     * The ids of the main output rows that were passed on before the name they carry was committed:
     * an inserted row whose id is not in the table yet, or an updated row whose new name is not.
     */
    List<Object> passedOnBeforeTheirCommit() {
      List<Object> ids = new ArrayList<>();
      for (int i = 0; i < output.size(); i++) {
        String committed = committedAtOutput.get(i);
        if (!output.get(i)[1].equals(committed)) {
          ids.add(output.get(i)[0]);
        }
      }
      return ids;
    }

    /** The value of the first field of every main output row, in order. */
    List<Object> outputKeys() {
      return output.stream().map(r -> r[0]).collect(Collectors.toList());
    }

    /** The value of the first field of every error row, in order. */
    List<Object> errorKeys() {
      return errors.stream().map(r -> r[0]).collect(Collectors.toList());
    }

    List<Object> errorField(String name) {
      return errors.stream()
          .map(r -> r[errorRowMeta.indexOfValue(name)])
          .collect(Collectors.toList());
    }

    List<Object> errorCodes() {
      return errorField(ERROR_CODES);
    }

    /** What kind of message each error row carries, see {@link InsertUpdateRun#kind(String)}. */
    List<String> errorKinds() {
      return errorField(ERROR_DESCRIPTIONS).stream()
          .map(d -> kind((String) d))
          .collect(Collectors.toList());
    }

    /**
     * lines input (looked up), output (inserted), updated, skipped (found, nothing to update),
     * written (to the main output), rejected (to the error hop).
     */
    List<Long> counters() {
      return List.of(
          linesInput, linesOutput, linesUpdated, linesSkipped, linesWritten, linesRejected);
    }

    boolean logged(String text) {
      return basicLog.stream().anyMatch(line -> line.contains(text));
    }

    boolean loggedDetailed(String text) {
      return detailedLog.stream().anyMatch(line -> line.contains(text));
    }
  }

  Result run(List<Object[]> rows) throws Exception {
    return run(idNameRowMeta(), rows);
  }

  Result run(IRowMeta inputRowMeta, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    InsertUpdateMeta meta = new InsertUpdateMeta();
    meta.setDefault();
    meta.setConnection(databaseMeta.getName());
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    InsertUpdateLookupField lookup = meta.getInsertUpdateLookupField();
    lookup.setSchemaName("");
    lookup.setTableName("target");
    lookup.getLookupKeys().add(new InsertUpdateKeyField("id", "id", "="));
    lookup.getValueFields().add(new InsertUpdateValue("id", "id", false));
    lookup.getValueFields().add(new InsertUpdateValue("name", "name", true));
    configure.accept(meta);

    TransformMeta transformMeta = new TransformMeta(TRANSFORM, meta);
    pipelineMeta.addTransform(transformMeta);

    LocalPipelineEngine pipeline = new LocalPipelineEngine();
    // putRow waits up to three seconds for a pipeline to start running
    pipeline.setRunning(true);
    // The row sets are only read after the run: keep putRow from pausing a millisecond per row, as
    // it does for a row set that is nearly full, so that large runs can be timed
    pipeline.setRowSetSize(Integer.MAX_VALUE / 2);

    Queue<Object[]> input = new ArrayDeque<>();
    for (Object[] row : rows) {
      input.add(row.clone());
    }
    Result result = new Result();
    CommitWatcher watcher = watchCommits ? new CommitWatcher(databaseMeta) : null;
    IRowSet mainRowSet = new WatchedRowSet(watcher, result.committedAtOutput);
    mainRowSet.setThreadNameFromToCopy(TRANSFORM, 0, "main", 0);
    IRowSet errorRowSet = errorHop ? new WatchedRowSet(watcher, result.committedAtError) : null;

    InsertUpdateData data = new InsertUpdateData();
    InsertUpdate transform =
        new InsertUpdate(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            result.outputSizeAtRead.add(mainRowSet.size());
            result.errorSizeAtRead.add(errorRowSet == null ? 0 : errorRowSet.size());
            return input.poll();
          }

          @Override
          public void logBasic(String message) {
            result.basicLog.add(message);
            super.logBasic(message);
          }

          @Override
          public boolean isDetailed() {
            return true;
          }

          @Override
          public void logDetailed(String message) {
            result.detailedLog.add(message);
            super.logDetailed(message);
          }

          @Override
          public void logError(String message) {
            result.errorLog.add(message);
            super.logError(message);
          }

          @Override
          public void logError(String message, Throwable e) {
            result.errorLog.add(message + " " + e);
            super.logError(message, e);
          }
        };
    transform.addRowSetToOutputRowSets(mainRowSet);

    if (errorHop) {
      TransformMeta target = new TransformMeta(ERROR_TARGET, new InsertUpdateMeta());
      pipelineMeta.addTransform(target);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
      TransformErrorMeta errorMeta =
          new TransformErrorMeta(
              transformMeta, target, NR_ERRORS, ERROR_DESCRIPTIONS, ERROR_FIELDS, ERROR_CODES);
      errorMeta.setEnabled(true);
      transformMeta.setTransformErrorMeta(errorMeta);

      errorRowSet.setThreadNameFromToCopy(TRANSFORM, 0, ERROR_TARGET, 0);
      transform.addRowSetToOutputRowSets(errorRowSet);
      transform.identifyErrorOutput();
    }

    if (recovery != null) {
      transform.setVariable("HOP_DATABASE_BATCH_RECOVERY", recovery);
    }
    if (connectionGroup != null) {
      transform.getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
    }
    result.initialized = transform.init();
    result.recoveredBatches = data.recoveredBatches;
    result.useSavepoints = data.useSavepoints;
    if (result.initialized && shape != null) {
      data.db.setConnection(SimulatedBatchDriver.wrap(data.db.getConnection(), shape));
    }
    try {
      if (result.initialized) {
        afterInit.after(transform, data);
        // The loop of the engine's run thread, with a hook between two rows
        int processed = 0;
        while (transform.processRow() && !transform.isStopped()) {
          afterRow.after(++processed, transform);
          if (transform.isStopped()) {
            break;
          }
        }
      }
    } catch (Exception e) {
      result.thrown = e;
      transform.setErrors(1);
      transform.stopAll();
    } finally {
      transform.dispose();
      if (connectionGroup != null) {
        endTransaction(transform.getErrors() == 0);
      }
      if (watcher != null) {
        watcher.close();
      }
    }

    drain(mainRowSet, result.output);
    result.outputRowMeta = mainRowSet.getRowMeta();
    if (errorRowSet != null) {
      drain(errorRowSet, result.errors);
      result.errorRowMeta = errorRowSet.getRowMeta();
    }
    result.linesInput = transform.getLinesInput();
    result.linesOutput = transform.getLinesOutput();
    result.linesWritten = transform.getLinesWritten();
    result.linesUpdated = transform.getLinesUpdated();
    result.linesSkipped = transform.getLinesSkipped();
    result.linesRejected = transform.getLinesRejected();
    result.errorCount = transform.getErrors();
    result.stopped = transform.isStopped();
    return result;
  }

  /** Commits or rolls back the transaction of {@link #connectionGroup}, as the pipeline would. */
  private void endTransaction(boolean commit) throws HopException {
    for (Database database : DatabaseConnectionMap.getInstance().getDatabases(connectionGroup)) {
      try {
        if (commit) {
          database.commit(true);
        } else {
          database.rollback(true);
        }
      } finally {
        database.closeConnectionOnly();
      }
    }
  }

  /** A main output or error row set that asks the watcher about every row put on it. */
  private static final class WatchedRowSet extends QueueRowSet {
    private final CommitWatcher watcher;
    private final List<String> committed;

    private WatchedRowSet(CommitWatcher watcher, List<String> committed) {
      this.watcher = watcher;
      this.committed = committed;
    }

    @Override
    public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
      if (watcher != null) {
        committed.add(watcher.committedName(rowData[0]));
      }
      return super.putRow(rowMeta, rowData);
    }
  }

  /** Reads the committed name of an id over a connection of its own, in auto-commit. */
  private static final class CommitWatcher {
    private final Database db;
    private final PreparedStatement select;

    private CommitWatcher(DatabaseMeta databaseMeta) throws Exception {
      db = new Database(new LoggingObject("watcher"), new Variables(), databaseMeta);
      db.connect();
      db.getConnection().setAutoCommit(true);
      select = db.getConnection().prepareStatement("SELECT name FROM target WHERE id = ?");
    }

    private String committedName(Object id) {
      try {
        select.setLong(1, ((Number) id).longValue());
        try (ResultSet resultSet = select.executeQuery()) {
          return resultSet.next() ? resultSet.getString(1) : null;
        }
      } catch (SQLException e) {
        throw new IllegalStateException("Unable to read the committed row of id " + id, e);
      }
    }

    private void close() {
      try {
        select.close();
      } catch (SQLException e) {
        // closing the connection closes it as well
      }
      db.disconnect();
    }
  }

  private static void drain(IRowSet rowSet, List<Object[]> into) {
    Object[] row;
    while ((row = rowSet.getRowImmediate()) != null) {
      into.add(row);
    }
  }

  // Input helpers

  /** id (Integer), name (String). */
  static IRowMeta idNameRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    return rowMeta;
  }

  /** Rows 1..n named rN, except the bad ones, which the table refuses. */
  static List<Object[]> rows(int n, long... bad) {
    return rows(n, id -> contains(bad, id) ? BAD : "r" + id);
  }

  /** Rows 1..n with the given name. */
  static List<Object[]> rows(int n, LongFunction<String> name) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      rows.add(new Object[] {id, name.apply(id)});
    }
    return rows;
  }

  /** One row (id, name). */
  static Object[] row(long id, String name) {
    return new Object[] {id, name};
  }

  static boolean contains(long[] ids, long id) {
    for (long candidate : ids) {
      if (candidate == id) {
        return true;
      }
    }
    return false;
  }

  /** 1..n as longs, without the given ids. */
  static List<Object> idsWithout(int n, long... without) {
    List<Object> ids = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      if (!contains(without, id)) {
        ids.add(id);
      }
    }
    return ids;
  }

  static List<Object> ids(long... ids) {
    List<Object> list = new ArrayList<>();
    for (long id : ids) {
      list.add(id);
    }
    return list;
  }

  /** The odd ids up to n: the rows {@link #createTarget(DatabaseMeta, int)} puts in the table. */
  static long[] odd(int n) {
    long[] ids = new long[(n + 1) / 2];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = 2L * i + 1;
    }
    return ids;
  }

  /**
   * The table after a run over rows 1..n, of which the odd ids were in the table and the even ones
   * not: "id:rN" for the ids that were written, "id:x" for an odd id that was not, and no line for
   * an even id that was not.
   */
  static List<String> tableWith(int n, LongPredicate written) {
    return tableWith(n, odd(n), written);
  }

  /** The same for a table that held the given ids. */
  static List<String> tableWith(int n, long[] existing, LongPredicate written) {
    TreeSet<Long> all = new TreeSet<>();
    for (long id : existing) {
      all.add(id);
    }
    for (long id = 1; id <= n; id++) {
      all.add(id);
    }
    List<String> lines = new ArrayList<>();
    for (long id : all) {
      if (id <= n && written.test(id)) {
        lines.add(id + ":r" + id);
      } else if (contains(existing, id)) {
        lines.add(id + ":" + SEED);
      }
    }
    return lines;
  }

  /** The table after a run in which every row but the given ones was written. */
  static List<String> tableWrittenExcept(int n, long... notWritten) {
    return tableWith(n, id -> !contains(notWritten, id));
  }

  /** How many of the ids are even: rows that are inserted, as the odd ones are in the table. */
  static long inserts(List<Object> ids) {
    return ids.stream().filter(id -> ((Long) id) % 2 == 0).count();
  }

  /** How many of the ids are odd: rows that are updated. */
  static long updates(List<Object> ids) {
    return ids.size() - inserts(ids);
  }

  // Error description helpers

  static final String ROW = "row";
  static final String BATCH = "batch";
  static final String DRIVER = "driver";
  static final String LOOKUP = "lookup";

  /**
   * Classifies an error description: {@code row} for the exception of a single insert or update,
   * {@code batch} for the exception of a whole batch, {@code lookup} for a lookup that failed,
   * {@code driver} for an exception of the JDBC driver itself, such as one chained to a batch
   * failure.
   */
  static String kind(String description) {
    if (description == null) {
      return null;
    }
    if (description.contains("a batch of rows")) {
      return BATCH;
    }
    if (description.contains("Error inserting/updating row")) {
      return ROW;
    }
    if (description.contains("Error looking up row in database")) {
      return LOOKUP;
    }
    if (description.startsWith("org.h2.")
        || description.startsWith("org.postgresql.")
        || description.startsWith("org.duckdb.")
        || description.startsWith("java.sql.")) {
      return DRIVER;
    }
    return "other: " + description;
  }

  // Database helpers

  /** Creates {@link #CREATE_TARGET} holding the odd ids up to n, all named {@link #SEED}. */
  static void createTarget(DatabaseMeta databaseMeta, int n) throws Exception {
    createTarget(databaseMeta, odd(n));
  }

  /** Creates {@link #CREATE_TARGET} holding the given ids, all named {@link #SEED}. */
  static void createTarget(DatabaseMeta databaseMeta, long... existing) throws Exception {
    List<String> statements = new ArrayList<>();
    statements.add(CREATE_TARGET);
    StringBuilder insert = null;
    for (int i = 0; i < existing.length; i++) {
      if (insert == null) {
        insert = new StringBuilder("INSERT INTO target (id, name) VALUES ");
      } else {
        insert.append(", ");
      }
      insert.append('(').append(existing[i]).append(", '").append(SEED).append("')");
      if ((i + 1) % 1000 == 0 || i == existing.length - 1) {
        statements.add(insert.toString());
        insert = null;
      }
    }
    execute(databaseMeta, statements.toArray(new String[0]));
  }

  static void execute(DatabaseMeta databaseMeta, String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
      if (!db.isAutoCommit()) {
        db.commit(true);
      }
    }
  }

  /** The rows of a query, each row joined as "a:b:c". */
  static List<String> query(DatabaseMeta databaseMeta, String sql) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      List<String> lines = new ArrayList<>();
      List<Object[]> rows = db.getRows(sql, -1);
      int columns = db.getReturnRowMeta().size();
      for (Object[] row : rows) {
        StringBuilder line = new StringBuilder();
        for (int i = 0; i < columns; i++) {
          if (i > 0) {
            line.append(':');
          }
          line.append(row[i]);
        }
        lines.add(line.toString());
      }
      return lines;
    }
  }

  static List<String> table(DatabaseMeta databaseMeta) throws Exception {
    return query(databaseMeta, SELECT_TARGET);
  }
}
