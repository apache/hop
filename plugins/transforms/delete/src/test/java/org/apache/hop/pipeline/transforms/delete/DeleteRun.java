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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.map.DatabaseConnectionMap;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
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
 * Runs one Delete end to end (init, processRow until done, dispose) against a real database and
 * records everything it can be observed doing: the rows on the main output and on the error hop
 * (with the error fields), the transform's counters and its log lines.
 *
 * <p>The main output and the error hop are real row sets, so the engine's own {@code putRow} and
 * {@code putError} run and update the counters as they would in a pipeline. The loop is the one of
 * the engine's run thread: processRow until it returns false or the transform is stopped, then
 * dispose; an exception out of processRow sets the errors and stops the transform, as there.
 *
 * <p>By default the transform deletes from table {@code target} by {@code id = id}, for input rows
 * (id, name). A row fails when its id is still referenced from {@code target_child}: the foreign
 * key refuses the delete.
 */
final class DeleteRun {

  static final String TRANSFORM = "delete";
  static final String ERROR_TARGET = "delete errors";
  static final String NR_ERRORS = "nr_errors";
  static final String ERROR_DESCRIPTIONS = "error_descriptions";
  static final String ERROR_FIELDS = "error_fields";
  static final String ERROR_CODES = "error_codes";

  static final String CREATE_TARGET =
      "CREATE TABLE target (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL)";

  /**
   * A child table whose rows reference ids of the target: deleting such an id fails on H2,
   * PostgreSQL, DuckDB and Derby alike (NO ACTION).
   */
  static final String CREATE_CHILD =
      "CREATE TABLE target_child (parent_id BIGINT NOT NULL REFERENCES target (id))";

  static final String SELECT_IDS = "SELECT id FROM target ORDER BY id";

  /** Something to do after the transform processed a row. */
  @FunctionalInterface
  interface IAfterRow {
    /**
     * @param processed how many times processRow returned true so far
     */
    void after(int processed, Delete transform) throws Exception;
  }

  private final DatabaseMeta databaseMeta;
  private int commitSize = 100;
  private boolean batch;
  private boolean errorHop;
  private String recovery;
  private SimulatedBatchDriver.Shape shape;
  private Consumer<DeleteMeta> configure = meta -> {};
  private IAfterRow afterRow = (processed, transform) -> {};
  private boolean watchCommits;
  private String connectionGroup;
  private LogLevel logLevel;

  private DeleteRun(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  static DeleteRun on(DatabaseMeta databaseMeta) {
    return new DeleteRun(databaseMeta);
  }

  DeleteRun commitSize(int commitSize) {
    this.commitSize = commitSize;
    return this;
  }

  DeleteRun batch(boolean batch) {
    this.batch = batch;
    return this;
  }

  DeleteRun errorHop(boolean errorHop) {
    this.errorHop = errorHop;
    return this;
  }

  /** Sets the variable HOP_DATABASE_BATCH_RECOVERY for the run; null (the default) leaves it. */
  DeleteRun recovery(String recovery) {
    this.recovery = recovery;
    return this;
  }

  /** Puts a driver that handles a failed batch differently in front of the H2 connection. */
  DeleteRun driver(SimulatedBatchDriver.Shape shape) {
    this.shape = shape;
    return this;
  }

  DeleteRun configure(Consumer<DeleteMeta> configure) {
    this.configure = configure;
    return this;
  }

  DeleteRun afterRow(IAfterRow afterRow) {
    this.afterRow = afterRow;
    return this;
  }

  /** Runs the transform at this log level, to see its detailed log lines. */
  DeleteRun logLevel(LogLevel logLevel) {
    this.logLevel = logLevel;
    return this;
  }

  /**
   * Looks up, over a connection of its own, whether the table still holds the id of every row the
   * moment it is passed on: {@link Result#presentAtOutput} and {@link Result#presentAtError}.
   */
  DeleteRun watchCommits() {
    this.watchCommits = true;
    return this;
  }

  /**
   * Runs the transform the way a transactional pipeline does: its database joins this connection
   * group, whose transaction is committed (or rolled back when the transform failed) after the
   * transform is disposed. Register the extension point of {@link
   * SharedConnectionRun#registerConnectionGroupExtensionPoint()} first.
   */
  DeleteRun connectionGroup(String connectionGroup) {
    this.connectionGroup = connectionGroup;
    return this;
  }

  /** What one run did. */
  static final class Result {
    boolean initialized;
    boolean batchMode;
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

    long linesRead;
    long linesInput;
    long linesOutput;
    long linesWritten;
    long linesUpdated;
    long linesRejected;
    long errorCount;
    boolean stopped;

    /** What processRow threw, if anything. */
    Throwable thrown;

    /**
     * With {@link DeleteRun#watchCommits()}: for each main output row, whether its id was still in
     * the table the moment the row was passed on, as another connection sees it; null for a key
     * that is no number.
     */
    final List<Boolean> presentAtOutput = new ArrayList<>();

    /** The same for the rows on the error hop. */
    final List<Boolean> presentAtError = new ArrayList<>();

    /** The ids of the main output rows that were passed on before their delete was committed. */
    List<Object> passedOnBeforeTheirCommit() {
      List<Object> ids = new ArrayList<>();
      for (int i = 0; i < output.size(); i++) {
        if (Boolean.TRUE.equals(presentAtOutput.get(i))) {
          ids.add(output.get(i)[0]);
        }
      }
      return ids;
    }

    /** The ids of the error rows whose delete was already committed when they were sent there. */
    List<Object> deletedBeforeTheErrorHop() {
      List<Object> ids = new ArrayList<>();
      for (int i = 0; i < errors.size(); i++) {
        if (Boolean.FALSE.equals(presentAtError.get(i))) {
          ids.add(errors.get(i)[0]);
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

    /** What kind of message each error row carries, see {@link DeleteRun#kind(String)}. */
    List<String> errorKinds() {
      return errorField(ERROR_DESCRIPTIONS).stream()
          .map(d -> kind((String) d))
          .collect(Collectors.toList());
    }

    /**
     * Lines read, updated (Delete counts a delete it ran as an update), written (to the output),
     * rejected (to the error hop), input and output; Delete counts neither of the last two.
     */
    List<Long> counters() {
      return List.of(linesRead, linesUpdated, linesWritten, linesRejected, linesInput, linesOutput);
    }

    boolean logged(String text) {
      return basicLog.stream().anyMatch(line -> line.contains(text));
    }
  }

  /** Lines read, updated, written and rejected; lines input and output are always 0. */
  static List<Long> counters(long read, long updated, long written, long rejected) {
    return List.of(read, updated, written, rejected, 0L, 0L);
  }

  Result run(List<Object[]> rows) throws Exception {
    return run(idNameRowMeta(), rows);
  }

  Result run(IRowMeta inputRowMeta, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    DeleteMeta meta = new DeleteMeta();
    meta.setDefault();
    meta.setConnection(databaseMeta.getName());
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    meta.getLookup().setSchemaName("");
    meta.getLookup().setTableName("target");
    meta.getLookup().getFields().add(new DeleteKeyField("id", "=", "id", null));
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
    IRowSet mainRowSet = new WatchedRowSet(watcher, result.presentAtOutput);
    mainRowSet.setThreadNameFromToCopy(TRANSFORM, 0, "main", 0);
    IRowSet errorRowSet = errorHop ? new WatchedRowSet(watcher, result.presentAtError) : null;

    DeleteData data = new DeleteData();
    Delete transform =
        new Delete(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            result.outputSizeAtRead.add(mainRowSet.size());
            result.errorSizeAtRead.add(errorRowSet == null ? 0 : errorRowSet.size());
            Object[] row = input.poll();
            if (row != null) {
              // As the engine's getRow does
              incrementLinesRead();
            }
            return row;
          }

          @Override
          public void logBasic(String message) {
            result.basicLog.add(message);
            super.logBasic(message);
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
    if (logLevel != null) {
      transform.setLogLevel(logLevel);
    }

    if (errorHop) {
      TransformMeta target = new TransformMeta(ERROR_TARGET, new DeleteMeta());
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
    // Whether the deletes go out in batches at all: the way Delete always ran them, or recovered
    result.batchMode = data.batchMode || data.recoveredBatches;
    result.recoveredBatches = data.recoveredBatches;
    result.useSavepoints = data.useSavepoints;
    if (result.initialized && shape != null) {
      data.db.setConnection(SimulatedBatchDriver.wrap(data.db.getConnection(), shape));
    }
    try {
      if (result.initialized) {
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
    result.linesRead = transform.getLinesRead();
    result.linesInput = transform.getLinesInput();
    result.linesOutput = transform.getLinesOutput();
    result.linesWritten = transform.getLinesWritten();
    result.linesUpdated = transform.getLinesUpdated();
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
    private final List<Boolean> present;

    private WatchedRowSet(CommitWatcher watcher, List<Boolean> present) {
      this.watcher = watcher;
      this.present = present;
    }

    @Override
    public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
      if (watcher != null) {
        present.add(watcher.isPresent(rowData[0]));
      }
      return super.putRow(rowMeta, rowData);
    }
  }

  /** Looks up whether an id is committed in the table, over a connection of its own. */
  private static final class CommitWatcher {
    private final Database db;
    private final PreparedStatement select;

    private CommitWatcher(DatabaseMeta databaseMeta) throws Exception {
      db = new Database(new LoggingObject("watcher"), new Variables(), databaseMeta);
      db.connect();
      db.getConnection().setAutoCommit(true);
      select = db.getConnection().prepareStatement("SELECT id FROM target WHERE id = ?");
    }

    /**
     * @return whether the id is in the table, or null for a key that is no number: no such row can
     *     be in the table
     */
    private Boolean isPresent(Object id) {
      if (!(id instanceof Long key)) {
        return null;
      }
      try {
        select.setLong(1, key);
        try (ResultSet resultSet = select.executeQuery()) {
          return resultSet.next();
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

  /** Rows 1..n named rN. */
  static List<Object[]> rows(int n) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      rows.add(new Object[] {id, "r" + id});
    }
    return rows;
  }

  /**
   * Rows 1..n, except that the given ids carry a key that is no number: binding it to the DELETE
   * fails in Hop, before the database sees the row.
   */
  static List<Object[]> rowsWithUnboundKeys(int n, long... unbound) {
    List<Object[]> rows = rows(n);
    for (long id : unbound) {
      rows.get((int) id - 1)[0] = "no number " + id;
    }
    return rows;
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

  // Error description helpers

  /** The exception of a single delete that failed. */
  static final String ROW = "row";

  /** The exception of a whole batch. */
  static final String BATCH = "batch";

  /** An exception of the JDBC driver itself, such as one chained to a batch failure. */
  static final String DRIVER = "driver";

  /** A key Hop could not bind to the DELETE. */
  static final String BIND = "bind";

  /** A row that could not run because the driver closed the statement after an earlier failure. */
  static final String CLOSED = "closed";

  /**
   * Classifies an error description, see {@link #ROW}, {@link #BATCH}, {@link #DRIVER}, {@link
   * #BIND} and {@link #CLOSED}.
   */
  static String kind(String description) {
    if (description == null) {
      return null;
    }
    if (description.contains("Statement was closed")) {
      return CLOSED;
    }
    if (description.contains("Error setting value #")) {
      return BIND;
    }
    if (description.contains("Error updating batch")
        || description.contains("Error deleting a batch of rows")) {
      return BATCH;
    }
    if (description.contains("Error inserting/updating row")) {
      return ROW;
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

  /**
   * Creates {@link #CREATE_TARGET} holding ids 1..n, all named x, and {@link #CREATE_CHILD}
   * referencing the given ids, so that deleting those fails.
   */
  static void createTarget(DatabaseMeta databaseMeta, int n, long... referenced) throws Exception {
    List<String> statements = new ArrayList<>();
    statements.add(CREATE_TARGET);
    statements.add(CREATE_CHILD);
    StringBuilder insert = null;
    for (long id = 1; id <= n; id++) {
      if (insert == null) {
        insert = new StringBuilder("INSERT INTO target (id, name) VALUES ");
      } else {
        insert.append(", ");
      }
      insert.append('(').append(id).append(", 'x')");
      if (id % 1000 == 0 || id == n) {
        statements.add(insert.toString());
        insert = null;
      }
    }
    for (long id : referenced) {
      statements.add("INSERT INTO target_child (parent_id) VALUES (" + id + ")");
    }
    execute(databaseMeta, statements.toArray(new String[0]));
  }

  static void execute(DatabaseMeta databaseMeta, String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
    }
  }

  /** The ids left in the target table, in order. */
  static List<Object> remaining(DatabaseMeta databaseMeta) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      List<Object> ids = new ArrayList<>();
      for (Object[] row : db.getRows(SELECT_IDS, -1)) {
        ids.add(((Number) row[0]).longValue());
      }
      return ids;
    }
  }
}
