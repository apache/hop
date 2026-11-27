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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs one Synchronize after merge end to end (init, processRow until done, dispose) against a real
 * database and records everything it can be observed doing: the rows on the main output and on the
 * error hop (with the error fields), the transform's counters and its basic and error log lines.
 *
 * <p>The main output and the error hop are real row sets, so the engine's own {@code putRow} and
 * {@code putError} run and update the counters as they would in a pipeline. The loop is the one of
 * the engine's run thread: processRow until it returns false or the transform is stopped, then
 * dispose; an exception out of processRow sets the errors and stops the transform, as there.
 *
 * <p>The input rows are (id, name, op), and (id, name, op, tbl) when the table name comes from a
 * field. The operation field {@code op} holds {@link #INSERT}, {@link #UPDATE}, {@link #DELETE} or
 * anything else, which the transform skips. The transform looks the rows up by {@code id = id} and
 * writes column {@code name}. The tables refuse {@link #BAD} as a name, a second row with the same
 * id, and the delete of a row a child row refers to.
 */
final class SynchronizeAfterMergeRun {

  static final String TRANSFORM = "synchronize";
  static final String ERROR_TARGET = "synchronize errors";
  static final String NR_ERRORS = "nr_errors";
  static final String ERROR_DESCRIPTIONS = "error_descriptions";
  static final String ERROR_FIELDS = "error_fields";
  static final String ERROR_CODES = "error_codes";

  static final String INSERT = "insert";
  static final String UPDATE = "update";
  static final String DELETE = "delete";

  /** An operation the transform doesn't know, so it skips the row. */
  static final String NOTHING = "identical";

  /** The value of {@code name} that the tables refuse, by a check constraint. */
  static final String BAD = "bad";

  /** The value of {@code name} every seeded row starts with. */
  static final String SEED = "x";

  /** The table the transform writes to, unless the table name comes from a field. */
  static final String TARGET = "target";

  /** Something to do after the transform processed a row. */
  @FunctionalInterface
  interface IAfterRow {
    /**
     * @param processed how many times processRow returned true so far
     */
    void after(int processed, SynchronizeAfterMerge transform) throws Exception;
  }

  private final DatabaseMeta databaseMeta;
  private String table = TARGET;
  private int commitSize = 100;
  private boolean batch;
  private boolean errorHop;
  private boolean lookup;
  private boolean tableNameInField;
  private String recovery;
  private Consumer<SynchronizeAfterMergeMeta> configure = meta -> {};
  private IAfterRow afterRow = (processed, transform) -> {};
  private boolean watchCommits;
  private String connectionGroup;
  private SimulatedBatchDriver.Shape shape;
  private boolean debug;

  private SynchronizeAfterMergeRun(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  static SynchronizeAfterMergeRun on(DatabaseMeta databaseMeta) {
    return new SynchronizeAfterMergeRun(databaseMeta);
  }

  SynchronizeAfterMergeRun table(String table) {
    this.table = table;
    return this;
  }

  SynchronizeAfterMergeRun commitSize(int commitSize) {
    this.commitSize = commitSize;
    return this;
  }

  SynchronizeAfterMergeRun batch(boolean batch) {
    this.batch = batch;
    return this;
  }

  SynchronizeAfterMergeRun errorHop(boolean errorHop) {
    this.errorHop = errorHop;
    return this;
  }

  /** "Perform lookup": updates and deletes look their row up first. */
  SynchronizeAfterMergeRun lookup(boolean lookup) {
    this.lookup = lookup;
    return this;
  }

  /** The table name comes from field {@code tbl} of the input rows. */
  SynchronizeAfterMergeRun tableNameInField(boolean tableNameInField) {
    this.tableNameInField = tableNameInField;
    return this;
  }

  /** Sets the variable HOP_DATABASE_BATCH_RECOVERY for the run; null (the default) leaves it. */
  SynchronizeAfterMergeRun recovery(String recovery) {
    this.recovery = recovery;
    return this;
  }

  SynchronizeAfterMergeRun configure(Consumer<SynchronizeAfterMergeMeta> configure) {
    this.configure = configure;
    return this;
  }

  SynchronizeAfterMergeRun afterRow(IAfterRow afterRow) {
    this.afterRow = afterRow;
    return this;
  }

  /** Logs on the debug level as well, see {@link Result#debugLog}. */
  SynchronizeAfterMergeRun debug() {
    this.debug = true;
    return this;
  }

  /** Puts a driver that handles a failed batch differently in front of the H2 connection. */
  SynchronizeAfterMergeRun driver(SimulatedBatchDriver.Shape shape) {
    this.shape = shape;
    return this;
  }

  /**
   * Looks up, over a connection of its own, what the table holds for the id of every row the moment
   * it is passed on: {@link Result#committedAtOutput} and {@link Result#committedAtError}.
   */
  SynchronizeAfterMergeRun watchCommits() {
    this.watchCommits = true;
    return this;
  }

  /**
   * Runs the transform the way a transactional pipeline does: its database joins this connection
   * group, whose transaction is committed (or rolled back when the transform failed) after the
   * transform is disposed. Register the extension point of {@code
   * SharedConnectionRun.registerConnectionGroupExtensionPoint()} first.
   */
  SynchronizeAfterMergeRun connectionGroup(String connectionGroup) {
    this.connectionGroup = connectionGroup;
    return this;
  }

  /** What one run did. */
  static final class Result {
    boolean initialized;

    /**
     * Whether the statements go out in batches at all: as batch mode always ran them, or recovered.
     */
    boolean batchMode;

    boolean recoveredBatches;
    boolean useSavepoints;

    final List<Object[]> output = new ArrayList<>();
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
    final List<String> debugLog = new ArrayList<>();
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
     * With {@link SynchronizeAfterMergeRun#watchCommits()}: the committed name of each main output
     * row's id the moment the row was passed on, as another connection sees it; null when the id is
     * not in the table.
     */
    final List<String> committedAtOutput = new ArrayList<>();

    /** The same for the rows on the error hop. */
    final List<String> committedAtError = new ArrayList<>();

    /**
     * The labels of the main output rows that were passed on before what they did was committed: an
     * insert or update whose name is not in the table yet, a delete whose row is still there.
     */
    List<String> passedOnBeforeTheirCommit() {
      List<String> labels = new ArrayList<>();
      for (int i = 0; i < output.size(); i++) {
        Object[] row = output.get(i);
        String committed = committedAtOutput.get(i);
        String op = (String) row[2];
        boolean done =
            switch (op) {
              case INSERT, UPDATE -> row[1].equals(committed);
              case DELETE -> committed == null;
              default -> true;
            };
        if (!done) {
          labels.add(label(row));
        }
      }
      return labels;
    }

    /** The label of every main output row, in order, see {@link #label(Object[])}. */
    List<String> outputLabels() {
      return output.stream().map(SynchronizeAfterMergeRun::label).collect(Collectors.toList());
    }

    /** The label of every error row, in order. */
    List<String> errorLabels() {
      return errors.stream().map(SynchronizeAfterMergeRun::label).collect(Collectors.toList());
    }

    /** The value of the given error field of every error row, in order. */
    List<Object> errorField(String name) {
      return errors.stream()
          .map(r -> r[errorRowMeta.indexOfValue(name)])
          .collect(Collectors.toList());
    }

    List<Object> errorCodes() {
      return errorField(ERROR_CODES);
    }

    /** What kind of message each error row carries, see {@link #kind(String)}. */
    List<String> errorKinds() {
      return errorField(ERROR_DESCRIPTIONS).stream()
          .map(d -> kind((String) d))
          .collect(Collectors.toList());
    }

    /**
     * lines input (lookups), output (inserts), updated (updates and deletes), skipped, written (to
     * the main output), rejected (to the error hop).
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

    /** How many debug lines contain the text, with {@link SynchronizeAfterMergeRun#debug()}. */
    long debugLines(String text) {
      return debugLog.stream().filter(line -> line.contains(text)).count();
    }

    boolean loggedError(String text) {
      return errorLog.stream().anyMatch(line -> line.contains(text));
    }
  }

  /**
   * The label of an input row: the first letter of its operation and its id, "i3" for the insert of
   * id 3, "s4" for a row that is skipped; with the table from a field, "@" and the table follow.
   */
  static String label(Object[] row) {
    String op = (String) row[2];
    String kind =
        switch (op == null ? "" : op) {
          case INSERT -> "i";
          case UPDATE -> "u";
          case DELETE -> "d";
          default -> "s";
        };
    String label = kind + row[0];
    if (row.length > 3 && row[3] instanceof String tbl) {
      label += "@" + tbl;
    }
    return label;
  }

  Result run(List<Object[]> rows) throws Exception {
    return run(tableNameInField ? idNameOpTableRowMeta() : idNameOpRowMeta(), rows);
  }

  Result run(IRowMeta inputRowMeta, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    SynchronizeAfterMergeMeta meta = meta(databaseMeta.getName(), table, commitSize);
    meta.setUsingBatchUpdates(batch);
    meta.setPerformingLookup(lookup);
    if (tableNameInField) {
      meta.setTableNameInField(true);
      meta.setTableNameField("tbl");
      meta.getLookup().setTableName("");
    }
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
    CommitWatcher watcher = watchCommits ? new CommitWatcher(databaseMeta, table) : null;
    IRowSet mainRowSet = new WatchedRowSet(watcher, result.committedAtOutput);
    mainRowSet.setThreadNameFromToCopy(TRANSFORM, 0, "main", 0);
    IRowSet errorRowSet = errorHop ? new WatchedRowSet(watcher, result.committedAtError) : null;

    SynchronizeAfterMergeData data = new SynchronizeAfterMergeData();
    SynchronizeAfterMerge transform =
        new SynchronizeAfterMerge(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
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
          public void logDetailed(String message) {
            result.detailedLog.add(message);
            super.logDetailed(message);
          }

          @Override
          public boolean isDetailed() {
            return true;
          }

          @Override
          public boolean isDebug() {
            return debug;
          }

          @Override
          public void logDebug(String message) {
            result.debugLog.add(message);
            super.logDebug(message);
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
      TransformMeta target = new TransformMeta(ERROR_TARGET, new SynchronizeAfterMergeMeta());
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
      transform.setVariable(Const.HOP_DATABASE_BATCH_RECOVERY, recovery);
    }
    if (connectionGroup != null) {
      transform.getExtensionDataMap().put(Const.CONNECTION_GROUP, connectionGroup);
    }
    result.initialized = transform.init();
    result.batchMode = data.batchMode || data.recoveredBatches;
    result.recoveredBatches = data.recoveredBatches;
    result.useSavepoints = data.useSavepoints;
    try {
      if (result.initialized) {
        if (shape != null) {
          data.db.setConnection(SimulatedBatchDriver.wrap(data.db.getConnection(), shape));
        }
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

  /**
   * The transform on the given connection and table: operation field {@code op}, key {@code id =
   * id}, column {@code id} written on insert only and column {@code name} always.
   */
  static SynchronizeAfterMergeMeta meta(String connection, String table, int commitSize) {
    SynchronizeAfterMergeMeta meta = new SynchronizeAfterMergeMeta();
    meta.setConnection(connection);
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setOperationOrderField("op");
    meta.setOrderInsert(INSERT);
    meta.setOrderUpdate(UPDATE);
    meta.setOrderDelete(DELETE);
    meta.getLookup().setSchemaName("");
    meta.getLookup().setTableName(table);
    SynchronizeAfterMergeMeta.KeyCondition key = new SynchronizeAfterMergeMeta.KeyCondition();
    key.setColumnName("id");
    key.setCondition("=");
    key.setFieldName("id");
    key.setFieldName2("");
    meta.getLookup().getKeyConditions().add(key);
    SynchronizeAfterMergeMeta.ValueUpdate id = new SynchronizeAfterMergeMeta.ValueUpdate();
    id.setColumnName("id");
    id.setFieldName("id");
    id.setUpdate(false);
    SynchronizeAfterMergeMeta.ValueUpdate name = new SynchronizeAfterMergeMeta.ValueUpdate();
    name.setColumnName("name");
    name.setFieldName("name");
    name.setUpdate(true);
    meta.getLookup().getValueUpdates().add(id);
    meta.getLookup().getValueUpdates().add(name);
    return meta;
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
        committed.add(watcher.committedName(rowData));
      }
      return super.putRow(rowMeta, rowData);
    }
  }

  /** Reads the committed name of an id over a connection of its own, in auto-commit. */
  private static final class CommitWatcher {
    private final Database db;
    private final String table;
    private final Map<String, PreparedStatement> selects = new HashMap<>();

    private CommitWatcher(DatabaseMeta databaseMeta, String table) throws Exception {
      this.table = table;
      db = new Database(new LoggingObject("watcher"), new Variables(), databaseMeta);
      db.connect();
      db.getConnection().setAutoCommit(true);
    }

    private String committedName(Object[] row) {
      String rowTable = row.length > 3 && row[3] instanceof String tbl ? tbl : table;
      try {
        PreparedStatement select = selects.get(rowTable);
        if (select == null) {
          select =
              db.getConnection().prepareStatement("SELECT name FROM " + rowTable + " WHERE id = ?");
          selects.put(rowTable, select);
        }
        select.setLong(1, (Long) row[0]);
        try (ResultSet resultSet = select.executeQuery()) {
          return resultSet.next() ? resultSet.getString(1) : null;
        }
      } catch (SQLException e) {
        throw new IllegalStateException("Unable to read the committed row of id " + row[0], e);
      }
    }

    private void close() {
      for (PreparedStatement select : selects.values()) {
        try {
          select.close();
        } catch (SQLException e) {
          // closing the connection closes it as well
        }
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

  /** id (Integer), name (String), op (String). */
  static IRowMeta idNameOpRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    rowMeta.addValueMeta(new ValueMetaString("name"));
    rowMeta.addValueMeta(new ValueMetaString("op"));
    return rowMeta;
  }

  /** id (Integer), name (String), op (String), tbl (String). */
  static IRowMeta idNameOpTableRowMeta() {
    IRowMeta rowMeta = idNameOpRowMeta();
    rowMeta.addValueMeta(new ValueMetaString("tbl"));
    return rowMeta;
  }

  static Object[] row(String op, long id, String name) {
    return new Object[] {id, name, op};
  }

  /** Inserts id, named "r" and the id. */
  static Object[] insert(long id) {
    return row(INSERT, id, "r" + id);
  }

  /** Updates id to "r" and the id. */
  static Object[] update(long id) {
    return row(UPDATE, id, "r" + id);
  }

  /** Deletes id. */
  static Object[] delete(long id) {
    return row(DELETE, id, "r" + id);
  }

  /** A row the transform skips: its operation is none of the three. */
  static Object[] nothing(long id) {
    return row(NOTHING, id, "r" + id);
  }

  /** The row with the name the tables refuse. */
  static Object[] bad(Object[] row) {
    Object[] bad = row.clone();
    bad[1] = BAD;
    return bad;
  }

  /** The row with another name. */
  static Object[] named(Object[] row, String name) {
    Object[] named = row.clone();
    named[1] = name;
    return named;
  }

  /** The row for the given table, with the table name in field {@code tbl}. */
  static Object[] in(String table, Object[] row) {
    return new Object[] {row[0], row[1], row[2], table};
  }

  static List<Object[]> rows(Object[]... rows) {
    return List.of(rows);
  }

  static List<String> labels(String... labels) {
    return List.of(labels);
  }

  // Error description helpers

  static final String ROW = "row";
  static final String BATCH = "batch";
  static final String DRIVER = "driver";
  static final String NOT_FOUND = "not-found";
  static final String NOT_ATTEMPTED = "not-attempted";

  /**
   * Classifies an error description: {@code not-found} for a key that is not in the table, {@code
   * not-attempted} for a row of a batch that the database never ran, {@code batch} for the
   * exception of a whole batch, {@code row} for the exception of a single statement, {@code driver}
   * for an exception of the JDBC driver itself, such as one chained to a batch failure.
   */
  static String kind(String description) {
    if (description == null) {
      return null;
    }
    if (description.contains("Entry to update with following key could not be found")) {
      return NOT_FOUND;
    }
    if (description.contains("not sent to the database")) {
      return NOT_ATTEMPTED;
    }
    if (description.contains("Error updating batch")
        || description.contains("Error batch inserting")) {
      return BATCH;
    }
    if (description.contains("Error inserting/updating row")) {
      return ROW;
    }
    if (description.startsWith("org.h2.")
        || description.startsWith("org.postgresql.")
        || description.startsWith("org.duckdb.")
        || description.startsWith("org.apache.derby.")
        || description.startsWith("java.sql.")) {
      return DRIVER;
    }
    return "other: " + description;
  }

  // Database helpers

  /** The target table, refusing {@link #BAD} as a name. */
  static String createTable(String table) {
    return "CREATE TABLE "
        + table
        + " (id BIGINT NOT NULL PRIMARY KEY, name VARCHAR(20) NOT NULL CHECK (name <> '"
        + BAD
        + "'))";
  }

  /** A table of rows that refer to rows of the target table, so those can't be deleted. */
  static String createChildTable(String table) {
    return "CREATE TABLE "
        + table
        + "_child (id BIGINT NOT NULL PRIMARY KEY, parent BIGINT NOT NULL REFERENCES "
        + table
        + " (id))";
  }

  /**
   * Creates the table and its child table: the table holds the given ids named {@link #SEED}, the
   * child table nothing.
   */
  static void createTarget(DatabaseMeta databaseMeta, String table, long... ids) throws Exception {
    List<String> statements = new ArrayList<>();
    statements.add(createTable(table));
    statements.add(createChildTable(table));
    if (ids.length > 0) {
      StringBuilder insert = new StringBuilder("INSERT INTO " + table + " (id, name) VALUES ");
      for (int i = 0; i < ids.length; i++) {
        if (i > 0) {
          insert.append(", ");
        }
        insert.append('(').append(ids[i]).append(", '").append(SEED).append("')");
      }
      statements.add(insert.toString());
    }
    execute(databaseMeta, statements.toArray(new String[0]));
  }

  /** Adds a child row for each of the given ids, so that their delete fails. */
  static void refer(DatabaseMeta databaseMeta, String table, long... ids) throws Exception {
    List<String> statements = new ArrayList<>();
    for (long id : ids) {
      statements.add(
          "INSERT INTO " + table + "_child (id, parent) VALUES (" + id + ", " + id + ")");
    }
    execute(databaseMeta, statements.toArray(new String[0]));
  }

  static long[] range(long from, long to) {
    long[] ids = new long[(int) (to - from + 1)];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = from + i;
    }
    return ids;
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

  /** The table after a run, as "id:name" in id order. */
  static List<String> table(DatabaseMeta databaseMeta, String table) throws Exception {
    return query(databaseMeta, "SELECT id, name FROM " + table + " ORDER BY id");
  }

  static List<String> table(DatabaseMeta databaseMeta) throws Exception {
    return table(databaseMeta, TARGET);
  }
}
