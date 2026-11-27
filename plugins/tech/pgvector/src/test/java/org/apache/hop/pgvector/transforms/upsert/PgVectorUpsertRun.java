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

package org.apache.hop.pgvector.transforms.upsert;

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
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs one pgvector upsert end to end (init, processRow until done, dispose) against a real
 * PostgreSQL with pgvector, and records everything it can be observed doing: the rows on the main
 * output and on the error hop (with the error fields), in the order they left, the transform's
 * counters and its log lines.
 *
 * <p>The main output and the error hop are real row sets, so the engine's own {@code putRow} and
 * {@code putError} run and update the counters as they would in a pipeline. The loop is the one of
 * the engine's run thread: processRow until it returns false or the transform is stopped, then
 * dispose; an exception out of processRow sets the errors and stops the transform, as there.
 *
 * <p>Input rows are (document_id, chunk_index, chunk_text, embedding), the embedding a JSON array.
 * A row is named by the id the transform builds for it: {@code <document_id>_<chunk_index>}.
 */
final class PgVectorUpsertRun {

  static final String CONNECTION = "pg";
  static final String TRANSFORM = "upsert";
  static final String ERROR_TARGET = "upsert errors";
  static final String NR_ERRORS = "nr_errors";
  static final String ERROR_DESCRIPTIONS = "error_descriptions";
  static final String ERROR_FIELDS = "error_fields";
  static final String ERROR_CODES = "error_codes";

  /** The content the table refuses, by a check constraint. */
  static final String BAD = "bad";

  /** An embedding of three dimensions, which the vector(2) column refuses. */
  static final String WRONG_DIMENSION = "[1,2,3]";

  static final String EMBEDDING = "[1,2]";

  /** Something to do after the transform processed a row. */
  @FunctionalInterface
  interface IAfterRow {
    /**
     * @param processed how many times processRow returned true so far
     */
    void after(int processed, PgVectorUpsert transform) throws Exception;
  }

  private final DatabaseMeta databaseMeta;
  private final String table;
  private int commitSize = 100;
  private boolean errorHop;
  private String recovery;
  private Consumer<PgVectorUpsertMeta> configure = meta -> {};
  private IAfterRow afterRow = (processed, transform) -> {};
  private boolean watchCommits;
  private boolean discardRows;
  private String connectionGroup;

  private PgVectorUpsertRun(DatabaseMeta databaseMeta, String table) {
    this.databaseMeta = databaseMeta;
    this.table = table;
  }

  static PgVectorUpsertRun on(DatabaseMeta databaseMeta, String table) {
    return new PgVectorUpsertRun(databaseMeta, table);
  }

  PgVectorUpsertRun commitSize(int commitSize) {
    this.commitSize = commitSize;
    return this;
  }

  PgVectorUpsertRun errorHop(boolean errorHop) {
    this.errorHop = errorHop;
    return this;
  }

  /** Sets the variable HOP_DATABASE_BATCH_RECOVERY for the run; null (the default) leaves it. */
  PgVectorUpsertRun recovery(String recovery) {
    this.recovery = recovery;
    return this;
  }

  PgVectorUpsertRun deleteDocumentBeforeUpsert() {
    return configure(meta -> meta.setDeleteDocumentBeforeUpsert(true));
  }

  PgVectorUpsertRun configure(Consumer<PgVectorUpsertMeta> configure) {
    this.configure = this.configure.andThen(configure);
    return this;
  }

  PgVectorUpsertRun afterRow(IAfterRow afterRow) {
    this.afterRow = afterRow;
    return this;
  }

  /**
   * Looks up, over a connection of its own, what the table holds for every row the moment it is
   * passed on: {@link Result#committedAtOutput} and {@link Result#committedAtError}.
   */
  PgVectorUpsertRun watchCommits() {
    this.watchCommits = true;
    return this;
  }

  /**
   * Counts the rows on the main output and the error hop without keeping them, for runs too large
   * to hold: a row set that fills up slows the transform down by a millisecond per row. Only the
   * counters of the result are filled in.
   */
  PgVectorUpsertRun discardRows() {
    this.discardRows = true;
    return this;
  }

  /**
   * Runs the transform the way a transactional pipeline does: its database joins this connection
   * group, whose transaction is committed (or rolled back when the transform failed) after the
   * transform is disposed. Register the extension point of {@code
   * SharedConnectionRun.registerConnectionGroupExtensionPoint()} first.
   */
  PgVectorUpsertRun connectionGroup(String connectionGroup) {
    this.connectionGroup = connectionGroup;
    return this;
  }

  /** What one run did. */
  static final class Result {
    boolean initialized;
    boolean batchMode;
    boolean useSafePoints;
    boolean recoveredBatches;

    final List<Object[]> output = new ArrayList<>();
    final List<Object[]> errors = new ArrayList<>();
    IRowMeta errorRowMeta;

    /** "out:" or "err:" and the name of the row, for every row in the order it left. */
    final List<String> events = new ArrayList<>();

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
    long linesWritten;
    long linesRejected;
    long linesOutput;
    long linesUpdated;
    long errorCount;
    boolean stopped;

    /** What processRow threw, if anything. */
    Throwable thrown;

    /**
     * With {@link PgVectorUpsertRun#watchCommits()}: the committed content of each main output
     * row's id the moment the row was passed on, as another connection sees it; null when the id is
     * not in the table.
     */
    final List<String> committedAtOutput = new ArrayList<>();

    /** The same for the rows on the error hop. */
    final List<String> committedAtError = new ArrayList<>();

    /**
     * The main output rows that were passed on before the content they carry was committed. Rows
     * that store nothing (no content or no embedding) are left out.
     */
    List<String> passedOnBeforeTheirCommit() {
      List<String> names = new ArrayList<>();
      for (int i = 0; i < output.size(); i++) {
        Object[] row = output.get(i);
        if (!stores(row)) {
          continue;
        }
        if (!row[2].equals(committedAtOutput.get(i))) {
          names.add(name(row));
        }
      }
      return names;
    }

    /** The error rows whose content was committed when they were sent to the error hop. */
    List<String> rejectedButCommitted() {
      List<String> names = new ArrayList<>();
      for (int i = 0; i < errors.size(); i++) {
        Object[] row = errors.get(i);
        if (row[2] != null && row[2].equals(committedAtError.get(i))) {
          names.add(name(row));
        }
      }
      return names;
    }

    List<String> outputNames() {
      return output.stream().map(PgVectorUpsertRun::name).collect(Collectors.toList());
    }

    List<String> errorNames() {
      return errors.stream().map(PgVectorUpsertRun::name).collect(Collectors.toList());
    }

    List<Object> errorField(String name) {
      return errors.stream()
          .map(r -> r[errorRowMeta.indexOfValue(name)])
          .collect(Collectors.toList());
    }

    List<Object> errorCodes() {
      return errorField(ERROR_CODES);
    }

    List<Object> errorDescriptions() {
      return errorField(ERROR_DESCRIPTIONS);
    }

    /** lines read, written (to the output), rejected (to the error hop), output, updated. */
    List<Long> counters() {
      return List.of(linesRead, linesWritten, linesRejected, linesOutput, linesUpdated);
    }

    boolean logged(String text) {
      return basicLog.stream().anyMatch(line -> line.contains(text));
    }

    boolean loggedDetailed(String text) {
      return detailedLog.stream().anyMatch(line -> line.contains(text));
    }

    boolean loggedError(String text) {
      return errorLog.stream().anyMatch(line -> line.contains(text));
    }
  }

  Result run(List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(provider);

    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setConnection(databaseMeta.getName());
    meta.setSchemaName("public");
    meta.setTableName(table);
    meta.setDocumentIdField("document_id");
    meta.setChunkIndexField("chunk_index");
    meta.setContentField("chunk_text");
    meta.setEmbeddingField("embedding");
    meta.setEmbeddingDimensions("2");
    // The tests create their tables: the schema options switch the connection to autocommit for
    // the DDL, which is not what they are about.
    meta.setCreateTableIfMissing(false);
    meta.setCreateHnswIndex(false);
    meta.setCommitSize(Integer.toString(commitSize));
    configure.accept(meta);

    TransformMeta transformMeta = new TransformMeta(TRANSFORM, meta);
    pipelineMeta.addTransform(transformMeta);

    LocalPipelineEngine pipeline = new LocalPipelineEngine();
    // putRow waits up to three seconds for a pipeline to start running
    pipeline.setRunning(true);

    IRowMeta inputRowMeta = inputRowMeta();
    Queue<Object[]> input = new ArrayDeque<>();
    for (Object[] row : rows) {
      input.add(row.clone());
    }
    Result result = new Result();
    CommitWatcher watcher = watchCommits ? new CommitWatcher(databaseMeta, table) : null;
    IRowSet mainRowSet =
        discardRows
            ? new DiscardingRowSet()
            : new WatchedRowSet(watcher, result.committedAtOutput, result.events, "out");
    mainRowSet.setThreadNameFromToCopy(TRANSFORM, 0, "main", 0);
    IRowSet errorRowSet = null;
    if (errorHop) {
      errorRowSet =
          discardRows
              ? new DiscardingRowSet()
              : new WatchedRowSet(watcher, result.committedAtError, result.events, "err");
    }
    IRowSet errors = errorRowSet;

    PgVectorUpsertData data = new PgVectorUpsertData();
    PgVectorUpsert transform =
        new PgVectorUpsert(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public IHopMetadataProvider getMetadataProvider() {
            return provider;
          }

          @Override
          public Object[] getRow() {
            result.outputSizeAtRead.add(mainRowSet.size());
            result.errorSizeAtRead.add(errors == null ? 0 : errors.size());
            Object[] row = input.poll();
            if (row != null) {
              // What the engine's getRow counts
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
          public boolean isDetailed() {
            return true;
          }

          @Override
          public void logDetailed(String message) {
            result.detailedLog.add(message);
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
      // After the transform: its constructor looks at the hops of the pipeline.
      TransformMeta target = new TransformMeta(ERROR_TARGET, new PgVectorUpsertMeta());
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
      // Set on the first row, when the transform connects
      result.batchMode = data.batchMode;
      result.useSafePoints = data.useSafePoints;
      result.recoveredBatches = data.recoveredBatches;
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
    result.linesRead = transform.getLinesRead();
    result.linesWritten = transform.getLinesWritten();
    result.linesRejected = transform.getLinesRejected();
    result.linesOutput = transform.getLinesOutput();
    result.linesUpdated = transform.getLinesUpdated();
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

  /** A row set that takes every row and keeps none, so it never fills up. */
  private static final class DiscardingRowSet extends QueueRowSet {
    @Override
    public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
      setRowMeta(rowMeta);
      return true;
    }
  }

  /** A main output or error row set that notes every row put on it. */
  private static final class WatchedRowSet extends QueueRowSet {
    private final CommitWatcher watcher;
    private final List<String> committed;
    private final List<String> events;
    private final String stream;

    private WatchedRowSet(
        CommitWatcher watcher, List<String> committed, List<String> events, String stream) {
      this.watcher = watcher;
      this.committed = committed;
      this.events = events;
      this.stream = stream;
    }

    @Override
    public boolean putRow(IRowMeta rowMeta, Object[] rowData) {
      events.add(stream + ":" + name(rowData));
      if (watcher != null) {
        committed.add(watcher.committedContent(rowData));
      }
      return super.putRow(rowMeta, rowData);
    }
  }

  /** Reads the committed content of a row's id over a connection of its own, in auto-commit. */
  private static final class CommitWatcher {
    private final Database db;
    private final PreparedStatement select;

    private CommitWatcher(DatabaseMeta databaseMeta, String table) throws Exception {
      db = new Database(new LoggingObject("watcher"), new Variables(), databaseMeta);
      db.connect();
      db.getConnection().setAutoCommit(true);
      select =
          db.getConnection().prepareStatement("SELECT content FROM " + table + " WHERE id = ?");
    }

    private String committedContent(Object[] row) {
      if (row[0] == null) {
        return null;
      }
      try {
        select.setString(1, name(row));
        try (ResultSet resultSet = select.executeQuery()) {
          return resultSet.next() ? resultSet.getString(1) : null;
        }
      } catch (SQLException e) {
        throw new IllegalStateException("Unable to read the committed row " + name(row), e);
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

  /** document_id (String), chunk_index (Integer), chunk_text (String), embedding (String). */
  static IRowMeta inputRowMeta() {
    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaString("document_id"));
    rowMeta.addValueMeta(new ValueMetaInteger("chunk_index"));
    rowMeta.addValueMeta(new ValueMetaString("chunk_text"));
    rowMeta.addValueMeta(new ValueMetaString("embedding"));
    return rowMeta;
  }

  /** Whether the transform writes the row to the table: it has content and an embedding. */
  static boolean stores(Object[] row) {
    return row[2] != null
        && !((String) row[2]).isEmpty()
        && row[3] != null
        && !((String) row[3]).isBlank();
  }

  /** The id the transform builds for a row, which names it in the tests. */
  static String name(Object[] row) {
    return row[0] + "_" + (row[1] == null ? 0 : row[1]);
  }

  static Object[] row(String document, long chunk, String content, String embedding) {
    return new Object[] {document, chunk, content, embedding};
  }

  /** Chunks 1..n of document "doc" with content rN; the bad ones have a vector of 3 dimensions. */
  static List<Object[]> rows(int n, long... bad) {
    List<Object[]> rows = new ArrayList<>();
    for (long chunk = 1; chunk <= n; chunk++) {
      rows.add(row("doc", chunk, "r" + chunk, contains(bad, chunk) ? WRONG_DIMENSION : EMBEDDING));
    }
    return rows;
  }

  static boolean contains(long[] values, long value) {
    for (long candidate : values) {
      if (candidate == value) {
        return true;
      }
    }
    return false;
  }

  /** The names doc_1..doc_n, without the given chunks. */
  static List<String> namesWithout(int n, long... without) {
    List<String> names = new ArrayList<>();
    for (long chunk = 1; chunk <= n; chunk++) {
      if (!contains(without, chunk)) {
        names.add("doc_" + chunk);
      }
    }
    return names;
  }

  /** The names of the given chunks of document "doc". */
  static List<String> names(long... chunks) {
    List<String> names = new ArrayList<>();
    for (long chunk : chunks) {
      names.add("doc_" + chunk);
    }
    return names;
  }

  /** The events of rows 1..n of document "doc" that leave in input order. */
  static List<String> eventsInInputOrder(int n, long... bad) {
    List<String> events = new ArrayList<>();
    for (long chunk = 1; chunk <= n; chunk++) {
      events.add((contains(bad, chunk) ? "err:" : "out:") + "doc_" + chunk);
    }
    return events;
  }

  /** The table after a run in which chunks 1..n were stored except the given ones: id:content. */
  static List<String> tableStoredExcept(int n, long... notStored) {
    List<String> lines = new ArrayList<>();
    for (long chunk = 1; chunk <= n; chunk++) {
      if (!contains(notStored, chunk)) {
        lines.add("doc_" + chunk + ":r" + chunk);
      }
    }
    lines.sort(null);
    return lines;
  }

  // Database helpers

  /**
   * Creates the table the transform writes to: content is at most 20 characters and must not be
   * {@link #BAD}, the embedding has two dimensions.
   */
  static void createTable(DatabaseMeta databaseMeta, String table) throws Exception {
    execute(
        databaseMeta,
        "DROP TABLE IF EXISTS " + table,
        "CREATE TABLE "
            + table
            + " (id TEXT PRIMARY KEY, document_id TEXT, chunk_index INTEGER,"
            + " content VARCHAR(20) NOT NULL CHECK (content <> '"
            + BAD
            + "'), embedding vector(2))");
  }

  /** Stores chunks 0..chunks-1 of a document with content "old", as an earlier run would. */
  static void seed(DatabaseMeta databaseMeta, String table, String document, int chunks)
      throws Exception {
    List<String> statements = new ArrayList<>();
    for (int chunk = 0; chunk < chunks; chunk++) {
      statements.add(
          "INSERT INTO "
              + table
              + " (id, document_id, chunk_index, content, embedding) VALUES ('"
              + document
              + "_"
              + chunk
              + "', '"
              + document
              + "', "
              + chunk
              + ", 'old', '[0,1]')");
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

  /** The table as "id:content" lines, sorted. */
  static List<String> table(DatabaseMeta databaseMeta, String table) throws Exception {
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      List<String> lines = new ArrayList<>();
      for (Object[] row : db.getRows("SELECT id, content FROM " + table, -1)) {
        lines.add(row[0] + ":" + row[1]);
      }
      // Sorted here: the order of the database's collation is not the one of Java
      lines.sort(null);
      return lines;
    }
  }
}
