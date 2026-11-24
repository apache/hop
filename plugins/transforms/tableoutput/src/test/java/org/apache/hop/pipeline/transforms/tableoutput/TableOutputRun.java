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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.QueueRowSet;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseBatchException;
import org.apache.hop.core.exception.HopDatabaseException;
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
 * Runs one Table output end to end (init, processRow until done, dispose) against a real database
 * and records everything it can be observed doing: the rows on the main output and on the error hop
 * (with the error fields), and the transform's counters.
 *
 * <p>The main output and the error hop are real row sets, so the engine's own {@code putRow} and
 * {@code putError} run and update the counters as they would in a pipeline.
 */
final class TableOutputRun {

  static final String TRANSFORM = "table output";
  static final String ERROR_TARGET = "table output errors";
  static final String NR_ERRORS = "nr_errors";
  static final String ERROR_DESCRIPTIONS = "error_descriptions";
  static final String ERROR_FIELDS = "error_fields";
  static final String ERROR_CODES = "error_codes";

  private final DatabaseMeta databaseMeta;
  private String table = "target";
  private int commitSize = 100;
  private boolean batch;
  private boolean errorHop;
  private Consumer<TableOutputMeta> configure = meta -> {};

  /**
   * Value of the variable HOP_DATABASE_BATCH_RECOVERY for the run. LEGACY (the default) asks for
   * the batch handling Table output always had, so these runs pin that handling also on databases
   * whose batch failure contract has been validated; null leaves the variable unset.
   */
  private String recovery = "LEGACY";

  private TableOutputRun(DatabaseMeta databaseMeta) {
    this.databaseMeta = databaseMeta;
  }

  static TableOutputRun on(DatabaseMeta databaseMeta) {
    return new TableOutputRun(databaseMeta);
  }

  TableOutputRun table(String table) {
    this.table = table;
    return this;
  }

  TableOutputRun commitSize(int commitSize) {
    this.commitSize = commitSize;
    return this;
  }

  TableOutputRun batch(boolean batch) {
    this.batch = batch;
    return this;
  }

  TableOutputRun errorHop(boolean errorHop) {
    this.errorHop = errorHop;
    return this;
  }

  TableOutputRun recovery(String recovery) {
    this.recovery = recovery;
    return this;
  }

  TableOutputRun configure(Consumer<TableOutputMeta> configure) {
    this.configure = configure;
    return this;
  }

  /** What one run did. */
  static final class Result {
    boolean initialized;
    boolean batchMode;
    boolean useSafePoints;
    boolean recoveredBatches;
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

    long linesOutput;
    long linesWritten;
    long linesRejected;
    long errorCount;
    boolean stopped;

    /** The value of the first field of every main output row, in order. */
    List<Object> outputKeys() {
      return output.stream().map(r -> r[0]).collect(Collectors.toList());
    }

    /** The value of the first field of every error row, in order. */
    List<Object> errorKeys() {
      return errors.stream().map(r -> r[0]).collect(Collectors.toList());
    }

    List<String> errorCodes() {
      return errors.stream()
          .map(r -> (String) r[errorRowMeta.indexOfValue(ERROR_CODES)])
          .collect(Collectors.toList());
    }

    List<String> errorDescriptions() {
      return errors.stream()
          .map(r -> (String) r[errorRowMeta.indexOfValue(ERROR_DESCRIPTIONS)])
          .collect(Collectors.toList());
    }

    /** What kind of message each error row carries, see {@link #kind(String)}. */
    List<String> errorKinds() {
      return errorDescriptions().stream().map(TableOutputRun::kind).collect(Collectors.toList());
    }

    /** The key the database named in each error row's message, see {@link #failingKey(String)}. */
    List<Long> errorMessageKeys() {
      return errorDescriptions().stream()
          .map(TableOutputRun::failingKey)
          .collect(Collectors.toList());
    }

    List<Object> errorField(String name) {
      return errors.stream()
          .map(r -> r[errorRowMeta.indexOfValue(name)])
          .collect(Collectors.toList());
    }
  }

  Result run(IRowMeta inputRowMeta, List<Object[]> rows) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    TableOutputMeta meta = new TableOutputMeta();
    meta.setDefault();
    meta.setConnection(databaseMeta.getName());
    meta.setSchemaName("");
    meta.setTableName(table);
    meta.setCommitSize(Integer.toString(commitSize));
    meta.setUseBatchUpdate(batch);
    configure.accept(meta);

    TransformMeta transformMeta = new TransformMeta(TRANSFORM, meta);
    pipelineMeta.addTransform(transformMeta);

    LocalPipelineEngine pipeline = new LocalPipelineEngine();
    // putRow waits up to three seconds for a pipeline to start running
    pipeline.setRunning(true);

    Queue<Object[]> input = new ArrayDeque<>();
    for (Object[] row : rows) {
      input.add(row.clone());
    }
    IRowSet mainRowSet = new QueueRowSet();
    mainRowSet.setThreadNameFromToCopy(TRANSFORM, 0, "main", 0);
    IRowSet errorRowSet = errorHop ? new QueueRowSet() : null;
    Result result = new Result();

    TableOutputData data = new TableOutputData();
    TableOutput transform =
        new TableOutput(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
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
        };
    transform.addRowSetToOutputRowSets(mainRowSet);

    if (errorHop) {
      TransformMeta target = new TransformMeta(ERROR_TARGET, meta);
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
    result.initialized = transform.init();
    result.batchMode = data.batchMode;
    result.useSafePoints = data.useSafePoints;
    result.recoveredBatches = data.recoveredBatches;
    try {
      if (result.initialized) {
        while (transform.processRow()) {
          // keep going
        }
      }
    } finally {
      transform.dispose();
    }

    drain(mainRowSet, result.output);
    result.outputRowMeta = mainRowSet.getRowMeta();
    if (errorRowSet != null) {
      drain(errorRowSet, result.errors);
      result.errorRowMeta = errorRowSet.getRowMeta();
    }
    result.linesOutput = transform.getLinesOutput();
    result.linesWritten = transform.getLinesWritten();
    result.linesRejected = transform.getLinesRejected();
    result.errorCount = transform.getErrors();
    result.stopped = transform.isStopped();
    return result;
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

  /** Rows 1..n, named r1..rn. */
  static List<Object[]> idNameRows(int n) {
    List<Object[]> rows = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      rows.add(new Object[] {id, "r" + id});
    }
    return rows;
  }

  /** 1..n as longs, without the given ids. */
  static List<Object> idsWithout(int n, long... without) {
    List<Object> ids = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      boolean skip = false;
      for (long w : without) {
        skip |= w == id;
      }
      if (!skip) {
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

  /** "id:rN" for 1..n, except the seeded ids, which read "id:seed". */
  static List<String> tableWithSeeds(int n, long... seeded) {
    List<String> lines = new ArrayList<>();
    for (long id = 1; id <= n; id++) {
      boolean seed = false;
      for (long s : seeded) {
        seed |= s == id;
      }
      lines.add(id + ":" + (seed ? "seed" : "r" + id));
    }
    return lines;
  }

  // Error description helpers

  static final String ROW = "row";
  static final String BATCH = "batch";
  static final String NOT_SENT = "not-sent";
  static final String DRIVER = "driver";

  /**
   * Classifies an error description: {@code row} for the HopDatabaseException of a single insert,
   * {@code batch} for the HopDatabaseBatchException of a whole batch, {@code not-sent} for the "row
   * was not sent" message, {@code driver} for a driver exception chained to the batch failure.
   */
  static String kind(String description) {
    if (description == null) {
      return null;
    }
    if (description.startsWith(HopDatabaseBatchException.class.getName() + ": ")
        && description.contains("Error updating batch")) {
      return BATCH;
    }
    if (description.startsWith(HopDatabaseException.class.getName() + ": ")
        && description.contains("Error inserting/updating row")) {
      return ROW;
    }
    if (description.startsWith("This row was not sent to the database")) {
      return NOT_SENT;
    }
    if (description.startsWith("org.h2.jdbc.") || description.startsWith("org.postgresql.")) {
      return DRIVER;
    }
    return "other: " + description;
  }

  private static final Pattern H2_KEY = Pattern.compile("CAST\\((\\d+) AS BIGINT\\)");
  private static final Pattern PG_KEY = Pattern.compile("Key \\(id\\)=\\((\\d+)\\)");

  /** The id the database names as the duplicate in a message (H2 or PostgreSQL), or null. */
  static Long failingKey(String description) {
    if (description == null) {
      return null;
    }
    for (Pattern pattern : new Pattern[] {H2_KEY, PG_KEY}) {
      Matcher matcher = pattern.matcher(description);
      if (matcher.find()) {
        return Long.parseLong(matcher.group(1));
      }
    }
    return null;
  }

  // Database helpers

  static void execute(DatabaseMeta databaseMeta, String... statements) throws Exception {
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
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
}
