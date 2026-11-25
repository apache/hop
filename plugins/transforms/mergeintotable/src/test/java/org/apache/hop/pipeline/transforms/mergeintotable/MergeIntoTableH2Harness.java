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

package org.apache.hop.pipeline.transforms.mergeintotable;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.database.IDatabase;
import org.apache.hop.core.database.dml.DmlSyntax;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.duckdb.DuckDBDatabaseMeta;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs a real {@link MergeIntoTable} against an in-memory H2 database. The connection's dialect is
 * {@link MergeTestH2Database}, so each test decides which MERGE syntax (if any) the database has.
 * Only the row hand-off with neighbouring transforms (getRow / putRow / putError) is replaced. The
 * same harness also runs on any other connection, see {@link
 * #MergeIntoTableH2Harness(DatabaseMeta)}.
 */
class MergeIntoTableH2Harness {

  static final String CONNECTION = "merge-h2";

  /** A row handed to the error stream. */
  record ErrorRow(Object[] row, String description, String code) {}

  final DatabaseMeta databaseMeta;
  final PipelineMeta pipelineMeta;
  final List<Object[]> output = new ArrayList<>();
  final List<ErrorRow> errors = new ArrayList<>();
  final List<String> loggedErrors = new ArrayList<>();
  final Deque<Object[]> input = new ArrayDeque<>();

  static void registerH2() throws HopException {
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    // The JDBC driver is loaded through the plugin of the dialect's class.
    DatabasePluginType.getInstance().registerClassPathPlugin(MergeTestH2Database.class);
  }

  static void registerDuckDb() throws HopException {
    DatabasePluginType.getInstance().registerClassPathPlugin(DuckDBDatabaseMeta.class);
  }

  /** An in-memory H2 database with its own dialect, as it declares its DML syntax. */
  static DatabaseMeta h2() {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            CONNECTION,
            "H2",
            "Native",
            "",
            "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "",
            "",
            "");
    databaseMeta.setName(CONNECTION);
    return databaseMeta;
  }

  /** A DuckDB database in the given file, with its own dialect. */
  static DatabaseMeta duckDb(String file) {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(CONNECTION, "DuckDB", "Native", "", file, "", "", "");
    databaseMeta.setName(CONNECTION);
    return databaseMeta;
  }

  MergeIntoTableH2Harness(DmlSyntax syntax) throws HopException {
    this(h2(), syntax, true);
  }

  /** Runs on the given connection with the DML syntax its dialect declares. */
  static MergeIntoTableH2Harness on(DatabaseMeta databaseMeta) throws HopException {
    return new MergeIntoTableH2Harness(databaseMeta, null, false);
  }

  private MergeIntoTableH2Harness(DatabaseMeta databaseMeta, DmlSyntax syntax, boolean testSyntax)
      throws HopException {
    this.databaseMeta = databaseMeta;
    if (testSyntax) {
      databaseMeta.setIDatabase(dialect(databaseMeta.getIDatabase(), syntax));
    }

    pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
  }

  /** The H2 connection settings on a dialect that declares the given MERGE syntax. */
  static IDatabase dialect(IDatabase h2, DmlSyntax syntax) {
    MergeTestH2Database dialect = new MergeTestH2Database(syntax);
    dialect.setPluginId(MergeTestH2Database.TYPE);
    dialect.setPluginName(h2.getPluginName());
    dialect.setAccessType(h2.getAccessType());
    dialect.setHostname(h2.getHostname());
    dialect.setDatabaseName(h2.getDatabaseName());
    dialect.setUsername(h2.getUsername());
    dialect.setPassword(h2.getPassword());
    return dialect;
  }

  /** Runs SQL statements on the test database (DDL / inserts). */
  void execute(String... statements) throws HopException {
    try (Database db = new Database(new LoggingObject("h2-setup"), new Variables(), databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
      // DuckDB refuses a commit in auto-commit mode, where there is nothing left to commit.
      if (!autoCommit(db)) {
        db.commit(true);
      }
    }
  }

  private static boolean autoCommit(Database db) throws HopException {
    try {
      return db.getConnection().getAutoCommit();
    } catch (SQLException e) {
      throw new HopException(e);
    }
  }

  /** Reads rows from the test database on a connection of its own. */
  List<Object[]> query(String sql) throws HopException {
    try (Database db = new Database(new LoggingObject("h2-check"), new Variables(), databaseMeta)) {
      db.connect();
      return db.getRows(sql, -1);
    }
  }

  /** Builds the transform on {@link #input}; call {@link #run} or drive it yourself. */
  MergeIntoTable create(MergeIntoTableMeta meta, IRowMeta inputRowMeta, boolean errorHandling) {
    meta.setConnection(CONNECTION);
    TransformMeta transformMeta = new TransformMeta("merge", meta);
    pipelineMeta.addTransform(transformMeta);

    MergeIntoTable transform =
        new MergeIntoTable(
            transformMeta,
            meta,
            new MergeIntoTableData(),
            0,
            pipelineMeta,
            new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            Object[] row = input.poll();
            if (row != null) {
              incrementLinesRead();
            }
            return row;
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            output.add(row);
          }

          @Override
          public void putError(
              IRowMeta rowMeta,
              Object[] row,
              long nrErrors,
              String errorDescriptions,
              String fieldNames,
              String errorCodes) {
            errors.add(new ErrorRow(row, errorDescriptions, errorCodes));
            incrementLinesRejected();
          }

          @Override
          public void logError(String message) {
            loggedErrors.add(message);
            super.logError(message);
          }
        };

    // Error handling is switched on after the transform is built, so the constructor does not go
    // looking for row sets towards the error target; putError is replaced above anyway.
    if (errorHandling) {
      TransformMeta target = new TransformMeta("errors", new MergeIntoTableMeta());
      pipelineMeta.addTransform(target);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
      TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
      errorMeta.setEnabled(true);
      transformMeta.setTransformErrorMeta(errorMeta);
    }
    return transform;
  }

  /**
   * Runs the transform over the given rows: init, every row, dispose.
   *
   * @return the transform, to read its counters and errors
   */
  MergeIntoTable run(
      MergeIntoTableMeta meta, IRowMeta inputRowMeta, List<Object[]> rows, boolean errorHandling)
      throws HopException {
    input.addAll(rows);
    MergeIntoTable transform = create(meta, inputRowMeta, errorHandling);
    if (!transform.init()) {
      throw new HopException("init failed: " + loggedErrors);
    }
    try {
      while (transform.processRow()) {
        // keep going
      }
    } finally {
      transform.dispose();
    }
    return transform;
  }
}
