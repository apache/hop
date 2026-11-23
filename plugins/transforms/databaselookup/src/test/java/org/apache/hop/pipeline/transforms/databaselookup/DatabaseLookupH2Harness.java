/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.databaselookup;

import static org.mockito.Mockito.spy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

/**
 * Runs a real {@link DatabaseLookup} against an in-memory H2 database: the transform's own code
 * (connection, table metadata, load-all query, cache, lookups) is exercised for real; only the row
 * hand-off with neighbouring transforms (getRow / putRow / putError) is replaced.
 */
class DatabaseLookupH2Harness {

  static final String CONNECTION = "lookup-h2";

  /** A row handed to the error stream. */
  record ErrorRow(Object[] row, String description, String fields, String code) {}

  final DatabaseMeta databaseMeta;
  final List<Object[]> output = new ArrayList<>();
  final List<ErrorRow> errors = new ArrayList<>();
  final List<Database> databases = new ArrayList<>();

  /** Spy the connections so tests can count lookups; off for timing runs. */
  boolean spyDatabases = true;

  static void registerH2() throws HopException {
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  DatabaseLookupH2Harness() {
    databaseMeta =
        new DatabaseMeta(
            CONNECTION,
            "H2",
            "Native",
            "",
            "mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1",
            "",
            "",
            "");
  }

  /** Runs SQL statements on the test database (DDL / inserts). */
  void execute(String... statements) throws HopException {
    try (Database db = new Database(new LoggingObject("h2-setup"), new Variables(), databaseMeta)) {
      db.connect();
      for (String statement : statements) {
        db.execStatement(statement);
      }
      db.commit(true);
    }
  }

  /** Bulk insert through a prepared batch, for the benchmark sized tables. */
  void insertBatch(String sql, List<Object[]> rows) throws Exception {
    try (Database db = new Database(new LoggingObject("h2-setup"), new Variables(), databaseMeta)) {
      db.connect();
      java.sql.Connection connection = db.getConnection();
      connection.setAutoCommit(false);
      try (java.sql.PreparedStatement ps = connection.prepareStatement(sql)) {
        int n = 0;
        for (Object[] row : rows) {
          for (int i = 0; i < row.length; i++) {
            ps.setObject(i + 1, row[i]);
          }
          ps.addBatch();
          if (++n % 10_000 == 0) {
            ps.executeBatch();
          }
        }
        ps.executeBatch();
      }
      connection.commit();
    }
  }

  /** Builds the transform; call {@link #run} or drive it yourself. */
  DatabaseLookup create(
      DatabaseLookupMeta meta,
      DatabaseLookupData data,
      IRowMeta inputRowMeta,
      Deque<Object[]> input,
      boolean errorHandling)
      throws HopException {
    meta.setConnection(CONNECTION);

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    TransformMeta transformMeta = new TransformMeta("lookup", meta);
    pipelineMeta.addTransform(transformMeta);
    DatabaseLookup lookup =
        new DatabaseLookup(transformMeta, meta, data, 0, pipelineMeta, new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            return input.poll();
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
            errors.add(new ErrorRow(row, errorDescriptions, fieldNames, errorCodes));
          }

          @Override
          Database getDatabase(DatabaseMeta dbMeta) {
            Database db = super.getDatabase(dbMeta);
            if (spyDatabases) {
              db = spy(db);
            }
            databases.add(db);
            return db;
          }
        };

    // Error handling is switched on after the transform is built, so the constructor does not go
    // looking for row sets towards the error target; putError is replaced below anyway.
    if (errorHandling) {
      TransformMeta target = new TransformMeta("errors", new DatabaseLookupMeta());
      pipelineMeta.addTransform(target);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
      TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
      errorMeta.setEnabled(true);
      transformMeta.setTransformErrorMeta(errorMeta);
    }

    return lookup;
  }

  /** Runs the transform over all the given input rows and returns the output rows. */
  List<Object[]> run(
      DatabaseLookupMeta meta, IRowMeta inputRowMeta, List<Object[]> rows, boolean errorHandling)
      throws HopException {
    DatabaseLookupData data = new DatabaseLookupData();
    DatabaseLookup lookup = create(meta, data, inputRowMeta, new ArrayDeque<>(rows), errorHandling);
    if (!lookup.init()) {
      throw new HopException("init failed");
    }
    try {
      while (lookup.processRow()) {
        // keep going
      }
      if (lookup.getErrors() > 0) {
        throw new HopException("transform reported errors");
      }
    } finally {
      lookup.dispose();
    }
    return output;
  }

  /** The main lookup connection (the first one opened, in init). */
  Database mainDatabase() {
    return databases.get(0);
  }
}
