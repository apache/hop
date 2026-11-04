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

package org.apache.hop.pipeline.transforms.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformErrorMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Update with "use batch update" against a real JDBC batch. H2, like SQL Server and MySQL, runs the
 * whole batch and marks the rows that failed in the update counts.
 *
 * <p>A failed batch used to surface on the row that ran it, at the commit size: the error hop got
 * rows 100, 200, ... instead of the ones that failed, and the failed rows had already gone to the
 * output.
 */
class UpdateBatchH2Test {

  private static final String CONNECTION = "h2";
  private static final int ROWS = 250;
  private static final int COMMIT_SIZE = 100;
  private static final int BAD_EVERY = 47;

  private DatabaseMeta databaseMeta;

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
    DatabaseMeta.clearDatabaseInterfacesMap();
  }

  @BeforeEach
  void setUp() throws Exception {
    String databaseName =
        "mem:update" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", databaseName, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatement("CREATE TABLE target (id BIGINT PRIMARY KEY, name VARCHAR(5))");
      db.execStatement("INSERT INTO target SELECT x, 'x' FROM SYSTEM_RANGE(1, " + ROWS + ")");
    }
  }

  private static boolean bad(long id) {
    return id % BAD_EVERY == 0;
  }

  /** Every row is set to 'a', except the bad ones, whose name is too long for the column. */
  private static Queue<Object[]> input() {
    Queue<Object[]> input = new ConcurrentLinkedQueue<>();
    for (long id = 1; id <= ROWS; id++) {
      input.add(new Object[] {id, bad(id) ? "toolong" : "a"});
    }
    return input;
  }

  private String table() throws Exception {
    StringJoiner content = new StringJoiner(",");
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      for (Object[] row : db.getRows("SELECT id, name FROM target ORDER BY id", -1)) {
        content.add(row[0] + ":" + row[1]);
      }
    }
    return content.toString();
  }

  private static String expectedTable(boolean updated) {
    StringJoiner content = new StringJoiner(",");
    for (long id = 1; id <= ROWS; id++) {
      content.add(id + ":" + (updated && !bad(id) ? "a" : "x"));
    }
    return content.toString();
  }

  private static Set<Long> ids(List<Object[]> rows) {
    Set<Long> ids = new TreeSet<>();
    for (Object[] row : rows) {
      assertTrue(ids.add((Long) row[0]), "row " + row[0] + " was passed on twice");
    }
    return ids;
  }

  /** An Update in batch mode, with an error hop when errors isn't null. */
  private Update update(Queue<Object[]> input, List<Object[]> output, List<Object[]> errors)
      throws Exception {
    IRowMeta inputRowMeta = new RowMeta();
    inputRowMeta.addValueMeta(new ValueMetaInteger("id"));
    inputRowMeta.addValueMeta(new ValueMetaString("name"));

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);

    UpdateMeta meta = new UpdateMeta();
    meta.setDefault();
    meta.setConnection(CONNECTION);
    meta.setCommitSize(Integer.toString(COMMIT_SIZE));
    meta.setUseBatchUpdate(true);
    meta.getLookupField().setSchemaName("");
    meta.getLookupField().setTableName("target");
    meta.getLookupField().getLookupKeys().add(new UpdateKeyField("id", "id", "="));
    meta.getLookupField().getUpdateFields().add(new UpdateField("name", "name"));
    TransformMeta transformMeta = new TransformMeta("update", meta);
    pipelineMeta.addTransform(transformMeta);

    Update update =
        new Update(
            transformMeta, meta, new UpdateData(), 0, pipelineMeta, new LocalPipelineEngine()) {
          @Override
          public IRowMeta getInputRowMeta() {
            return inputRowMeta;
          }

          @Override
          public Object[] getRow() {
            Object[] row = input.poll();
            return row == null ? null : row.clone();
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
            errors.add(row);
          }
        };

    if (errors != null) {
      // After the transform: its constructor looks at the hops of the pipeline.
      TransformMeta target = new TransformMeta("errors", new UpdateMeta());
      pipelineMeta.addTransform(target);
      pipelineMeta.addPipelineHop(new PipelineHopMeta(transformMeta, target));
      TransformErrorMeta errorMeta = new TransformErrorMeta(transformMeta, target);
      errorMeta.setEnabled(true);
      transformMeta.setTransformErrorMeta(errorMeta);
    }
    return update;
  }

  private static void run(Update update) {
    assertTrue(update.init());
    try {
      while (update.processRow()) {
        // keep going
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    } finally {
      update.dispose();
    }
  }

  @Test
  void theRowsThatFailedGoToTheErrorHop() throws Exception {
    List<Object[]> output = new ArrayList<>();
    List<Object[]> errors = new ArrayList<>();
    Update update = update(input(), output, errors);

    run(update);

    Set<Long> expectedErrors = new TreeSet<>();
    Set<Long> expectedOutput = new TreeSet<>();
    for (long id = 1; id <= ROWS; id++) {
      (bad(id) ? expectedErrors : expectedOutput).add(id);
    }
    // 47, 94, 141, 188 and 235: the last one is in the partial batch run at the end of the input.
    assertEquals(expectedErrors, ids(errors));
    assertEquals(expectedOutput, ids(output));
    assertEquals(0, update.getErrors());
    assertEquals(expectedOutput.size(), update.getLinesUpdated());
    assertEquals(expectedTable(true), table());
  }

  @Test
  void withoutErrorHopAFailedBatchStopsTheTransformAndRollsBack() throws Exception {
    List<Object[]> output = new ArrayList<>();
    Update update = update(input(), output, null);

    run(update);

    assertEquals(1, update.getErrors());
    // Row 47 fails the first batch: none of its rows was passed on.
    assertTrue(output.isEmpty(), "rows of a failed batch went to the output");
    assertEquals(expectedTable(false), table());
  }
}
