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
package org.apache.hop.pipeline.transforms.dbproc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabasePluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Call DB procedure with auto-commit off, on H2: the procedure inserts a row through the caller's
 * connection and fails for id 4. A run that completes, or is stopped without an error, commits the
 * calls that ran; a run that fails leaves none of its calls in the database.
 */
public class DBProcH2TransactionTest {

  private static final String CONNECTION = "dbproc-h2";

  private DatabaseMeta databaseMeta;

  /** The procedure: inserts the id, and refuses id 4. H2 hands it the caller's connection. */
  public static void insertOrFail(Connection connection, int id) throws SQLException {
    if (id == 4) {
      throw new SQLException("id 4 is refused");
    }
    try (PreparedStatement insert =
        connection.prepareStatement("INSERT INTO CALLED (ID) VALUES (?)")) {
      insert.setInt(1, id);
      insert.executeUpdate();
    }
  }

  @BeforeAll
  static void setUpOnce() throws Exception {
    HopClientEnvironment.init();
    DatabasePluginType.getInstance().registerClassPathPlugin(H2DatabaseMeta.class);
  }

  @BeforeEach
  void setUp() throws Exception {
    String name =
        "mem:dbproc" + UUID.randomUUID().toString().replace("-", "") + ";DB_CLOSE_DELAY=-1";
    databaseMeta = new DatabaseMeta(CONNECTION, "H2", "Native", "", name, "", "sa", "");
    try (Database db = new Database(new LoggingObject("setup"), new Variables(), databaseMeta)) {
      db.connect();
      db.execStatements(
          "CREATE TABLE CALLED (ID INTEGER);"
              + "CREATE ALIAS INSERT_OR_FAIL FOR \""
              + DBProcH2TransactionTest.class.getName()
              + ".insertOrFail\";");
    }
  }

  private List<Integer> called() throws Exception {
    List<Integer> ids = new ArrayList<>();
    try (Database db = new Database(new LoggingObject("check"), new Variables(), databaseMeta)) {
      db.connect();
      try (ResultSet rs =
          db.getConnection().createStatement().executeQuery("SELECT ID FROM CALLED ORDER BY ID")) {
        while (rs.next()) {
          ids.add(rs.getInt(1));
        }
      }
    }
    return ids;
  }

  /** Runs the transform over the ids; stopAfter stops the run once that many rows were read. */
  private DBProc run(boolean autoCommit, int stopAfter, long... ids) throws Exception {
    DBProcMeta meta = new DBProcMeta();
    meta.setConnection(CONNECTION);
    meta.setProcedure("INSERT_OR_FAIL");
    meta.setAutoCommit(autoCommit);
    DBProcMeta.ProcArgument argument = new DBProcMeta.ProcArgument();
    argument.setName("id");
    argument.setDirection("IN");
    argument.setType("Integer");
    List<DBProcMeta.ProcArgument> arguments = new ArrayList<>();
    arguments.add(argument);
    meta.setArguments(arguments);

    PipelineMeta pipelineMeta = new PipelineMeta();
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(DatabaseMeta.class).save(databaseMeta);
    pipelineMeta.setMetadataProvider(metadataProvider);
    TransformMeta transformMeta = new TransformMeta("call procedure", meta);
    pipelineMeta.addTransform(transformMeta);

    IRowMeta rowMeta = new RowMeta();
    rowMeta.addValueMeta(new ValueMetaInteger("id"));
    Deque<Object[]> input = new ArrayDeque<>();
    for (long id : ids) {
      input.add(new Object[] {id});
    }
    LocalPipelineEngine pipeline = new LocalPipelineEngine();
    pipeline.setRunning(true);
    int[] read = {0};
    DBProcData data = new DBProcData();
    DBProc transform =
        new DBProc(transformMeta, meta, data, 0, pipelineMeta, pipeline) {
          @Override
          public Object[] getRow() {
            if (stopAfter >= 0 && read[0] == stopAfter) {
              setStopped(true);
              return null;
            }
            read[0]++;
            return input.poll();
          }

          @Override
          public IRowMeta getInputRowMeta() {
            return rowMeta;
          }

          @Override
          public void putRow(IRowMeta rowMeta, Object[] row) {
            // the calls are what matters here
          }
        };
    assertTrue(transform.init(), "init");
    // As when a transform in front sends the rows: the procedure is called once per input row.
    data.readsRows = true;
    try {
      // As the pipeline's run thread: a failed or stopped transform gets no more rows.
      while (transform.processRow() && !transform.isStopped() && transform.getErrors() == 0) {
        // keep going
      }
    } catch (HopException e) {
      // As the pipeline does when processRow throws: an error is counted, then it is disposed.
      transform.setErrors(1);
    } finally {
      transform.dispose();
    }
    return transform;
  }

  @Test
  void aCompletedRunCommitsEveryCall() throws Exception {
    DBProc transform = run(false, -1, 1, 2, 3);

    assertEquals(0, transform.getErrors());
    assertEquals(List.of(1, 2, 3), called());
  }

  @Test
  void aFailedRunLeavesNoneOfItsCallsInTheDatabase() throws Exception {
    DBProc transform = run(false, -1, 1, 2, 3, 4, 5);

    assertTrue(transform.getErrors() > 0);
    assertEquals(List.of(), called());
  }

  /** A stop without an error ends the pipeline as a success: the calls that ran are committed. */
  @Test
  void aStoppedRunCommitsTheCallsThatRan() throws Exception {
    DBProc transform = run(false, 2, 1, 2, 3);

    assertEquals(0, transform.getErrors());
    assertEquals(List.of(1, 2), called());
  }

  @Test
  void withAutoCommitEveryCallStaysAsItRan() throws Exception {
    DBProc transform = run(true, -1, 1, 2, 3, 4, 5);

    assertTrue(transform.getErrors() > 0);
    assertEquals(List.of(1, 2, 3), called());
  }
}
