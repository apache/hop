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

package org.apache.hop.pipeline.transforms.tableinput;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databases.h2.H2DatabaseMeta;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.dummy.DummyMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test for the classic engine's partitioned Table Input read: when the four {@code
 * jdbc_partition_*} properties are set, N transform copies each read their own slice of the table
 * (Spark-compatible ranges), so the union of all copies equals the full table with no duplicates
 * and no missing rows.
 */
class TableInputPartitionedPipelineIT {

  private static final String CONNECTION_NAME = "table-input-partitioned-it";
  private static final int ROWS = 1000;

  private static String jdbcUrl;

  @BeforeAll
  static void initHop() throws Exception {
    HopClientEnvironment.init();
    org.apache.hop.core.database.DatabasePluginType.getInstance()
        .registerClassPathPlugin(H2DatabaseMeta.class);

    jdbcUrl = "jdbc:h2:mem:partitioned_it;DB_CLOSE_DELAY=-1";
    try (Connection connection = DriverManager.getConnection(jdbcUrl);
        Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE TABLE T ( id INT PRIMARY KEY, val VARCHAR(20) )");
      for (int i = 0; i < ROWS; i++) {
        stmt.execute("INSERT INTO T (id, val) VALUES (" + i + ", 'v" + i + "')");
      }
    }
  }

  private DatabaseMeta connectionMeta() {
    DatabaseMeta databaseMeta =
        new DatabaseMeta(
            CONNECTION_NAME,
            "H2",
            "Native",
            "",
            "mem:partitioned_it;DB_CLOSE_DELAY=-1",
            "",
            "",
            "");
    databaseMeta.setSupportsTimestampDataType(true);
    return databaseMeta;
  }

  /** Actual JDBC url used for the H2 connection (DatabaseMeta adds the jdbc:h2: prefix). */
  private String jdbcUrl() {
    return "jdbc:h2:" + connectionMeta().getDatabaseName();
  }

  /** Builds a pipeline TableInput (N copies) -> Dummy and returns the collected id rows. */
  private List<Long> runPipeline(int copies, int partitions) throws Exception {
    PipelineMeta pipelineMeta = new PipelineMeta();
    pipelineMeta.setName(getClass().getName());
    MemoryMetadataProvider provider = new MemoryMetadataProvider();
    provider.getSerializer(DatabaseMeta.class).save(connectionMeta());
    pipelineMeta.setMetadataProvider(provider);

    TableInputMeta meta = new TableInputMeta();
    meta.setConnection(CONNECTION_NAME);
    meta.setSql("SELECT id, val FROM T");
    meta.setUseNamedParameters(false);
    meta.setJdbcPartitionColumn("id");
    meta.setJdbcLowerBound("0");
    meta.setJdbcUpperBound(Integer.toString(ROWS));
    meta.setJdbcNumPartitions(Integer.toString(partitions));

    String tableInputPluginId =
        org.apache.hop.core.plugins.PluginRegistry.getInstance()
            .getPluginId(TransformPluginType.class, meta);
    TransformMeta tableInput = new TransformMeta(tableInputPluginId, "TableInput", meta);
    tableInput.setCopies(copies);
    pipelineMeta.addTransform(tableInput);

    DummyMeta dummyMeta = new DummyMeta();
    String dummyPluginId =
        org.apache.hop.core.plugins.PluginRegistry.getInstance()
            .getPluginId(TransformPluginType.class, dummyMeta);
    TransformMeta dummy = new TransformMeta(dummyPluginId, "Output", dummyMeta);
    pipelineMeta.addTransform(dummy);
    pipelineMeta.addPipelineHop(new PipelineHopMeta(tableInput, dummy));

    LocalPipelineEngine pipeline = new LocalPipelineEngine(pipelineMeta);
    pipeline.setVariables(new Variables());
    pipeline.setPreview(true); // surface init errors in the exception
    try {
      pipeline.prepareExecution();
    } catch (Exception e) {
      throw e;
    }

    ITransform output = pipeline.getRunThread("Output", 0);
    List<Long> ids = new ArrayList<>();
    output.addRowListener(
        new org.apache.hop.pipeline.transform.RowAdapter() {
          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) {
            ids.add(((Number) row[0]).longValue());
          }
        });

    pipeline.startThreads();
    pipeline.waitUntilFinished();

    if (pipeline.getErrors() != 0) {
      String logText =
          org.apache.hop.core.logging.HopLogStore.getAppender()
              .getBuffer(pipeline.getLogChannelId(), true)
              .toString();
      System.err.println("=== PIPELINE LOG (errors) ===\n" + logText);
    }

    assertEquals(0, pipeline.getErrors(), "pipeline must run without errors");
    return ids;
  }

  @Test
  void copiesMatchPartitionsReadsAllRowsOnce() throws Exception {
    int partitions = 4;
    List<Long> ids = runPipeline(partitions, partitions);
    assertEquals(ROWS, ids.size(), "all rows must be read exactly once");
    Set<Long> unique = new HashSet<>(ids);
    assertEquals(ROWS, unique.size(), "no duplicates");
    for (long i = 0; i < ROWS; i++) {
      assertTrue(unique.contains(i), "row " + i + " must be present");
    }
  }

  @Test
  void extraCopiesDoNotDuplicateRows() throws Exception {
    // 4 configured partitions but 6 copies: copies 4 and 5 read empty ranges
    List<Long> ids = runPipeline(6, 4);
    assertEquals(ROWS, ids.size(), "extra copies must not duplicate rows");
  }

  @Test
  void singlePartitionReadsWholeTable() throws Exception {
    List<Long> ids = runPipeline(1, 1);
    assertEquals(ROWS, ids.size());
  }

  private static final class LoggingObjectForTest extends LoggingObject {
    LoggingObjectForTest() {
      super("TableInputPartitionedPipelineIT");
    }
  }
}
