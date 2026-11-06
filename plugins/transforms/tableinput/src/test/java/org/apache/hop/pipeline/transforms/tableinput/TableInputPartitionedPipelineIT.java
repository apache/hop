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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  private List<Long> runPipeline(int copies, int partitions, boolean preview) throws Exception {
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
    pipeline.setPreview(preview); // preview must be explicit; normal runs must be partitioned
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
      throw new IllegalStateException("pipeline ran with errors");
    }

    return ids;
  }

  @Test
  void copiesMatchPartitionsReadsAllRowsOnce() throws Exception {
    int partitions = 4;
    List<Long> ids = runPipeline(partitions, partitions, false);
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
    List<Long> ids = runPipeline(6, 4, false);
    assertEquals(ROWS, ids.size(), "extra copies must not duplicate rows");
  }

  @Test
  void singlePartitionReadsWholeTable() throws Exception {
    List<Long> ids = runPipeline(1, 1, false);
    assertEquals(ROWS, ids.size());
  }

  @Test
  void singleCopyWithMultipleConfiguredPartitionsReadsWholeTable() throws Exception {
    // A single copy is not a partitioned read: the whole result is returned without predicates,
    // so all rows appear exactly once even though 4 partitions are configured.
    List<Long> ids = runPipeline(1, 4, false);
    assertEquals(ROWS, ids.size());
    Set<Long> unique = new HashSet<>(ids);
    assertEquals(ROWS, unique.size(), "single copy must not duplicate rows");
    for (long i = 0; i < ROWS; i++) {
      assertTrue(unique.contains(i), "row " + i + " must be present");
    }
  }

  @Test
  void previewWithSingleCopyReadsWholeTable() throws Exception {
    // Preview (single clone, like the GUI) reads the whole result even though 4 partitions are
    // configured: no partition predicates are applied.
    List<Long> ids = runPipeline(1, 4, true);
    assertEquals(ROWS, ids.size());
    Set<Long> unique = new HashSet<>(ids);
    assertEquals(ROWS, unique.size(), "preview must not slice by partition");
  }

  @Test
  void multiCopyPreviewReadsWholeTablePerCopyLikeUnpartitioned() throws Exception {
    // With several copies and preview, every copy reads the whole table (no predicates), exactly
    // like an unpartitioned multi-copy Table Input run: N copies x full result.
    List<Long> ids = runPipeline(4, 4, true);
    assertEquals(ROWS * 4L, ids.size());
  }

  @Test
  void insufficientCopiesFailInNonPreviewRun() {
    // Fewer copies than configured partitions must fail (preview full-read exempts the single
    // reader; normal multi-copy runs must slice per copy).
    assertThrows(
        Exception.class,
        () -> runPipeline(2, 4, false),
        "2 copies for 4 configured partitions must not silently miss rows");
  }

  @Test
  void invalidReversedBoundsFailEvenInPreview() throws Exception {
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
    meta.setJdbcLowerBound("1000");
    meta.setJdbcUpperBound("0");
    meta.setJdbcNumPartitions("4");

    String tableInputPluginId =
        org.apache.hop.core.plugins.PluginRegistry.getInstance()
            .getPluginId(TransformPluginType.class, meta);
    TransformMeta tableInput = new TransformMeta(tableInputPluginId, "TableInput", meta);
    tableInput.setCopies(1);
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
    pipeline.setPreview(true);
    // invalid bounds must be rejected, not silently read as the whole table
    assertThrows(Exception.class, pipeline::prepareExecution);
  }

  @Test
  void nonNumericBoundsFailEvenInPreview() throws Exception {
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
    meta.setJdbcLowerBound("abc");
    meta.setJdbcUpperBound("0");
    meta.setJdbcNumPartitions("4");

    String tableInputPluginId =
        org.apache.hop.core.plugins.PluginRegistry.getInstance()
            .getPluginId(TransformPluginType.class, meta);
    TransformMeta tableInput = new TransformMeta(tableInputPluginId, "TableInput", meta);
    tableInput.setCopies(1);
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
    pipeline.setPreview(true);
    assertThrows(Exception.class, pipeline::prepareExecution);
  }

  private static final class LoggingObjectForTest extends LoggingObject {
    LoggingObjectForTest() {
      super("TableInputPartitionedPipelineIT");
    }
  }
}
