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
 *
 */

package org.apache.hop.execution.caching;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataBuilder;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachingFileExecutionInfoLocationTest {

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("hop-caching-exec-info-test");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempDir != null) {
      FileUtils.deleteDirectory(tempDir.toFile());
    }
  }

  @Test
  void testInitializeCreateFolderTrue() throws Exception {
    Path targetDir = tempDir.resolve(UUID.randomUUID().toString());
    assertFalse(Files.exists(targetDir));

    CachingFileExecutionInfoLocation location = new CachingFileExecutionInfoLocation();
    location.setRootFolder(targetDir.toAbsolutePath().toString());
    assertTrue(location.isCreateParentFolder());

    location.initialize(new Variables(), null);
    try {
      assertTrue(Files.exists(targetDir));
    } finally {
      location.close();
    }
  }

  @Test
  void testInitializeCreateFolderFalse() throws Exception {
    Path targetDir = tempDir.resolve(UUID.randomUUID().toString());
    assertFalse(Files.exists(targetDir));

    CachingFileExecutionInfoLocation location = new CachingFileExecutionInfoLocation();
    location.setRootFolder(targetDir.toAbsolutePath().toString());
    location.setCreateParentFolder(false);
    assertFalse(location.isCreateParentFolder());

    location.initialize(new Variables(), null);
    try {
      assertFalse(Files.exists(targetDir));
    } finally {
      location.close();
    }
  }

  /**
   * GUI path: getExecutionData(parentId, null) must aggregate per-transform sample data (Beam/Spark
   * style) so PipelineExecutionViewer can show rows without a separate "all-transforms" owner.
   */
  @Test
  void getExecutionDataNullIdAggregatesChildSamples() throws Exception {
    Path root = tempDir.resolve("gui-agg");
    String parentId = "parent-" + UUID.randomUUID();

    CachingFileExecutionInfoLocation location = new CachingFileExecutionInfoLocation();
    location.setRootFolder(root.toAbsolutePath().toString());
    location.initialize(new Variables(), null);

    Execution parent = new Execution();
    parent.setId(parentId);
    parent.setName("spark-transforms");
    parent.setExecutionType(ExecutionType.Pipeline);
    parent.setExecutionStartDate(new Date());
    parent.setRegistrationDate(new Date());
    location.registerExecution(parent);

    String ownerId = parentId + "|checksum|0";
    ExecutionData data =
        ExecutionDataBuilder.of()
            .withParentId(parentId)
            .withOwnerId(ownerId)
            .withExecutionType(ExecutionType.Transform)
            .withCollectionDate(new Date())
            .withFinished(true)
            .build();
    // Minimal non-empty payload so aggregate sees content
    data.setSetMetaData(
        Map.of(
            "FirstOutput/checksum.0",
            new ExecutionDataSetMeta(
                "FirstOutput/checksum.0", ownerId, "checksum", "0", "First output rows")));
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"v1"});
    data.setDataSets(
        Map.of(
            "FirstOutput/checksum.0",
            new RowBuffer(new RowMetaBuilder().addString("c").build(), rows)));
    location.registerData(data);
    try {
      // Same CacheEntry the GUI uses after load: aggregate without a second process
      ExecutionData loaded = location.getExecutionData(parentId, null);
      assertNotNull(loaded, "GUI expects aggregated data for parentId + null child");
      assertNotNull(loaded.getSetMetaData());
      assertTrue(loaded.getSetMetaData().containsKey("FirstOutput/checksum.0"));
      assertTrue(location.findChildIds(ExecutionType.Pipeline, parentId).contains(ownerId));
    } finally {
      location.close();
    }
  }

  /**
   * Simulates Spark driver + executor: separate location instances (separate in-memory caches)
   * sharing the same root folder. Executor registerData must load the parent from disk and survive
   * a later driver close() without wiping childExecutionData.
   */
  @Test
  void registerDataFromSeparateProcessSurvivesDriverClose() throws Exception {
    Path root = tempDir.resolve("shared");
    String parentId = "parent-" + UUID.randomUUID();

    CachingFileExecutionInfoLocation driver = new CachingFileExecutionInfoLocation();
    driver.setRootFolder(root.toAbsolutePath().toString());
    driver.initialize(new Variables(), null);

    Execution parent = new Execution();
    parent.setId(parentId);
    parent.setName("spark-transforms");
    parent.setExecutionType(ExecutionType.Pipeline);
    parent.setExecutionStartDate(new Date());
    parent.setRegistrationDate(new Date());
    driver.registerExecution(parent);

    // Parent must be on disk immediately for executors
    assertTrue(Files.exists(root.resolve(parentId + ".json")));

    // Executor-side location (fresh cache)
    CachingFileExecutionInfoLocation executor = new CachingFileExecutionInfoLocation();
    executor.setRootFolder(root.toAbsolutePath().toString());
    executor.initialize(new Variables(), null);

    ExecutionData data =
        ExecutionDataBuilder.of()
            .withParentId(parentId)
            .withOwnerId(parentId + "|CheckSum|0")
            .withExecutionType(ExecutionType.Transform)
            .withCollectionDate(new Date())
            .withFinished(true)
            .build();
    executor.registerData(data);
    executor.close();

    // Driver final flush must not drop samples written by the executor
    driver.close();

    CachingFileExecutionInfoLocation reader = new CachingFileExecutionInfoLocation();
    reader.setRootFolder(root.toAbsolutePath().toString());
    reader.initialize(new Variables(), null);
    try {
      ExecutionData loaded = reader.getExecutionData(parentId, parentId + "|CheckSum|0");
      assertNotNull(loaded, "expected sample data after multi-writer merge");
      assertTrue(loaded.isFinished());
    } finally {
      reader.close();
    }
  }
}
