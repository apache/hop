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

package org.apache.hop.workflow.actions.deleteexecutioninfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.execution.local.FileExecutionInfoLocation;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ActionDeleteExecutionInfoTest {

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    HopLogStore.init();
    tempDir = Files.createTempDirectory("hop-delete-exec-info");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempDir != null) {
      FileUtils.deleteDirectory(tempDir.toFile());
    }
  }

  @Test
  void ageDeletesOnlyTheOldExecutionAndKeepsTheLocation() throws Exception {
    MemoryMetadataProvider provider = provider();
    FileExecutionInfoLocation stored = fileLocation(provider);
    stored.registerExecution(execution("old", new Date(1_000L)));
    stored.registerExecution(
        execution("recent", new Date(System.currentTimeMillis() + 86_400_000L)));

    ActionDeleteExecutionInfo action = action(provider);
    action.setAge("1");
    action.setUnit(ExecutionInfoAgeUnit.DAYS);
    action.setDeleteAll(false);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(0, result.getNrErrors());
    assertEquals(1, result.getNrLinesDeleted());
    assertNull(stored.getExecution("old"));
    assertNotNull(stored.getExecution("recent"));
    assertNotNull(provider.getSerializer(ExecutionInfoLocation.class).load("exec-loc"));
  }

  @Test
  void deleteAllRemovesEveryExecution() throws Exception {
    MemoryMetadataProvider provider = provider();
    FileExecutionInfoLocation stored = fileLocation(provider);
    stored.registerExecution(execution("old", new Date(1_000L)));
    stored.registerExecution(execution("recent", new Date()));

    ActionDeleteExecutionInfo action = action(provider);
    action.setDeleteAll(true);

    Result result = action.execute(new Result(), 0);

    assertTrue(result.getResult());
    assertEquals(2, result.getNrLinesDeleted());
    assertTrue(stored.getExecutionIds(false, 0).isEmpty());
    assertNotNull(provider.getSerializer(ExecutionInfoLocation.class).load("exec-loc"));
  }

  private MemoryMetadataProvider provider() {
    return new MemoryMetadataProvider();
  }

  private FileExecutionInfoLocation fileLocation(MemoryMetadataProvider provider) throws Exception {
    FileExecutionInfoLocation fileLocation =
        new FileExecutionInfoLocation(tempDir.resolve("root").toString());
    fileLocation.initialize(new Variables(), provider);
    ExecutionInfoLocation metadata =
        new ExecutionInfoLocation("exec-loc", "", "2000", "5000", null, null, fileLocation);
    provider.getSerializer(ExecutionInfoLocation.class).save(metadata);
    return fileLocation;
  }

  private ActionDeleteExecutionInfo action(MemoryMetadataProvider provider) {
    ActionDeleteExecutionInfo action = new ActionDeleteExecutionInfo("cleanup");
    action.setLocationName("exec-loc");
    action.setMetadataProvider(provider);
    action.setLog(new LogChannel(action));
    return action;
  }

  private static Execution execution(String id, Date start) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    execution.setExecutionType(ExecutionType.Pipeline);
    execution.setExecutionStartDate(start);
    return execution;
  }
}
