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

package org.apache.hop.execution.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileExecutionInfoLocationTest {

  private Path tempDir;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("hop-exec-info-test");
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

    FileExecutionInfoLocation location =
        new FileExecutionInfoLocation(targetDir.toAbsolutePath().toString());
    assertTrue(location.isCreateParentFolder());

    location.initialize(new Variables(), null);
    assertTrue(Files.exists(targetDir));
  }

  @Test
  void testInitializeCreateFolderFalse() throws Exception {
    Path targetDir = tempDir.resolve(UUID.randomUUID().toString());
    assertFalse(Files.exists(targetDir));

    FileExecutionInfoLocation location =
        new FileExecutionInfoLocation(targetDir.toAbsolutePath().toString());
    location.setCreateParentFolder(false);
    assertFalse(location.isCreateParentFolder());

    location.initialize(new Variables(), null);
    assertFalse(Files.exists(targetDir));
  }

  @Test
  void deleteExecutionsRemovesParentAndChild() throws Exception {
    FileExecutionInfoLocation location = openLocation();
    location.registerExecution(execution("parent", null, new Date(1_000L)));
    location.registerExecution(execution("child", "parent", new Date(1_000L)));
    location.registerExecution(execution("other", null, new Date(2_000L)));

    int deleted = location.deleteExecutions(null);

    assertEquals(2, deleted);
    assertNull(location.getExecution("parent"));
    assertNull(location.getExecution("child"));
    assertNull(location.getExecution("other"));
    assertTrue(location.getExecutionIds(true, 0).isEmpty());
  }

  @Test
  void deleteExecutionsKeepsANewerExecution() throws Exception {
    FileExecutionInfoLocation location = openLocation();
    location.registerExecution(execution("old", null, new Date(1_000L)));
    location.registerExecution(execution("old-child", "old", new Date(1_000L)));
    Date recentStart = new Date(System.currentTimeMillis() + 86_400_000L);
    location.registerExecution(execution("recent", null, recentStart));

    int deleted = location.deleteExecutions(new Date());

    assertEquals(1, deleted);
    assertNull(location.getExecution("old"));
    assertNull(location.getExecution("old-child"));
    assertEquals("recent", location.getExecution("recent").getId());
  }

  @Test
  void deleteExecutionRemovesDescendantsWithoutReadingThePipelineXml() throws Exception {
    FileExecutionInfoLocation location = openLocation();
    Execution parent = execution("parent", null, new Date(1_000L));
    parent.setExecutorXml("x".repeat(20_000));
    parent.setMetadataJson("y".repeat(20_000));
    location.registerExecution(parent);
    location.registerExecution(execution("child", "parent", new Date(1_000L)));
    location.registerExecution(execution("grandchild", "child", new Date(1_000L)));

    assertTrue(location.deleteExecution("parent"));

    assertNull(location.getExecution("parent"));
    assertNull(location.getExecution("child"));
    assertNull(location.getExecution("grandchild"));
  }

  private FileExecutionInfoLocation openLocation() throws Exception {
    FileExecutionInfoLocation location =
        new FileExecutionInfoLocation(tempDir.resolve("root").toString());
    location.initialize(new Variables(), null);
    return location;
  }

  private static Execution execution(String id, String parentId, Date start) {
    Execution execution = new Execution();
    execution.setId(id);
    execution.setName(id);
    execution.setExecutionType(ExecutionType.Pipeline);
    execution.setParentId(parentId);
    execution.setExecutionStartDate(start);
    return execution;
  }
}
