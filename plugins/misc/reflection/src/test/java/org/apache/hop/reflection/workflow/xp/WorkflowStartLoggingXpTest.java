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

package org.apache.hop.reflection.workflow.xp;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.reflection.workflow.meta.WorkflowLog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkflowStartLoggingXpTest {

  @TempDir Path tempDir;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            WorkflowLog.class.getName(), MetadataPluginType.class, HopMetadata.class);
  }

  /**
   * A workflow a client sends to Hop Server as XML has no filename there. A Workflow Log that only
   * logs specific workflows cannot match it, and used to fail the workflow with a
   * NullPointerException instead. See issue #8597.
   */
  @Test
  void workflowWithoutFilenameIsNotMatchedAgainstWorkflowsToLog() throws Exception {
    Path loggingPipeline = Files.createFile(tempDir.resolve("workflow-log.hpl"));

    WorkflowLog workflowLog = new WorkflowLog("a-workflow-log");
    workflowLog.setEnabled(true);
    workflowLog.setExecutingAtStart(true);
    workflowLog.setPipelineFilename(loggingPipeline.toString());
    workflowLog.setWorkflowToLog(List.of(tempDir.resolve("some-workflow.hwf").toString()));

    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    metadataProvider.getSerializer(WorkflowLog.class).save(workflowLog);

    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getMetadataProvider()).thenReturn(metadataProvider);
    when(workflow.getFilename()).thenReturn(null);

    assertDoesNotThrow(
        () ->
            new WorkflowStartLoggingXp()
                .callExtensionPoint(mock(ILogChannel.class), new Variables(), workflow));
  }
}
