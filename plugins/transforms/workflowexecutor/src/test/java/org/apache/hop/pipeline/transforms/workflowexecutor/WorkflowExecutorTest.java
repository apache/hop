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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for {@link WorkflowExecutor} runtime behaviour. */
class WorkflowExecutorTest {

  @RegisterExtension
  static final RestoreHopEngineEnvironmentExtension env =
      new RestoreHopEngineEnvironmentExtension();

  @BeforeEach
  void initHop() throws HopException {
    HopEnvironment.init();
  }

  private WorkflowExecutor newExecutor(WorkflowExecutorMeta meta, WorkflowExecutorData data) {
    TransformMeta transformMeta = mock(TransformMeta.class);
    when(transformMeta.getName()).thenReturn("workflow_executor");
    when(transformMeta.isPartitioned()).thenReturn(false);
    PipelineMeta pipelineMeta = mock(PipelineMeta.class);
    when(pipelineMeta.findTransform(anyString())).thenReturn(transformMeta);
    LocalPipelineEngine pipeline = spy(new LocalPipelineEngine());
    WorkflowExecutor executor =
        new WorkflowExecutor(transformMeta, meta, data, 0, pipelineMeta, pipeline);
    executor.setMetadataProvider(new MemoryMetadataProvider());
    return executor;
  }

  @Test
  void initFailsWhenStaticFilenameMissing() {
    WorkflowExecutorMeta meta = new WorkflowExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename("");
    meta.setRunConfigurationName("local");

    WorkflowExecutor executor = newExecutor(meta, new WorkflowExecutorData());
    assertFalse(executor.init());
  }

  @Test
  void initSucceedsWhenFilenameComesFromField() {
    WorkflowExecutorMeta meta = new WorkflowExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(true);
    meta.setFilenameField("child_path");
    meta.setFilename("/tmp/ignored-default.hwf");
    meta.setRunConfigurationName("local");

    WorkflowExecutor executor = newExecutor(meta, new WorkflowExecutorData());
    assertTrue(executor.init());
  }

  @Test
  void initFailsWhenFilenameFieldMissing() {
    WorkflowExecutorMeta meta = new WorkflowExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(true);
    meta.setFilenameField("");
    meta.setRunConfigurationName("local");

    WorkflowExecutor executor = newExecutor(meta, new WorkflowExecutorData());
    assertFalse(executor.init());
  }

  @Test
  void initLoadsChildWorkflowFromFilesystemPath() throws URISyntaxException {
    String path =
        Paths.get(
                Objects.requireNonNull(
                        WorkflowExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/workflowexecutor/minimal-child.hwf"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    WorkflowExecutorMeta meta = new WorkflowExecutorMeta();
    meta.setDefault();
    meta.setFilenameInField(false);
    meta.setFilename(path);
    meta.setRunConfigurationName("local");

    WorkflowExecutor executor = newExecutor(meta, new WorkflowExecutorData());
    assertTrue(executor.init());
    assertNotNull(executor.getData().executorWorkflowMeta);
  }

  @Test
  void loadWorkflowMetaHonorsExplicitFilenameOverWrongMetaFilename()
      throws HopException, URISyntaxException {
    String goodPath =
        Paths.get(
                Objects.requireNonNull(
                        WorkflowExecutorTest.class.getResource(
                            "/org/apache/hop/pipeline/transforms/workflowexecutor/minimal-child.hwf"))
                    .toURI())
            .toAbsolutePath()
            .toString();

    WorkflowExecutorMeta meta = new WorkflowExecutorMeta();
    meta.setDefault();
    meta.setFilename("/this/path/does/not/exist/bad-child.hwf");

    WorkflowMeta loaded =
        WorkflowExecutorMeta.loadWorkflowMeta(
            meta, goodPath, new MemoryMetadataProvider(), new Variables());

    assertNotNull(loaded);
    assertFalse(loaded.getActions().isEmpty());
  }
}
