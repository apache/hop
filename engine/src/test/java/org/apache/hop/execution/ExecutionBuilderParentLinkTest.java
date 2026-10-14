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

package org.apache.hop.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;

class ExecutionBuilderParentLinkTest {

  @Test
  void workflowActionStoresTheWorkflowAsParent() {
    @SuppressWarnings("unchecked")
    IWorkflowEngine<WorkflowMeta> workflow = mock(IWorkflowEngine.class);
    when(workflow.getLogChannelId()).thenReturn("workflow-1");

    ILoggingObject loggingParent =
        mock(ILoggingObject.class, withSettings().extraInterfaces(IAction.class));
    IAction action = (IAction) loggingParent;
    when(action.getParentWorkflow()).thenReturn(workflow);
    when(loggingParent.getLogChannelId()).thenReturn("action-1");

    assertEquals("workflow-1", ExecutionBuilder.executionParentId(loggingParent));
    assertEquals("action-1", ExecutionBuilder.launchedById(loggingParent));
  }

  @Test
  void transformStoresThePipelineAsParent() {
    @SuppressWarnings("unchecked")
    IPipelineEngine<PipelineMeta> pipeline = mock(IPipelineEngine.class);
    when(pipeline.getLogChannelId()).thenReturn("pipeline-1");

    ILoggingObject loggingParent =
        mock(ILoggingObject.class, withSettings().extraInterfaces(ITransform.class));
    ITransform transform = (ITransform) loggingParent;
    when(transform.getPipeline()).thenReturn(pipeline);
    when(loggingParent.getLogChannelId()).thenReturn("transform-1");

    assertEquals("pipeline-1", ExecutionBuilder.executionParentId(loggingParent));
    assertEquals("transform-1", ExecutionBuilder.launchedById(loggingParent));
  }

  @Test
  void rootExecutorHasNoLauncher() {
    ILoggingObject root = mock(ILoggingObject.class);
    when(root.getLogChannelId()).thenReturn("root-1");

    assertEquals("root-1", ExecutionBuilder.executionParentId(root));
    assertNull(ExecutionBuilder.launchedById(root));
    assertNull(ExecutionBuilder.executionParentId(null));
    assertNull(ExecutionBuilder.launchedById(null));
  }

  @Test
  void actionWithoutAWorkflowKeepsItsOwnId() {
    ILoggingObject loggingParent =
        mock(ILoggingObject.class, withSettings().extraInterfaces(IAction.class));
    when(((IAction) loggingParent).getParentWorkflow()).thenReturn(null);
    when(loggingParent.getLogChannelId()).thenReturn("action-1");

    assertEquals("action-1", ExecutionBuilder.executionParentId(loggingParent));
    assertNull(ExecutionBuilder.launchedById(loggingParent));
  }

  @Test
  void buildCopiesTheLauncher() {
    Execution execution =
        ExecutionBuilder.of()
            .withName("pipeline")
            .withId("pipe-1")
            .withParentId("workflow-1")
            .withLaunchedById("action-1")
            .build();

    assertEquals("workflow-1", execution.getParentId());
    assertEquals("action-1", execution.getLaunchedById());
  }
}
