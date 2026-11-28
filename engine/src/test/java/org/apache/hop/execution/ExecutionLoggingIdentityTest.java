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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RestoreHopEngineEnvironmentExtension.class)
class ExecutionLoggingIdentityTest {

  @BeforeAll
  static void init() throws HopException {
    HopEnvironment.init();
  }

  @Test
  void repeatedSameNameActionClonesHaveFreshIdsAndInheritWorkflowContext() {
    final var workflow = workflow("owner", null);
    workflow.setLogLevel(LogLevel.DEBUG);
    workflow.setContainerId("action-test-container");
    workflow.setVariable("EXECUTION_IDENTITY_TEST_VALUE", "inherited");

    final var template = new ActionStart("repeated action");
    template.setParentWorkflow(workflow);
    final var ids = new HashSet<String>();
    ids.add(template.getLogChannelId());

    // Match successive invocations of the same configured action in Workflow.
    for (int invocation = 0; invocation < 3; invocation++) {
      final var action = template.clone();
      action.copyFrom(workflow);
      action.setParentWorkflow(workflow);
      final var id = action.getLogChannelId();

      assertNotNull(id);
      assertTrue(ids.add(id), "Each invocation must have a fresh action log channel");
      assertEquals(template.getName(), action.getName());
      assertEquals(id, action.getLogChannel().getLogChannelId());
      assertEquals(id, action.getVariable(Const.INTERNAL_VARIABLE_ACTION_ID));
      assertSame(workflow, action.getParentWorkflow());
      assertSame(workflow, action.getParent());
      final var registered = LoggingRegistry.getInstance().getLoggingObject(id);
      assertNotNull(registered);
      assertEquals(workflow.getLogChannelId(), registered.getParent().getLogChannelId());
      assertEquals(LogLevel.DEBUG, action.getLogLevel());
      assertEquals(LogLevel.DEBUG, action.getLogChannel().getLogLevel());
      assertEquals(workflow.getContainerId(), action.getContainerId());
      assertEquals(workflow.getContainerId(), action.getLogChannel().getContainerObjectId());
      assertEquals("inherited", action.getVariable("EXECUTION_IDENTITY_TEST_VALUE"));
      assertSame(workflow, action.getParentVariables());
    }
  }

  @Test
  void actionLauncherIsLoggingParentButOwningWorkflowIsPersistedParent() throws HopException {
    final var owner = workflow("action owner", null);
    final var action = new ActionStart("launch child").clone();
    action.copyFrom(owner);
    action.setParentWorkflow(owner);

    assertEquals(owner.getLogChannelId(), ExecutionBuilder.executionParentId(action));
    assertEquals(action.getLogChannelId(), ExecutionBuilder.launchedById(action));

    final var child = workflow("action child", action);
    assertLauncherLinks(child, action, owner.getLogChannelId());
  }

  @Test
  void transformLauncherIsLoggingParentButOwningPipelineIsPersistedParent() throws HopException {
    @SuppressWarnings("unchecked")
    final IPipelineEngine<PipelineMeta> owner = mock(IPipelineEngine.class);
    when(owner.getLogChannelId()).thenReturn("owning-pipeline-id");

    final var launcher =
        mock(ILoggingObject.class, withSettings().extraInterfaces(ITransform.class));
    final var transform = (ITransform) launcher;
    when(transform.getPipeline()).thenReturn(owner);
    when(launcher.getLogChannelId()).thenReturn("launching-transform-id");
    when(launcher.getObjectName()).thenReturn("launch child");
    when(launcher.getObjectType()).thenReturn(LoggingObjectType.TRANSFORM);
    when(launcher.getLogLevel()).thenReturn(LogLevel.BASIC);
    when(launcher.getParent()).thenReturn(owner);

    assertEquals(owner.getLogChannelId(), ExecutionBuilder.executionParentId(launcher));
    assertEquals(launcher.getLogChannelId(), ExecutionBuilder.launchedById(launcher));

    final var child = workflow("transform child", launcher);
    assertLauncherLinks(child, launcher, owner.getLogChannelId());
  }

  private void assertLauncherLinks(
      final LocalWorkflowEngine child, final ILoggingObject launcher, final String ownerId)
      throws HopException {
    assertSame(launcher, child.getParent());
    final var loggingObject =
        LoggingRegistry.getInstance().getLoggingObject(child.getLogChannelId());
    assertNotNull(loggingObject);
    assertEquals(launcher.getLogChannelId(), loggingObject.getParent().getLogChannelId());

    final var execution = ExecutionBuilder.fromExecutor(child).build();
    assertEquals(child.getLogChannelId(), execution.getId());
    assertEquals(ownerId, execution.getParentId());
    assertEquals(launcher.getLogChannelId(), execution.getLaunchedById());
    assertNotEquals(loggingObject.getParent().getLogChannelId(), execution.getParentId());
  }

  private LocalWorkflowEngine workflow(final String name, final ILoggingObject parent) {
    final var meta = new WorkflowMeta();
    meta.setName(name);
    meta.addAction(new ActionMeta(new ActionStart("START")));
    final var workflow = new LocalWorkflowEngine(meta, parent);
    workflow.setMetadataProvider(new MemoryMetadataProvider());
    final var result = workflow.startExecution();
    assertEquals(0L, result.getNrErrors());
    assertTrue(result.getResult());
    return workflow;
  }
}
