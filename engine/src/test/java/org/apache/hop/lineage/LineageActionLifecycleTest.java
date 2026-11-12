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

package org.apache.hop.lineage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Result;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.model.LineageEvent;
import org.apache.hop.lineage.model.RunLifecycleLineagePayload;
import org.apache.hop.lineage.model.RunLifecyclePhase;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowExecutionExtension;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionBase;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * An action run is identified by the log channel of the action clone the workflow executes. Its
 * STARTED and FINISHED events have to carry that id, not the workflow's: the action is a job of its
 * own ({@code <workflow>/<action>}), and a pipeline it starts names that same id as its parent.
 */
class LineageActionLifecycleTest {

  @SuppressWarnings("unchecked")
  private static IWorkflowEngine<WorkflowMeta> workflow() {
    IWorkflowEngine<WorkflowMeta> wf = mock(IWorkflowEngine.class);
    WorkflowMeta meta = new WorkflowMeta();
    meta.setName("nightly-load");
    when(wf.getWorkflowMeta()).thenReturn(meta);
    when(wf.getLogChannelId()).thenReturn("workflow-run-id");
    return wf;
  }

  private static ActionBase executingAction() {
    ActionBase action = mock(ActionBase.class);
    ILogChannel log = mock(ILogChannel.class);
    when(log.getLogChannelId()).thenReturn("action-run-id");
    when(action.getName()).thenReturn("Run orders");
    when(action.getDescription()).thenReturn("Loads the orders");
    when(action.getLogChannel()).thenReturn(log);
    return action;
  }

  @Test
  void actionStartCarriesTheExecutingActionsRunId() {
    LineageHub hub = mock(LineageHub.class);
    IWorkflowEngine<WorkflowMeta> wf = workflow();
    ActionBase action = executingAction();

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      List<IActionListener> listeners = new ArrayList<>();
      when(wf.getActionListeners()).thenReturn(listeners);
      doAnswer(invocation -> listeners.add(invocation.getArgument(0)))
          .when(wf)
          .addActionListener(any());

      LineageActionStartListener.register(wf);
      listeners.get(0).beforeExecution(wf, new ActionMeta(action), action);
    }

    ArgumentCaptor<LineageEvent> event = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub).emit(event.capture());
    assertEquals(
        RunLifecyclePhase.STARTED,
        ((RunLifecycleLineagePayload) event.getValue().getPayload()).getPhase());
    assertEquals("action-run-id", event.getValue().getContext().getLogChannelId());
    assertEquals("Run orders", event.getValue().getContext().getActionName());
    assertEquals("nightly-load", event.getValue().getContext().getWorkflowName());
    assertEquals(
        "Loads the orders",
        event
            .getValue()
            .getContext()
            .getAttributes()
            .get(LineageRunLifecycleEmitter.ATTR_DESCRIPTION));
  }

  @Test
  void actionStartListenerIsRegisteredOncePerWorkflow() {
    IWorkflowEngine<WorkflowMeta> wf = workflow();
    List<IActionListener> listeners = new ArrayList<>();
    when(wf.getActionListeners()).thenReturn(listeners);
    doAnswer(invocation -> listeners.add(invocation.getArgument(0)))
        .when(wf)
        .addActionListener(any());

    LineageActionStartListener.register(wf);
    LineageActionStartListener.register(wf);

    verify(wf, times(1)).addActionListener(any());
  }

  @Test
  void actionFinishCarriesTheExecutingActionsRunId() {
    LineageHub hub = mock(LineageHub.class);
    WorkflowExecutionExtension extension =
        new WorkflowExecutionExtension(
            workflow(), new Result(), new ActionMeta(executingAction()), true);
    extension.actionLogChannelId = "action-run-id";
    extension.actionExecutionResult = new Result();

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitWorkflowAction(extension, RunLifecyclePhase.FINISHED, null);
    }

    ArgumentCaptor<LineageEvent> event = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub).emit(event.capture());
    assertEquals("action-run-id", event.getValue().getContext().getLogChannelId());
  }

  @Test
  void skippedActionEmitsNothing() {
    LineageHub hub = mock(LineageHub.class);
    // An extension point told the workflow not to execute the action: no clone, no run id.
    WorkflowExecutionExtension extension =
        new WorkflowExecutionExtension(
            workflow(), new Result(), new ActionMeta(executingAction()), false);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      LineageRunLifecycleEmitter.emitWorkflowAction(extension, RunLifecyclePhase.FINISHED, null);
    }

    verify(hub, never()).emit(any());
  }

  private static IActionListener registeredListener(IWorkflowEngine<WorkflowMeta> wf) {
    List<IActionListener> listeners = new ArrayList<>();
    when(wf.getActionListeners()).thenReturn(listeners);
    doAnswer(invocation -> listeners.add(invocation.getArgument(0)))
        .when(wf)
        .addActionListener(any());
    LineageActionStartListener.register(wf);
    return listeners.get(0);
  }

  // The listener runs inside the workflow's action execution: a lineage problem may not fail it.
  @Test
  void aFailingStartEmitDoesNotFailTheAction() {
    LineageHub hub = mock(LineageHub.class);
    doThrow(new IllegalStateException("sink exploded")).when(hub).emit(any());
    IWorkflowEngine<WorkflowMeta> wf = workflow();
    ILogChannel workflowLog = mock(ILogChannel.class);
    when(wf.getLogChannel()).thenReturn(workflowLog);
    ActionBase action = executingAction();
    IActionListener listener = registeredListener(wf);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      assertDoesNotThrow(() -> listener.beforeExecution(wf, new ActionMeta(action), action));
    }

    verify(workflowLog).logError(anyString(), any(Throwable.class));
  }

  // execute() threw: the workflow skips WorkflowAfterActionExecution, so the run is closed as
  // FAILED when the workflow ends, under the run id its STARTED carried.
  @Test
  void anActionThatThrewIsClosedAsFailedWhenTheWorkflowEnds() {
    LineageHub hub = mock(LineageHub.class);
    IWorkflowEngine<WorkflowMeta> wf = workflow();
    ActionBase action = executingAction();
    IActionListener listener = registeredListener(wf);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      listener.beforeExecution(wf, new ActionMeta(action), action);
      LineageActionStartListener.failUnfinishedActions(wf);
      // Closed once: a second end of the workflow has nothing left to close.
      LineageActionStartListener.failUnfinishedActions(wf);
    }

    ArgumentCaptor<LineageEvent> events = ArgumentCaptor.forClass(LineageEvent.class);
    verify(hub, times(2)).emit(events.capture());
    LineageEvent failed = events.getAllValues().get(1);
    assertEquals(
        RunLifecyclePhase.FAILED, ((RunLifecycleLineagePayload) failed.getPayload()).getPhase());
    assertEquals("action-run-id", failed.getContext().getLogChannelId());
    assertEquals(
        LineageRunLifecycleEmitter.FAILED,
        failed.getContext().getAttributes().get(LineageRunLifecycleEmitter.OUTCOME));
  }

  @Test
  void anActionThatEndedIsNotClosedAgain() {
    LineageHub hub = mock(LineageHub.class);
    IWorkflowEngine<WorkflowMeta> wf = workflow();
    ActionBase action = executingAction();
    IActionListener listener = registeredListener(wf);

    try (MockedStatic<LineageHub> staticHub = Mockito.mockStatic(LineageHub.class)) {
      staticHub.when(LineageHub::getInstance).thenReturn(hub);
      listener.beforeExecution(wf, new ActionMeta(action), action);
      LineageActionStartListener.actionEnded(wf, "action-run-id");
      LineageActionStartListener.failUnfinishedActions(wf);
    }

    verify(hub, times(1)).emit(any());
  }
}
