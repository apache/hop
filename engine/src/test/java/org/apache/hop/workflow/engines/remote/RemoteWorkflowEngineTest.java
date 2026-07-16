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

package org.apache.hop.workflow.engines.remote;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Result;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.www.HopServerWorkflowStatus;
import org.apache.hop.www.RemoteHopServer;
import org.junit.jupiter.api.Test;

class RemoteWorkflowEngineTest {

  private static final String WORKFLOW_NAME = "remote-workflow";
  private static final String CONTAINER_ID = "container-1";

  /**
   * The workflow metrics view reads the tracker of the engine, so a workflow running on a server
   * has to have the tracker it reports copied into the engine. See issue #3685.
   */
  @Test
  void getWorkflowStatusFillsTheTracker() throws Exception {
    RemoteWorkflowEngine engine = createEngine(createServerTracker());

    engine.getWorkflowStatus();

    WorkflowTracker tracker = engine.getWorkflowTracker();
    assertEquals(WORKFLOW_NAME, tracker.getWorkflowName());
    assertEquals(2, tracker.nrWorkflowTrackers());
    assertEquals("an action", tracker.getWorkflowTracker(0).getActionResult().getActionName());
    assertEquals("Start of action", tracker.getWorkflowTracker(0).getActionResult().getComment());
    assertEquals("Action finished", tracker.getWorkflowTracker(1).getActionResult().getComment());
  }

  /**
   * The tracker instance has to be kept: the GUI holds on to it from the moment the workflow starts
   * and would otherwise keep reading an object that is never updated.
   */
  @Test
  void getWorkflowStatusKeepsTheTrackerInstance() throws Exception {
    RemoteWorkflowEngine engine = createEngine(createServerTracker());
    WorkflowTracker trackerBefore = engine.getWorkflowTracker();

    engine.getWorkflowStatus();

    assertSame(trackerBefore, engine.getWorkflowTracker());
    assertEquals(2, trackerBefore.nrWorkflowTrackers());
  }

  /** A server that does not report a tracker simply leaves the tracker of the engine alone. */
  @Test
  void getWorkflowStatusWithoutTrackerLeavesTrackerEmpty() throws Exception {
    RemoteWorkflowEngine engine = createEngine(null);

    engine.getWorkflowStatus();

    assertEquals(0, engine.getWorkflowTracker().nrWorkflowTrackers());
  }

  private RemoteWorkflowEngine createEngine(WorkflowTracker<WorkflowMeta> serverTracker)
      throws Exception {
    RemoteWorkflowEngine engine = new RemoteWorkflowEngine();

    WorkflowMeta workflowMeta = mock(WorkflowMeta.class);
    when(workflowMeta.getName()).thenReturn(WORKFLOW_NAME);
    engine.setWorkflowMeta(workflowMeta);
    engine.containerId = CONTAINER_ID;

    HopServerWorkflowStatus status =
        new HopServerWorkflowStatus(WORKFLOW_NAME, CONTAINER_ID, "Finished");
    status.setResult(new Result());
    status.setWorkflowTracker(serverTracker);

    RemoteHopServer hopServer = mock(RemoteHopServer.class);
    when(hopServer.requestWorkflowStatus(any(), eq(WORKFLOW_NAME), eq(CONTAINER_ID), anyInt()))
        .thenReturn(status);
    engine.hopServer = hopServer;

    return engine;
  }

  private WorkflowTracker<WorkflowMeta> createServerTracker() {
    WorkflowTracker<WorkflowMeta> serverTracker = new WorkflowTracker<>((WorkflowMeta) null);
    serverTracker.setWorkflowName(WORKFLOW_NAME);

    ActionResult start = new ActionResult();
    start.setActionName("an action");
    start.setComment("Start of action");
    serverTracker.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, start));

    ActionResult end = new ActionResult();
    end.setActionName("an action");
    end.setComment("Action finished");
    end.setResult(new Result());
    serverTracker.addWorkflowTracker(new WorkflowTracker<>((WorkflowMeta) null, end));

    return serverTracker;
  }
}
