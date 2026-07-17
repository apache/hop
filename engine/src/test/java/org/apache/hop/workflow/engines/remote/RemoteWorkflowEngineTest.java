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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.WorkflowTracker;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.ActionResult;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
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

  /**
   * The run configuration a remote run configuration hands the workflow to is used on the server.
   * When that leads back to a remote run configuration the workflow keeps being handed on and
   * registered again and again. See issue #4086.
   */
  @Test
  void runConfigurationThatRefersToItselfIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    saveRunConfiguration(metadataProvider, remoteRunConfiguration("remote", "remote"));

    HopException e =
        assertThrows(HopException.class, () -> validateChain(metadataProvider, "remote"));

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /** The name of the run configuration to use on the server can hold a variable. */
  @Test
  void runConfigurationThatRefersToItselfThroughAVariableIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    saveRunConfiguration(metadataProvider, remoteRunConfiguration("remote", "${RUN_CONFIG}"));

    RemoteWorkflowEngine engine = new RemoteWorkflowEngine();
    engine.setMetadataProvider(metadataProvider);
    engine.setVariable("RUN_CONFIG", "remote");

    HopException e =
        assertThrows(
            HopException.class,
            () ->
                engine.validateRunConfigurationChain(
                    metadataProvider.getSerializer(WorkflowRunConfiguration.class).load("remote")));

    assertTrue(e.getMessage().contains("remote -> remote"), "The chain should be reported: " + e);
  }

  /** A chain of remote run configurations that leads back to an earlier one is rejected. */
  @Test
  void cyclicalRunConfigurationChainIsRejected() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    saveRunConfiguration(metadataProvider, remoteRunConfiguration("first", "second"));
    saveRunConfiguration(metadataProvider, remoteRunConfiguration("second", "first"));

    HopException e =
        assertThrows(HopException.class, () -> validateChain(metadataProvider, "first"));

    assertTrue(
        e.getMessage().contains("first -> second -> first"), "The chain should be reported: " + e);
  }

  /** Handing the workflow to a local run configuration is what a remote one is meant to do. */
  @Test
  void runConfigurationThatLeadsToALocalOneIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    saveRunConfiguration(metadataProvider, remoteRunConfiguration("remote", "local"));
    saveRunConfiguration(
        metadataProvider,
        new WorkflowRunConfiguration(
            "local", "", null, new LocalWorkflowRunConfiguration(), false));

    assertDoesNotThrow(() -> validateChain(metadataProvider, "remote"));
  }

  /** A run configuration that only exists on the server cannot be followed from here. */
  @Test
  void runConfigurationThatIsUnknownHereIsAccepted() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    saveRunConfiguration(
        metadataProvider, remoteRunConfiguration("remote", "only-known-on-the-server"));

    assertDoesNotThrow(() -> validateChain(metadataProvider, "remote"));
  }

  private static WorkflowRunConfiguration remoteRunConfiguration(
      String name, String runConfigurationName) {
    RemoteWorkflowRunConfiguration engineConfiguration = new RemoteWorkflowRunConfiguration();
    engineConfiguration.setHopServerName("a-server");
    engineConfiguration.setRunConfigurationName(runConfigurationName);
    return new WorkflowRunConfiguration(name, "", null, engineConfiguration, false);
  }

  private static void saveRunConfiguration(
      MemoryMetadataProvider metadataProvider, WorkflowRunConfiguration runConfiguration)
      throws HopException {
    metadataProvider.getSerializer(WorkflowRunConfiguration.class).save(runConfiguration);
  }

  private static void validateChain(MemoryMetadataProvider metadataProvider, String name)
      throws HopException {
    RemoteWorkflowEngine engine = new RemoteWorkflowEngine();
    engine.setMetadataProvider(metadataProvider);
    engine.validateRunConfigurationChain(
        metadataProvider.getSerializer(WorkflowRunConfiguration.class).load(name));
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
