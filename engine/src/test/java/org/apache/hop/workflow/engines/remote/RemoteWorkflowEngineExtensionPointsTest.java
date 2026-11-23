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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * On the client, a remote workflow run fires WorkflowStart and WorkflowFinish as the local engine
 * does: once each, WorkflowFinish before the execution finished listeners, also when the run fails.
 */
class RemoteWorkflowEngineExtensionPointsTest {

  static final List<String> FIRED = new CopyOnWriteArrayList<>();

  @ExtensionPoint(
      id = "RemoteWorkflowTestStartXp",
      extensionPointId = "WorkflowStart",
      description = "")
  public static class StartXp implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow) {
      if (workflow instanceof RemoteWorkflowEngine) {
        FIRED.add("WorkflowStart");
      }
    }
  }

  @ExtensionPoint(
      id = "RemoteWorkflowTestFinishXp",
      extensionPointId = "WorkflowFinish",
      description = "")
  public static class FinishXp implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow) {
      if (workflow instanceof RemoteWorkflowEngine) {
        FIRED.add("WorkflowFinish");
      }
    }
  }

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        StartXp.class.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
    registry.registerPluginClass(
        FinishXp.class.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
  }

  @BeforeEach
  void clear() {
    FIRED.clear();
  }

  @Test
  void aRunFiresStartThenFinishBeforeTheFinishedListeners() {
    RemoteWorkflowEngine engine = engine(false);
    engine.addExecutionFinishedListener(workflow -> FIRED.add("finished listener"));

    engine.startExecution();

    assertEquals(List.of("WorkflowStart", "WorkflowFinish", "finished listener"), FIRED);
  }

  @Test
  void aRunThatCannotBeSubmittedStillFiresFinishOnce() {
    RemoteWorkflowEngine engine = engine(true);
    engine.addExecutionFinishedListener(workflow -> FIRED.add("finished listener"));

    engine.startExecution();

    assertEquals(List.of("WorkflowFinish", "finished listener"), FIRED);
    assertEquals(1, engine.getResult().getNrErrors());
  }

  /** An engine that talks to no server: the submission succeeds or fails, the run ends at once. */
  private static RemoteWorkflowEngine engine(boolean submissionFails) {
    RemoteWorkflowEngine engine =
        new RemoteWorkflowEngine() {
          @Override
          protected void submitToRemoteServer() throws HopException {
            if (submissionFails) {
              throw new HopException("The Hop Server is not there");
            }
          }

          @Override
          public void monitorRemoteWorkflowUntilFinished() {
            // The remote run ended
          }
        };
    WorkflowMeta workflowMeta = new WorkflowMeta();
    workflowMeta.setName("remote-extension-points");
    engine.setWorkflowMeta(workflowMeta);
    return engine;
  }
}
