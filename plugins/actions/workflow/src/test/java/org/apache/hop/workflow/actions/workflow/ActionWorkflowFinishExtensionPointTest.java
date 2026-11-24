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

package org.apache.hop.workflow.actions.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.Result;
import org.apache.hop.core.annotations.Action;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.ExtensionPointPluginType;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.actions.start.ActionStart;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A child workflow that a Workflow action runs on the local engine fires WorkflowFinish once, like
 * the workflow that runs it. The thread the action runs the child on fired it a second time.
 */
class ActionWorkflowFinishExtensionPointTest {

  /** The name of every workflow WorkflowFinish was fired for, once per call. */
  static final List<String> FINISHED = new CopyOnWriteArrayList<>();

  @ExtensionPoint(
      id = "ActionWorkflowTestFinishXp",
      extensionPointId = "WorkflowFinish",
      description = "")
  public static class FinishXp implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {
    @Override
    public void callExtensionPoint(
        ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow) {
      FINISHED.add(workflow.getWorkflowMeta().getName());
    }
  }

  @TempDir Path tempDir;

  @BeforeAll
  static void initHop() throws Exception {
    HopEnvironment.init();
    PluginRegistry registry = PluginRegistry.getInstance();
    registry.registerPluginClass(
        FinishXp.class.getName(), ExtensionPointPluginType.class, ExtensionPoint.class);
    registry.registerPluginClass(
        ActionWorkflow.class.getName(), ActionPluginType.class, Action.class);
  }

  @BeforeEach
  void clear() {
    FINISHED.clear();
  }

  @Test
  void aChildWorkflowFiresWorkflowFinishOnce() throws Exception {
    MemoryMetadataProvider metadataProvider = new MemoryMetadataProvider();
    WorkflowRunConfiguration local =
        new WorkflowRunConfiguration("local", "", null, new LocalWorkflowRunConfiguration(), false);
    metadataProvider.getSerializer(WorkflowRunConfiguration.class).save(local);

    // The child: only a Start action
    WorkflowMeta child = new WorkflowMeta();
    child.setName("child");
    child.addAction(new ActionMeta(new ActionStart("Start")));
    Path childFile = tempDir.resolve("child.hwf");
    Files.writeString(childFile, child.getXml(new Variables()), StandardCharsets.UTF_8);

    // The parent: Start, then a Workflow action that runs the child and waits for it
    WorkflowMeta parent = new WorkflowMeta();
    parent.setName("parent");
    parent.setMetadataProvider(metadataProvider);
    ActionMeta start = new ActionMeta(new ActionStart("Start"));
    ActionWorkflow runChild = new ActionWorkflow("run child");
    runChild.setFileName(childFile.toString());
    runChild.setRunConfiguration("local");
    runChild.setWaitingToFinish(true);
    ActionMeta runChildMeta = new ActionMeta(runChild);
    parent.addAction(start);
    parent.addAction(runChildMeta);
    parent.addWorkflowHop(new WorkflowHopMeta(start, runChildMeta));

    LocalWorkflowEngine engine =
        new LocalWorkflowEngine(
            parent, new LoggingObject("ActionWorkflowFinishExtensionPointTest"));
    engine.setMetadataProvider(metadataProvider);
    Result result = engine.startExecution();

    assertTrue(result.getResult(), "the parent workflow ran: " + result.getLogText());
    assertEquals(List.of("child", "parent"), FINISHED);
  }
}
