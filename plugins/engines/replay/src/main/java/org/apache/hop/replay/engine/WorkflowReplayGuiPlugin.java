/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.replay.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.execution.Execution;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.replay.ReplayDefaults;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.execution.ExecutionPerspective;
import org.apache.hop.ui.hopgui.perspective.execution.IExecutionViewer;
import org.apache.hop.ui.hopgui.perspective.execution.WorkflowExecutionViewer;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.workflow.dialog.WorkflowExecutionConfigurationDialog;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.config.IWorkflowEngineRunConfiguration;
import org.apache.hop.workflow.config.WorkflowRunConfiguration;

@GuiPlugin
public class WorkflowReplayGuiPlugin {

  public static final String TOOLBAR_ITEM_REPLAY = "WorkflowExecutionViewer-Toolbar-10200-Replay";

  private static final Class<?> PKG = WorkflowReplayGuiPlugin.class;

  @GuiToolbarElement(
      root = WorkflowExecutionViewer.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REPLAY,
      toolTip = "i18n::WorkflowReplayGuiPlugin.Toolbar.Tooltip",
      image = "ui/images/replay.svg")
  public void replayWorkflow() {
    try {
      IExecutionViewer activeViewer = ExecutionPerspective.getInstance().getActiveViewer();
      if (!(activeViewer instanceof WorkflowExecutionViewer viewer)) {
        return;
      }
      WorkflowMeta viewerMeta = viewer.getWorkflowMeta();
      if (viewerMeta == null) {
        return;
      }

      HopGui hopGui = viewer.getHopGui();
      Execution execution = viewer.getExecution();
      ExplorerPerspective explorer = HopGui.getExplorerPerspective();

      HopGuiWorkflowGraph workflowGraph = null;
      if (execution != null && execution.getId() != null) {
        workflowGraph = explorer.findWorkflow(execution.getId());
      }
      if (workflowGraph == null && viewerMeta.getFilename() != null) {
        workflowGraph = explorer.findOpenWorkflow(viewerMeta.getFilename());
      }

      WorkflowMeta targetWorkflowMeta;
      if (workflowGraph != null) {
        targetWorkflowMeta = workflowGraph.getWorkflowMeta();
      } else if (viewerMeta.getFilename() != null) {
        try {
          targetWorkflowMeta =
              new WorkflowMeta(
                  hopGui.getVariables(), viewerMeta.getFilename(), hopGui.getMetadataProvider());
        } catch (Exception e) {
          targetWorkflowMeta = viewerMeta;
        }
      } else {
        targetWorkflowMeta = viewerMeta;
      }

      WorkflowExecutionConfiguration executionConfiguration = new WorkflowExecutionConfiguration();
      executionConfiguration.setGatheringMetrics(true);
      String replayConfig = findFirstReplayRunConfiguration(hopGui.getMetadataProvider());
      if (replayConfig != null) {
        executionConfiguration.setRunConfiguration(replayConfig);
      }
      executionConfiguration.setLogLevel(DefaultLogLevel.getLogLevel());

      Map<String, String> variableMap = new HashMap<>();
      executionConfiguration.setVariablesMap(variableMap);
      executionConfiguration.getUsedVariables(targetWorkflowMeta, hopGui.getVariables());
      if (execution != null && execution.getVariableValues() != null) {
        for (String name : new ArrayList<>(variableMap.keySet())) {
          if (name.startsWith(Const.INTERNAL_VARIABLE_PREFIX)) {
            continue;
          }
          String previous = execution.getVariableValues().get(name);
          if (previous != null) {
            variableMap.put(name, previous);
          }
        }
      }
      if (execution != null && execution.getParameterValues() != null) {
        executionConfiguration.getParametersMap().putAll(execution.getParameterValues());
      }
      if (execution != null && StringUtils.isNotEmpty(execution.getId())) {
        variableMap.put(ReplayDefaults.VARIABLE_SOURCE_EXECUTION_ID, execution.getId());
      }

      WorkflowExecutionConfigurationDialog dialog =
          new WorkflowExecutionConfigurationDialog(
              hopGui.getShell(), executionConfiguration, targetWorkflowMeta);
      if (!dialog.open()) {
        return;
      }

      if (workflowGraph == null) {
        if (targetWorkflowMeta.getFilename() != null) {
          IHopFileTypeHandler handler =
              hopGui.fileDelegate.fileOpen(targetWorkflowMeta.getFilename());
          if (handler instanceof HopGuiWorkflowGraph graph) {
            workflowGraph = graph;
          }
        } else {
          IHopFileTypeHandler handler = explorer.addWorkflow(targetWorkflowMeta);
          if (handler instanceof HopGuiWorkflowGraph graph) {
            workflowGraph = graph;
          }
        }
      }
      if (workflowGraph == null) {
        return;
      }
      explorer.setActiveFileTypeHandler(workflowGraph);
      explorer.activate();
      workflowGraph.workflowRunDelegate.launchConfiguredWorkflow(
          workflowGraph.getWorkflowMeta(), executionConfiguration);
    } catch (Exception e) {
      HopGui hopGui = HopGui.getInstance();
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "WorkflowReplayGuiPlugin.Error.Title"),
          BaseMessages.getString(PKG, "WorkflowReplayGuiPlugin.Error.Message"),
          e);
    }
  }

  public static String findFirstReplayRunConfiguration(IHopMetadataProvider metadataProvider) {
    if (metadataProvider == null) {
      return null;
    }
    try {
      IHopMetadataSerializer<WorkflowRunConfiguration> serializer =
          metadataProvider.getSerializer(WorkflowRunConfiguration.class);
      List<WorkflowRunConfiguration> runConfigs = serializer.loadAll();
      for (WorkflowRunConfiguration runConfig : runConfigs) {
        IWorkflowEngineRunConfiguration engineConfig = runConfig.getEngineRunConfiguration();
        if (engineConfig != null && "Replay".equals(engineConfig.getEnginePluginId())) {
          return runConfig.getName();
        }
      }
    } catch (Exception e) {
      // Leave the run configuration for the user to pick.
    }
    return null;
  }
}
