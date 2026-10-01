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

package org.apache.hop.ai.ui;

import org.apache.hop.ai.advisor.AiAdvisorLocations;
import org.apache.hop.ai.advisor.AiAdvisorOpenRequest;
import org.apache.hop.ai.advisors.workflow.WorkflowAiAdvisor;
import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowContext;
import org.apache.hop.workflow.WorkflowMeta;

@GuiPlugin(description = "i18n::WorkflowAiGuiPlugin.Description", classLoaderGroup = "hop-ai")
public class WorkflowAiGuiPlugin {

  public static final String TOOLBAR_ITEM_AI_HELP = "HopGuiWorkflowGraph-ToolBar-10048-ai-help";

  @GuiToolbarElement(
      root = HopGuiWorkflowGraph.GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_AI_HELP,
      toolTip = "i18n::WorkflowAiGuiPlugin.Toolbar.AiHelp.Tooltip",
      image = "ai-provider.svg")
  public void openAiAdvisorToolbar() {
    HopGuiWorkflowGraph workflowGraph = HopGui.getActiveWorkflowGraph();
    if (workflowGraph == null) {
      return;
    }
    openAiAdvisor(workflowGraph, null);
  }

  @GuiContextAction(
      id = "workflow-graph-zzz-ai-help",
      parentId = HopGuiWorkflowContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::WorkflowAiGuiPlugin.AiHelp.Name",
      tooltip = "i18n::WorkflowAiGuiPlugin.AiHelp.Tooltip",
      image = "ai-provider.svg",
      category =
          "i18n:org.apache.hop.ui.hopgui.file.workflow:HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void openAiAdvisorWorkflowContext(HopGuiWorkflowContext context) {
    if (context.getWorkflowGraph() != null) {
      openAiAdvisor(context.getWorkflowGraph(), null);
    }
  }

  @GuiContextAction(
      id = "workflow-graph-action-zzz-ai-help",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::WorkflowAiGuiPlugin.AiHelp.Name",
      tooltip = "i18n::WorkflowAiGuiPlugin.AiHelp.Tooltip",
      image = "ai-provider.svg",
      category =
          "i18n:org.apache.hop.ui.hopgui.file.workflow:HopGuiWorkflowGraph.ContextualAction.Category.Basic.Text",
      categoryOrder = "1")
  public void openAiAdvisorActionContext(HopGuiWorkflowActionContext context) {
    if (context.getWorkflowGraph() != null) {
      String focus = context.getActionMeta() != null ? context.getActionMeta().getName() : null;
      openAiAdvisor(context.getWorkflowGraph(), focus);
    }
  }

  private static void openAiAdvisor(HopGuiWorkflowGraph workflowGraph, String focusActionName) {
    WorkflowMeta workflowMeta = workflowGraph.getWorkflowMeta();
    if (workflowMeta == null) {
      return;
    }
    AiAdvisorOpenRequest request = new AiAdvisorOpenRequest();
    request.setAdvisorPluginId(WorkflowAiAdvisor.ID);
    request.setLocation(AiAdvisorLocations.WORKFLOW_GRAPH);
    request.setAreaLabel(
        BaseMessages.getString(WorkflowAiGuiPlugin.class, "WorkflowAiGuiPlugin.Area.Label"));
    request.setPreferFloatingWindow(true);
    request.setArtifact(workflowMeta);
    request.setArtifactName(workflowMeta.getName());
    request.setArtifactKind("workflow");
    request.setTitle(workflowMeta.getName());
    request.setFocusNodeName(focusActionName);
    request.setLogSupplier(() -> AiAdvisorLogSupport.readWorkflowLog(workflowGraph));
    AiAdvisorViews.openSession(workflowGraph.getHopGui(), request);
  }
}
