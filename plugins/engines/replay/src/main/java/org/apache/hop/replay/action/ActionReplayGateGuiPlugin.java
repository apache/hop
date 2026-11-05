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

package org.apache.hop.replay.action;

import org.apache.hop.core.action.GuiContextAction;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.replay.ReplayGate;
import org.apache.hop.replay.ReplayGateDialog;
import org.apache.hop.replay.ReplayGateUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.workflow.context.HopGuiWorkflowActionContext;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;

@GuiPlugin
public class ActionReplayGateGuiPlugin {

  private static final Class<?> PKG = ActionReplayGateGuiPlugin.class;

  @GuiContextAction(
      id = "workflow-graph-action-11500-configure-replay-gate",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Modify,
      name = "i18n::ReplayAction.ConfigureGate.Label",
      tooltip = "i18n::ReplayAction.ConfigureGate.ToolTip",
      image = "replay-gate.svg",
      category = "i18n::ReplayAction.Category",
      categoryOrder = "8")
  public void configureReplayGate(HopGuiWorkflowActionContext context) {
    HopGui hopGui = HopGui.getInstance();
    try {
      WorkflowMeta workflowMeta = context.getWorkflowMeta();
      ActionMeta actionMeta = context.getActionMeta();
      IVariables variables = context.getWorkflowGraph().getVariables();

      ReplayGate gate = ReplayGateUtil.getReplayGate(actionMeta);
      if (gate == null) {
        gate = new ReplayGate();
      }

      ReplayGateDialog dialog =
          new ReplayGateDialog(hopGui.getActiveShell(), variables, workflowMeta, actionMeta, gate);
      if (dialog.openGate()) {
        ReplayGateUtil.storeReplayGate(actionMeta, gate);
        workflowMeta.setChanged();
        context.getWorkflowGraph().updateGui();
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getActiveShell(),
          BaseMessages.getString(PKG, "ReplayAction.Dialog.Error.Title"),
          BaseMessages.getString(PKG, "ReplayAction.Dialog.Error.Message"),
          e);
    }
  }

  @GuiContextAction(
      id = "workflow-graph-action-11501-clear-replay-gate",
      parentId = HopGuiWorkflowActionContext.CONTEXT_ID,
      type = GuiActionType.Delete,
      name = "i18n::ReplayAction.ClearGate.Label",
      tooltip = "i18n::ReplayAction.ClearGate.ToolTip",
      image = "replay-gate.svg",
      category = "i18n::ReplayAction.Category",
      categoryOrder = "8")
  public void clearReplayGate(HopGuiWorkflowActionContext context) {
    WorkflowMeta workflowMeta = context.getWorkflowMeta();
    ActionMeta actionMeta = context.getActionMeta();
    ReplayGateUtil.clearReplayGate(actionMeta);
    workflowMeta.setChanged();
    context.getWorkflowGraph().updateGui();
  }
}
