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

package org.apache.hop.ui.hopgui.file.workflow.context;

import org.apache.hop.core.file.IHasFilename;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.dataorch.HopDataOrchestrationPerspective;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;

import java.util.ArrayList;
import java.util.List;

public class HopGuiWorkflowActionContext extends BaseGuiContextHandler
    implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiWorkflowActionContext";

  private WorkflowMeta workflowMeta;
  private ActionMeta actionMeta;
  private HopGuiWorkflowGraph workflowGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiWorkflowActionContext> lambdaBuilder;

  public HopGuiWorkflowActionContext(
      WorkflowMeta workflowMeta,
      ActionMeta actionMeta,
      HopGuiWorkflowGraph workflowGraph,
      Point click) {
    super();
    this.workflowMeta = workflowMeta;
    this.actionMeta = actionMeta;
    this.workflowGraph = workflowGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a action.
   *
   * @return The list of supported actions
   */
  @Override
  public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Put references at the start since we use those things a lot
    //
    IAction action = actionMeta.getAction();
    String[] objectDescriptions = action.getReferencedObjectDescriptions();
    for (int i = 0; objectDescriptions != null && i < objectDescriptions.length; i++) {
      final String objectDescription = objectDescriptions[i];
      if (action.isReferencedObjectEnabled()[i]) {
        final int index = i;
        GuiAction openReferencedAction =
            new GuiAction(
                "action-open-referenced-" + objectDescription,
                GuiActionType.Info,
                "open: " + objectDescription,
                "This opens up the file referenced in the action",
                "ui/images/open.svg",
                (shiftAction, controlAction, t) ->
                    openReferencedObject(action, objectDescription, index));
        openReferencedAction.setCategory("Basic");
        openReferencedAction.setCategoryOrder("1");
        actions.add(openReferencedAction);
      }
    }

    // Get the actions from the plugins, sorted by ID...
    //
    List<GuiAction> pluginActions = getPluginActions(true);
    if (pluginActions != null) {
      for (GuiAction pluginAction : pluginActions) {
        actions.add(lambdaBuilder.createLambda(pluginAction, this, workflowGraph));
      }
    }

    return actions;
  }

  private void openReferencedObject(IAction action, String objectDescription, int index) {
    HopGui hopGui = HopGui.getInstance();
    try {
      IHasFilename hasFilename =
          action.loadReferencedObject(
              index, workflowMeta.getMetadataProvider(), workflowGraph.getVariables());
      if (hasFilename != null) {
        String filename = workflowGraph.getVariables().resolve(hasFilename.getFilename());

        // Is this object already loaded?
        //
        HopDataOrchestrationPerspective perspective = HopGui.getDataOrchestrationPerspective();
        TabItemHandler tabItemHandler = perspective.findTabItemHandlerWithFilename(filename);
        if (tabItemHandler != null) {
          perspective.switchToTab(tabItemHandler);
        } else {
          hopGui.fileDelegate.fileOpen(filename);
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          hopGui.getShell(),
          "Error",
          "Error opening referenced object '" + objectDescription + "'",
          e);
    }
  }

  /**
   * Gets workflowMeta
   *
   * @return value of workflowMeta
   */
  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  /** @param workflowMeta The workflowMeta to set */
  public void setWorkflowMeta(WorkflowMeta workflowMeta) {
    this.workflowMeta = workflowMeta;
  }

  /**
   * Gets actionCopy
   *
   * @return value of actionCopy
   */
  public ActionMeta getActionMeta() {
    return actionMeta;
  }

  /** @param actionMeta The actionCopy to set */
  public void setActionMeta(ActionMeta actionMeta) {
    this.actionMeta = actionMeta;
  }

  /**
   * Gets workflow graph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /** @param workflowGraph The workflow graph to set */
  public void setWorkflowGraph(HopGuiWorkflowGraph workflowGraph) {
    this.workflowGraph = workflowGraph;
  }

  /**
   * Gets click
   *
   * @return value of click
   */
  public Point getClick() {
    return click;
  }

  /** @param click The click to set */
  public void setClick(Point click) {
    this.click = click;
  }

  /**
   * Gets lambdaBuilder
   *
   * @return value of lambdaBuilder
   */
  public GuiActionLambdaBuilder<HopGuiWorkflowActionContext> getLambdaBuilder() {
    return lambdaBuilder;
  }

  /** @param lambdaBuilder The lambdaBuilder to set */
  public void setLambdaBuilder(GuiActionLambdaBuilder<HopGuiWorkflowActionContext> lambdaBuilder) {
    this.lambdaBuilder = lambdaBuilder;
  }
}
