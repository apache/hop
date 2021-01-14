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

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowHopMeta;
import org.apache.hop.workflow.WorkflowMeta;

import java.util.ArrayList;
import java.util.List;

public class HopGuiWorkflowHopContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiWorkflowHopContext";

  private WorkflowMeta workflowMeta;
  private WorkflowHopMeta hopMeta;
  private HopGuiWorkflowGraph workflowGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiWorkflowHopContext> lambdaBuilder;

  public HopGuiWorkflowHopContext( WorkflowMeta workflowMeta, WorkflowHopMeta hopMeta, HopGuiWorkflowGraph workflowGraph, Point click ) {
    this.workflowMeta = workflowMeta;
    this.hopMeta = hopMeta;
    this.workflowGraph = workflowGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a workflow note.
   * These are picked up from the annotations, mostly in WorkflowGraph. Plugins can add actions as well.
   *
   * @return The list of supported actions
   */
  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = getPluginActions( true );
    if ( pluginActions != null ) {
      for ( GuiAction pluginAction : pluginActions ) {
        actions.add( lambdaBuilder.createLambda( pluginAction, this, workflowGraph ) );
      }
    }

    return actions;
  }

  /**
   * Gets workflowMeta
   *
   * @return value of workflowMeta
   */
  public WorkflowMeta getWorkflowMeta() {
    return workflowMeta;
  }

  /**
   * @param workflowMeta The workflowMeta to set
   */
  public void setWorkflowMeta( WorkflowMeta workflowMeta ) {
    this.workflowMeta = workflowMeta;
  }

  /**
   * Gets hopMeta
   *
   * @return value of hopMeta
   */
  public WorkflowHopMeta getHopMeta() {
    return hopMeta;
  }

  /**
   * @param hopMeta The hopMeta to set
   */
  public void setHopMeta( WorkflowHopMeta hopMeta ) {
    this.hopMeta = hopMeta;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The pipelineGraph to set
   */
  public void setWorkflowGraph( HopGuiWorkflowGraph workflowGraph ) {
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

  /**
   * @param click The click to set
   */
  public void setClick( Point click ) {
    this.click = click;
  }


}
