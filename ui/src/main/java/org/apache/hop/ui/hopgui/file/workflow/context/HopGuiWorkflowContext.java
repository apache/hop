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

import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.action.GuiAction;
import org.apache.hop.core.gui.plugin.action.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.action.GuiActionType;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HopGuiWorkflowContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiWorkflowContext";

  private WorkflowMeta workflowMeta;
  private HopGuiWorkflowGraph workflowGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiWorkflowContext> lambdaBuilder;

  public HopGuiWorkflowContext( WorkflowMeta workflowMeta, HopGuiWorkflowGraph workflowGraph, Point click ) {
    this.workflowMeta = workflowMeta;
    this.workflowGraph = workflowGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }


  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a workflow.
   * We'll add the creation of every possible action as well as the modification of the workflow itself from the annotations.
   *
   * @return The list of supported actions
   */
  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> guiActions = new ArrayList<>();

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = getPluginActions( true );
    if ( pluginActions != null ) {
      for ( GuiAction pluginAction : pluginActions ) {
        guiActions.add( lambdaBuilder.createLambda( pluginAction, this, workflowGraph ) );
      }
    }

    // Also add all the entry creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> actionPlugins = registry.getPlugins( ActionPluginType.class );
    for ( IPlugin actionPlugin : actionPlugins ) {
     
        GuiAction createActionGuiAction =
          new GuiAction( "workflow-graph-create-workflow-action-" + actionPlugin.getIds()[ 0 ], GuiActionType.Create, actionPlugin.getName(), actionPlugin.getDescription(), actionPlugin.getImageFile(),
            (shiftClicked, controlClicked, t) -> {
              workflowGraph.workflowActionDelegate.newAction( workflowMeta, actionPlugin.getIds()[ 0 ], actionPlugin.getName(), controlClicked, click );
            }
          );
        createActionGuiAction.getKeywords().addAll( Arrays.asList(actionPlugin.getKeywords()));
        createActionGuiAction.setCategory( actionPlugin.getCategory() );
        createActionGuiAction.setCategoryOrder( "9999_"+actionPlugin.getCategory() ); // sort alphabetically
      try {
        createActionGuiAction.setClassLoader( registry.getClassLoader( actionPlugin ) );
      } catch ( HopPluginException e ) {
        LogChannel.UI.logError( "Unable to get classloader for action plugin " + actionPlugin.getIds()[ 0 ], e );
      }
      createActionGuiAction.getKeywords().add( actionPlugin.getCategory() );
      guiActions.add( createActionGuiAction );
    }

    return guiActions;
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
   * Gets workflowGraph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The workflowGraph to set
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

  /**
   * Gets lambdaBuilder
   *
   * @return value of lambdaBuilder
   */
  public GuiActionLambdaBuilder<HopGuiWorkflowContext> getLambdaBuilder() {
    return lambdaBuilder;
  }

  /**
   * @param lambdaBuilder The lambdaBuilder to set
   */
  public void setLambdaBuilder( GuiActionLambdaBuilder<HopGuiWorkflowContext> lambdaBuilder ) {
    this.lambdaBuilder = lambdaBuilder;
  }
}
