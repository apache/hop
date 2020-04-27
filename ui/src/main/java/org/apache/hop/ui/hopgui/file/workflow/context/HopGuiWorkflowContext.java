/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

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
  private HopGuiWorkflowGraph jobGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiWorkflowContext> lambdaBuilder;

  public HopGuiWorkflowContext( WorkflowMeta workflowMeta, HopGuiWorkflowGraph jobGraph, Point click ) {
    this.workflowMeta = workflowMeta;
    this.jobGraph = jobGraph;
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
        guiActions.add( lambdaBuilder.createLambda( pluginAction, this, jobGraph ) );
      }
    }

    // Add the special entries : Start and Dummy first
    //
    guiActions.add(createStartGuiAction());
    guiActions.add(createDummyGuiAction());

    // Also add all the entry creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<IPlugin> actionPlugins = registry.getPlugins( ActionPluginType.class );
    for ( IPlugin actionPlugin : actionPlugins ) {
      if (!actionPlugin.getIds()[0].equals( WorkflowMeta.STRING_SPECIAL )) {
        GuiAction createActionGuiAction =
          new GuiAction( "workflow-graph-create-workflow-action-" + actionPlugin.getIds()[ 0 ], GuiActionType.Create, actionPlugin.getName(), actionPlugin.getDescription(), actionPlugin.getImageFile(),
            (shiftClicked, controlClicked, t) -> {
              jobGraph.workflowEntryDelegate.newJobEntry( workflowMeta, actionPlugin.getIds()[ 0 ], actionPlugin.getName(), controlClicked, click );
            }
          );
        createActionGuiAction.getKeywords().addAll( Arrays.asList(actionPlugin.getKeywords()));
      try {
        createActionGuiAction.setClassLoader( registry.getClassLoader( actionPlugin ) );
      } catch ( HopPluginException e ) {
        LogChannel.UI.logError( "Unable to get classloader for action plugin " + actionPlugin.getIds()[ 0 ], e );
      }
      createActionGuiAction.getKeywords().add( actionPlugin.getCategory() );
      guiActions.add( createActionGuiAction );
      }
    }

    return guiActions;
  }

  private GuiAction createStartGuiAction() {
    return new GuiAction( "workflow-graph-create-workflow-action-start", GuiActionType.Create, WorkflowMeta.STRING_SPECIAL_START, null, "ui/images/STR.svg",
      (shiftClicked, controlClicked, t) -> {
          jobGraph.workflowEntryDelegate.newJobEntry( workflowMeta, WorkflowMeta.STRING_SPECIAL, WorkflowMeta.STRING_SPECIAL_START, controlClicked, click );
        }
      );
  }

  private GuiAction createDummyGuiAction() {
    return new GuiAction( "workflow-graph-create-workflow-action-dummy", GuiActionType.Create, WorkflowMeta.STRING_SPECIAL_DUMMY, null, "ui/images/DUM.svg",
      (shiftClicked, controlClicked, t) -> {
        jobGraph.workflowEntryDelegate.newJobEntry( workflowMeta, WorkflowMeta.STRING_SPECIAL, WorkflowMeta.STRING_SPECIAL_DUMMY, controlClicked, click );
      }
    );
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
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiWorkflowGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiWorkflowGraph jobGraph ) {
    this.jobGraph = jobGraph;
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
