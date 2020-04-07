package org.apache.hop.ui.hopgui.file.workflow.context;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiWorkflowNoteContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiWorkflowNoteContext";

  private WorkflowMeta workflowMeta;
  private NotePadMeta notePadMeta;
  private HopGuiWorkflowGraph jobGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiWorkflowNoteContext> lambdaBuilder;

  public HopGuiWorkflowNoteContext( WorkflowMeta workflowMeta, NotePadMeta notePadMeta, HopGuiWorkflowGraph jobGraph, Point click ) {
    this.workflowMeta = workflowMeta;
    this.notePadMeta = notePadMeta;
    this.jobGraph = jobGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a workflow note.
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
        actions.add( lambdaBuilder.createLambda( pluginAction, jobGraph, this, jobGraph ) );
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
   * Gets notePadMeta
   *
   * @return value of notePadMeta
   */
  public NotePadMeta getNotePadMeta() {
    return notePadMeta;
  }

  /**
   * @param notePadMeta The notePadMeta to set
   */
  public void setNotePadMeta( NotePadMeta notePadMeta ) {
    this.notePadMeta = notePadMeta;
  }

  /**
   * Gets pipelineGraph
   *
   * @return value of pipelineGraph
   */
  public HopGuiWorkflowGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The pipelineGraph to set
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


}
