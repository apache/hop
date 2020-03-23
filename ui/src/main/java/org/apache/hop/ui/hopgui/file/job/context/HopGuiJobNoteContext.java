package org.apache.hop.ui.hopgui.file.job.context;

import org.apache.hop.core.NotePadMeta;
import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiJobNoteContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiJobNoteContext";

  private JobMeta jobMeta;
  private NotePadMeta notePadMeta;
  private HopGuiJobGraph jobGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiJobNoteContext> lambdaBuilder;

  public HopGuiJobNoteContext( JobMeta jobMeta, NotePadMeta notePadMeta, HopGuiJobGraph jobGraph, Point click ) {
    this.jobMeta = jobMeta;
    this.notePadMeta = notePadMeta;
    this.jobGraph = jobGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  @Override public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a job note.
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
   * Gets jobMeta
   *
   * @return value of jobMeta
   */
  public JobMeta getJobMeta() {
    return jobMeta;
  }

  /**
   * @param jobMeta The jobMeta to set
   */
  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
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
   * Gets transGraph
   *
   * @return value of transGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The transGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
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
