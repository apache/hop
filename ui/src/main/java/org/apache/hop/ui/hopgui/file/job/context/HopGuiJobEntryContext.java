package org.apache.hop.ui.hopgui.file.job.context;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.job.JobMeta;
import org.apache.hop.job.entry.JobEntryCopy;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiJobEntryContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiJobEntryContext";

  private JobMeta jobMeta;
  private JobEntryCopy jobEntryCopy;
  private HopGuiJobGraph jobGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiJobEntryContext> lambdaBuilder;

  public HopGuiJobEntryContext( JobMeta jobMeta, JobEntryCopy jobEntryCopy, HopGuiJobGraph jobGraph, Point click ) {
    super();
    this.jobMeta = jobMeta;
    this.jobEntryCopy = jobEntryCopy;
    this.jobGraph = jobGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a job entry.
   *
   * @return The list of supported actions
   */
  @Override public List<GuiAction> getSupportedActions() {
    List<GuiAction> actions = new ArrayList<>();

    // Get the actions from the plugins, sorted by ID...
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
   * Gets jobEntryCopy
   *
   * @return value of jobEntryCopy
   */
  public JobEntryCopy getJobEntryCopy() {
    return jobEntryCopy;
  }

  /**
   * @param jobEntryCopy The jobEntryCopy to set
   */
  public void setJobEntryCopy( JobEntryCopy jobEntryCopy ) {
    this.jobEntryCopy = jobEntryCopy;
  }

  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
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

  /**
   * Gets lambdaBuilder
   *
   * @return value of lambdaBuilder
   */
  public GuiActionLambdaBuilder<HopGuiJobEntryContext> getLambdaBuilder() {
    return lambdaBuilder;
  }

  /**
   * @param lambdaBuilder The lambdaBuilder to set
   */
  public void setLambdaBuilder( GuiActionLambdaBuilder<HopGuiJobEntryContext> lambdaBuilder ) {
    this.lambdaBuilder = lambdaBuilder;
  }
}
