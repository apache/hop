package org.apache.hop.ui.hopgui.file.trans.context;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.trans.HopGuiTransGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiTransStepContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiTransStepContext";

  private TransMeta transMeta;
  private StepMeta stepMeta;
  private HopGuiTransGraph transGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiTransStepContext> lambdaBuilder;

  public HopGuiTransStepContext( TransMeta transMeta, StepMeta stepMeta, HopGuiTransGraph transGraph, Point click ) {
    super();
    this.transMeta = transMeta;
    this.stepMeta = stepMeta;
    this.transGraph = transGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }

  public String getContextId() {
    return CONTEXT_ID;
  }

  /**
   * Create a list of supported actions on a transformation.
   * We'll add the creation of every possible step as well as the modification of the transformation itself.
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
        actions.add( lambdaBuilder.createLambda( pluginAction, transGraph, this, transGraph ) );
      }
    }

    return actions;
  }


  /**
   * Gets transMeta
   *
   * @return value of transMeta
   */
  public TransMeta getTransMeta() {
    return transMeta;
  }

  /**
   * @param transMeta The transMeta to set
   */
  public void setTransMeta( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }

  /**
   * Gets stepMeta
   *
   * @return value of stepMeta
   */
  public StepMeta getStepMeta() {
    return stepMeta;
  }

  /**
   * @param stepMeta The stepMeta to set
   */
  public void setStepMeta( StepMeta stepMeta ) {
    this.stepMeta = stepMeta;
  }

  /**
   * Gets transGraph
   *
   * @return value of transGraph
   */
  public HopGuiTransGraph getTransGraph() {
    return transGraph;
  }

  /**
   * @param transGraph The transGraph to set
   */
  public void setTransGraph( HopGuiTransGraph transGraph ) {
    this.transGraph = transGraph;
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
