package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambdaBuilder;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.ui.hopgui.context.BaseGuiContextHandler;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;

import java.util.ArrayList;
import java.util.List;

public class HopGuiTransContext extends BaseGuiContextHandler implements IGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiTransContext";

  private TransMeta transMeta;
  private HopGuiTransGraph transGraph;
  private Point click;
  private GuiActionLambdaBuilder<HopGuiTransContext> lambdaBuilder;

  public HopGuiTransContext( TransMeta transMeta, HopGuiTransGraph transGraph, Point click ) {
    this.transMeta = transMeta;
    this.transGraph = transGraph;
    this.click = click;
    this.lambdaBuilder = new GuiActionLambdaBuilder<>();
  }


  @Override public String getContextId() {
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

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = getPluginActions( true );
    if (pluginActions!=null) {
      for (GuiAction pluginAction : pluginActions) {
        actions.add( lambdaBuilder.createLambda( pluginAction, transGraph, this ) );
      }
    }

    // Also add all the step creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<PluginInterface> stepPlugins = registry.getPlugins( StepPluginType.class );
    for ( PluginInterface stepPlugin : stepPlugins ) {
      GuiAction createStepAction =
        new GuiAction( "transgraph-create-step-" + stepPlugin.getIds()[ 0 ], GuiActionType.Create, stepPlugin.getName(), stepPlugin.getDescription(), stepPlugin.getImageFile(),
          t -> transGraph.transStepDelegate.newStep( transMeta, stepPlugin.getIds()[ 0 ], stepPlugin.getName(), stepPlugin.getDescription(), false, true, click )
        );
      createStepAction.getKeywords().add(stepPlugin.getCategory());
      actions.add( createStepAction );
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
