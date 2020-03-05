package org.apache.hop.ui.hopgui.file.trans;

import org.apache.hop.core.gui.Point;
import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiActionLambda;
import org.apache.hop.core.gui.plugin.GuiActionType;
import org.apache.hop.core.gui.plugin.IGuiAction;
import org.apache.hop.core.gui.plugin.IGuiActionLambda;
import org.apache.hop.core.plugins.PluginInterface;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.StepPluginType;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.step.StepMeta;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopui.trans.TransGraph;

import java.util.ArrayList;
import java.util.List;

public class HopGuiTransContext implements IGuiContextHandler {
  private TransMeta transMeta;
  private HopGuiTransGraph transGraph;
  private Point click;

  public HopGuiTransContext( TransMeta transMeta, HopGuiTransGraph transGraph, Point click ) {
    this.transMeta = transMeta;
    this.transGraph = transGraph;
    this.click = click;
  }

  /**
   * Create a list of supported actions on a transformation.
   * We'll add the creation of every possible step as well as the modification of the transformation itself.
   *
   * @return The list of supported actions
   */
  @Override public List<IGuiAction> getSupportedActions() {
    List<IGuiAction> actions = new ArrayList<>(  );

    // Edit the transformation...
    //
    actions.add( new GuiAction( "transgraph-edit-transformation", GuiActionType.Modify, "Edit transformation", "Edit transformation properties", "ui/images/TRN.svg",
      t -> transGraph.settings() ) );

    // Create a note ...
    //
    actions.add( new GuiAction( "transgraph-create-note", GuiActionType.Create, "Create a note", "Create a new note", "ui/images/new.svg",
      t-> transGraph.newNote() ) );

    // Also add all the step creation actions...
    //
    PluginRegistry registry = PluginRegistry.getInstance();
    List<PluginInterface> stepPlugins = registry.getPlugins( StepPluginType.class );
    for (PluginInterface stepPlugin : stepPlugins) {
      GuiAction createStepAction = new GuiAction( "transgraph-create-step-" + stepPlugin.getIds()[ 0 ], GuiActionType.Create, stepPlugin.getName(), stepPlugin.getDescription(), stepPlugin.getImageFile(),
        t -> {
          transGraph.transStepDelegate.newStep( transMeta, stepPlugin.getIds()[0], stepPlugin.getName(), stepPlugin.getDescription(), false, true, click );
        }
      );
      actions.add(createStepAction);
    }

    return actions;
  }
}
