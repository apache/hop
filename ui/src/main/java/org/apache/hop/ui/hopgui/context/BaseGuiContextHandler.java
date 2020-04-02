package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiAction;
import org.apache.hop.core.gui.plugin.GuiRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class BaseGuiContextHandler {

  public static final String CONTEXT_ID = "HopGuiPipelineTransformContext";

  public BaseGuiContextHandler() {
  }

  public abstract String getContextId();

  /**
   * Create a list of supported actions from the plugin GUI registry.
   * If this is indicated as such the actions will be sorted by ID to deliver a consistent user experience
   *
   * @param sortActionsById true if the actions need to be sorted by ID
   * @return The list of supported actions
   */
  protected List<GuiAction> getPluginActions( boolean sortActionsById ) {
    List<GuiAction> actions = new ArrayList<>();

    // Get the actions from the plugins...
    //
    List<GuiAction> pluginActions = GuiRegistry.getInstance().getGuiContextActions( getContextId() );
    if ( pluginActions != null && sortActionsById ) {
      Collections.sort( pluginActions, new Comparator<GuiAction>() {
        @Override public int compare( GuiAction a1, GuiAction a2 ) {
          return a1.getId().compareTo( a2.getId() );
        }
      } );
      actions.addAll( pluginActions );
    }

    return actions;
  }
}
