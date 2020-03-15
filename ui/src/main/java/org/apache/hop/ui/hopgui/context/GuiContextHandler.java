package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiAction;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles actions for a certain context.
 * For example, the main HopGui dialog registers a bunch of context handlers for MetaStore objects, asks the various perspectives, ...
 */
public class GuiContextHandler implements IGuiContextHandler {
  private List<GuiAction> supportedActions;

  public GuiContextHandler() {
    supportedActions = new ArrayList<>();
  }

  /**
   * Gets supportedActions
   *
   * @return value of supportedActions
   */
  @Override public List<GuiAction> getSupportedActions() {
    return supportedActions;
  }

  /**
   * @param supportedActions The supportedActions to set
   */
  public void setSupportedActions( List<GuiAction> supportedActions ) {
    this.supportedActions = supportedActions;
  }
}
