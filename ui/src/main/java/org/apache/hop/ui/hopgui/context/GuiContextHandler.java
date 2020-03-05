package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.IGuiAction;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles actions for a certain context.
 * For example, the main HopGui dialog registers a bunch of context handlers for MetaStore objects, asks the various perspectives, ...
 */
public class GuiContextHandler implements IGuiContextHandler {
  private List<IGuiAction> supportedActions;

  public GuiContextHandler() {
    supportedActions = new ArrayList<>();
  }

  /**
   * Gets supportedActions
   *
   * @return value of supportedActions
   */
  @Override public List<IGuiAction> getSupportedActions() {
    return supportedActions;
  }

  /**
   * @param supportedActions The supportedActions to set
   */
  public void setSupportedActions( List<IGuiAction> supportedActions ) {
    this.supportedActions = supportedActions;
  }
}
