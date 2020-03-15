package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiAction;

import java.util.List;

/**
 * This class handles actions for a certain context.
 * For example, the main HopGui dialog registers a bunch of context handlers for MetaStore objects, asks the various perspectives, ...
 */
public interface IGuiContextHandler {
  /**
   * @return Get a list of all the supported actions by this context handler
   */
  List<GuiAction> getSupportedActions();
}
