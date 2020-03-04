package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.IGuiAction;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.List;

/**
 * This class handles actions for a certain context.
 * For example, the main HopGui dialog registers a bunch of context handlers for MetaStore objects, asks the various perspectives, ...
 */
public interface IGuiContextHandler {
  /**
   * @return Get a list of all the supported actions by this context handler
   */
  List<IGuiAction> getSupportedActions();
}
