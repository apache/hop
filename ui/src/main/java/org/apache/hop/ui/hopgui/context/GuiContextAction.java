package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.GuiAction;

public class GuiContextAction {
  private GuiContextHandler handler;
  private GuiAction action;

  public GuiContextAction( GuiContextHandler handler, GuiAction action ) {
    this.handler = handler;
    this.action = action;
  }

  /**
   * Gets handler
   *
   * @return value of handler
   */
  public GuiContextHandler getHandler() {
    return handler;
  }

  /**
   * @param handler The handler to set
   */
  public void setHandler( GuiContextHandler handler ) {
    this.handler = handler;
  }

  /**
   * Gets action
   *
   * @return value of action
   */
  public GuiAction getAction() {
    return action;
  }

  /**
   * @param action The action to set
   */
  public void setAction( GuiAction action ) {
    this.action = action;
  }
}
