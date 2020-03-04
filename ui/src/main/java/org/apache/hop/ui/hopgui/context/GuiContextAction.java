package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.IGuiAction;

public class GuiContextAction {
  private GuiContextHandler handler;
  private IGuiAction action;

  public GuiContextAction( GuiContextHandler handler, IGuiAction action ) {
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
  public IGuiAction getAction() {
    return action;
  }

  /**
   * @param action The action to set
   */
  public void setAction( IGuiAction action ) {
    this.action = action;
  }
}
