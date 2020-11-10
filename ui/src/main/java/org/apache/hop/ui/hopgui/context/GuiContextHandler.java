package org.apache.hop.ui.hopgui.context;

import org.apache.hop.core.gui.plugin.action.GuiAction;

import java.util.List;

public class GuiContextHandler implements IGuiContextHandler {

  private String contextId;
  private List<GuiAction> supportedActions;

  public GuiContextHandler( String contextId, List<GuiAction> supportedActions ) {
    this.contextId = contextId;
    this.supportedActions = supportedActions;
  }

  /**
   * Gets contextId
   *
   * @return value of contextId
   */
  @Override public String getContextId() {
    return contextId;
  }

  /**
   * @param contextId The contextId to set
   */
  public void setContextId( String contextId ) {
    this.contextId = contextId;
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
