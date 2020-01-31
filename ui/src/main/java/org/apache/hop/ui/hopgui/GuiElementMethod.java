package org.apache.hop.ui.hopgui;

import org.apache.hop.core.gui.plugin.GuiMenuElement;
import org.apache.hop.core.gui.plugin.GuiToolbarElement;

import java.lang.reflect.Method;

public class GuiElementMethod {
  public GuiMenuElement menuElement;
  public GuiToolbarElement toolBarElement;
  public Method method;

  public GuiElementMethod( GuiMenuElement menuElement, Method method ) {
    this.menuElement = menuElement;
    this.method = method;
  }

  public GuiElementMethod( GuiToolbarElement toolBarElement, Method method ) {
    this.toolBarElement = toolBarElement;
    this.method = method;
  }

  /**
   * Gets menuElement
   *
   * @return value of menuElement
   */
  public GuiMenuElement getMenuElement() {
    return menuElement;
  }

  /**
   * @param menuElement The menuElement to set
   */
  public void setMenuElement( GuiMenuElement menuElement ) {
    this.menuElement = menuElement;
  }

  /**
   * Gets toolBarElement
   *
   * @return value of toolBarElement
   */
  public GuiToolbarElement getToolBarElement() {
    return toolBarElement;
  }

  /**
   * @param toolBarElement The toolBarElement to set
   */
  public void setToolBarElement( GuiToolbarElement toolBarElement ) {
    this.toolBarElement = toolBarElement;
  }

  /**
   * Gets method
   *
   * @return value of method
   */
  public Method getMethod() {
    return method;
  }

  /**
   * @param method The method to set
   */
  public void setMethod( Method method ) {
    this.method = method;
  }
}