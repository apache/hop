package org.apache.hop.core.gui.plugin;

/**
 * This interface allows a method in a GuiPlugin to be identified as a contributor to the Hop UI
 */

public interface IGuiAction {

  /**
   * @return The Unique ID of this action
   */
  String getId();

  /**
   * The general type of action allowing us to filter in general
   * @return The action type
   */
  GuiActionType getType();

  /**
   * @return The name or short description of the action
   */
  String getName();

  /**
   * @return The long description of the action
   */
  String getTooltip();

  /**
   * @return The iconic representation of the action
   */
  String getImage();

  /**
   * @return The lambda to execute this action
   */
  IGuiActionLambda getActionLambda();


}
