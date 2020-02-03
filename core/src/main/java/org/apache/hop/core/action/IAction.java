package org.apache.hop.core.action;

/**
 * An action that can be taken
 */
public interface IAction {
  /**
   * @return ID or code of the action
   */
  String getId();

  /**
   * @return The name or short description of the action
   */
  String getName();

  /**
   * @return The iconic representation of the action
   */
  String getIcon();
}
