package org.apache.hop.trans.engine;

/**
 * An identifiable component of an execution engine {@link IPipelineEngine}
 * In a transformation engine this would be a step
 */
public interface IEngineComponent {

  /**
   * @return The component name
   */
  String getName();

  /**
   * @return The copy number (0 of higher for parallel runs)
   */
  int getCopyNr();


  /**
   * @return The log channel ID or null if there is no separate log channel.
   */
  String getLogChannelId();

  /**
   * Retrieve the logging text of this component in the engine
   *
   * @return logging text
   */
  String getLogText();

  /**
   * @return true if this component is running/active
   */
  boolean isRunning();

  /**
   * @return true if the component is selected in the user interface
   */
  boolean isSelected();

  /**
   * @return The number of errors in this component
   */
  long getErrors();
}
