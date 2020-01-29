package org.apache.hop.trans.engine;

import org.apache.hop.core.logging.LogChannelInterface;

/**
 * An identifiable component of an execution engine {@link IEngine}
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
   * @return The log channel or null if there is no separate log channel.
   */
  LogChannelInterface getLogChannel();

  /**
   * Retrieve the logging text of this component in the engine
   * @return logging text
   */
  String getLogText();
}
