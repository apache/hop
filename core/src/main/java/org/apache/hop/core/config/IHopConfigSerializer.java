package org.apache.hop.core.config;

import org.apache.hop.core.exception.HopException;

import java.util.Map;

public interface IHopConfigSerializer {

  /**
   * Write the hop configuration map to the provided file
   * @param filename The name of the file to write to
   * @param configMap The configuration options to write
   * @throws HopException In case something goes wrong
   */
  void writeToFile(String filename, Map<String, Object> configMap) throws HopException;

  /**
   * Read the configurations map from a file
   * @param filename The name of the config file
   * @return The options map
   * @throws HopException In case something goes wrong.
   */
  Map<String, Object> readFromFile( String filename) throws HopException;
}
