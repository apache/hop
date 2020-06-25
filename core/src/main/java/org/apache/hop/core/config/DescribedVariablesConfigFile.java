package org.apache.hop.core.config;

import org.apache.hop.core.config.plugin.ConfigFile;

public class DescribedVariablesConfigFile extends ConfigFile implements IConfigFile {
  private String configFilename;

  public DescribedVariablesConfigFile( String configFilename ) {
    super();
    this.configFilename = configFilename;
    this.serializer = new ConfigFileSerializer();
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  @Override public String getConfigFilename() {
    return configFilename;
  }

  /**
   * @param configFilename The configFilename to set
   */
  @Override public void setConfigFilename( String configFilename ) {
    this.configFilename = configFilename;
  }
}
