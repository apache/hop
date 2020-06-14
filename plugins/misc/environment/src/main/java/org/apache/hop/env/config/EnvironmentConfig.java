package org.apache.hop.env.config;

import java.util.HashMap;
import java.util.Map;

public class EnvironmentConfig {

  public static final String HOP_CONFIG_ENVIRONMENT_CONFIG = "environmentConfig";
  public static final String HOP_CONFIG_ENVIRONMENT_FILE = "environment.json";

  private boolean enabled;

  private boolean openingLastEnvironmentAtStartup;

  private String environmentConfigFilename;

  private Map<String, String> environmentFolders;

  public EnvironmentConfig() {
    enabled = true;
    openingLastEnvironmentAtStartup = true;
    environmentFolders = new HashMap<>();
    environmentConfigFilename = HOP_CONFIG_ENVIRONMENT_FILE;
  }

  public EnvironmentConfig( EnvironmentConfig config ) {
    this();
    enabled = config.enabled;
    openingLastEnvironmentAtStartup = config.openingLastEnvironmentAtStartup;
    environmentFolders.putAll( config.environmentFolders);
    environmentConfigFilename = config.environmentConfigFilename;
  }

  /**
   * Gets enabled
   *
   * @return value of enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * @param enabled The enabled to set
   */
  public void setEnabled( boolean enabled ) {
    this.enabled = enabled;
  }

  /**
   * Gets openingLastEnvironmentAtStartup
   *
   * @return value of openingLastEnvironmentAtStartup
   */
  public boolean isOpeningLastEnvironmentAtStartup() {
    return openingLastEnvironmentAtStartup;
  }

  /**
   * @param openingLastEnvironmentAtStartup The openingLastEnvironmentAtStartup to set
   */
  public void setOpeningLastEnvironmentAtStartup( boolean openingLastEnvironmentAtStartup ) {
    this.openingLastEnvironmentAtStartup = openingLastEnvironmentAtStartup;
  }

  /**
   * Gets environmentFolders
   *
   * @return value of environmentFolders
   */
  public Map<String, String> getEnvironmentFolders() {
    return environmentFolders;
  }

  /**
   * @param environmentFolders The environmentFolders to set
   */
  public void setEnvironmentFolders( Map<String, String> environmentFolders ) {
    this.environmentFolders = environmentFolders;
  }

  /**
   * Gets environmentConfigFilename
   *
   * @return value of environmentConfigFilename
   */
  public String getEnvironmentConfigFilename() {
    return environmentConfigFilename;
  }

  /**
   * @param environmentConfigFilename The environmentConfigFilename to set
   */
  public void setEnvironmentConfigFilename( String environmentConfigFilename ) {
    this.environmentConfigFilename = environmentConfigFilename;
  }
}
