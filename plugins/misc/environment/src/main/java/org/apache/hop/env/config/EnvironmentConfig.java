package org.apache.hop.env.config;

public class EnvironmentConfig {

  public static final String HOP_CONFIG_ENVIRONMENT_CONFIG = "environmentConfig";

  private boolean enabled;

  private boolean openingLastEnvironmentAtStartup;

  public EnvironmentConfig() {
    enabled = true;
    openingLastEnvironmentAtStartup = true;
  }

  public EnvironmentConfig(EnvironmentConfig c) {
    enabled = c.enabled;
    openingLastEnvironmentAtStartup = c.openingLastEnvironmentAtStartup;
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

}
