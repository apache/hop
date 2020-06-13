package org.apache.hop.env.config;

import org.apache.hop.env.environment.Environment;

import java.util.ArrayList;
import java.util.List;

public class EnvironmentConfig {

  public static final String HOP_CONFIG_ENVIRONMENT_CONFIG = "environmentConfig";

  private boolean enabled;

  private boolean openingLastEnvironmentAtStartup;

  private List<Environment> environments;

  public EnvironmentConfig() {
    enabled = true;
    openingLastEnvironmentAtStartup = true;
    environments = new ArrayList<>();
  }

  public EnvironmentConfig(EnvironmentConfig c) {
    this();
    enabled = c.enabled;
    openingLastEnvironmentAtStartup = c.openingLastEnvironmentAtStartup;
    environments.addAll(c.environments);
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
   * Gets environments
   *
   * @return value of environments
   */
  public List<Environment> getEnvironments() {
    return environments;
  }

  /**
   * @param environments The environments to set
   */
  public void setEnvironments( List<Environment> environments ) {
    this.environments = environments;
  }
}
