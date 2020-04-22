package org.apache.hop.env.config;

import org.apache.hop.metastore.persist.MetaStoreAttribute;
import org.apache.hop.metastore.persist.MetaStoreElementType;

@MetaStoreElementType(
  name = "Hop Environment Configuration",
  description = "These options allow you to configure the environment system itself"
)
public class EnvironmentConfig {

  public static final String SYSTEM_CONFIG_NAME = "system";

  @MetaStoreAttribute
  private String name;

  @MetaStoreAttribute
  private String lastUsedEnvironment;

  @MetaStoreAttribute
  private boolean enabled;

  @MetaStoreAttribute
  private boolean openingLastEnvironmentAtStartup;

  public EnvironmentConfig() {
    name = SYSTEM_CONFIG_NAME;
    lastUsedEnvironment = null;
    enabled = true;
    openingLastEnvironmentAtStartup = false;
  }

  public EnvironmentConfig(EnvironmentConfig c) {
    name = c.name;
    lastUsedEnvironment = c.lastUsedEnvironment;
    enabled = c.enabled;
    openingLastEnvironmentAtStartup = c.openingLastEnvironmentAtStartup;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets lastUsedEnvironment
   *
   * @return value of lastUsedEnvironment
   */
  public String getLastUsedEnvironment() {
    return lastUsedEnvironment;
  }

  /**
   * @param lastUsedEnvironment The lastUsedEnvironment to set
   */
  public void setLastUsedEnvironment( String lastUsedEnvironment ) {
    this.lastUsedEnvironment = lastUsedEnvironment;
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
