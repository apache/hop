package org.apache.hop.projects.environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A project lifecycle environment describes the state of a project and its configuration.
 */
public class LifecycleEnvironment {

  private String name;

  private String purpose;

  private String projectName;

  private List<String> configurationFiles;

  public LifecycleEnvironment() {
    configurationFiles = new ArrayList<>();
  }

  public LifecycleEnvironment( String name, String purpose, String projectName, List<String> configurationFiles ) {
    this.name = name;
    this.purpose = purpose;
    this.projectName = projectName;
    this.configurationFiles = configurationFiles;
  }

  public LifecycleEnvironment( LifecycleEnvironment env ) {
    this.name = env.name;
    this.purpose = env.purpose;
    this.projectName = env.projectName;
    this.configurationFiles = new ArrayList<>(env.configurationFiles);
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    LifecycleEnvironment that = (LifecycleEnvironment) o;
    return Objects.equals( name, that.name );
  }

  @Override public int hashCode() {
    return Objects.hash( name );
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
   * Gets purpose
   *
   * @return value of purpose
   */
  public String getPurpose() {
    return purpose;
  }

  /**
   * @param purpose The purpose to set
   */
  public void setPurpose( String purpose ) {
    this.purpose = purpose;
  }

  /**
   * Gets projectName
   *
   * @return value of projectName
   */
  public String getProjectName() {
    return projectName;
  }

  /**
   * @param projectName The projectName to set
   */
  public void setProjectName( String projectName ) {
    this.projectName = projectName;
  }

  /**
   * Gets configurationFiles
   *
   * @return value of configurationFiles
   */
  public List<String> getConfigurationFiles() {
    return configurationFiles;
  }

  /**
   * @param configurationFiles The configurationFiles to set
   */
  public void setConfigurationFiles( List<String> configurationFiles ) {
    this.configurationFiles = configurationFiles;
  }
}
