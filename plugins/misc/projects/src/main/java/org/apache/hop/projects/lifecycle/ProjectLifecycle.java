package org.apache.hop.projects.lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ProjectLifecycle {

  private String name;

  private List<String> lifecycleEnvironments;

  private List<String> configurationFiles;

  public ProjectLifecycle() {
    this.lifecycleEnvironments = new ArrayList<>();
    this.configurationFiles = new ArrayList<>();
  }

  public ProjectLifecycle( String name, List<String> lifecycleEnvironments, List<String> configurationFiles ) {
    this.name = name;
    this.lifecycleEnvironments = lifecycleEnvironments;
    this.configurationFiles = configurationFiles;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    ProjectLifecycle that = (ProjectLifecycle) o;
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
   * Gets lifecycleEnvironments
   *
   * @return value of lifecycleEnvironments
   */
  public List<String> getLifecycleEnvironments() {
    return lifecycleEnvironments;
  }

  /**
   * @param lifecycleEnvironments The lifecycleEnvironments to set
   */
  public void setLifecycleEnvironments( List<String> lifecycleEnvironments ) {
    this.lifecycleEnvironments = lifecycleEnvironments;
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
