package org.apache.hop.projects.project;

import org.apache.commons.io.FilenameUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;

import java.io.File;
import java.util.Objects;

public class ProjectConfig {

  public static final String DEFAULT_PROJECT_CONFIG_FILENAME = "project-config.json";

  protected String projectName;
  protected String projectHome;
  protected String configFilename;

  public ProjectConfig() {
    super();
    this.configFilename = DEFAULT_PROJECT_CONFIG_FILENAME;
  }

  public ProjectConfig( String projectName, String projectHome, String configFilename ) {
    super();
    this.projectName = projectName;
    this.projectHome = projectHome;
    this.configFilename = configFilename;
  }

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    ProjectConfig that = (ProjectConfig) o;
    return Objects.equals( projectName, that.projectName );
  }

  @Override public int hashCode() {
    return Objects.hash( projectName );
  }

  public String getActualProjectConfigFilename(  IVariables variables ) {
    String actualHomeFolder = variables.environmentSubstitute( getProjectHome() );
    String actualConfigFilename = variables.environmentSubstitute( getConfigFilename() );
    return FilenameUtils.concat( actualHomeFolder, actualConfigFilename );
  }

  public Project loadProject( IVariables variables ) throws HopException {
    String configFilename = getActualProjectConfigFilename( variables );
    Project project = new Project(configFilename);
    if (new File(configFilename).exists()) {
      project.readFromFile();
    }
    return project;
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
   * Gets projectHome
   *
   * @return value of projectHome
   */
  public String getProjectHome() {
    return projectHome;
  }

  /**
   * @param projectHome The projectHome to set
   */
  public void setProjectHome( String projectHome ) {
    this.projectHome = projectHome;
  }

  /**
   * Gets configFilename
   *
   * @return value of configFilename
   */
  public String getConfigFilename() {
    return configFilename;
  }

  /**
   * @param configFilename The configFilename to set
   */
  public void setConfigFilename( String configFilename ) {
    this.configFilename = configFilename;
  }
}
