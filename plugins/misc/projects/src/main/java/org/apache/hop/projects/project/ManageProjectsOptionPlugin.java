/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.projects.project;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import picocli.CommandLine;

import java.util.List;

@ConfigPlugin(
  id = "ManageProjectsOptionPlugin",
  description = "Allows command line editing of the projects"
)
public class ManageProjectsOptionPlugin implements IConfigOptions {

  @CommandLine.Option( names = { "-pc", "--project-create" }, description = "Create a new project. Also specify the name and its home" )
  private boolean createProject;

  @CommandLine.Option( names = { "-p", "--project" }, description = "The name of the project to manage" )
  private String projectName;

  @CommandLine.Option( names = { "-ph", "--project-home" }, description = "The home directory of the project" )
  private String projectHome;

  @CommandLine.Option( names = { "-pr", "--project-parent" }, description = "The name of the parent project to inherit metadata and variables from" )
  private String projectParent;

  @CommandLine.Option( names = { "-pf", "--project-config-file" }, description = "The configuration file relative to the home folder. The default value is project-config.json" )
  private String projectConfigFile;

  @CommandLine.Option( names = { "-ps", "--project-description" }, description = "The description of the project" )
  private String projectDescription;

  @CommandLine.Option( names = { "-pp", "--project-company" }, description = "The company" )
  private String projectCompany;

  @CommandLine.Option( names = { "-pt", "--project-department" }, description = "The department" )
  private String projectDepartment;

  @CommandLine.Option( names = { "-pa", "--project-metadata-base" }, description = "The metadata base folder (relative to home)" )
  private String projectMetadataBaseFolder;

  @CommandLine.Option( names = { "-pu", "--project-unit-tests-base" }, description = "The unit tests base folder (relative to home)" )
  private String projectUnitTestsBasePath;

  @CommandLine.Option( names = { "-pb", "--project-datasets-base" }, description = "The data sets CSV folder (relative to home)" )
  private String projectDataSetsCsvFolder;

  @CommandLine.Option( names = { "-px", "--project-enforce-execution" }, description = "Validate before execution that a workflow or pipeline is located in the project home folder or a sub-folder (true/false)." )
  private String projectEnforceExecutionInHome;

  @CommandLine.Option( names = { "-pv", "--project-variables" }, description = "A list of variable=value combinations separated by a comma", split = ",")
  private String[] projectVariables;

  @CommandLine.Option( names = { "-pm", "--project-modify" }, description = "Modify a project" )
  private boolean modifyProject;

  @CommandLine.Option( names = { "-pd", "--project-delete" }, description = "Delete a project" )
  private boolean deleteProject;

  @CommandLine.Option( names = { "-pl", "-projects-list" }, description = "List the defined projects" )
  private boolean listProjects;


  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables ) throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    try {
      boolean changed = false;
      if ( createProject ) {
        createProject( log, config, variables );
        changed = true;
      } else if ( modifyProject ) {
        modifyProject( log, config, variables );
        changed = true;
      } else if ( deleteProject ) {
        deleteProject( log, config, variables );
        changed = true;
      } else if ( listProjects ) {
        listProjects( log, config, variables );
        changed = true;
      }
      return changed;
    } catch ( Exception e ) {
      throw new HopException( "Error handling environment configuration options", e );
    }

  }

  private void listProjects( ILogChannel log, ProjectsConfig config, IVariables variables ) throws HopException {
    log.logBasic( "Projects:" );
    List<String> names = config.listProjectConfigNames();
    for ( String name : names ) {
      ProjectConfig projectConfig = config.findProjectConfig( name );
      Project project = projectConfig.loadProject( Variables.getADefaultVariableSpace() );
      logProjectDetails( log, projectConfig, project );
    }
  }

  private void logProjectDetails( ILogChannel log, ProjectConfig projectConfig, Project project ) throws HopException {
    String projectHome = projectConfig.getProjectHome();
    log.logBasic( "  " + projectConfig.getProjectName() + " : " + projectHome );
    if (StringUtils.isNotEmpty(project.getParentProjectName())) {
      log.logBasic("    Parent project: " + project.getParentProjectName());
    }
    log.logBasic( "    Configuration file: " + projectConfig.getActualProjectConfigFilename( Variables.getADefaultVariableSpace() ) );
    if (!project.getDescribedVariables().isEmpty()) {
      log.logBasic( "    Described variables: " );
      for ( DescribedVariable variable : project.getDescribedVariables() ) {
        log.logBasic( "      " + variable.getName() + " = " + variable.getValue() + ( StringUtils.isEmpty( variable.getDescription() ) ? "" : " (" + variable.getDescription() + ")" ) );
      }
    }
  }

  private void deleteProject( ILogChannel log, ProjectsConfig config, IVariables variables ) throws Exception {
    validateProjectNameSpecified();

    ProjectConfig projectConfig = config.findProjectConfig( projectName );
    if ( projectConfig == null ) {
      throw new HopException( "Project '" + projectName + "' doesn't exists, it can't be deleted" );
    }
    config.removeProjectConfig( projectName );
    ProjectsConfigSingleton.saveConfig();

    log.logBasic( "Project '"+projectName+" was removed from the configuration" );
  }

  private void modifyProject( ILogChannel log, ProjectsConfig config, IVariables variables ) throws Exception {
    validateProjectNameSpecified();

    ProjectConfig projectConfig = config.findProjectConfig( projectName );
    if (projectConfig==null) {
      throw new HopException( "Project '" + projectName + "' doesn't exists, it can't be modified" );
    }

    if (StringUtils.isNotEmpty(projectHome)) {
      projectConfig.setProjectHome( projectHome );
    }
    if (StringUtils.isNotEmpty(projectConfigFile)) {
      projectConfig.setConfigFilename( projectConfigFile );
    }

    config.addProjectConfig( projectConfig );

    String projectConfigFilename = projectConfig.getActualProjectConfigFilename( variables );

    Project project = projectConfig.loadProject( variables );

    log.logBasic( "Project configuration for '" + projectName + "' was modified in "+ HopConfig.getInstance().getConfigFilename() );

    if (modifyProjectSettings(project)) {

      // Check to see if there's not a loop in the project parent hierarchy
      //
      project.verifyProjectsChain( projectName, variables );

      project.saveToFile();
      log.logBasic( "Project settings for '" + projectName + "' were modified in file " + projectConfigFilename );
    }
  }

  private boolean modifyProjectSettings( Project project ) {
    boolean changed = false;
    if (StringUtils.isNotEmpty(projectParent)) {
      project.setParentProjectName( projectParent );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectDescription)) {
      project.setDescription( projectDescription );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectCompany)) {
      project.setCompany( projectCompany );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectDepartment)) {
      project.setDepartment( projectDepartment );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectMetadataBaseFolder)) {
      project.setMetadataBaseFolder( projectMetadataBaseFolder );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectUnitTestsBasePath)) {
      project.setUnitTestsBasePath( projectUnitTestsBasePath );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectDataSetsCsvFolder)) {
      project.setDataSetsCsvFolder( projectDataSetsCsvFolder );
      changed=true;
    }
    if (StringUtils.isNotEmpty(projectEnforceExecutionInHome)) {
      boolean enabled = "y".equalsIgnoreCase( projectEnforceExecutionInHome ) || "true".equalsIgnoreCase( projectEnforceExecutionInHome );
      project.setEnforcingExecutionInHome( enabled );
      changed=true;
    }
    if (projectVariables!=null) {
      for (String projectVariable : projectVariables) {
        int equalsIndex = projectVariable.indexOf( "=" );
        if (equalsIndex>0) {
          String varName = projectVariable.substring( 0, equalsIndex );
          String varValue = projectVariable.substring( equalsIndex + 1 );
          DescribedVariable describedVariable = new DescribedVariable( varName, varValue, "" );
          project.setDescribedVariable( describedVariable );
          changed = true;
        }
      }
    }

    return changed;
  }


  private void createProject( ILogChannel log, ProjectsConfig config, IVariables variables ) throws Exception {
    validateProjectNameSpecified();
    validateProjectHomeSpecified();

    log.logBasic( "Creating project '" + projectName + "'" );

    ProjectConfig projectConfig = config.findProjectConfig( projectName );
    if ( projectConfig!=null ) {
      throw new HopException( "Project '" + projectName + "' already exists." );
    }

    projectConfig=new ProjectConfig(projectName, projectHome, Const.NVL(projectConfigFile, ProjectConfig.DEFAULT_PROJECT_CONFIG_FILENAME));

    config.addProjectConfig( projectConfig );
    ProjectsConfigSingleton.saveConfig();

    log.logBasic( "Project '" + projectName + "' was created for home folder : " + projectHome );

    Project project = projectConfig.loadProject( variables );
    project.setParentProjectName( config.getStandardParentProject() );
    modifyProjectSettings( project );

    // Check to see if there's not a loop in the project parent hierarchy
    //
    project.verifyProjectsChain( projectName, variables );

    // Always save, even if it's an empty file
    //
    project.saveToFile();

    log.logBasic( "Configuration file for project '" + projectName + "' was saved to : " + project.getConfigFilename() );
  }

  private void validateProjectNameSpecified() throws Exception {
    if ( StringUtil.isEmpty( projectName ) ) {
      throw new HopException( "Please specify the name of the project" );
    }
  }

  private void validateProjectHomeSpecified() throws Exception {
    if ( StringUtil.isEmpty( projectHome ) ) {
      throw new HopException( "Please specify the home directory of the project to create" );
    }
  }

  /**
   * Gets createProject
   *
   * @return value of createProject
   */
  public boolean isCreateProject() {
    return createProject;
  }

  /**
   * @param createProject The createProject to set
   */
  public void setCreateProject( boolean createProject ) {
    this.createProject = createProject;
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
   * Gets projectConfigFile
   *
   * @return value of projectConfigFile
   */
  public String getProjectConfigFile() {
    return projectConfigFile;
  }

  /**
   * @param projectConfigFile The projectConfigFile to set
   */
  public void setProjectConfigFile( String projectConfigFile ) {
    this.projectConfigFile = projectConfigFile;
  }

  /**
   * Gets projectDescription
   *
   * @return value of projectDescription
   */
  public String getProjectDescription() {
    return projectDescription;
  }

  /**
   * @param projectDescription The projectDescription to set
   */
  public void setProjectDescription( String projectDescription ) {
    this.projectDescription = projectDescription;
  }

  /**
   * Gets projectCompany
   *
   * @return value of projectCompany
   */
  public String getProjectCompany() {
    return projectCompany;
  }

  /**
   * @param projectCompany The projectCompany to set
   */
  public void setProjectCompany( String projectCompany ) {
    this.projectCompany = projectCompany;
  }

  /**
   * Gets projectDepartment
   *
   * @return value of projectDepartment
   */
  public String getProjectDepartment() {
    return projectDepartment;
  }

  /**
   * @param projectDepartment The projectDepartment to set
   */
  public void setProjectDepartment( String projectDepartment ) {
    this.projectDepartment = projectDepartment;
  }

  /**
   * Gets projectMetadataBaseFolder
   *
   * @return value of projectMetadataBaseFolder
   */
  public String getProjectMetadataBaseFolder() {
    return projectMetadataBaseFolder;
  }

  /**
   * @param projectMetadataBaseFolder The projectMetadataBaseFolder to set
   */
  public void setProjectMetadataBaseFolder( String projectMetadataBaseFolder ) {
    this.projectMetadataBaseFolder = projectMetadataBaseFolder;
  }

  /**
   * Gets projectUnitTestsBasePath
   *
   * @return value of projectUnitTestsBasePath
   */
  public String getProjectUnitTestsBasePath() {
    return projectUnitTestsBasePath;
  }

  /**
   * @param projectUnitTestsBasePath The projectUnitTestsBasePath to set
   */
  public void setProjectUnitTestsBasePath( String projectUnitTestsBasePath ) {
    this.projectUnitTestsBasePath = projectUnitTestsBasePath;
  }

  /**
   * Gets projectDataSetsCsvFolder
   *
   * @return value of projectDataSetsCsvFolder
   */
  public String getProjectDataSetsCsvFolder() {
    return projectDataSetsCsvFolder;
  }

  /**
   * @param projectDataSetsCsvFolder The projectDataSetsCsvFolder to set
   */
  public void setProjectDataSetsCsvFolder( String projectDataSetsCsvFolder ) {
    this.projectDataSetsCsvFolder = projectDataSetsCsvFolder;
  }

  /**
   * Gets projectEnforceExecutionInHome
   *
   * @return value of projectEnforceExecutionInHome
   */
  public String getProjectEnforceExecutionInHome() {
    return projectEnforceExecutionInHome;
  }

  /**
   * @param projectEnforceExecutionInHome The projectEnforceExecutionInHome to set
   */
  public void setProjectEnforceExecutionInHome( String projectEnforceExecutionInHome ) {
    this.projectEnforceExecutionInHome = projectEnforceExecutionInHome;
  }

  /**
   * Gets projectVariables
   *
   * @return value of projectVariables
   */
  public String[] getProjectVariables() {
    return projectVariables;
  }

  /**
   * @param projectVariables The projectVariables to set
   */
  public void setProjectVariables( String[] projectVariables ) {
    this.projectVariables = projectVariables;
  }

  /**
   * Gets modifyProject
   *
   * @return value of modifyProject
   */
  public boolean isModifyProject() {
    return modifyProject;
  }

  /**
   * @param modifyProject The modifyProject to set
   */
  public void setModifyProject( boolean modifyProject ) {
    this.modifyProject = modifyProject;
  }

  /**
   * Gets deleteProject
   *
   * @return value of deleteProject
   */
  public boolean isDeleteProject() {
    return deleteProject;
  }

  /**
   * @param deleteProject The deleteProject to set
   */
  public void setDeleteProject( boolean deleteProject ) {
    this.deleteProject = deleteProject;
  }

  /**
   * Gets listProjects
   *
   * @return value of listProjects
   */
  public boolean isListProjects() {
    return listProjects;
  }

  /**
   * @param listProjects The listProjects to set
   */
  public void setListProjects( boolean listProjects ) {
    this.listProjects = listProjects;
  }

  /**
   * Gets projectParent
   *
   * @return value of projectParent
   */
  public String getProjectParent() {
    return projectParent;
  }

  /**
   * @param projectParent The projectParent to set
   */
  public void setProjectParent( String projectParent ) {
    this.projectParent = projectParent;
  }
}

