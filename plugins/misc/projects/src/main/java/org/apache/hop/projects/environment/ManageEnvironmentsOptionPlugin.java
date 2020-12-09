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

package org.apache.hop.projects.environment;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.DescribedVariablesConfigFile;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHasHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.List;

@ConfigPlugin(
  id = "ManageEnvironmentsOptionPlugin",
  description = "Allows command line editing of the lifecycle environments"
)
public class ManageEnvironmentsOptionPlugin implements IConfigOptions {

  @CommandLine.Option( names = { "-ec", "--environment-create" }, description = "Create a new project lifecycle environment. Also specify its name, purpose, the project name and the configuration files." )
  private boolean createEnvironment;

  @CommandLine.Option( names = { "-e", "--environment" }, description = "The name of the lifecycle environment to manage" )
  private String environmentName;

  @CommandLine.Option( names = { "-eu", "--environment-purpose" }, description = "The purpose of the environment: Development, Testing, Production, CI, ..." )
  private String environmentPurpose;

  @CommandLine.Option( names = { "-ep", "--environment-project" }, description = "The project for the environment" )
  private String environmentProject;

  @CommandLine.Option( names = { "-eg", "--environment-config-files" }, description = "A list of configuration files for this lifecycle environment, comma separated", split = ",")
  private String[] environmentConfigFiles;

  @CommandLine.Option( names = { "-em", "--environment-modify" }, description = "Modify a lifecycle environment" )
  private boolean modifyEnvironment;

  @CommandLine.Option( names = { "-ed", "--environment-delete" }, description = "Delete a lifecycle environment" )
  private boolean deleteEnvironment;

  @CommandLine.Option( names = { "-el", "--environments-list" }, description = "List the defined lifecycle environments" )
  private boolean listEnvironments;


  @Override public boolean handleOption( ILogChannel log, IHasHopMetadataProvider hasHopMetadataProvider, IVariables variables ) throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    try {
      boolean changed = false;
      if ( createEnvironment ) {
        createEnvironment( log, config, variables, hasHopMetadataProvider );
        changed = true;
      } else if ( modifyEnvironment ) {
        modifyEnvironment( log, config, variables, hasHopMetadataProvider );
        changed = true;
      } else if ( deleteEnvironment ) {
        deleteEnvironment( log, config, variables );
        changed = true;
      } else if ( listEnvironments ) {
        listEnvironments( log, config, variables );
        changed = true;
      }
      return changed;
    } catch ( Exception e ) {
      throw new HopException( "Error handling project lifecycle environments configuration options", e );
    }
  }

  private void listEnvironments( ILogChannel log, ProjectsConfig config, IVariables variables ) throws HopException {

    log.logBasic( "Lifecycle environments:" );
    List<String> names = config.listEnvironmentNames();
    for ( String name : names ) {
      LifecycleEnvironment environment = config.findEnvironment( name );
      logEnvironmentDetails(log, environment);
    }
  }

  private void logEnvironmentDetails( ILogChannel log, LifecycleEnvironment environment ) {
    log.logBasic( "  "+environment.getName() );
    log.logBasic( "    Purpose: "+environment.getPurpose() );
    log.logBasic( "    Configuration files: " );
    log.logBasic( "    Project name: "+environment.getProjectName() );
    for ( String configurationFile : environment.getConfigurationFiles() ) {
      log.logBasic( "      Config file: " + configurationFile);
    }
  }

  private void deleteEnvironment( ILogChannel log, ProjectsConfig config, IVariables variables ) throws Exception {
    validateEnvironmentNameSpecified();
    LifecycleEnvironment environment = config.findEnvironment( environmentName );
    if ( environment==null) {
      throw new HopException( "Lifecycle environment '" + environmentName + "' doesn't exists, it can't be deleted" );
    }
    config.removeEnvironment( environmentName );
    ProjectsConfigSingleton.saveConfig();
    log.logBasic( "Lifecycle environment '" + environmentName + "' was deleted from Hop configuration file "+ HopConfig.getInstance().getConfigFilename() );
  }

  private void modifyEnvironment( ILogChannel log, ProjectsConfig config, IVariables variables, IHasHopMetadataProvider hasHopMetadataProvider ) throws Exception {
    validateEnvironmentNameSpecified();
    LifecycleEnvironment environment = config.findEnvironment( environmentName );
    if ( environment==null) {
      throw new HopException( "Environment '" + environmentName + "' doesn't exists, it can't be modified" );
    }

    if (updateEnvironmentDetails( environment )) {
      config.addEnvironment( environment );
      log.logBasic( "Lifecycle environment '" + environmentName + "' was modified in Hop configuration file "+ HopConfig.getInstance().getConfigFilename()  );
      logEnvironmentDetails( log, environment );
      ProjectsConfigSingleton.saveConfig();
    } else {
      log.logBasic( "Environment '" + environmentName + "' was not modified." );
    }

    enableProject( log, environment, variables, hasHopMetadataProvider, config );
    validateConfigFiles( log, variables, environment );
  }

  private void createEnvironment( ILogChannel log, ProjectsConfig config, IVariables variables, IHasHopMetadataProvider hasHopMetadataProvider ) throws Exception {
    validateEnvironmentNameSpecified();

    LifecycleEnvironment environment = config.findEnvironment( environmentName );
    log.logBasic( "Creating environment '" + environmentName + "'" );
    if ( environment!=null ) {
      throw new HopException( "Environment '" + environmentName + "' already exists." );
    }
    environment = new LifecycleEnvironment();
    environment.setName( environmentName );

    updateEnvironmentDetails( environment );

    config.addEnvironment( environment );
    ProjectsConfigSingleton.saveConfig();

    log.logBasic( "Environment '" + environmentName + "' was created in Hop configuration file "+ HopConfig.getInstance().getConfigFilename()  );

    enableProject( log, environment, variables, hasHopMetadataProvider, config);

    validateConfigFiles(log, variables, environment);
    logEnvironmentDetails( log, environment );
  }

  private void enableProject( ILogChannel log, LifecycleEnvironment environment, IVariables variables, IHasHopMetadataProvider hasHopMetadataProvider,
                              ProjectsConfig config ) throws HopException {
    ProjectConfig projectConfig = config.findProjectConfig( environment.getProjectName() );
    if (projectConfig==null) {
      log.logBasic( "Warning: referenced project '"+environment.getProjectName()+"' doesn't exist" );
    } else {
      Project project = projectConfig.loadProject( variables );

      // Change variables, metadata, ... for the project
      //
      ProjectsUtil.enableProject( log, environment.getProjectName(), project, variables, environment.getConfigurationFiles(), environmentName, hasHopMetadataProvider );
    }
  }

  private void validateConfigFiles(ILogChannel log, IVariables variables, LifecycleEnvironment environment) throws Exception {
    if (environment==null || environmentConfigFiles==null){
      return;
    }
    for (String environmentConfigFilename : environment.getConfigurationFiles()) {
      String realEnvConfFilename = variables.resolve(environmentConfigFilename);
      DescribedVariablesConfigFile variablesConfigFile = new DescribedVariablesConfigFile( realEnvConfFilename );
      // Create the config file if it doesn't exist
      if (!new File(realEnvConfFilename).exists()) {
        variablesConfigFile.saveToFile();
        log.logBasic( "Created empty environment configuration file : "+realEnvConfFilename );
      } else {
        log.logBasic( "Found existing environment configuration file: "+realEnvConfFilename );
      }
    }
  }

  private boolean updateEnvironmentDetails( LifecycleEnvironment environment ) {
    boolean changed = false;
    if (StringUtils.isNotEmpty(environmentPurpose)) {
      environment.setPurpose( environmentPurpose );
      changed = true;
    }
    if (StringUtils.isNotEmpty( environmentProject )) {
      environment.setProjectName( environmentProject );
      changed = true;
    }
    if (environmentConfigFiles!=null && environmentConfigFiles.length>0) {
      environment.getConfigurationFiles().clear();
      environment.getConfigurationFiles().addAll( Arrays.asList(environmentConfigFiles) );
      changed = true;
    }
    return changed;
  }

  private void validateEnvironmentNameSpecified() throws Exception {
    if ( StringUtil.isEmpty( environmentName ) ) {
      throw new HopException( "Please specify the name of the environment to create" );
    }
  }

  /**
   * Gets createEnvironment
   *
   * @return value of createEnvironment
   */
  public boolean isCreateEnvironment() {
    return createEnvironment;
  }

  /**
   * @param createEnvironment The createEnvironment to set
   */
  public void setCreateEnvironment( boolean createEnvironment ) {
    this.createEnvironment = createEnvironment;
  }

  /**
   * Gets environmentName
   *
   * @return value of environmentName
   */
  public String getEnvironmentName() {
    return environmentName;
  }

  /**
   * @param environmentName The environmentName to set
   */
  public void setEnvironmentName( String environmentName ) {
    this.environmentName = environmentName;
  }

  /**
   * Gets environmentPurpose
   *
   * @return value of environmentPurpose
   */
  public String getEnvironmentPurpose() {
    return environmentPurpose;
  }

  /**
   * @param environmentPurpose The environmentPurpose to set
   */
  public void setEnvironmentPurpose( String environmentPurpose ) {
    this.environmentPurpose = environmentPurpose;
  }

  /**
   * Gets environmentProject
   *
   * @return value of environmentProject
   */
  public String getEnvironmentProject() {
    return environmentProject;
  }

  /**
   * @param environmentProject The environmentProject to set
   */
  public void setEnvironmentProject( String environmentProject ) {
    this.environmentProject = environmentProject;
  }

  /**
   * Gets environmentConfigFiles
   *
   * @return value of environmentConfigFiles
   */
  public String[] getEnvironmentConfigFiles() {
    return environmentConfigFiles;
  }

  /**
   * @param environmentConfigFiles The environmentConfigFiles to set
   */
  public void setEnvironmentConfigFiles( String[] environmentConfigFiles ) {
    this.environmentConfigFiles = environmentConfigFiles;
  }

  /**
   * Gets modifyEnvironment
   *
   * @return value of modifyEnvironment
   */
  public boolean isModifyEnvironment() {
    return modifyEnvironment;
  }

  /**
   * @param modifyEnvironment The modifyEnvironment to set
   */
  public void setModifyEnvironment( boolean modifyEnvironment ) {
    this.modifyEnvironment = modifyEnvironment;
  }

  /**
   * Gets deleteEnvironment
   *
   * @return value of deleteEnvironment
   */
  public boolean isDeleteEnvironment() {
    return deleteEnvironment;
  }

  /**
   * @param deleteEnvironment The deleteEnvironment to set
   */
  public void setDeleteEnvironment( boolean deleteEnvironment ) {
    this.deleteEnvironment = deleteEnvironment;
  }

  /**
   * Gets listEnvironments
   *
   * @return value of listEnvironments
   */
  public boolean isListEnvironments() {
    return listEnvironments;
  }

  /**
   * @param listEnvironments The listEnvironments to set
   */
  public void setListEnvironments( boolean listEnvironments ) {
    this.listEnvironments = listEnvironments;
  }
}
