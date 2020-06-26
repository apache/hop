package org.apache.hop.projects.environment;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import picocli.CommandLine;

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


  @Override public boolean handleOption( ILogChannel log, IHopMetadataProvider metadataProvider, IVariables variables ) throws HopException {
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();

    try {
      boolean changed = false;
      if ( createEnvironment ) {
        createEnvironment( log, config );
        changed = true;
      } else if ( modifyEnvironment ) {
        modifyEnvironment( log, config );
        changed = true;
      } else if ( deleteEnvironment ) {
        deleteEnvironment( log, config );
        changed = true;
      } else if ( listEnvironments ) {
        listEnvironments( log, config );
        changed = true;
      }
      return changed;
    } catch ( Exception e ) {
      throw new HopException( "Error handling project lifecycle environments configuration options", e );
    }
  }

  private void listEnvironments( ILogChannel log, ProjectsConfig config ) throws HopException {

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

  private void deleteEnvironment( ILogChannel log, ProjectsConfig config ) throws Exception {
    validateEnvironmentNameSpecified();
    LifecycleEnvironment environment = config.findEnvironment( environmentName );
    if ( environment==null) {
      throw new HopException( "Lifecycle environment '" + environmentName + "' doesn't exists, it can't be deleted" );
    }
    config.removeEnvironment( environmentName );
    ProjectsConfigSingleton.saveConfig();
  }

  private void modifyEnvironment( ILogChannel log, ProjectsConfig config ) throws Exception {
    validateEnvironmentNameSpecified();
    LifecycleEnvironment environment = config.findEnvironment( environmentName );
    if ( environment==null) {
      throw new HopException( "Environment '" + environmentName + "' doesn't exists, it can't be modified" );
    }

    if (updateEnvironmentDetails( environment )) {
      config.addEnvironment( environment );
      log.logBasic( "Environment '" + environmentName + "' was modified." );
      logEnvironmentDetails( log, environment );
    } else {
      log.logBasic( "Environment '" + environmentName + "' was not modified." );
    }

  }

  private void createEnvironment( ILogChannel log, ProjectsConfig config ) throws Exception {
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
    log.logBasic( "Project '" + environmentName + "' was created." );
    logEnvironmentDetails( log, environment );
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
}

