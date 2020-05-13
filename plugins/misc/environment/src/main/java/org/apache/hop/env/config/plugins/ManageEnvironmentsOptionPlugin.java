package org.apache.hop.env.config.plugins;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.plugin.ConfigPlugin;
import org.apache.hop.core.config.plugin.IConfigOptions;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.environment.EnvironmentVariable;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.persist.MetaStoreFactory;
import picocli.CommandLine;

import java.util.List;

@ConfigPlugin(
  id = "ManageEnvironmentsOptionPlugin",
  description = "Allows command line editing of the environments"
)
public class ManageEnvironmentsOptionPlugin implements IConfigOptions {

  @CommandLine.Option( names = { "-ec", "-environment-create" }, description = "Create an environment. Also specify the name and its home" )
  private boolean createEnvironment;

  @CommandLine.Option( names = { "-e", "-environment" }, description = "The name of the environment to manage" )
  private String environmentName;

  @CommandLine.Option( names = { "-eh", "--environments-home" }, description = "The home directory of the environment" )
  private String environmentHome;

  @CommandLine.Option( names = { "-ev", "--environment-variables" }, description = "The variables to be set in the environment", split = ",")
  private String[] environmentVariables;

  @CommandLine.Option( names = { "-em", "-environment-modify" }, description = "Modify an environment" )
  private boolean modifyEnvironment;

  @CommandLine.Option( names = { "-ed", "-environment-delete" }, description = "Delete an environment" )
  private boolean deleteEnvironment;

  @CommandLine.Option( names = { "-el", "-environment-list" }, description = "List the defined environments" )
  private boolean listEnvironments;


  @Override public boolean handleOption( ILogChannel log, IMetaStore metaStore, IVariables variables ) throws HopException {
    try {
      // See if the environment already exists
      //
      if (!EnvironmentSingleton.isInitialized()) {
        EnvironmentSingleton.initializeEnvironments();
      }
      MetaStoreFactory<Environment> factory = EnvironmentSingleton.getEnvironmentFactory();
      boolean changed=false;
      if ( createEnvironment) {
        createEnvironment( log, factory );
        changed=true;
      } else if ( modifyEnvironment) {
        modifyEnvironment( log, factory );
        changed=true;
      } else if ( deleteEnvironment) {
        deleteEnvironment( log, factory );
        changed=true;
      } else if ( listEnvironments) {
        listEnvironments( log, factory );
        changed=true;
      }
      return changed;
    } catch(Exception e) {
      throw new HopException("Error handling environment configuration options", e);
    }

  }

  private void listEnvironments( ILogChannel log, MetaStoreFactory<Environment> factory ) throws MetaStoreException {

    log.logBasic( "Environments:" );
    List<String> names = factory.getElementNames();
    for (String name : names) {
      Environment environment = factory.loadElement( name );
      log.logBasic( environment.getName()+" : "+environment.getEnvironmentHomeFolder() );
      for (EnvironmentVariable variable : environment.getVariables()) {
        log.logBasic( "  "+variable.getName()+" = "+variable.getValue()+ ( StringUtils.isEmpty(variable.getDescription()) ? "" : " ("+variable.getDescription()+")") );
      }
    }
  }

  private void deleteEnvironment( ILogChannel log, MetaStoreFactory<Environment> factory ) throws Exception {
    validateEnvironmentNameSpecified();
    if ( !factory.elementExists( environmentName ) ) {
      throw new HopException( "Environment '" + environmentName + "' doesn't exists, it can't be deleted" );
    }
    factory.deleteElement( environmentName );
  }

  private void modifyEnvironment( ILogChannel log, MetaStoreFactory<Environment> factory ) throws Exception {
    validateEnvironmentNameSpecified();
    if ( !factory.elementExists( environmentName ) ) {
      throw new HopException( "Environment '" + environmentName + "' doesn't exists, it can't be modified" );
    }
    Environment environment = factory.loadElement( environmentHome );

    updateEnvironmentHome( environment );
    updateEnvironmentVariables( environment );

    factory.saveElement( environment );
    log.logBasic( "Environment '" + environment.getName() + "' was modified.");
  }



  private void createEnvironment( ILogChannel log, MetaStoreFactory<Environment> factory ) throws Exception {
    validateEnvironmentNameSpecified();
    validateEnvironmentHomeSpecified();

    Environment environment = new Environment();
    environment.setName( environmentName );

    updateEnvironmentHome(environment);
    updateEnvironmentVariables(environment);

    log.logBasic( "Creating environment '"+environmentName+"'" );
    if ( factory.elementExists( environmentName ) ) {
      throw new HopException( "Environment '" + environmentName + "' already exists." );
    }

    factory.saveElement( environment );
    log.logBasic( "Environment '" + environment.getName() + "' was created for home folder : " + environment.getEnvironmentHomeFolder() );
  }



  private void validateEnvironmentNameSpecified() throws Exception {
    if ( StringUtil.isEmpty( environmentName ) ) {
      throw new HopException( "Please specify the name of the environment to create" );
    }
  }

  private void validateEnvironmentHomeSpecified() throws Exception {
    if ( StringUtil.isEmpty( environmentHome ) ) {
      throw new HopException( "Please specify the home directory of the environment to create" );
    }
  }

  private void updateEnvironmentHome( Environment environment ) {
    if (environmentHome!=null) {
      environment.setEnvironmentHomeFolder( environmentHome );
    }
  }

  private void updateEnvironmentVariables( Environment environment ) {
    if ( environmentVariables != null ) {
      for ( String environmentVariable : environmentVariables ) {
        int indexOfEquals = environmentVariable.indexOf( '=' );
        if ( indexOfEquals > 0 ) {
          String key = environmentVariable.substring( 0, indexOfEquals );
          String value = environmentVariable.substring( indexOfEquals + 1 );
          if ( StringUtils.isNotEmpty( value ) ) {

            EnvironmentVariable variable = new EnvironmentVariable( key, value, "" );

            // If the variable already exists, update it...
            //
            List<EnvironmentVariable> variables = environment.getVariables();
            int index = variables.indexOf( variable );
            if (index>=0) {
              variables.set(index, variable);
            } else {
              variables.add(variable);
            }
          }
        }
      }
    }
  }
}

