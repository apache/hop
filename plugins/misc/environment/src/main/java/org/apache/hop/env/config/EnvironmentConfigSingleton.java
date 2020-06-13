package org.apache.hop.env.config;

import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnvironmentConfigSingleton {

  private static EnvironmentConfigSingleton configSingleton;

  private EnvironmentConfig config;

  private EnvironmentConfigSingleton() {
    // Load from the HopConfig store
    //
    Object configObject = HopConfig.getConfigMap().get( EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_CONFIG );
    if ( configObject == null ) {
      config = new EnvironmentConfig();
    } else {
      // The way Jackson stores these simple POJO is with a map per default...
      // So we don't really need to mess around with Deserializer and so on.
      // This way we can keep the class name out of the JSON as well.
      //
      Map<String, Object> map = (Map<String, Object>) configObject;
      config = new EnvironmentConfig();
      config.setEnabled( (Boolean) map.get( "enabled" ) );
      config.setOpeningLastEnvironmentAtStartup( (Boolean) map.get( "openingLastEnvironmentAtStartup" ) );

      List envList = (List) map.get( "environments" );
      if ( envList != null ) {
        for ( Object env : envList ) {
          Map<String, Object> envMap = (Map<String, Object>) env;
          Environment environment = new Environment();
          environment.setName( (String) envMap.get( "name" ) );
          environment.setDescription( (String) envMap.get( "description" ) );
          environment.setVersion( (String) envMap.get( "version" ) );
          environment.setCompany( (String) envMap.get( "company" ) );
          environment.setDepartment( (String) envMap.get( "department" ) );
          environment.setProject( (String) envMap.get( "project" ) );
          environment.setEnvironmentHomeFolder( (String) envMap.get( "environmentHomeFolder" ) );
          environment.setMetadataBaseFolder( (String) envMap.get( "metadataBaseFolder" ) );
          environment.setUnitTestsBasePath( (String) envMap.get( "unitTestsBasePath" ) );
          environment.setDataSetsCsvFolder( (String) envMap.get( "dataSetsCsvFolder" ) );
          environment.setEnforcingExecutionInHome( (Boolean) envMap.get( "enforcingExecutionInHome" ) );
          environment.setEnforcingExecutionInHome( (Boolean) envMap.get( "enforcingExecutionInHome" ) );
          List<Object> envVariables = (List<Object>) envMap.get("variables");
          for (Object envVariable : envVariables) {
            EnvironmentVariable environmentVariable = new EnvironmentVariable();
            Map<String,Object> envVarMap = (Map<String, Object>) envVariable;
            environmentVariable.setName( (String) envVarMap.get("name") );
            environmentVariable.setDescription( (String) envVarMap.get("description") );
            environmentVariable.setValue( (String) envVarMap.get("value") );
            environment.getVariables().add(environmentVariable);
          }
          config.getEnvironments().add( environment );
        }
      }
    }
  }

  private static void saveConfig() {
    HopConfig.saveOption( EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_CONFIG, configSingleton.config );
  }

  public static EnvironmentConfig getConfig() {
    if ( configSingleton == null ) {
      configSingleton = new EnvironmentConfigSingleton();
    }
    return configSingleton.config;
  }

  /**
   * Add or replace the provided environments in the list.
   *
   * @param environment
   */
  public static void save( Environment environment ) throws HopException {
    List<Environment> environments = getConfig().getEnvironments();
    int index = environments.indexOf( environment );
    if ( index < 0 ) {
      environments.add( environment );
    } else {
      environments.set( index, environment );
    }
    saveConfig();
    HopConfig.saveToFile();
  }

  public static boolean exists( String environmentName ) {
    for ( Environment environment : getConfig().getEnvironments() ) {
      if ( environment.getName().equals( environmentName ) ) {
        return true;
      }
    }
    return false;
  }

  public static Environment load( String environmentName ) {
    for ( Environment environment : getConfig().getEnvironments() ) {
      if ( environment.getName().equals( environmentName ) ) {
        return environment;
      }
    }
    return null;
  }

  private static int indexOf( String environmentName ) {
    List<Environment> environments = getConfig().getEnvironments();
    for ( int i = 0; i < environments.size(); i++ ) {
      if ( environments.get( i ).getName().equals( environmentName ) ) {
        return i;
      }
    }
    return -1;
  }

  public static Environment delete( String environmentName ) throws HopException {
    int index = indexOf( environmentName );
    if ( index < 0 ) {
      return null;
    } else {
      Environment environment = getConfig().getEnvironments().remove( index );
      saveConfig();
      HopConfig.saveToFile();
      return environment;
    }
  }

  public static List<String> getEnvironmentNames() {
    List<String> list = new ArrayList<>();
    for ( Environment environment : getConfig().getEnvironments() ) {
      list.add( environment.getName() );
    }
    Collections.sort( list );
    return list;
  }

}
