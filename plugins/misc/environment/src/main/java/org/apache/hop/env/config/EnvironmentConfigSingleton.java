package org.apache.hop.env.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.config.HopConfig;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.environment.Environment;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
      try {
        ObjectMapper mapper = new ObjectMapper();
        config = mapper.readValue( new Gson().toJson( configObject ), EnvironmentConfig.class );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error reading environments configuration, check property '" + EnvironmentConfig.HOP_CONFIG_ENVIRONMENT_CONFIG + "' in the Hop config json file", e );
        config = new EnvironmentConfig();
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

  public static final List<Environment> loadAllEnvironments() {
    IVariables variables = Variables.getADefaultVariableSpace();
    List<Environment> environments = new ArrayList<>();
    for ( String environmentName : getEnvironmentNames() ) {
      try {
        Environment environment = load( environmentName );
        environments.add( environment );
      } catch ( Exception e ) {
        LogChannel.GENERAL.logError( "Error loading environment", e );
      }
    }
    return environments;
  }

  public static final String getActualEnvironmentConfigFilename( String homeFolder ) {
    IVariables variables = Variables.getADefaultVariableSpace();
    String actualHomeFolder = variables.environmentSubstitute( homeFolder );
    String actualConfigFilename = variables.environmentSubstitute( getConfig().getEnvironmentConfigFilename() );
    return FilenameUtils.concat( actualHomeFolder, actualConfigFilename );
  }

  public static final String getEnvironmentHomeFolder(String environmentName) {
    return getConfig().getEnvironmentFolders().get(environmentName);
  }

  /**
   * Save the environment to disk.
   *
   * @param environment
   */
  public static void save( String environmentName, Environment environment ) throws HopException {
    String homeFolder = getEnvironmentHomeFolder( environmentName );
    if ( StringUtils.isEmpty(homeFolder)) {
      throw new HopException("The environment '"+environmentName+"' is not configured yet. The home folder for it is not defined.");
    }

    String envFile = getActualEnvironmentConfigFilename( homeFolder );
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.writerWithDefaultPrettyPrinter().writeValue( new File( envFile ), environment );
    } catch ( Exception e ) {
      throw new HopException( "Error saving environment to file '" + envFile + "'", e );
    }
  }

  public static boolean exists( String environmentName ) {
    return StringUtils.isNotEmpty( getEnvironmentHomeFolder( environmentName ) );
  }

  public static Environment load( String environmentName ) throws HopException {
    String homeFolder = getEnvironmentHomeFolder( environmentName );
    if ( StringUtils.isEmpty(homeFolder)) {
      throw new HopException("The environment '"+environmentName+"' is not configured yet. The home folder for it is not defined.");
    }

    String envFileName = getActualEnvironmentConfigFilename( homeFolder );
    File envFile = new File(envFileName);
    if (!envFile.exists()) {
      return new Environment(); // just return an empty new one. This is not an error
    }
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      return objectMapper.readValue( envFile, Environment.class );
    } catch ( Exception e ) {
      throw new HopException( "Error load environment from file '" + envFileName + "'", e );
    }
  }

  /**
   * It just removes from the index, not the file itself
   *
   * @param environmentName
   */
  public static void delete( String environmentName ) throws HopException {
    getConfig().getEnvironmentFolders().remove( environmentName );
    saveConfig();
    HopConfig.saveToFile();
  }

  public static List<String> getEnvironmentNames() {
    List<String> list = new ArrayList<>(getConfig().getEnvironmentFolders().keySet());
    Collections.sort( list );
    return list;
  }

  public static void createEnvironment( String environmentName, String environmentHomeFolder ) throws HopException {
    getConfig().getEnvironmentFolders().put(environmentName, environmentHomeFolder);
    saveConfig();
    HopConfig.saveToFile();
  }
}
