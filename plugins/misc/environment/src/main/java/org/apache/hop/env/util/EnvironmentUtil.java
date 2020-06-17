package org.apache.hop.env.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;

public class EnvironmentUtil {

  public static final String VARIABLE_ENVIRONMENT_HOME = "ENVIRONMENT_HOME";
  public static final String VARIABLE_DATASETS_BASE_PATH = "DATASETS_BASE_PATH";
  public static final String VARIABLE_UNIT_TESTS_BASE_PATH = "UNIT_TESTS_BASE_PATH";

  public static final String STRING_ENVIRONMENT_AUDIT_GROUP = "environments";
  public static final String STRING_ENVIRONMENT_AUDIT_TYPE = "environment";



  /**
   * Enable the specified environment
   * Force reload of a number of settings
   *
   * @param log the log channel to log to
   * @param environmentName
   * @param environment
   * @param variables
   * @throws HopException
   * @throws HopException
   */
  public static void enableEnvironment( ILogChannel log, String environmentName, Environment environment, IVariables variables ) throws HopException {

    // Variable system variables but also apply them to variables
    // We'll use those to change the loaded variables in HopGui
    //
    String environmentHomeFolder = EnvironmentConfigSingleton.getEnvironmentHomeFolder( environmentName );
    if (StringUtils.isEmpty(environmentHomeFolder)) {
      throw new HopException("Error enabling environment "+environmentName+": it is not available or has no home folder configured");
    }
    environment.modifyVariables( variables, environmentName, environmentHomeFolder );

    // Change the metadata
    //
    HopGui.getInstance().setMetadataProvider( HopMetadataUtil.getStandardHopMetadataProvider( variables ) );

    // We store the environment in the HopGui namespace
    //
    HopNamespace.setNamespace( environmentName );

    // Signal others that we have a new active environment
    //
    ExtensionPointHandler.callExtensionPoint( log, Defaults.EXTENSION_POINT_ENVIRONMENT_ACTIVATED, environmentName );
  }

  public static void validateFileInEnvironment( ILogChannel log, String transFilename, String environmentName, String environmentHome, IVariables space ) throws HopException, FileSystemException {
    if ( StringUtils.isNotEmpty( transFilename ) ) {
      // See that this filename is located under the environment home folder
      //
      log.logBasic( "Validation against environment home : " + environmentHome );

      FileObject envHome = HopVfs.getFileObject( environmentHome );
      FileObject transFile = HopVfs.getFileObject( transFilename );
      if ( !isInSubDirectory( transFile, envHome ) ) {
        throw new HopException( "The transformation file '" + transFilename + "' does not live in the configured environment home folder : '" + environmentHome + "'" );
      }
    }
  }

  private static boolean isInSubDirectory( FileObject file, FileObject directory ) throws FileSystemException {

    String filePath = file.getName().getPath();
    String directoryPath = directory.getName().getPath();

    // Same?
    if ( filePath.equals( directoryPath ) ) {
      System.out.println( "Found " + filePath + " in directory " + directoryPath );
      return true;
    }

    if ( filePath.startsWith( directoryPath ) ) {
      return true;
    }

    FileObject parent = file.getParent();
    if ( parent != null && isInSubDirectory( parent, directory ) ) {
      return true;
    }
    return false;
  }

  public static void validateFileInEnvironment( ILogChannel log, String executableFilename, IVariables space ) throws HopException, FileSystemException, HopException {

    if ( StringUtils.isEmpty( executableFilename ) ) {
      // Repo or remote
      return;
    }

    // What is the active environment?
    //
    String activeEnvironmentName = System.getProperty( Defaults.VARIABLE_ACTIVE_ENVIRONMENT );
    if ( StringUtils.isEmpty( activeEnvironmentName ) ) {
      // Nothing to be done here...
      //
      return;
    }

    log.logBasic( "Validating active environment '" + activeEnvironmentName + "'" );
    String homeFolder = EnvironmentConfigSingleton.getEnvironmentHomeFolder( activeEnvironmentName );
    if ( StringUtils.isEmpty( homeFolder ) ) {
      throw new HopException( "The home folder for active environment '" + activeEnvironmentName + "' is not defined" );
    }

    Environment environment = EnvironmentConfigSingleton.load( activeEnvironmentName );
    if ( environment == null ) {
      throw new HopException( "Active environment '" + activeEnvironmentName + "' couldn't be found. Fix your setup." );
    }

    if ( environment.isEnforcingExecutionInHome() ) {
      EnvironmentUtil.validateFileInEnvironment( log, executableFilename, activeEnvironmentName, homeFolder, space );
    }
  }

  public static Environment getEnvironment( String environmentName ) throws HopException {
    return EnvironmentConfigSingleton.load( environmentName );
  }
}
