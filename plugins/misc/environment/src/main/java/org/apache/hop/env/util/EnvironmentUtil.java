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
import org.apache.hop.metadata.api.IHopMetadataProvider;
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
   * @param environment
   * @param hopMetadataProvider
   * @throws HopException
   * @throws HopException
   */
  public static void enableEnvironment( ILogChannel log, Environment environment, IHopMetadataProvider hopMetadataProvider, IVariables variables ) throws HopException, HopException {

    // Variable system variables but also apply them to variables
    // We'll use those to change the loaded variables in HopGui
    //
    environment.modifyVariables( variables );

    // Change the metadata
    //
    HopGui.getInstance().setMetadataProvider( HopMetadataUtil.getStandardHopMetadataProvider( variables ) );

    // We store the environment in the HopGui namespace
    //
    HopNamespace.setNamespace( environment.getName() );

    // Signal others that we have a new active environment
    //
    ExtensionPointHandler.callExtensionPoint( log, Defaults.EXTENSION_POINT_ENVIRONMENT_ACTIVATED, environment.getName() );
  }

  public static void validateFileInEnvironment( ILogChannel log, String transFilename, Environment environment, IVariables space ) throws HopException, FileSystemException {
    if ( StringUtils.isNotEmpty( transFilename ) ) {
      // See that this filename is located under the environment home folder
      //
      String environmentHome = space.environmentSubstitute( environment.getEnvironmentHomeFolder() );
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
    String activeEnvironment = System.getProperty( Defaults.VARIABLE_ACTIVE_ENVIRONMENT );
    if ( StringUtils.isEmpty( activeEnvironment ) ) {
      // Nothing to be done here...
      //
      return;
    }

    log.logBasic( "Validating active environment '" + activeEnvironment + "'" );
    Environment environment = EnvironmentConfigSingleton.load( activeEnvironment );
    if ( environment == null ) {
      throw new HopException( "Active environment '" + activeEnvironment + "' couldn't be found. Fix your setup." );
    }

    if ( environment.isEnforcingExecutionInHome() ) {
      EnvironmentUtil.validateFileInEnvironment( log, executableFilename, environment, space );
    }
  }

  public static Environment getEnvironment( String environmentName ) throws HopException {
    return EnvironmentConfigSingleton.load( environmentName );
  }
}
