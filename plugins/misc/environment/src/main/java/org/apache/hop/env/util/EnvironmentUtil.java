package org.apache.hop.env.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.session.EnvironmentSessionUtil;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.ui.hopgui.HopGui;

public class EnvironmentUtil {

  public static final String VARIABLE_ENVIRONMENT_HOME = "ENVIRONMENT_HOME";
  public static final String VARIABLE_DATASETS_BASE_PATH = "DATASETS_BASE_PATH";
  public static final String VARIABLE_UNIT_TESTS_BASE_PATH = "UNIT_TESTS_BASE_PATH";


  /**
   * Enable the specified environment
   * Force reload of a number of settings
   *
   * @param environment
   * @param delegatingMetaStore
   * @throws HopException
   * @throws MetaStoreException
   */
  public static void enableEnvironment( Environment environment, DelegatingMetaStore delegatingMetaStore ) throws HopException, MetaStoreException {

    // Variable system variables but also apply them to variables
    // We'll use those to change the loaded variables in Spoon
    //
    IVariables variables = new Variables();
    environment.modifyIVariables( variables, true );

    // Create Hop home folder in case it doesn't exist
    //
    HopClientEnvironment.createHopHome();

    // Force reload of the Hop environment
    //
    EnvUtil.environmentInit();

    // Initialize the logging back-end.
    //
    HopLogStore.init();

    // Restart the environment
    //
    HopEnvironment.shutdown();
    HopEnvironment.init();

    // Modify local loaded metastore...
    //
    if ( delegatingMetaStore != null ) {
      IMetaStore metaStore = delegatingMetaStore.getMetaStore( Const.HOP_METASTORE_NAME );
      if ( metaStore != null ) {
        int index = delegatingMetaStore.getMetaStoreList().indexOf( metaStore );
        metaStore = MetaStoreConst.openLocalHopMetaStore();
        delegatingMetaStore.getMetaStoreList().set( index, metaStore );
        delegatingMetaStore.setActiveMetaStoreName( metaStore.getName() );
      }
    }

    // Signal others that we have a new active environment
    //
    ExtensionPointHandler.callExtensionPoint( LogChannel.GENERAL, Defaults.EXTENSION_POINT_ENVIRONMENT_ACTIVATED, environment.getName() );

    // See if we need to restore the default Spoon session for this environment
    // The name of the session is the name of the environment
    // This will cause the least amount of issues.
    //
    HopGui hopGui = HopGui.getInstance();
    if ( hopGui != null ) {

      // We store the environment in the HopGui namespace
      //
      hopGui.setNamespace( environment.getName() );

      // Clear last used, fill it with something useful.
      //
      IVariables hopGuiVariables = Variables.getADefaultVariableSpace();
      hopGui.setVariables( hopGuiVariables );
      for ( String variable : variables.listVariables() ) {
        String value = variables.getVariable( variable );
        if ( !variable.startsWith( Const.INTERNAL_VARIABLE_PREFIX ) ) {
          hopGuiVariables.setVariable( variable, value );
        }
      }

      if ( environment.isAutoRestoringHopGuiSession() ) {
        EnvironmentSessionUtil.restoreHopGuiSession();
      }

      // Refresh the currently active file
      //
      hopGui.getActivePerspective().getActiveFileTypeHandler().updateGui();
    }
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

  public static void validateFileInEnvironment( ILogChannel log, String executableFilename, IVariables space ) throws HopException, FileSystemException, MetaStoreException {

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
    Environment environment = EnvironmentSingleton.getEnvironmentFactory().loadElement( activeEnvironment );
    if ( environment == null ) {
      throw new HopException( "Active environment '" + activeEnvironment + "' couldn't be found. Fix your setup." );
    }

    if ( environment.isEnforcingExecutionInHome() ) {
      EnvironmentUtil.validateFileInEnvironment( log, executableFilename, environment, space );
    }
  }
}
