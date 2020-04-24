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
import org.apache.hop.env.gui.EnvironmentGuiPlugin;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.metastore.MetaStoreConst;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.metastore.api.exceptions.MetaStoreException;
import org.apache.hop.metastore.stores.delegate.DelegatingMetaStore;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.Date;

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
   * @param environment
   * @param delegatingMetaStore
   * @throws HopException
   * @throws MetaStoreException
   */
  public static void enableEnvironment( Environment environment, DelegatingMetaStore delegatingMetaStore ) throws HopException, MetaStoreException {

    // Variable system variables but also apply them to variables
    // We'll use those to change the loaded variables in HopGui
    //
    IVariables variables = Variables.getADefaultVariableSpace();
    environment.modifyVariables( variables, true );  // TODO: do we need to change System?

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
    // Set this in hopGui so new files will inherit from it.
    //
    HopGui hopGui = HopGui.getInstance();
    if ( hopGui != null ) {

      // Before we switch the namespace in HopGui, save the state of the perspectives
      //
      hopGui.auditDelegate.writeLastOpenFiles();

      // Now we can close all files if they're all saved (or changes are ignored)
      //
      if (!hopGui.fileDelegate.saveGuardAllFiles()) {
        // Abort the environment change
        return;
      }

      // Close 'm all
      //
      hopGui.fileDelegate.closeAllFiles();

      // We store the environment in the HopGui namespace
      //
      hopGui.setNamespace( environment.getName() );

      // We need to change the currently set variables in the newly loaded files
      //
      hopGui.setVariables( variables );

      // Re-open last open files for the namespace
      //
      hopGui.auditDelegate.openLastFiles();

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

      // Refresh the currently active file
      //
      hopGui.getActivePerspective().getActiveFileTypeHandler().updateGui();

      // Update the toolbar combo
      //
      EnvironmentGuiPlugin.selectEnvironmentInList(environment.getName());

      // Also add this as an event so we know what the environment usage history is
      //
      AuditEvent envUsedEvent = new AuditEvent( STRING_ENVIRONMENT_AUDIT_GROUP, STRING_ENVIRONMENT_AUDIT_TYPE, environment.getName(), "open", new Date() );
      hopGui.getAuditManager().storeEvent( envUsedEvent );
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

  public static Environment getEnvironment( String environmentName ) throws MetaStoreException {
    return EnvironmentSingleton.getEnvironmentFactory().loadElement( environmentName );
  }
}
