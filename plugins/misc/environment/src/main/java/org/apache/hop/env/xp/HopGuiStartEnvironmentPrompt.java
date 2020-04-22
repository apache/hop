package org.apache.hop.env.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.environment.EnvironmentsDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.env.config.EnvironmentConfig;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.env.util.EnvironmentUtil;

import java.util.List;

@ExtensionPoint(
  id = "HopGuiStartEnvironmentPrompt",
  description = "Ask the user for the environment to load",
  extensionPointId = "HopGuiStart"
)
/**
 * set the debug level right before the step starts to run
 */
public class HopGuiStartEnvironmentPrompt implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel logChannelInterface, Object o ) throws HopException {

    HopGui spoon = HopGui.getInstance();
    IVariables space = Variables.getADefaultVariableSpace();

    // Where is the metastore for the environment
    //
    String environmentMetastoreLocation = space.getVariable( Defaults.VARIABLE_ENVIRONMENT_METASTORE_FOLDER );
    if ( StringUtils.isEmpty( environmentMetastoreLocation ) ) {
      environmentMetastoreLocation = Defaults.ENVIRONMENT_METASTORE_FOLDER;
    }
    // Build the metastore for it.
    //
    try {
      EnvironmentSingleton.initialize( environmentMetastoreLocation );

      List<String> environmentNames = EnvironmentSingleton.getEnvironmentFactory().getElementNames();

      EnvironmentConfigSingleton.initialize( EnvironmentSingleton.getEnvironmentMetaStore() );

      EnvironmentConfig config = EnvironmentConfigSingleton.getConfig();

      // Only move forward if the environment system is enabled...
      //
      if ( EnvironmentConfigSingleton.getConfig().isEnabled() ) {

        String selectedEnvironment;
        if ( config.isOpeningLastEnvironmentAtStartup() ) {
          selectedEnvironment = config.getLastUsedEnvironment();
        } else {
          EnvironmentsDialog environmentsDialog = new EnvironmentsDialog( spoon.getShell(), spoon.getMetaStore() );
          selectedEnvironment = environmentsDialog.open();
        }

        if ( StringUtils.isNotEmpty( selectedEnvironment ) ) {

          Environment environment = EnvironmentSingleton.getEnvironmentFactory().loadElement( selectedEnvironment );
          if ( environment == null ) {
            // Environment no longer exists, pop up dialog
            //
            EnvironmentsDialog environmentsDialog = new EnvironmentsDialog( spoon.getShell(), spoon.getMetaStore() );
            selectedEnvironment = environmentsDialog.open();
            if ( selectedEnvironment == null ) {
              return; // Canceled
            }
            environment = EnvironmentSingleton.getEnvironmentFactory().loadElement( selectedEnvironment );
          }

          // Save the last used configuration
          //
          EnvironmentConfigSingleton.getConfig().setLastUsedEnvironment( selectedEnvironment );
          EnvironmentConfigSingleton.saveConfig();

          // Double check!
          //
          if ( environment != null ) {
            logChannelInterface.logBasic( "Setting environment : '" + environment.getName() + "'" );

            // Set system variables for KETTLE_HOME, HOP_METASTORE_FOLDER, ...
            //
            EnvironmentUtil.enableEnvironment( environment, spoon.getMetaStore() );
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( spoon.getShell(), "Error", "Error initializing the Hop Environment system", e );
    }
  }

}
