package org.apache.hop.env.xp;


import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.env.config.EnvironmentConfig;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.util.Defaults;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.List;

@ExtensionPoint(
  id = "HopGuiInitEnvironmentInitialisation",
  description = "Initialize the hop environments singleton",
  extensionPointId = "HopGuiInit"
)
/**
 * set the debug level right before the step starts to run
 */
public class HopGuiInitEnvironmentInitialisation implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel logChannelInterface, Object o ) throws HopException {
    IVariables variables = Variables.getADefaultVariableSpace();

    // Where is the metastore for the environment
    //
    String environmentMetastoreLocation = variables.getVariable( Defaults.VARIABLE_ENVIRONMENT_METASTORE_FOLDER );
    if ( StringUtils.isEmpty( environmentMetastoreLocation ) ) {
      environmentMetastoreLocation = Defaults.ENVIRONMENT_METASTORE_FOLDER;
    }

    // Build the metastore for it.
    //
    try {
      EnvironmentSingleton.initialize( environmentMetastoreLocation );

      EnvironmentConfigSingleton.initialize( EnvironmentSingleton.getEnvironmentMetaStore() );
    } catch ( Exception e ) {
      throw new HopException("Error initializing the Environment system", e );
    }
  }

}
