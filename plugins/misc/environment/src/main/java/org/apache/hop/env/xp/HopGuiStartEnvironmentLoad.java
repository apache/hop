package org.apache.hop.env.xp;


import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.config.EnvironmentConfig;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.gui.EnvironmentGuiPlugin;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.List;

@ExtensionPoint(
  id = "HopGuiStartEnvironmentLoad",
  description = "Load the previously used environment",
  extensionPointId = "HopGuiStart"
)
/**
 * set the debug level right before the step starts to run
 */
public class HopGuiStartEnvironmentLoad implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel logChannelInterface, Object o ) throws HopException {

    HopGui hopGui = HopGui.getInstance();
    IVariables variables = hopGui.getVariables();

    try {
      EnvironmentConfig config = EnvironmentConfigSingleton.getConfig();

      // Only move forward if the environment system is enabled...
      //
      if ( EnvironmentConfigSingleton.getConfig().isEnabled() ) {

        logChannelInterface.logBasic( "Environments enabled" );

        if ( config.isOpeningLastEnvironmentAtStartup() ) {

          logChannelInterface.logBasic( "Opening last environment at startup" );

          // What is the last used environment?
          //
          List<AuditEvent> auditEvents = AuditManager.getActive().findEvents(
            EnvironmentUtil.STRING_ENVIRONMENT_AUDIT_GROUP,
            EnvironmentUtil.STRING_ENVIRONMENT_AUDIT_TYPE,
            true
          );
          if ( !auditEvents.isEmpty() ) {

            logChannelInterface.logBasic( "Audit events found for hop-gui/environment : " + auditEvents.size() );

            for ( AuditEvent auditEvent : auditEvents ) {
              String lastEnvironmentName = auditEvent.getName();

              if ( StringUtils.isNotEmpty( lastEnvironmentName ) ) {

                if ( EnvironmentConfigSingleton.exists( lastEnvironmentName ) ) {
                  Environment environment = EnvironmentConfigSingleton.load( lastEnvironmentName );

                  logChannelInterface.logBasic( "Enabling environment : '" + lastEnvironmentName + "'" );

                  // Set system variables for HOP_HOME, HOP_METADATA_FOLDER, ...
                  // Sets the namespace in HopGui to the name of the environment
                  //
                  EnvironmentGuiPlugin.enableHopGuiEnvironment( lastEnvironmentName, environment );

                  // Don't open the files twice
                  //
                  HopGui.getInstance().setOpeningLastFiles( false );
                  break;// we have an environment
                } else {
                  logChannelInterface.logError( "The last used environment '" + lastEnvironmentName + "' no longer exists" );
                }
              }
            }
          } else {
            logChannelInterface.logBasic( "No last environments history found" );
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error initializing the Environment system", e );
    }
  }

}
