package org.apache.hop.projects.xp;


import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.history.AuditEvent;
import org.apache.hop.history.AuditManager;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.environment.LifecycleEnvironment;
import org.apache.hop.projects.gui.ProjectsGuiPlugin;
import org.apache.hop.projects.project.Project;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;

import java.util.List;

@ExtensionPoint(
  id = "HopGuiStartProjectLoad",
  description = "Load the previously used project",
  extensionPointId = "HopGuiStart"
)
/**
 * set the debug level right before the step starts to run
 */
public class HopGuiStartProjectLoad implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel logChannelInterface, Object o ) throws HopException {

    HopGui hopGui = HopGui.getInstance();
    IVariables variables = hopGui.getVariables();

    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();

      // Only move forward if the projects system is enabled...
      //
      if ( ProjectsConfigSingleton.getConfig().isEnabled() ) {

        logChannelInterface.logBasic( "Projects enabled" );

        if ( config.isOpeningLastProjectAtStartup() ) {

          logChannelInterface.logBasic( "Opening last project at startup" );

          // What is the last used project?
          //
          List<AuditEvent> auditEvents = AuditManager.getActive().findEvents(
            ProjectsUtil.STRING_PROJECTS_AUDIT_GROUP,
            ProjectsUtil.STRING_PROJECT_AUDIT_TYPE,
            true
          );
          if ( !auditEvents.isEmpty() ) {

            logChannelInterface.logBasic( "Audit events found for hop-gui/project : " + auditEvents.size() );

            for ( AuditEvent auditEvent : auditEvents ) {
              String lastProjectName = auditEvent.getName();

              if ( StringUtils.isNotEmpty( lastProjectName ) ) {

                ProjectConfig projectConfig = config.findProjectConfig( lastProjectName );

                if ( projectConfig != null ) {
                  Project project = projectConfig.loadProject( variables );

                  logChannelInterface.logBasic( "Enabling project : '" + lastProjectName + "'" );

                  LifecycleEnvironment environment = null;
                  List<LifecycleEnvironment> environments = config.findEnvironmentsOfProject( lastProjectName );
                  if ( !environments.isEmpty() ) {
                    environment = environments.get( 0 );
                  }

                  // Set system variables for HOP_HOME, HOP_METADATA_FOLDER, ...
                  // Sets the namespace in HopGui to the name of the project
                  //
                  ProjectsGuiPlugin.enableHopGuiProject( lastProjectName, project, environment );

                  // Don't open the files twice
                  //
                  HopGui.getInstance().setOpeningLastFiles( false );
                  break;// we have an project
                } else {
                  // Silently ignore, not that important
                  // logChannelInterface.logError( "The last used project '" + lastProjectName + "' no longer exists" );
                }
              }
            }
          } else {
            logChannelInterface.logBasic( "No last projects history found" );
          }
        }
      }
    } catch ( Exception e ) {
      new ErrorDialog( hopGui.getShell(), "Error", "Error initializing the Projects system", e );
    }
  }

}
