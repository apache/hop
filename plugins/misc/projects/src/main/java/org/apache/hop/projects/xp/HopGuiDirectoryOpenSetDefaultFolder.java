package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.ui.core.dialog.IDirectoryDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;

@ExtensionPoint(
  id = "HopGuiDirectoryOpenSetDefaultFolder",
  extensionPointId = "HopGuiFileDirectoryDialog",
  description = "When HopGui asks for a folder a directory dialog is shown. We want to set the default folder to the project home folder"
)
public class HopGuiDirectoryOpenSetDefaultFolder implements IExtensionPoint<HopGuiDirectoryDialogExtension> {

  @Override public void callExtensionPoint( ILogChannel log, HopGuiDirectoryDialogExtension ext ) throws HopException {
    IVariables variables = HopGui.getInstance().getVariables();
    String projectName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(projectName)) {
      return;
    }
    try {
      ProjectsConfig config = ProjectsConfigSingleton.getConfig();
      ProjectConfig projectConfig = config.findProjectConfig( projectName );
      if (projectConfig==null) {
        return;
      }
      String homeFolder = projectConfig.getProjectHome();
      if (homeFolder!=null) {
        IDirectoryDialog dialog = ext.getDirectoryDialog();
        dialog.setFilterPath(homeFolder);
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for project "+projectName, e );
    }
  }
}
