package org.apache.hop.projects.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.projects.config.ProjectsConfig;
import org.apache.hop.projects.config.ProjectsConfigSingleton;
import org.apache.hop.projects.project.ProjectConfig;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;

import java.io.File;

@ExtensionPoint( id = "HopGuiFileReplaceHomeVariable",
extensionPointId = "HopGuiFileOpenedDialog",
description = "Replace ${PROJECT_HOME} in selected filenames as a best practice aid"
)
public class HopGuiFileReplaceHomeVariable implements IExtensionPoint<HopGuiFileOpenedExtension> {

  // TODO make this optional

  @Override public void callExtensionPoint( ILogChannel log, HopGuiFileOpenedExtension ext ) {

    // Is there an active project?
    //
    IVariables variables;
    if (ext.variables==null) {
      variables = HopGui.getInstance().getVariables();
    } else {
      variables = ext.variables;
    }
    String projectName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(projectName)) {
      return;
    }
    ProjectsConfig config = ProjectsConfigSingleton.getConfig();
    ProjectConfig projectConfig = config.findProjectConfig( projectName );
    if (projectConfig==null) {
      return;
    }
    String homeFolder = projectConfig.getProjectHome();
    try {
      if ( StringUtils.isNotEmpty(homeFolder)) {

        File file = new File(ext.filename);
        String absoluteFile = file.getAbsolutePath();

        File home = new File(homeFolder);
        String absoluteHome = home.getAbsolutePath();
        // Make it always end with a / or \
        if (!absoluteHome.endsWith( Const.FILE_SEPARATOR )) {
          absoluteHome+=Const.FILE_SEPARATOR;
        }

        // Replace the project home variable in the filename
        //
        if (absoluteFile.startsWith( absoluteHome )) {
          ext.filename = "${"+ ProjectsUtil.VARIABLE_PROJECT_HOME +"}/"+absoluteFile.substring( absoluteHome.length() );
        }
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for project "+projectName, e );
    }
  }
}
