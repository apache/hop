package org.apache.hop.env.xp;

import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenedExtension;

import java.io.File;

@ExtensionPoint( id = "HopGuiFileReplaceHomeVariable",
extensionPointId = "HopGuiFileOpenedDialog",
description = "Replace ${ENVIRONMENT_HOME} in selected filenames as a best practice aid"
)
public class HopGuiFileReplaceHomeVariable implements IExtensionPoint<HopGuiFileOpenedExtension> {

  // TODO make this optional

  @Override public void callExtensionPoint( ILogChannel log, HopGuiFileOpenedExtension ext ) {

    // Is there an active environment?
    //
    HopGui hopGui = HopGui.getInstance();
    String environmentName = hopGui.getNamespace();
    if ( StringUtil.isEmpty(environmentName)) {
      return;
    }
    try {
      Environment environment = EnvironmentUtil.getEnvironment(environmentName);
      if (environment!=null) {

        File file = new File(ext.filename);
        String absoluteFile = file.getAbsolutePath();

        IVariables variables;
        if (ext.variables==null) {
          variables = hopGui.getVariables();
        } else {
          variables = ext.variables;
        }

        File home = new File(environment.getActualHomeFolder(variables));
        String absoluteHome = home.getAbsolutePath();
        // Make it always end with a / or \
        if (!absoluteHome.endsWith( Const.FILE_SEPARATOR )) {
          absoluteHome+=Const.FILE_SEPARATOR;
        }

        // Replace the environment home variable in the filename
        //
        if (absoluteFile.startsWith( absoluteHome )) {
          ext.filename = "${"+EnvironmentUtil.VARIABLE_ENVIRONMENT_HOME+"}/"+absoluteFile.substring( absoluteHome.length() );
        }
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for environment "+environmentName, e );
    }
  }
}
