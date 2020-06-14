package org.apache.hop.env.xp;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
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
    IVariables variables;
    if (ext.variables==null) {
      variables = HopGui.getInstance().getVariables();
    } else {
      variables = ext.variables;
    }
    String environmentName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(environmentName)) {
      return;
    }
    String homeFolder = variables.environmentSubstitute( EnvironmentConfigSingleton.getEnvironmentHomeFolder( environmentName ) );
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
