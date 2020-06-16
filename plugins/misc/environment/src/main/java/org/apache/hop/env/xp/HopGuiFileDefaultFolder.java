package org.apache.hop.env.xp;

import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.env.config.EnvironmentConfigSingleton;
import org.apache.hop.ui.core.dialog.IFileDialog;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;

public class HopGuiFileDefaultFolder implements IExtensionPoint<HopGuiFileDialogExtension> {

  @Override public void callExtensionPoint( ILogChannel log, HopGuiFileDialogExtension ext ) {

    // Is there an active environment?
    //
    IVariables variables = HopGui.getInstance().getVariables();
    String environmentName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(environmentName)) {
      return;
    }
    try {
      String homeFolder = variables.environmentSubstitute( EnvironmentConfigSingleton.getEnvironmentHomeFolder( environmentName ) );
      if (homeFolder!=null) {
        IFileDialog dialog = ext.getFileDialog();
        dialog.setFilterPath(homeFolder);
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for environment "+environmentName, e );
    }
  }
}
