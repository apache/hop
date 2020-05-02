package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiDirectoryDialogExtension;
import org.eclipse.swt.widgets.DirectoryDialog;

@ExtensionPoint(
  id = "HopGuiDirectoryOpenSetDefaultFolder",
  extensionPointId = "HopGuiFileDirectoryDialog",
  description = "When HopGui asks for a folder a directory dialog is shown. We want to set the default folder to the environment home folder"
)
public class HopGuiDirectoryOpenSetDefaultFolder implements IExtensionPoint<HopGuiDirectoryDialogExtension> {

  @Override public void callExtensionPoint( ILogChannel log, HopGuiDirectoryDialogExtension ext ) throws HopException {
    HopGui hopGui = HopGui.getInstance();
    String environmentName = HopNamespace.getNamespace();
    if ( StringUtil.isEmpty(environmentName)) {
      return;
    }
    try {
      Environment environment = EnvironmentUtil.getEnvironment(environmentName);
      if (environment!=null) {
        DirectoryDialog dialog = ext.getDirectoryDialog();
        dialog.setFilterPath(environment.getEnvironmentHomeFolder());
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for environment "+environmentName, e );
    }
  }
}
