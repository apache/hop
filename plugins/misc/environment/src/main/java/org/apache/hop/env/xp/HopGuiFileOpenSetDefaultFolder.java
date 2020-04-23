package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.environment.EnvironmentSingleton;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenExtension;
import org.eclipse.swt.widgets.FileDialog;

@ExtensionPoint(
  id = "HopGuiFileOpenSetDefaultFolder",
  extensionPointId = "HopGuiFileOpenDialog",
  description = "When HopGui opens a new file it presents a dialog. We want to set the default folder to the environment home folder"
)
public class HopGuiFileOpenSetDefaultFolder implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object o ) throws HopException {
    if (!(o instanceof HopGuiFileOpenExtension )) {
      return;
    }

    HopGuiFileOpenExtension ext = (HopGuiFileOpenExtension) o;

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
        FileDialog dialog = ext.getFileDialog();
        dialog.setFilterPath(environment.getEnvironmentHomeFolder());
      }
    } catch(Exception e) {
      log.logError( "Error setting default folder for environment "+environmentName, e );
    }
  }
}
