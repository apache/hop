package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.gui.EnvironmentGuiPlugin;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileOpenExtension;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.MessageBox;

/**
 * Used for create/update/delete of Environment metastore elements
 */
public class HopGuiEnvironmentChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object o ) throws HopException {
    if (!(o instanceof Environment)) {
      return;
    }

    // We want to reload the combo box in the Environment GUI plugin
    //
    EnvironmentGuiPlugin.refreshEnvironmentsList();

    if (!(o instanceof Environment)) {
      return;
    }

    Environment environment = (Environment) o;
    HopGui hopGui = HopGui.getInstance();

    // If this is the same environment as we're using, maybe we should reload things...
    //
    if (environment.getName().equals( hopGui.getNamespace() )) {
      MessageBox messageBox = new MessageBox( hopGui.getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION );
      messageBox.setText( "Switch?" );
      messageBox.setMessage( "Do you want to reload environment " + environment.getName() + "?" );
      int answer = messageBox.open();
      if ( ( answer & SWT.YES ) != 0 ) {
        try {
          EnvironmentUtil.enableEnvironment( environment, hopGui.getMetaStore() );
        } catch ( Exception e ) {
          new ErrorDialog( hopGui.getShell(), "Error", "Error reloading environment " + environment.getName(), e );
        }
      }
    }
  }
}
