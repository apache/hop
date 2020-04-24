package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;

@ExtensionPoint(
  id = "HopGuiEnvironmentUpdated",
  extensionPointId = "HopGuiMetaStoreElementUpdated",
  description = "When HopGui updates a new metastore element somewhere"
)
public class HopGuiEnvironmentUpdated extends HopGuiEnvironmentChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object o ) throws HopException {
    if (!(o instanceof Environment )) {
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
