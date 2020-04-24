package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.env.environment.Environment;
import org.apache.hop.env.gui.EnvironmentGuiPlugin;

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
  }
}
