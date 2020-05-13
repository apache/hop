package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.gui.TestingGuiPlugin;

/**
 * Used for create/update/delete of unit test metastore elements
 */
public class HopGuiUnitTestChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object object ) throws HopException {
    // We only respond to pipeline unit test changes
    //
    if (!(object instanceof PipelineUnitTest)) {
      return;
    }

    TestingGuiPlugin.refreshUnitTestsList();
  }
}
