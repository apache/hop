package org.apache.hop.git.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;

@ExtensionPoint(
  id = "HopGuiGitRepositoryCreated",
  extensionPointId = "HopGuiMetadataObjectCreated",
  description = "When HopGui create a new metadata object somewhere"
)
public class HopGuiGitRepositoryCreated extends HopGuiGitRepositoryChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object o ) throws HopException {
    super.callExtensionPoint( log, o );
  }
}
