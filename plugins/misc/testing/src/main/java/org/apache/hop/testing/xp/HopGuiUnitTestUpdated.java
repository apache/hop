package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.testing.PipelineUnitTest;

@ExtensionPoint(
  id = "HopGuiUnitTestUpdated",
  extensionPointId = "HopGuiMetaStoreElementUpdated",
  description = "When HopGui updates a new metastore element somewhere"
)
public class HopGuiUnitTestUpdated extends HopGuiUnitTestChanged implements IExtensionPoint {

}
