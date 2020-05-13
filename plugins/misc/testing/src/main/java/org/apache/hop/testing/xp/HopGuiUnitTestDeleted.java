package org.apache.hop.testing.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.testing.PipelineUnitTest;

@ExtensionPoint(
  id = "HopGuiUnitTestDeleted",
  extensionPointId = "HopGuiMetaStoreElementDeleted",
  description = "When HopGui deletes a new metastore element somewhere"
)
public class HopGuiUnitTestDeleted extends HopGuiUnitTestChanged implements IExtensionPoint {
}
