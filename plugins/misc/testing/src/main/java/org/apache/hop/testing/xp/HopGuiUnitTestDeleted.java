package org.apache.hop.testing.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;

@ExtensionPoint(
  id = "HopGuiUnitTestDeleted",
  extensionPointId = "HopGuiMetadataObjectDeleted",
  description = "When HopGui deletes a new metadata object somewhere"
)
public class HopGuiUnitTestDeleted extends HopGuiUnitTestChanged implements IExtensionPoint {
}
