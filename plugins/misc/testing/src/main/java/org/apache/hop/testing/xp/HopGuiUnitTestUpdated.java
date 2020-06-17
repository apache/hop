package org.apache.hop.testing.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;


@ExtensionPoint(
  id = "HopGuiUnitTestUpdated",
  extensionPointId = "HopGuiMetadataObjectUpdated",
  description = "When HopGui updates a new metadata object somewhere"
)
public class HopGuiUnitTestUpdated extends HopGuiUnitTestChanged implements IExtensionPoint {

}
