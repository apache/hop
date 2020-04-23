package org.apache.hop.env.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;

@ExtensionPoint(
  id = "HopGuiEnvironmentUpdated",
  extensionPointId = "HopGuiMetaStoreElementUpdated",
  description = "When HopGui updates a new metastore element somewhere"
)
public class HopGuiEnvironmentUpdated extends HopGuiEnvironmentChanged implements IExtensionPoint {
}
