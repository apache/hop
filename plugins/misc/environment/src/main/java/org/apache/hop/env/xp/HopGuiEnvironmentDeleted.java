package org.apache.hop.env.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;

@ExtensionPoint(
  id = "HopGuiEnvironmentDeleted",
  extensionPointId = "HopGuiMetaStoreElementDeleted",
  description = "When HopGui deletes a new metastore element somewhere"
)
public class HopGuiEnvironmentDeleted extends HopGuiEnvironmentChanged implements IExtensionPoint {
}
