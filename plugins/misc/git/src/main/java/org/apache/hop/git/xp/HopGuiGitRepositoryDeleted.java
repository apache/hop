package org.apache.hop.git.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;

@ExtensionPoint(
  id = "HopGuiGitRepositoryDeleted",
  extensionPointId = "HopGuiMetadataObjectDeleted",
  description = "When HopGui deletes a new metadata object somewhere"
)
public class HopGuiGitRepositoryDeleted extends HopGuiGitRepositoryChanged implements IExtensionPoint {
}
