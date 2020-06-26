package org.apache.hop.projects.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;

@ExtensionPoint(
  id = "HopGuiFileOpenSetDefaultFolder",
  extensionPointId = "HopGuiFileOpenDialog",
  description = "When HopGui opens a new file it presents a dialog. We want to set the default folder to the project home folder"
)
public class HopGuiFileOpenSetDefaultFolder extends HopGuiFileDefaultFolder implements IExtensionPoint<HopGuiFileDialogExtension> {

}
