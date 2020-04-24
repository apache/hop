package org.apache.hop.env.xp;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.ui.hopgui.delegates.HopGuiFileDialogExtension;

@ExtensionPoint(
  id = "HopGuiFileSaveSetDefaultFolder",
  extensionPointId = "HopGuiFileSaveDialog",
  description = "When HopGui saves a file under a new name it presents a dialog. We want to set the default folder to the environment home folder"
)
public class HopGuiFileSaveSetDefaultFolder extends HopGuiFileDefaultFolder implements IExtensionPoint<HopGuiFileDialogExtension> {

}
