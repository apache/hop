package org.apache.hop.ui.cluster;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Slave Server",
  description = "Defines a Hop Slave Server",
  iconImage = "ui/images/slave.svg"
)
public class SlaveServerGuiPlugin implements IGuiMetaStorePlugin<SlaveServer> {

  @Override public Class<SlaveServer> getMetastoreElementClass() {
    return SlaveServer.class;
  }
}
