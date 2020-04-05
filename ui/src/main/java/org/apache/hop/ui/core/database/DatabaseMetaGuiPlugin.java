package org.apache.hop.ui.core.database;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Database Connection",
  description = "A relational database connection",
  iconImage = "ui/images/CNC.svg"
)
public class DatabaseMetaGuiPlugin implements IGuiMetaStorePlugin<DatabaseMeta> {

  @Override public Class<DatabaseMeta> getMetastoreElementClass() {
    return DatabaseMeta.class;
  }
}
