package org.apache.hop.testing.gui;

import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.testing.DataSet;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Data set",
  description = "A fixed set of rows to help test pipelines",
  iconImage = "dataset.svg"
)
public class DataSetGuiPlugin implements IGuiMetaStorePlugin<DataSet>  {
  @Override public Class<DataSet> getMetaStoreElementClass() {
    return DataSet.class;
  }
}
