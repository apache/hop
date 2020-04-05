package org.apache.hop.ui.partition;

import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.partition.PartitionSchema;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Partition Schema",
  description = "Describes a partition schema",
  iconImage = "ui/images/partition_schema.svg"
)
public class PartitionSchemaGuiPlugin implements IGuiMetaStorePlugin<PartitionSchema> {

  @Override public Class<PartitionSchema> getMetastoreElementClass() {
    return PartitionSchema.class;
  }
}
