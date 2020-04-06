package org.apache.hop.ui.pipeline.config;

import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Pipeline Run Configuration",
  description = "Describes how and with which engine a pipeline is to be executed",
  iconImage = "ui/images/run.svg"
)
  public class PipelineRunConfigurationGuiPlugin implements IGuiMetaStorePlugin<PipelineRunConfiguration> {

  @Override public Class<PipelineRunConfiguration> getMetastoreElementClass() {
    return PipelineRunConfiguration.class;
  }
}
