package org.apache.hop.testing.gui;

import org.apache.hop.core.gui.plugin.GuiMetaStoreElement;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.ui.cluster.IGuiMetaStorePlugin;

@GuiPlugin
@GuiMetaStoreElement(
  name = "Pipeline Unit Test",
  description = "A pipeline test allows you to use data sets as input and validation",
  iconImage = "Test_tube_icon.svg"
)
public class PipelineUnitTestGuiPlugin implements IGuiMetaStorePlugin<PipelineUnitTest> {
  @Override public Class<PipelineUnitTest> getMetaStoreElementClass() {
    return PipelineUnitTest.class;
  }
}
