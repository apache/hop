package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.testing.gui.TestingGuiPlugin;
import org.apache.hop.pipeline.PipelineMeta;

@ExtensionPoint(
  extensionPointId = "HopGuiPipelineAfterClose",
  id = "HopGuiPipelineAfterClose",
  description = "Cleanup the active unit test for the closed pipeline"
)
public class HopGuiPipelineAfterClose implements IExtensionPoint<PipelineMeta> {

  @Override public void callExtensionPoint( ILogChannel log, PipelineMeta pipelineMeta ) throws HopException {
    TestingGuiPlugin.getInstance().getActiveTests().remove( pipelineMeta );
  }
}
