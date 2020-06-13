package org.apache.hop.testing.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.testing.PipelineUnitTest;
import org.apache.hop.testing.TestType;
import org.apache.hop.testing.gui.TestingGuiPlugin;

@ExtensionPoint(
  id = "HopGuiUnitTestCreateBeforeDialog",
  extensionPointId = "HopGuiMetadataObjectCreateBeforeDialog",
  description = "Changes the name of the default unit test and calculates a relative path"
)
public class HopGuiUnitTestCreateBeforeDialog extends HopGuiUnitTestChanged implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object object ) throws HopException {

    // Ignore all other metadata object changes
    //
    if (!(object instanceof PipelineUnitTest)) {
      return;
    }
    PipelineUnitTest test = (PipelineUnitTest) object;
    PipelineMeta pipelineMeta = TestingGuiPlugin.getActivePipelineMeta();
    if (pipelineMeta!=null) {
      test.setName( pipelineMeta.getName() + " UNIT" );
      test.setType( TestType.UNIT_TEST );
      test.setRelativeFilename( pipelineMeta.getFilename() );
    }
  }
}
