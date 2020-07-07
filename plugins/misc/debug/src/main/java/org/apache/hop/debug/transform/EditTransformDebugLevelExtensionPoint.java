package org.apache.hop.debug.transform;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.pipeline.extension.HopGuiPipelineGraphExtension;

@ExtensionPoint(
  id = "EditTransformDebugLevelExtensionPoint",
  extensionPointId = "PipelineGraphMouseUp",
  description = "Edit the custom transform debug level with a single click"
)
public class EditTransformDebugLevelExtensionPoint implements IExtensionPoint<HopGuiPipelineGraphExtension> {
  @Override public void callExtensionPoint( ILogChannel log, HopGuiPipelineGraphExtension ext ) throws HopException {
    try {
      if ( ext.getAreaOwner() == null || ext.getAreaOwner().getOwner() == null ) {
        return;
      }
      if ( !( ext.getAreaOwner().getOwner() instanceof TransformDebugLevel ) ) {
        return;
      }
      TransformDebugLevel debugLevel = (TransformDebugLevel) ext.getAreaOwner().getOwner();
      PipelineMeta pipelineMeta = ext.getPipelineGraph().getPipelineMeta();
      TransformMeta transformMeta = (TransformMeta) ext.getAreaOwner().getParent();
      IRowMeta inputRowMeta = pipelineMeta.getPrevTransformFields( transformMeta );

      TransformDebugLevelDialog dialog = new TransformDebugLevelDialog( HopGui.getInstance().getShell(), debugLevel, inputRowMeta );
      if ( dialog.open() ) {
        DebugLevelUtil.storeTransformDebugLevel(
          pipelineMeta.getAttributesMap().get( Defaults.DEBUG_GROUP ),
          transformMeta.getName(),
          debugLevel
        );

        ext.getPipelineGraph().redraw();
      }
    } catch(Exception e) {
      new ErrorDialog( HopGui.getInstance().getShell(), "Error", "Error editing transform debugging level", e );
    }
  }
}
