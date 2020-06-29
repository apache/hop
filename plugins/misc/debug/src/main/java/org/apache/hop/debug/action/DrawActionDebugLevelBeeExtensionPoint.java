package org.apache.hop.debug.action;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.debug.util.BeePainter;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.workflow.WorkflowPainterExtension;

import java.awt.image.BufferedImage;
import java.util.Map;

@ExtensionPoint(
  id = "DrawActionDebugLevelBeeExtensionPoint",
  description = "Draw a bee over a workflow entry which has debug level information stored",
  extensionPointId = "WorkflowPainterAction"
)
/**
 * We need to use the hop drawing logic because the workflow entry XP is not available
 */
public class DrawActionDebugLevelBeeExtensionPoint extends BeePainter implements IExtensionPoint<WorkflowPainterExtension> {

  private static BufferedImage beeImage;

  @Override public void callExtensionPoint( ILogChannel log, WorkflowPainterExtension ext ) {

    try {
      // The next statement sometimes causes an exception in WebSpoon
      // Keep it in the try/catch block
      //
      int iconSize = PropsUi.getInstance().getIconSize();

      Map<String, String> actionLevelMap = ext.workflowMeta.getAttributesMap().get( Defaults.DEBUG_GROUP );
      if ( actionLevelMap != null ) {

        ActionDebugLevel actionDebugLevel = DebugLevelUtil.getActionDebugLevel( actionLevelMap, ext.actionCopy.toString() );
        if (actionDebugLevel!=null) {
          drawBee( ext.gc, ext.x1, ext.y1, iconSize, this.getClass().getClassLoader() );
        }
      }
    } catch ( Exception e ) {
      // Ignore error, not that important
      // logChannelInterface.logError( "Unable to handle specific debug level", e );
    }
  }


}
