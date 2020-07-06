package org.apache.hop.debug.transform;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.debug.util.BeePainter;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelinePainterExtension;

import java.awt.image.BufferedImage;
import java.util.Map;

@ExtensionPoint(
  id = "DrawTransformDebugLevelBeeExtensionPoint",
  description = "Draw a bee over a transform which has debug level information stored",
  extensionPointId = "PipelinePainterTransform"
)
/**
 * Paint transforms that have a debug level set...
 */
public class DrawTransformDebugLevelBeeExtensionPoint extends BeePainter implements IExtensionPoint<PipelinePainterExtension> {

  private static BufferedImage beeImage;

  @Override public void callExtensionPoint( ILogChannel logChannelInterface, PipelinePainterExtension ext ) {
    try {
      // The next statement sometimes causes an exception in WebSpoon
      // Keep it in the try/catch block
      //
      Map<String, String> transformLevelMap = ext.pipelineMeta.getAttributesMap().get( Defaults.DEBUG_GROUP );

      if ( transformLevelMap != null ) {

        String transformName = ext.transformMeta.getName();

        final TransformDebugLevel debugLevel = DebugLevelUtil.getTransformDebugLevel( transformLevelMap, transformName );
        if ( debugLevel != null ) {
          Rectangle r = drawBee( ext.gc, ext.x1, ext.y1, ext.iconSize, this.getClass().getClassLoader() );
          ext.areaOwners.add( new AreaOwner( AreaOwner.AreaType.CUSTOM, r.x, r.y, r.width, r.height, ext.offset, ext.transformMeta, debugLevel) );
        }
      }
    } catch ( Exception e ) {
      // Ignore error, not that important
      // logChannelInterface.logError( "Unable to handle specific debug level", e );
    }
  }


}
