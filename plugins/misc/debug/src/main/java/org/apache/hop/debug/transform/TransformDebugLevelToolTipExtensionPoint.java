package org.apache.hop.debug.transform;

import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.gui.AreaOwner;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.debug.util.BeePainter;
import org.apache.hop.ui.hopgui.file.shared.HopGuiTooltipExtension;

@ExtensionPoint(
  id = "TransformDebugLevelToolTipExtensionPoint",
  description = "Show a tooltip when hovering over the bee",
  extensionPointId = "HopGuiPipelineGraphAreaHover"
)
public class TransformDebugLevelToolTipExtensionPoint extends BeePainter implements IExtensionPoint<HopGuiTooltipExtension> {

  @Override public void callExtensionPoint( ILogChannel log, HopGuiTooltipExtension ext ) {

    AreaOwner areaOwner = ext.areaOwner;
    try {
      if ( areaOwner.getOwner() instanceof TransformDebugLevel ) {
        TransformDebugLevel debugLevel = (TransformDebugLevel) areaOwner.getOwner();
        ext.tip.append( "Custom transform debug level: " + debugLevel.toString() );
      }
    } catch ( Exception e ) {
      // Ignore error, not that important
      // logChannelInterface.logError( "Unable to handle specific debug level", e );
    }
  }


}
