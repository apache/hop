package org.apache.hop.debug.util;

import org.apache.hop.core.gui.IGc;
import org.apache.hop.workflow.action.ActionCopy;

import java.awt.image.BufferedImage;
import java.util.Map;

public class BeePainter {

  private static BufferedImage beeImage;

  public static void drawBee( IGc gc, int x, int y, int iconSize, ClassLoader classLoader ) throws Exception {

    // Blow up the image and reduce it during painting. This gives a very crisp look
    //
    int blowupFactor = 6;
    if ( beeImage == null ) {
      beeImage = SvgLoader.transcodeSVGDocument( classLoader, "bee.svg", 30 * blowupFactor, 26 * blowupFactor );
    }

    float magnification = gc.getMagnification();
    gc.setTransform( 0, 0, magnification / blowupFactor );
    gc.drawImage( beeImage, blowupFactor * ( x + iconSize ), blowupFactor * ( y - iconSize / 3 ) );
    gc.setTransform( 0, 0, magnification );
  }
}
