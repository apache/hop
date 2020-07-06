package org.apache.hop.debug.util;

import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.gui.Rectangle;
import org.apache.hop.core.svg.SvgFile;

public class BeePainter {

  public Rectangle drawBee( IGc gc, int x, int y, int iconSize, ClassLoader classLoader ) throws Exception {
    int imageWidth = 16;
    int imageHeight = 16;
    int locationX = x + iconSize;
    int locationY = y + iconSize - imageHeight - 5; // -5 to prevent us from hitting the left bottom circle of the icon

    gc.drawImage( new SvgFile( "bee.svg", classLoader ), locationX, locationY, imageWidth, imageHeight,  gc.getMagnification(), 0 );
    return new Rectangle( locationX, locationY, imageWidth, imageHeight );
  }
}
