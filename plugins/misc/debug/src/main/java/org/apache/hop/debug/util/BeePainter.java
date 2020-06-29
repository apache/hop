package org.apache.hop.debug.util;

import org.apache.hop.core.gui.IGc;
import org.apache.hop.core.svg.SvgFile;

public class BeePainter {

  public void drawBee( IGc gc, int x, int y, int iconSize, ClassLoader classLoader ) throws Exception {

    float magnification = gc.getMagnification();
    SvgFile svgFile = new SvgFile( "bee.svg", classLoader );
    gc.drawImage( svgFile, x -2 + iconSize + iconSize/3, y + iconSize / 3, 16, 16, magnification, 0 );
    gc.setTransform( 0, 0, magnification );
  }
}
