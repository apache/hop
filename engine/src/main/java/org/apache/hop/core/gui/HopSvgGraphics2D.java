package org.apache.hop.core.gui;

import org.apache.batik.svggen.DOMGroupManager;
import org.apache.batik.svggen.ExtensionHandler;
import org.apache.batik.svggen.ImageHandler;
import org.apache.batik.svggen.SVGGeneratorContext;
import org.apache.batik.svggen.SVGGraphics2D;
import org.w3c.dom.Document;

import java.awt.font.TextLayout;

public class HopSvgGraphics2D extends SVGGraphics2D {
  public HopSvgGraphics2D( Document domFactory ) {
    super( domFactory );
  }

  public HopSvgGraphics2D( Document domFactory, ImageHandler imageHandler, ExtensionHandler extensionHandler, boolean textAsShapes ) {
    super( domFactory, imageHandler, extensionHandler, textAsShapes );
  }

  public HopSvgGraphics2D( SVGGeneratorContext generatorCtx, boolean textAsShapes ) {
    super( generatorCtx, textAsShapes );
  }

  public HopSvgGraphics2D( SVGGraphics2D g ) {
    super( g );
  }

  public DOMGroupManager getDomGroupManager() {
    return super.getDOMGroupManager();
  }

  @Override public void drawString( String str, int x, int y ) {

    if (str.contains( "\\n" )) {

      String[] lines = str.split( "\\n" );
      int lineX = x;
      int lineY = y;
      for (String line : lines) {
        TextLayout tl = new TextLayout( line, getFont(), getFontRenderContext() );
        drawString( line, lineX, lineY );
        lineY+=tl.getBounds().getHeight()+tl.getDescent();
      }

    } else {
      super.drawString( str, x, y );
    }
  }
}
