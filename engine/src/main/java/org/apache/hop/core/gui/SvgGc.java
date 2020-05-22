package org.apache.hop.core.gui;

import org.apache.batik.util.SVGConstants;
import org.apache.hop.core.SwingUniversalImage;
import org.apache.hop.core.SwingUniversalImageSvg;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.awt.geom.Rectangle2D;

import static org.apache.batik.svggen.DOMGroupManager.DRAW;

public class SvgGc extends SwingGc implements IGc {

  protected HopSvgGraphics2D gc;

  public SvgGc( HopSvgGraphics2D gc, Rectangle2D rect, int iconSize, int xOffset, int yOffset ) throws HopException {
    super( gc, rect, iconSize, xOffset, yOffset );
    this.gc = gc;
  }

  // Instead of drawing the image we're going to inline the image SVG into the SVG XML
  //
  @Override protected void drawImage( SwingUniversalImage image, int locationX, int locationY, int imageSize ) {
    if (!(image instanceof SwingUniversalImageSvg )) {
      super.drawImage( image, locationX, locationY, imageSize );
    }

    try {

      SwingUniversalImageSvg swingUniversalImageSvg = (SwingUniversalImageSvg) image;
      Document document = swingUniversalImageSvg.getSvg().getDocument();
      Node svgGraphicsNode = XmlHandler.getSubNode(document, "svg");

      Document domFactory = gc.getDOMFactory();

      Element svgSvg = domFactory.createElementNS( SVGConstants.SVG_NAMESPACE_URI, SVGConstants.SVG_SVG_TAG );

      svgSvg.setAttributeNS(null, SVGConstants.SVG_X_ATTRIBUTE, Integer.toString(locationX));
      svgSvg.setAttributeNS(null, SVGConstants.SVG_Y_ATTRIBUTE, Integer.toString(locationY));
      svgSvg.setAttributeNS(null, SVGConstants.SVG_WIDTH_ATTRIBUTE, Integer.toString(imageSize));
      svgSvg.setAttributeNS(null, SVGConstants.SVG_HEIGHT_ATTRIBUTE, Integer.toString(imageSize));
      svgSvg.setAttributeNS(null, SVGConstants.SVG_TRANSFORM_ATTRIBUTE, 100+"% "+100+"%");

      // Add all the elements from the SVG Image...
      //
      NodeList childNodes = svgGraphicsNode.getChildNodes();
      for (int c=0;c<childNodes.getLength();c++  ) {
        Node childNode = childNodes.item( c );

        // Let's skip metadata nodes...
        //
        boolean skip=false;
        if ("metadata".equals(childNode.getNodeName())) {
          skip = true;
        }
        if ("sodipodi:namedview".equals( childNode.getNodeName() )) {
          skip = true;
        }

        if (!skip) {
          // Copy this node over to the svgSvg element
          //
          Node childNodeCopy = domFactory.importNode( childNode, true );
          svgSvg.appendChild( childNodeCopy );
        }
      }

      gc.getDomGroupManager().addElement( svgSvg, DRAW );

    } catch(Exception e) {
      throw new RuntimeException("Error processing SVG image", e);
    }
  }

  @Override protected void drawImage( SwingUniversalImage image, int centerX, int centerY, double angle, int imageSize ) {
    super.drawImage( image, centerX, centerY, angle, imageSize );
  }
}
