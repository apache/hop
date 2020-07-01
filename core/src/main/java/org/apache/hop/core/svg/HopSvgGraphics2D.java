package org.apache.hop.core.svg;

import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.DOMGroupManager;
import org.apache.batik.svggen.ExtensionHandler;
import org.apache.batik.svggen.ImageHandler;
import org.apache.batik.svggen.SVGGeneratorContext;
import org.apache.batik.svggen.SVGGraphics2D;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.awt.font.TextLayout;
import java.io.StringWriter;

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

  public static HopSvgGraphics2D newDocument() {
    DOMImplementation domImplementation = GenericDOMImplementation.getDOMImplementation();

    // Create an instance of org.w3c.dom.Document.
    Document document = domImplementation.createDocument( SVGDOMImplementation.SVG_NAMESPACE_URI, "svg", null);

    HopSvgGraphics2D graphics2D = new HopSvgGraphics2D(document);

    return graphics2D;
  }

  public String toXml() throws TransformerException {
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty( OutputKeys.INDENT, "yes" );
    transformer.setOutputProperty( "{http://xml.apache.org/xslt}indent-amount", "2" );
    StreamResult streamResult = new StreamResult( new StringWriter() );
    DOMSource domSource = new DOMSource( getRoot() );
    transformer.transform( domSource, streamResult );
    return streamResult.getWriter().toString();
  }

}
