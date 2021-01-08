/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.svg;

import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.DOMGroupManager;
import org.apache.batik.svggen.ExtensionHandler;
import org.apache.batik.svggen.ImageHandler;
import org.apache.batik.svggen.SVGGeneratorContext;
import org.apache.batik.svggen.SVGGraphics2D;
import org.apache.batik.util.SVGConstants;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.awt.font.TextLayout;
import java.io.StringWriter;
import java.text.DecimalFormat;

import static org.apache.batik.svggen.DOMGroupManager.DRAW;
import static org.apache.batik.svggen.DOMGroupManager.FILL;

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

  private String format(double d) {
    return new DecimalFormat("0.###").format( d );
  }

  /**
   *  Embed the given SVG from the given node into this SVG 2D
   *
   * @param svgNode The source SVG node which is copied
   * @param filename The filename will be added as information (not if null)
   * @param x The x location to translate to
   * @param y The y location to translate to
   * @param width The width of the SVG to embed.
   * @param height The height of the SVG to embed
   * @param xMagnification The horizontal magnification
   * @param yMagnification The vertical magnification
   * @param angleDegrees The rotation angle in degrees (not radians)
   */
  public void embedSvg( Node svgNode, String filename, int x, int y, float width, float height, float xMagnification, float yMagnification, double angleDegrees ) {

    Document domFactory = getDOMFactory();
    float centreX = width / 2;
    float centreY = height / 2;

    // Add a <g> group tag
    // Do the magnification, translation and rotation in that group
    //
    Element svgG = domFactory.createElementNS( SVGConstants.SVG_NAMESPACE_URI, SVGConstants.SVG_G_TAG );
    getDomGroupManager().addElement( svgG, (short) (DRAW | FILL) );

    svgG.setAttributeNS( null, SVGConstants.SVG_STROKE_ATTRIBUTE, SVGConstants.SVG_NONE_VALUE );
    svgG.removeAttributeNS( null, SVGConstants.SVG_FILL_ATTRIBUTE );

    String transformString = "translate(" + x + " " + y + ") ";
    transformString += "scale(" + format(xMagnification) + " " + format(yMagnification) + ") ";
    transformString += "rotate(" + format(angleDegrees) + " " + format(centreX) + " " + format(centreY) + ")";
    svgG.setAttributeNS( null, SVGConstants.SVG_TRANSFORM_ATTRIBUTE, transformString );

    if (filename!=null) {
      // Just informational
      svgG.setAttributeNS( null, "filename", filename );
    }

    svgG.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:dc", "http://purl.org/dc/elements/1.1/");
    svgG.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:cc", "http://creativecommons.org/ns#" );
    svgG.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#" );
    svgG.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:sodipodi", "http://sodipodi.sourceforge.net/DTD/sodipodi-0.dtd" );
    svgG.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:inkscape", "http://www.inkscape.org/namespaces/inkscape" );

    // Add all the elements from the SVG Image...
    //
    copyChildren( domFactory, svgG, svgNode );
  }

  private void copyChildren( Document domFactory, Node target, Node svgImage ) {

    NodeList childNodes = svgImage.getChildNodes();
    for ( int c = 0; c < childNodes.getLength(); c++ ) {
      Node childNode = childNodes.item( c );

      /*
      if ( "metadata".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }
      if ( "defs".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }
      if ( "sodipodi:namedview".equals( childNode.getNodeName() ) ) {
        continue; // skip some junk
      }
       */

      // Copy this node over to the svgSvg element
      //
      Node childNodeCopy = domFactory.importNode( childNode, true );
      target.appendChild( childNodeCopy );
    }
  }
}
