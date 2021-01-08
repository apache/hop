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

import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.bridge.BridgeContext;
import org.apache.batik.bridge.DocumentLoader;
import org.apache.batik.bridge.GVTBuilder;
import org.apache.batik.bridge.UserAgent;
import org.apache.batik.bridge.UserAgentAdapter;
import org.apache.batik.gvt.GraphicsNode;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.w3c.dom.svg.SVGDocument;
import org.w3c.dom.svg.SVGSVGElement;

import java.awt.geom.Rectangle2D;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class SvgCache {
  private static SvgCache instance;

  private Map<String, SvgCacheEntry> fileDocumentMap;

  private SvgCache() {
    fileDocumentMap = new HashMap<>();
  }

  /**
   * Gets instance
   *
   * @return value of instance
   */
  public static SvgCache getInstance() {
    if ( instance == null ) {
      instance = new SvgCache();
    }
    return instance;
  }

  public synchronized static SvgCacheEntry findSvg( String filename ) {
    return getInstance().fileDocumentMap.get( filename );
  }

  public synchronized static SvgCacheEntry loadSvg( SvgFile svgFile ) throws HopException {

    SvgCacheEntry cacheEntry = findSvg( svgFile.getFilename() );
    if (cacheEntry!=null) {
      return cacheEntry;
    }

    try {
      String parser = XMLResourceDescriptor.getXMLParserClassName();
      SAXSVGDocumentFactory factory = new SAXSVGDocumentFactory( parser );
      InputStream svgStream = svgFile.getClassLoader().getResourceAsStream( svgFile.getFilename() );

      if ( svgStream == null ) {
        throw new HopException( "Unable to find file '" + svgFile.getFilename() + "'" );
      }
      SVGDocument svgDocument = factory.createSVGDocument( svgFile.getFilename(), svgStream );
      SVGSVGElement elSVG = svgDocument.getRootElement();

      float width=-1;
      float height=-1;
      float x = 0;
      float y = 0;

      // See if the element has a "width" and a "height" attribute...
      //
      String widthAttribute = elSVG.getAttribute( "width" );
      String heightAttribute = elSVG.getAttribute( "height" );
      if (widthAttribute!=null && heightAttribute!=null) {
        width = (float) Const.toDouble( widthAttribute.replace( "px", "" ).replace("mm", ""), -1.0d );
        height = (float) Const.toDouble( heightAttribute.replace( "px", "" ).replace("mm", ""), -1.0d );
      }
      String xAttribute = elSVG.getAttribute( "x" );
      String yAttribute = elSVG.getAttribute( "y" );
      if (xAttribute!=null && yAttribute!=null) {
        x = (float) Const.toDouble( xAttribute.replace( "px", "" ).replace("mm", ""), 0d );
        y = (float) Const.toDouble( yAttribute.replace( "px", "" ).replace("mm", ""), 0d );
      }

      // If we don't have width and height we'll have to calculate it...
      //
      if (width<=1 || height<=1) {
        // Figure out the primitives bounds...
        //
        UserAgent agent = new UserAgentAdapter();
        DocumentLoader loader= new DocumentLoader(agent);
        BridgeContext context = new BridgeContext(agent, loader);
        context.setDynamic(true);
        GVTBuilder builder= new GVTBuilder();
        GraphicsNode root= builder.build(context, svgDocument);

        // We need to go through the document to figure it out unfortunately.
        // It is slower but should always work.
        //
        Rectangle2D primitiveBounds = root.getPrimitiveBounds();

        width = (float) primitiveBounds.getWidth();
        height = (float) primitiveBounds.getHeight();
        x = (float) primitiveBounds.getX();
        y = (float) primitiveBounds.getY();
      }

      if (width<=1 || height<=1) {
        throw new HopException("Couldn't determine width or height of file : "+ svgFile.getFilename());
      }

      cacheEntry = new SvgCacheEntry( svgFile.getFilename(), svgDocument, Math.round(width), Math.round(height), Math.round( x ), Math.round( y ) );
      getInstance().fileDocumentMap.put( svgFile.getFilename(), cacheEntry );
      return cacheEntry;
    } catch ( Exception e ) {
      throw new HopException( "Error loading SVG file " + svgFile.getFilename(), e );
    }
  }

  public synchronized static void addSvg( String filename, SVGDocument svgDocument, int width, int height, int x, int y ) {
    getInstance().fileDocumentMap.put( filename, new SvgCacheEntry( filename, svgDocument, width, height, x, y ) );
  }

}
