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

package org.apache.hop.core;

import org.apache.batik.bridge.BridgeContext;
import org.apache.batik.bridge.DocumentLoader;
import org.apache.batik.bridge.GVTBuilder;
import org.apache.batik.bridge.UserAgentAdapter;
import org.apache.batik.gvt.GraphicsNode;
import org.apache.hop.core.svg.SvgImage;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Dimension2D;
import java.awt.image.BufferedImage;

public class SwingUniversalImageSvg extends SwingUniversalImage {
  private final SvgImage svg;
  private final GraphicsNode svgGraphicsNode;
  private final Dimension2D svgGraphicsSize;

  public SwingUniversalImageSvg( SvgImage svg ) {
    this.svg = svg;

    // get GraphicsNode and size from svg document
    UserAgentAdapter userAgentAdapter = new UserAgentAdapter();
    DocumentLoader documentLoader = new DocumentLoader( userAgentAdapter );
    BridgeContext ctx = new BridgeContext( userAgentAdapter, documentLoader );
    GVTBuilder builder = new GVTBuilder();
    svgGraphicsNode = builder.build( ctx, svg.getDocument() );
    svgGraphicsSize = ctx.getDocumentSize();
  }

  @Override
  public boolean isBitmap() {
    return false;
  }

  @Override
  protected void renderSimple( BufferedImage area ) {
    Graphics2D gc = createGraphics( area );

    render( gc, area.getWidth() / 2, area.getHeight() / 2, area.getWidth(), area.getHeight(), 0 );

    gc.dispose();
  }

  /**
   * Draw SVG image to Graphics2D.
   */
  @Override
  protected void render( Graphics2D gc, int centerX, int centerY, int width, int height, double angleRadians ) {
    render( gc, svgGraphicsNode, svgGraphicsSize, centerX, centerY, width, height, angleRadians );
  }

  public static void render( Graphics2D gc, GraphicsNode svgGraphicsNode, Dimension2D svgGraphicsSize, int centerX,
                             int centerY, int width, int height, double angleRadians ) {
    double scaleX = width / svgGraphicsSize.getWidth();
    double scaleY = height / svgGraphicsSize.getHeight();

    AffineTransform affineTransform = new AffineTransform();
    if ( centerX != 0 || centerY != 0 ) {
      affineTransform.translate( centerX, centerY );
    }
    affineTransform.scale( scaleX, scaleY );
    if ( angleRadians != 0 ) {
      affineTransform.rotate( angleRadians );
    }
    affineTransform.translate( -svgGraphicsSize.getWidth() / 2, -svgGraphicsSize.getHeight() / 2 );

    svgGraphicsNode.setTransform( affineTransform );

    svgGraphicsNode.paint( gc );
  }

  /**
   * Gets svg
   *
   * @return value of svg
   */
  public SvgImage getSvg() {
    return svg;
  }

  /**
   * Gets svgGraphicsNode
   *
   * @return value of svgGraphicsNode
   */
  public GraphicsNode getSvgGraphicsNode() {
    return svgGraphicsNode;
  }

  /**
   * Gets svgGraphicsSize
   *
   * @return value of svgGraphicsSize
   */
  public Dimension2D getSvgGraphicsSize() {
    return svgGraphicsSize;
  }
}
