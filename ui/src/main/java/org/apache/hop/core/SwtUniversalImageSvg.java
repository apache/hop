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

import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.bridge.BridgeContext;
import org.apache.batik.bridge.DocumentLoader;
import org.apache.batik.bridge.GVTBuilder;
import org.apache.batik.bridge.UserAgentAdapter;
import org.apache.batik.dom.util.DOMUtilities;
import org.apache.batik.ext.awt.image.codec.png.PNGRegistryEntry;
import org.apache.batik.ext.awt.image.spi.ImageTagRegistry;
import org.apache.batik.gvt.GraphicsNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.ui.core.PropsUi;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Display;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.svg.SVGDocument;
import org.w3c.dom.svg.SVGSVGElement;

import java.awt.*;
import java.awt.geom.Dimension2D;
import java.awt.image.BufferedImage;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SwtUniversalImageSvg extends SwtUniversalImage {
  private final GraphicsNode svgGraphicsNode;
  private final Dimension2D svgGraphicsSize;

  static {
    // workaround due to known issue in batik 1.8 - https://issues.apache.org/jira/browse/BATIK-1125
    ImageTagRegistry registry = ImageTagRegistry.getRegistry();
    registry.register(new PNGRegistryEntry());
  }

  public SwtUniversalImageSvg(SvgImage svg) {
    UserAgentAdapter userAgentAdapter = new UserAgentAdapter();
    GVTBuilder builder = new GVTBuilder();

    if (PropsUi.getInstance().isDarkMode()) {
      DOMImplementation domImplementation = SVGDOMImplementation.getDOMImplementation();
      SVGDocument clonedDocument =
          (SVGDocument) DOMUtilities.deepCloneDocument(svg.getDocument(), domImplementation);
      SVGSVGElement root = clonedDocument.getRootElement();

      Map<String, String> colorsMap = PropsUi.getInstance().getContrastingColorStrings();
      List<String> tags = Arrays.asList("path", "fill", "bordercolor", "fillcolor", "style", "text", "polygon", "rect");

      contrastColors(root, tags, colorsMap);

      BridgeContext ctx = new BridgeContext(userAgentAdapter);
      ctx.setDynamic(true);

      svgGraphicsNode = builder.build(ctx, clonedDocument);
      svgGraphicsSize = ctx.getDocumentSize();
    } else {
      // get GraphicsNode and size from svg document
      DocumentLoader documentLoader = new DocumentLoader(userAgentAdapter);
      BridgeContext ctx = new BridgeContext(userAgentAdapter, documentLoader);
      svgGraphicsNode = builder.build(ctx, svg.getDocument());
      svgGraphicsSize = ctx.getDocumentSize();
    }
  }

  private void contrastColors(
      SVGSVGElement root, List<String> tags, Map<String, String> colorsMap) {
    for (String tag : tags) {

      NodeList nodeList = root.getElementsByTagName(tag);

      for (int i = 0; i < nodeList.getLength(); i++) {
        Node node = nodeList.item(i);
        NamedNodeMap namedNodeMap = node.getAttributes();
        for (int x = 0; x < namedNodeMap.getLength(); x++) {
          Node namedNode = namedNodeMap.item(x);
          String value = namedNode.getNodeValue();

          boolean changed = false;

          if (StringUtils.isNotEmpty(value)) {
            String changedValue = value.toLowerCase();

            for (String oldColor : colorsMap.keySet()) {
              if (changedValue.contains(oldColor)) {
                String newColor = colorsMap.get(oldColor);
                changedValue = changedValue.replace(oldColor, newColor);
                changed = true;
              }
            }
            if (changed) {
              namedNode.setNodeValue(changedValue);
            }
          }
        }
      }
    }
  }

  @Override
  protected Image renderSimple(Device device) {
    return renderSimple(
        device,
        (int) Math.round(svgGraphicsSize.getWidth()),
        (int) Math.round(svgGraphicsSize.getHeight()));
  }

  @Override
  protected Image renderSimple(Device device, int width, int height) {
    BufferedImage area = SwingUniversalImage.createBitmap(width, height);
    Graphics2D gc = SwingUniversalImage.createGraphics(area);
    SwingUniversalImageSvg.render(
        gc, svgGraphicsNode, svgGraphicsSize, width / 2, height / 2, width, height, 0);
    gc.dispose();

    return swing2swt(device, area);
  }

  @Override
  protected Image renderRotated(Device device, int width, int height, double angleRadians) {
    BufferedImage doubleArea = SwingUniversalImage.createDoubleBitmap(width, height);

    Graphics2D gc = SwingUniversalImage.createGraphics(doubleArea);
    SwingUniversalImageSvg.render(
        gc,
        svgGraphicsNode,
        svgGraphicsSize,
        doubleArea.getWidth() / 2,
        doubleArea.getHeight() / 2,
        width,
        height,
        angleRadians);

    gc.dispose();

    return swing2swt(device, doubleArea);
  }
}
