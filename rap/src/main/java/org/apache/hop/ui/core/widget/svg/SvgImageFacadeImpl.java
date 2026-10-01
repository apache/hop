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

package org.apache.hop.ui.core.widget.svg;

import java.awt.geom.Dimension2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.apache.batik.anim.dom.SAXSVGDocumentFactory;
import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.util.XMLResourceDescriptor;
import org.apache.hop.core.SwingUniversalImageSvg;
import org.apache.hop.core.SwtUniversalImage;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.svg.SvgImage;
import org.apache.hop.core.svg.SvgSizing;
import org.apache.hop.core.xml.XmlHandler;
import org.eclipse.rap.rwt.RWT;
import org.eclipse.rap.rwt.internal.resources.ResourceDirectory;
import org.eclipse.rap.rwt.service.ResourceManager;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.internal.graphics.ExternalImageDescriptor;
import org.eclipse.swt.internal.graphics.ImageFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Publishes the sized SVG as an application resource and returns an {@link Image} whose client URL
 * is that resource while its bounds stay the logical size. The browser then draws the vector at
 * device resolution instead of scaling up a 1x bitmap.
 *
 * <p>The resource name is a digest of the sized XML, so one file serves every session, size and
 * theme (the dark-mode colour swap is already in the document) and never needs invalidating. The
 * registered file is also where such an image's pixels come from when a caller needs them ({@link
 * #rasterizeInternal}) and what a badge overlay is built from ({@link #overlayInternal}): nothing
 * has to be remembered per image.
 */
public class SvgImageFacadeImpl extends SvgImageFacade {

  static final String RESOURCE_PREFIX = "hop/svg/";
  private static final String LOCATION_PREFIX = ResourceDirectory.DIRNAME + "/";

  /** RWT's register() is not atomic; two sessions publishing the same icon must not interleave. */
  private static final Object REGISTER_LOCK = new Object();

  @Override
  protected Image createImageInternal(
      Device device, Document document, Dimension2D intrinsicSize, int width, int height) {
    try {
      String svg =
          SvgSizing.toXml(
              document, intrinsicSize.getWidth(), intrinsicSize.getHeight(), width, height);
      return publish(device, svg, width, height);
    } catch (Exception e) {
      // Typically no RAP context on this thread; the caller falls back to a bitmap.
      LogChannel.UI.logDebug("Falling back to a bitmap for an SVG icon: " + e.getMessage());
      return null;
    }
  }

  @Override
  protected ImageData rasterizeInternal(Image image, int zoom) {
    try {
      Document document = publishedSvg(image);
      if (document == null) {
        return null;
      }
      Rectangle bounds = image.getBounds();
      BufferedImage bitmap =
          new SwingUniversalImageSvg(new SvgImage(document))
              .getAsBitmapForSize(
                  SwtUniversalImage.pixelSize(bounds.width, zoom),
                  SwtUniversalImage.pixelSize(bounds.height, zoom));
      return SwtUniversalImage.toImageData(bitmap);
    } catch (Exception e) {
      LogChannel.UI.logDebug("Could not rasterise a vector icon: " + e.getMessage());
      return null;
    }
  }

  @Override
  protected Image overlayInternal(
      Device device, Image base, Image badge, int badgeSize, int margin) {
    try {
      Document baseDocument = publishedSvg(base);
      Document badgeDocument = publishedSvg(badge);
      if (baseDocument == null || badgeDocument == null) {
        return null;
      }
      Rectangle bounds = base.getBounds();
      // Nested <svg> elements keep each icon's own viewBox, so both scale exactly as they do alone.
      Document composite =
          SVGDOMImplementation.getDOMImplementation()
              .createDocument(SVGDOMImplementation.SVG_NAMESPACE_URI, "svg", null);
      Element root = composite.getDocumentElement();
      root.setAttribute("width", String.valueOf(bounds.width));
      root.setAttribute("height", String.valueOf(bounds.height));
      root.setAttribute("viewBox", "0 0 " + bounds.width + " " + bounds.height);
      root.appendChild(composite.importNode(baseDocument.getDocumentElement(), true));
      Element badgeElement =
          (Element) composite.importNode(badgeDocument.getDocumentElement(), true);
      badgeElement.setAttribute("x", String.valueOf(bounds.width - badgeSize - margin));
      badgeElement.setAttribute("y", String.valueOf(bounds.height - badgeSize - margin));
      badgeElement.setAttribute("width", String.valueOf(badgeSize));
      badgeElement.setAttribute("height", String.valueOf(badgeSize));
      root.appendChild(badgeElement);
      return publish(
          device, XmlHandler.getXmlString(composite, false, false), bounds.width, bounds.height);
    } catch (Exception e) {
      LogChannel.UI.logDebug("Could not overlay a vector icon: " + e.getMessage());
      return null;
    }
  }

  /** Registers the XML once under its digest and returns an image of the given bounds for it. */
  private static Image publish(Device device, String svg, int width, int height) throws Exception {
    byte[] bytes = svg.getBytes(StandardCharsets.UTF_8);
    String name = RESOURCE_PREFIX + digest(bytes) + ".svg";
    ResourceManager resourceManager = RWT.getResourceManager();
    synchronized (REGISTER_LOCK) {
      if (!resourceManager.isRegistered(name)) {
        resourceManager.register(name, new ByteArrayInputStream(bytes));
      }
    }
    String location = resourceManager.getLocation(name);
    return new ExternalImageDescriptor(location, width, height).createImage(device);
  }

  /** The registered SVG behind an image made by {@link #publish}, or null for any other image. */
  private static Document publishedSvg(Image image) throws Exception {
    if (image == null || image.isDisposed()) {
      return null;
    }
    String path = ImageFactory.getImagePath(image);
    if (path == null || !path.startsWith(LOCATION_PREFIX + RESOURCE_PREFIX)) {
      return null;
    }
    String name = path.substring(LOCATION_PREFIX.length());
    try (InputStream stream = RWT.getResourceManager().getRegisteredContent(name)) {
      if (stream == null) {
        return null;
      }
      return new SAXSVGDocumentFactory(XMLResourceDescriptor.getXMLParserClassName())
          .createDocument(name, stream);
    }
  }

  private static String digest(byte[] content) throws Exception {
    byte[] hash = MessageDigest.getInstance("SHA-256").digest(content);
    return HexFormat.of().formatHex(hash, 0, 16);
  }
}
