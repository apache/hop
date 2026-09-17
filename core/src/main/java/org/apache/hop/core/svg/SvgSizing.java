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

import java.math.BigDecimal;
import java.util.Locale;
import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.dom.util.DOMUtilities;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/** Serialises an SVG so that a browser draws it at an exact pixel size. */
public final class SvgSizing {

  private SvgSizing() {}

  /**
   * XML of a copy of {@code document} whose root asks for {@code width} x {@code height} CSS
   * pixels. Most Hop icons carry {@code width}/{@code height} but no {@code viewBox}; without one a
   * browser would clip the drawing to the new size instead of scaling it, so the intrinsic size
   * becomes the viewBox. The aspect ratio is not preserved, which is what the raster renderer in
   * {@code SwingUniversalImageSvg#render} does as well.
   *
   * <p>The copy is served from the web application's own origin, and a project SVG file is not
   * necessarily an icon someone vetted, so script elements, event-handler attributes, {@code
   * foreignObject} and script URLs are dropped: nothing in a picture may run.
   *
   * @param document the SVG, left untouched
   * @param intrinsicWidth the width the drawing was authored for, in user units
   * @param intrinsicHeight the height the drawing was authored for, in user units
   * @param width the requested width in CSS pixels
   * @param height the requested height in CSS pixels
   * @return the sized SVG as XML, with declaration
   * @throws HopException when the copy cannot be serialised
   */
  public static String toXml(
      Document document, double intrinsicWidth, double intrinsicHeight, int width, int height)
      throws HopException {
    Document sized =
        DOMUtilities.deepCloneDocument(document, SVGDOMImplementation.getDOMImplementation());
    Element root = sized.getDocumentElement();
    if (!root.hasAttribute("viewBox")) {
      root.setAttribute("viewBox", "0 0 " + plain(intrinsicWidth) + " " + plain(intrinsicHeight));
    }
    root.setAttribute("width", String.valueOf(width));
    root.setAttribute("height", String.valueOf(height));
    root.setAttribute("preserveAspectRatio", "none");
    stripActiveContent(root);
    return XmlHandler.getXmlString(sized, false, false);
  }

  /** Removes everything a browser could execute from {@code element} and its descendants. */
  static void stripActiveContent(Element element) {
    Node child = element.getFirstChild();
    while (child != null) {
      Node next = child.getNextSibling();
      if (child instanceof Element childElement) {
        String name = childElement.getLocalName();
        if ("script".equalsIgnoreCase(name) || "foreignObject".equalsIgnoreCase(name)) {
          element.removeChild(child);
        } else {
          stripActiveContent(childElement);
        }
      }
      child = next;
    }
    NamedNodeMap attributes = element.getAttributes();
    for (int i = attributes.getLength() - 1; i >= 0; i--) {
      Attr attribute = (Attr) attributes.item(i);
      String name =
          attribute.getLocalName() == null ? attribute.getName() : attribute.getLocalName();
      String value = attribute.getValue().trim().toLowerCase(Locale.ROOT);
      boolean handler = name.regionMatches(true, 0, "on", 0, 2);
      boolean scriptUrl =
          "href".equalsIgnoreCase(name)
              && (value.startsWith("javascript:")
                  || (value.startsWith("data:") && !value.startsWith("data:image/")));
      if (handler || scriptUrl) {
        element.removeAttributeNode(attribute);
      }
    }
  }

  /** {@code 24.0} as {@code 24}, {@code 23.5} as {@code 23.5}: no exponent, no trailing zeros. */
  private static String plain(double value) {
    return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
  }
}
