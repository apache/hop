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

import static org.apache.batik.svggen.DOMGroupManager.DRAW;
import static org.apache.batik.svggen.DOMGroupManager.FILL;

import java.awt.Font;
import java.awt.font.TextLayout;
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.batik.anim.dom.SVGDOMImplementation;
import org.apache.batik.dom.GenericDOMImplementation;
import org.apache.batik.svggen.DOMGroupManager;
import org.apache.batik.svggen.DefaultStyleHandler;
import org.apache.batik.svggen.SVGGeneratorContext;
import org.apache.batik.svggen.SVGGraphics2D;
import org.apache.batik.util.SVGConstants;
import org.apache.hop.core.xml.XmlHandler;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HopSvgGraphics2D extends SVGGraphics2D {

  private static final String W3_URL = "http://www.w3.org/2000/xmlns/";

  /**
   * The font families a browser is asked to try, in order, for text drawn with a sans-serif font.
   * The canvas painter (SvgGc in the engine) measures text with the first of these the JVM has, so
   * the browser draws with the same family whenever it has it too.
   */
  public static final String[] SANS_SERIF_FAMILIES = {
    "Verdana", "DejaVu Sans", "Lucida Sans", "Lucida Grande", "Arial", "Helvetica"
  };

  private static final String SANS_SERIF_STACK = toFontStack(SANS_SERIF_FAMILIES, "sans-serif");
  private static final String MONOSPACE_STACK =
      toFontStack(
          new String[] {"DejaVu Sans Mono", "Menlo", "Consolas", "Lucida Console"}, "monospace");
  private static final String SERIF_STACK =
      toFontStack(new String[] {"Georgia", "Times New Roman"}, "serif");

  /** Batik's rendering of the AWT logical fonts, mapped to a family stack a browser can use. */
  private static final Map<String, String> LOGICAL_FONT_STACKS = new HashMap<>();

  static {
    LOGICAL_FONT_STACKS.put("'Dialog'", SANS_SERIF_STACK);
    LOGICAL_FONT_STACKS.put("'SansSerif'", SANS_SERIF_STACK);
    LOGICAL_FONT_STACKS.put("sans-serif", SANS_SERIF_STACK);
    LOGICAL_FONT_STACKS.put("'DialogInput'", MONOSPACE_STACK);
    LOGICAL_FONT_STACKS.put("'Monospaced'", MONOSPACE_STACK);
    LOGICAL_FONT_STACKS.put("monospace", MONOSPACE_STACK);
    LOGICAL_FONT_STACKS.put("'Serif'", SERIF_STACK);
    LOGICAL_FONT_STACKS.put("serif", SERIF_STACK);
  }

  private final DecimalFormat formater;

  public HopSvgGraphics2D(Document domFactory) {
    super(createGeneratorContext(domFactory), false);

    formater = new DecimalFormat("0.###", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
  }

  private static SVGGeneratorContext createGeneratorContext(Document domFactory) {
    SVGGeneratorContext context = SVGGeneratorContext.createDefault(domFactory);
    context.setStyleHandler(new WebFontStyleHandler());
    return context;
  }

  private static String toFontStack(String[] families, String generic) {
    StringBuilder stack = new StringBuilder();
    for (String family : families) {
      stack.append('\'').append(family).append("', ");
    }
    return stack.append(generic).toString();
  }

  /**
   * Batik writes the AWT font family into the SVG: for the logical fonts that is a name like
   * 'Dialog' which no browser has, so the browser falls back to its default (serif) font. Replace
   * it with a family stack the browser can honour, and give any other family the sans-serif stack
   * as its fallback.
   */
  private static class WebFontStyleHandler extends DefaultStyleHandler {
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setStyle(Element element, Map styleMap, SVGGeneratorContext generatorContext) {
      Object family = styleMap.get(SVGConstants.SVG_FONT_FAMILY_ATTRIBUTE);
      if (family instanceof String familyString) {
        Map webStyleMap = new HashMap(styleMap);
        webStyleMap.put(SVGConstants.SVG_FONT_FAMILY_ATTRIBUTE, toWebFontFamily(familyString));
        styleMap = webStyleMap;
      }
      super.setStyle(element, styleMap, generatorContext);
    }
  }

  /**
   * @param family the font family as Batik writes it, quoted for a concrete family ('Verdana') or
   *     bare for a generic one (sans-serif)
   * @return a font family stack for the browser
   */
  static String toWebFontFamily(String family) {
    String stack = LOGICAL_FONT_STACKS.get(family);
    if (stack != null) {
      return stack;
    }
    if (family.contains(",")) {
      return family; // already a stack
    }
    if (SANS_SERIF_STACK.contains(family + ",")) {
      return SANS_SERIF_STACK; // one of the stack's own families: keep the order as it is
    }
    return family + ", " + SANS_SERIF_STACK;
  }

  /**
   * The font to measure {@code text} with so that the measurement reflects what a browser draws.
   *
   * <p>Java 2D only substitutes glyphs a font lacks for the logical fonts, never for a physical one
   * like the canvas font: a CJK name measured with 'DejaVu Sans' on a JVM without a CJK font is a
   * row of missing-glyph boxes, each roughly half the width of the ideograph the browser draws from
   * its own fallback font (#8528). For such text the logical SansSerif font, which falls back
   * through the platform font configuration the way the browser does, gives a usable width.
   *
   * @param font the font the text is drawn with
   * @param text the text to measure
   * @return {@code font} when it has every glyph of {@code text}, otherwise a logical font of the
   *     same style and size that does, or {@code null} when this JVM has no font for the text at
   *     all
   */
  public static Font measuringFont(Font font, String text) {
    if (font == null || text == null || text.isEmpty() || font.canDisplayUpTo(text) == -1) {
      return font;
    }
    Font logical = new Font(Font.SANS_SERIF, font.getStyle(), font.getSize());
    if (font.getSize2D() != font.getSize()) {
      logical = logical.deriveFont(font.getSize2D());
    }
    return logical.canDisplayUpTo(text) == -1 ? logical : null;
  }

  /**
   * Draw the string like Batik does, but pin the run to the width this JVM measured for it. The
   * browser may not have the font the text was laid out with; with {@code textLength} it stretches
   * or squeezes the letter spacing so that the text still starts and ends where the painter put it,
   * under the borders, hover areas and hop labels that were sized for it.
   *
   * <p>The width comes from {@link #measuringFont(Font, String)}: text this JVM has no glyphs for
   * is not pinned at all, since the only width known for it, that of missing-glyph boxes, would
   * squeeze the browser's glyphs into an unreadable run. The browser's natural layout is then the
   * best width there is.
   */
  @Override
  public void drawString(String s, float x, float y) {
    if (s == null || s.isEmpty() || getFont() == null || getFont().isTransformed()) {
      super.drawString(s, x, y);
      return;
    }
    Font measuringFont = measuringFont(getFont(), s);
    if (measuringFont == null) {
      super.drawString(s, x, y);
      return;
    }
    double width = measuringFont.getStringBounds(s, getFontRenderContext()).getWidth();
    if (width <= 0) {
      super.drawString(s, x, y);
      return;
    }

    SVGGeneratorContext context = getGeneratorContext();
    Element text =
        getDOMFactory().createElementNS(SVGConstants.SVG_NAMESPACE_URI, SVGConstants.SVG_TEXT_TAG);
    text.setAttributeNS(null, SVGConstants.SVG_X_ATTRIBUTE, context.doubleString(x));
    text.setAttributeNS(null, SVGConstants.SVG_Y_ATTRIBUTE, context.doubleString(y));
    text.setAttributeNS(null, SVGConstants.SVG_TEXT_LENGTH_ATTRIBUTE, format(width));
    text.setAttributeNS(
        SVGConstants.XML_NAMESPACE_URI,
        SVGConstants.XML_SPACE_QNAME,
        SVGConstants.XML_PRESERVE_VALUE);
    text.appendChild(getDOMFactory().createTextNode(s));
    getDomGroupManager().addElement(text, FILL);
  }

  public DOMGroupManager getDomGroupManager() {
    return super.getDOMGroupManager();
  }

  @Override
  public void drawString(String str, int x, int y) {

    if (str.contains("\\n")) {

      String[] lines = str.split("\\n");
      int lineX = x;
      int lineY = y;
      for (String line : lines) {
        TextLayout tl = new TextLayout(line, getFont(), getFontRenderContext());
        drawString(line, lineX, lineY);
        lineY += tl.getBounds().getHeight() + tl.getDescent();
      }

    } else {
      super.drawString(str, x, y);
    }
  }

  public static HopSvgGraphics2D newDocument() {
    DOMImplementation domImplementation = GenericDOMImplementation.getDOMImplementation();

    // Create an instance of org.w3c.dom.Document.
    Document document =
        domImplementation.createDocument(SVGDOMImplementation.SVG_NAMESPACE_URI, "svg", null);

    return new HopSvgGraphics2D(document);
  }

  public String toXml() throws TransformerException {
    Transformer transformer = XmlHandler.createSecureTransformerFactory().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    StreamResult streamResult = new StreamResult(new StringWriter());
    DOMSource domSource = new DOMSource(getRoot());
    transformer.transform(domSource, streamResult);
    return streamResult.getWriter().toString();
  }

  private String format(double d) {
    return formater.format(d);
  }

  /**
   * Embed the given SVG from the given node into this SVG 2D
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
  public void embedSvg(
      Node svgNode,
      String filename,
      int x,
      int y,
      float width,
      float height,
      float xMagnification,
      float yMagnification,
      double angleDegrees) {

    Document domFactory = getDOMFactory();
    float centreX = width / 2;
    float centreY = height / 2;

    // Add a <g> group tag
    // Do the magnification, translation and rotation in that group
    //
    Element svgG =
        domFactory.createElementNS(SVGConstants.SVG_NAMESPACE_URI, SVGConstants.SVG_G_TAG);
    getDomGroupManager().addElement(svgG, (short) (DRAW | FILL));

    svgG.setAttributeNS(null, SVGConstants.SVG_STROKE_ATTRIBUTE, SVGConstants.SVG_NONE_VALUE);
    svgG.removeAttributeNS(null, SVGConstants.SVG_FILL_ATTRIBUTE);

    String transformString = "translate(" + x + " " + y + ") ";
    transformString += "scale(" + format(xMagnification) + " " + format(yMagnification) + ") ";
    transformString +=
        "rotate(" + format(angleDegrees) + " " + format(centreX) + " " + format(centreY) + ")";
    svgG.setAttributeNS(null, SVGConstants.SVG_TRANSFORM_ATTRIBUTE, transformString);

    if (filename != null) {
      // Just informational
      svgG.setAttributeNS(null, "filename", filename);
    }

    svgG.setAttributeNS(W3_URL, "xmlns:dc", "http://purl.org/dc/elements/1.1/");
    svgG.setAttributeNS(W3_URL, "xmlns:cc", "http://creativecommons.org/ns#");
    svgG.setAttributeNS(W3_URL, "xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    svgG.setAttributeNS(
        W3_URL, "xmlns:sodipodi", "http://sodipodi.sourceforge.net/DTD/sodipodi-0.dtd");
    svgG.setAttributeNS(W3_URL, "xmlns:inkscape", "http://www.inkscape.org/namespaces/inkscape");

    // Add all the elements from the SVG Image...
    //
    copyChildren(domFactory, svgG, svgNode);
  }

  private void copyChildren(Document domFactory, Node target, Node svgImage) {

    NodeList childNodes = svgImage.getChildNodes();
    for (int c = 0; c < childNodes.getLength(); c++) {
      Node childNode = childNodes.item(c);

      // Copy this node over to the svgSvg element
      //
      Node childNodeCopy = domFactory.importNode(childNode, true);
      target.appendChild(childNodeCopy);
    }
  }
}
