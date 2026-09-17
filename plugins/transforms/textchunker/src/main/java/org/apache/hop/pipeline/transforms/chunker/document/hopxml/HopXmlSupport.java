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
package org.apache.hop.pipeline.transforms.chunker.document.hopxml;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hop.pipeline.transforms.chunker.document.SecretRedaction;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/** DOM helpers for Hop .hpl / .hwf XML. */
public final class HopXmlSupport {

  static final Set<String> SKIP_TAGS =
      Set.of(
          "GUI",
          "partitioning",
          "attributes",
          "attributes_hac",
          "custom_distribution",
          "transform_error_handling",
          "key_for_session_key",
          "is_key_private",
          "created_user",
          "created_date",
          "modified_user",
          "modified_date",
          "pipeline_version",
          "workflow_version",
          "capture_transform_performance",
          "transform_performance_capturing_delay",
          "transform_performance_capturing_size_limit",
          "name_sync_with_filename",
          "pipeline_status",
          "xloc",
          "yloc",
          "width",
          "heigth",
          "fontname",
          "fontsize",
          "fontbold",
          "fontitalic",
          "fontcolorred",
          "fontcolorgreen",
          "fontcolorblue",
          "backgroundcolorred",
          "backgroundcolorgreen",
          "backgroundcolorblue",
          "bordercolorred",
          "bordercolorgreen",
          "bordercolorblue");

  private static final int DEFAULT_MAX_CONFIG_CHARS = 6000;

  /** Matches XML attributes with double-quoted values (e.g. {@code value="a<b"}). */
  private static final Pattern DOUBLE_QUOTED_ATTR =
      Pattern.compile("(\\s(?:[\\w:-]+))=\"([^\"]*)\"");

  private HopXmlSupport() {}

  public static boolean looksLikeHopXml(String text) {
    if (text == null || text.isBlank()) {
      return false;
    }
    String trimmed = text.stripLeading();
    return trimmed.contains("<pipeline") || trimmed.contains("<workflow");
  }

  /**
   * Parse Hop XML, returning {@code null} when the document cannot be parsed even after sanitizing
   * unescaped {@code <} / {@code >} in attribute values (a common issue in real .hpl/.hwf files).
   */
  public static Document parseDocument(String xml) {
    if (xml == null || xml.isBlank()) {
      return null;
    }
    Document document = parseStrict(xml);
    if (document != null) {
      return document;
    }
    return parseStrict(sanitizeDoubleQuotedAttributes(xml));
  }

  /**
   * @deprecated Prefer {@link #parseDocument(String)} which does not throw.
   */
  public static Document parse(String xml) {
    Document document = parseDocument(xml);
    if (document == null) {
      throw new IllegalArgumentException("Invalid Hop XML");
    }
    return document;
  }

  static String sanitizeDoubleQuotedAttributes(String xml) {
    Matcher matcher = DOUBLE_QUOTED_ATTR.matcher(xml);
    StringBuilder result = new StringBuilder(xml.length() + 32);
    int last = 0;
    while (matcher.find()) {
      result.append(xml, last, matcher.start());
      String name = matcher.group(1);
      String value = matcher.group(2);
      if (value.indexOf('<') >= 0 || value.indexOf('>') >= 0) {
        value = escapeRawLtGtInAttributeValue(value);
      }
      result.append(name).append("=\"").append(value).append('"');
      last = matcher.end();
    }
    result.append(xml, last, xml.length());
    return result.toString();
  }

  private static String escapeRawLtGtInAttributeValue(String value) {
    return value.replace("<", "&lt;").replace(">", "&gt;");
  }

  private static Document parseStrict(String xml) {
    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
      factory.setExpandEntityReferences(false);
      factory.setNamespaceAware(false);
      DocumentBuilder builder = factory.newDocumentBuilder();
      return builder.parse(new InputSource(new StringReader(xml)));
    } catch (Exception e) {
      return null;
    }
  }

  public static Element firstChild(Element parent, String tagName) {
    if (parent == null) {
      return null;
    }
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node.getNodeType() == Node.ELEMENT_NODE && tagName.equals(node.getNodeName())) {
        return (Element) node;
      }
    }
    return null;
  }

  public static List<Element> children(Element parent, String tagName) {
    List<Element> result = new ArrayList<>();
    if (parent == null) {
      return result;
    }
    for (Node node = parent.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node.getNodeType() == Node.ELEMENT_NODE && tagName.equals(node.getNodeName())) {
        result.add((Element) node);
      }
    }
    return result;
  }

  public static String directChildText(Element parent, String tagName) {
    Element child = firstChild(parent, tagName);
    return child != null ? textContent(child).trim() : "";
  }

  public static String textContent(Element element) {
    StringBuilder sb = new StringBuilder();
    appendText(element, sb);
    return sb.toString();
  }

  private static void appendText(Node node, StringBuilder sb) {
    if (node.getNodeType() == Node.TEXT_NODE || node.getNodeType() == Node.CDATA_SECTION_NODE) {
      sb.append(node.getNodeValue());
      return;
    }
    for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
      appendText(child, sb);
    }
  }

  public static String formatParameters(Element parametersParent) {
    if (parametersParent == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Element parameter : children(parametersParent, "parameter")) {
      String name = directChildText(parameter, "name");
      if (name.isEmpty()) {
        continue;
      }
      String defaultValue = directChildText(parameter, "default_value");
      String description = directChildText(parameter, "description");
      sb.append("- ").append(name).append(": default=").append(defaultValue);
      if (!description.isEmpty()) {
        sb.append(" — ").append(description);
      }
      sb.append('\n');
    }
    return sb.toString().strip();
  }

  public static String formatNotepads(Element root, String notepadsTag) {
    Element notepads = firstChild(root, notepadsTag);
    if (notepads == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (Element notepad : children(notepads, "notepad")) {
      String note = directChildText(notepad, "note");
      if (!note.isEmpty()) {
        if (sb.length() > 0) {
          sb.append("\n\n");
        }
        sb.append(note.trim());
      }
    }
    return sb.toString().strip();
  }

  public static String serializeConfig(Element element) {
    return serializeConfig(element, DEFAULT_MAX_CONFIG_CHARS);
  }

  public static String serializeConfig(Element element, int maxChars) {
    StringBuilder sb = new StringBuilder();
    serializeElement(element, sb, 0, 4);
    String text = sb.toString().strip();
    if (text.length() > maxChars) {
      return text.substring(0, maxChars) + "\n...";
    }
    return text;
  }

  private static void serializeElement(Element element, StringBuilder sb, int depth, int maxDepth) {
    if (depth > maxDepth || SKIP_TAGS.contains(element.getTagName())) {
      return;
    }

    String tag = element.getTagName();
    List<Element> childElements = new ArrayList<>();
    StringBuilder textOnly = new StringBuilder();
    for (Node node = element.getFirstChild(); node != null; node = node.getNextSibling()) {
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        childElements.add((Element) node);
      } else if (node.getNodeType() == Node.TEXT_NODE
          || node.getNodeType() == Node.CDATA_SECTION_NODE) {
        textOnly.append(node.getNodeValue());
      }
    }

    String text = SecretRedaction.redact(tag, textOnly.toString().trim());
    if (childElements.isEmpty()) {
      if (!text.isEmpty() && !SKIP_TAGS.contains(tag)) {
        indent(sb, depth).append(tag).append(": ").append(text).append('\n');
      }
      return;
    }

    if (childElements.size() == 1 && text.isEmpty()) {
      indent(sb, depth).append(tag).append(":\n");
      serializeElement(childElements.get(0), sb, depth + 1, maxDepth);
      return;
    }

    indent(sb, depth).append(tag).append(":\n");
    if (!text.isEmpty()) {
      indent(sb, depth + 1).append(text).append('\n');
    }
    for (Element child : childElements) {
      serializeElement(child, sb, depth + 1, maxDepth);
    }
  }

  private static StringBuilder indent(StringBuilder sb, int depth) {
    sb.append("  ".repeat(Math.max(0, depth)));
    return sb;
  }

  public static List<Element> elementsByTag(NodeList nodeList) {
    List<Element> elements = new ArrayList<>();
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        elements.add((Element) node);
      }
    }
    return elements;
  }
}
