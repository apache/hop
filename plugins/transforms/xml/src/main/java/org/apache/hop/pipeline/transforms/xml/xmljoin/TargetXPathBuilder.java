/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.xml.xmljoin;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Writes the XPath statement of an XML join for a node picked in a sample target document.
 *
 * <p>The statement starts at the root, so it cannot find a node with the same name elsewhere. When
 * every element goes under one node, a step gets a position when its siblings share its name. When
 * every element goes under the node of its key, the element that holds the key gets {@code
 * [@key='?']} or {@code [key='?']} instead, and the steps above it get no position: the key alone
 * picks the node. The transform parses the target without namespaces, so a prefixed name is matched
 * on its full name.
 */
public final class TargetXPathBuilder {

  /** The placeholder the transform replaces by the key value of each source row. */
  public static final String KEY_PLACEHOLDER = "?";

  private TargetXPathBuilder() {}

  /**
   * A value in the target document that tells one node from the others: an attribute, or a child
   * element holding only text, of the picked node or one of its parents.
   *
   * @param holder the element the predicate goes on
   * @param key the attribute or child element with the value
   */
  public record KeyCandidate(Element holder, Node key) {

    /** The attribute or element name, which the key field of the source rows often shares. */
    public String name() {
      return key.getNodeName();
    }

    /** The value in the sample document. */
    public String value() {
      return key.getTextContent();
    }

    /** The predicate for the holder, with the placeholder for the key value. */
    String predicate() {
      String test = key instanceof Attr ? "@" + nameTest(name()) : nameTest(name());
      return "[" + test + "='" + KEY_PLACEHOLDER + "']";
    }

    /** The step of the holder with the predicate right after its name, as in /order[@id='?']. */
    String stepWithPredicate() {
      return "/" + nameTest(holder.getNodeName()) + predicate();
    }
  }

  /** The statement for exactly the picked node, when every element goes under one node. */
  public static String xpath(Element target) {
    StringBuilder xpath = new StringBuilder();
    for (Element element : pathTo(target)) {
      xpath.append('/').append(nameTest(element.getNodeName())).append(position(element));
    }
    return xpath.toString();
  }

  /** The statement for the node of each key, when every element goes under the node of its key. */
  public static String xpath(Element target, KeyCandidate key) {
    StringBuilder xpath = new StringBuilder();
    boolean belowKey = false;
    for (Element element : pathTo(target)) {
      xpath.append('/').append(nameTest(element.getNodeName()));
      if (element == key.holder()) {
        xpath.append(key.predicate());
        belowKey = true;
      } else if (belowKey) {
        xpath.append(position(element));
      }
    }
    return xpath.toString();
  }

  /**
   * The values that can pick the node out: the attributes, then the child elements that hold only
   * text, of the node itself and then of each of its parents.
   */
  public static List<KeyCandidate> keyCandidates(Element target) {
    List<KeyCandidate> candidates = new ArrayList<>();
    for (Node node = target; node instanceof Element element; node = node.getParentNode()) {
      NamedNodeMap attributes = element.getAttributes();
      for (int i = 0; i < attributes.getLength(); i++) {
        Node attribute = attributes.item(i);
        String name = attribute.getNodeName();
        if (!"xmlns".equals(name) && !name.startsWith("xmlns:")) {
          candidates.add(new KeyCandidate(element, attribute));
        }
      }
      for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
        if (child instanceof Element childElement
            && childElement != target
            && holdsOnlyText(childElement)) {
          candidates.add(new KeyCandidate(element, childElement));
        }
      }
    }
    return candidates;
  }

  /**
   * The candidate named like the key field, without minding case or a prefix. Null when there is
   * none.
   */
  public static KeyCandidate findByName(List<KeyCandidate> candidates, String keyField) {
    if (keyField == null || keyField.isBlank()) {
      return null;
    }
    for (KeyCandidate candidate : candidates) {
      if (localName(candidate.name()).equalsIgnoreCase(localName(keyField.trim()))) {
        return candidate;
      }
    }
    return null;
  }

  /** The statement the transform evaluates for a source row with the given key value. */
  public static String forKeyValue(String xpath, String keyValue) {
    return xpath.replace(KEY_PLACEHOLDER, keyValue);
  }

  /** True if an element has text and no child elements. */
  static boolean holdsOnlyText(Element element) {
    for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (child instanceof Element) {
        return false;
      }
    }
    return !element.getTextContent().isBlank();
  }

  private static Deque<Element> pathTo(Element target) {
    Deque<Element> path = new ArrayDeque<>();
    for (Node node = target; node instanceof Element element; node = node.getParentNode()) {
      path.addFirst(element);
    }
    return path;
  }

  /** "[n]" when siblings share the element's name, otherwise nothing. */
  private static String position(Element element) {
    String name = element.getNodeName();
    int before = 0;
    int after = 0;
    for (Node sibling = element.getPreviousSibling();
        sibling != null;
        sibling = sibling.getPreviousSibling()) {
      if (sibling instanceof Element && name.equals(sibling.getNodeName())) {
        before++;
      }
    }
    for (Node sibling = element.getNextSibling();
        sibling != null;
        sibling = sibling.getNextSibling()) {
      if (sibling instanceof Element && name.equals(sibling.getNodeName())) {
        after++;
      }
    }
    return before + after == 0 ? "" : "[" + (before + 1) + "]";
  }

  /**
   * A name test for the transform's XPath: a plain name, or for a prefixed name a match on the full
   * name, because a prefix would need a namespace the transform doesn't declare.
   */
  private static String nameTest(String name) {
    if (name.indexOf(':') < 0) {
      return name;
    }
    return "*[name()='" + name + "']";
  }

  private static String localName(String name) {
    int colon = name.indexOf(':');
    return colon < 0 ? name : name.substring(colon + 1);
  }
}
