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

package org.apache.hop.pipeline.transforms.webservices.wsdl;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/** Some DOM utility methods. */
public final class DomUtils {

  /**
   * Returns the first child element with the given name. Returns <code>null</code> if not found.
   *
   * @param parent parent element
   * @param localName name of the child element
   * @return child element, null if not found.
   */
  protected static Element getChildElementByName(Element parent, String localName) {
    NodeList children = parent.getChildNodes();

    for (int i = 0; i < children.getLength(); i++) {
      Node node = children.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element element = (Element) node;
        if (element.getLocalName().equals(localName)) {
          return element;
        }
      }
    }
    return null;
  }

  /**
   * Returns a list of child elements with the given name. Returns an empty list if there are no
   * such child elements.
   *
   * @param parent parent element
   * @param localName Local name of the child element
   * @return child elements
   */
  protected static List<Element> getChildElementsByName(Element parent, String localName) {
    List<Element> elements = new ArrayList<>();

    NodeList children = parent.getChildNodes();

    for (int i = 0; i < children.getLength(); i++) {
      Node node = children.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        Element element = (Element) node;
        if (element.getLocalName().equals(localName)) {
          elements.add(element);
        }
      }
    }
    return elements;
  }
}
