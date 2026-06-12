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
package org.apache.hop.vfs.webdav;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

class HopWireWebDavFileObjectTest {

  private static Element davElement(String localName) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    Document doc = factory.newDocumentBuilder().newDocument();
    return doc.createElementNS("DAV:", "d:" + localName);
  }

  private static Element cardDavElement(String localName) throws Exception {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    Document doc = factory.newDocumentBuilder().newDocument();
    return doc.createElementNS("urn:ietf:params:xml:ns:carddav", "card:" + localName);
  }

  @Test
  void plainCollection_singleNodeValue() throws Exception {
    assertTrue(HopWireWebDavFileObject.resourceTypeIndicatesCollection(davElement("collection")));
  }

  @Test
  void nextcloudAddressbook_listValueWithCollectionAndAddressbook() throws Exception {
    List<Node> value = List.of(davElement("collection"), cardDavElement("addressbook"));
    assertTrue(HopWireWebDavFileObject.resourceTypeIndicatesCollection(value));
  }

  @Test
  void plainFile_emptyResourceType() throws Exception {
    assertFalse(HopWireWebDavFileObject.resourceTypeIndicatesCollection(null));
    assertFalse(HopWireWebDavFileObject.resourceTypeIndicatesCollection(List.of()));
  }

  @Test
  void nonCollectionResourceType_isNotAFolder() throws Exception {
    assertFalse(
        HopWireWebDavFileObject.resourceTypeIndicatesCollection(
            List.of(cardDavElement("addressbook"))));
  }
}
