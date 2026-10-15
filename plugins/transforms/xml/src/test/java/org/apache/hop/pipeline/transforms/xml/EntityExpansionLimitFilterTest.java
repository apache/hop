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

package org.apache.hop.pipeline.transforms.xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.StringReader;
import org.apache.hop.core.Const;
import org.apache.hop.core.HopEnvironment;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The cap on entity text, with a limit of 100 characters. */
class EntityExpansionLimitFilterTest {

  private static final String ENTITY = "<!DOCTYPE r [<!ENTITY a \"0123456789\">]>";

  private String previousValue;

  @BeforeEach
  void init() throws Exception {
    HopEnvironment.init();
    previousValue = System.getProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, "Y");
  }

  @AfterEach
  void restore() {
    if (previousValue == null) {
      System.clearProperty(Const.XML_ALLOW_DOCTYPE_DECL);
    } else {
      System.setProperty(Const.XML_ALLOW_DOCTYPE_DECL, previousValue);
    }
  }

  private static Document read(String xml) throws DocumentException {
    SAXReader reader = Dom4JUtil.getSAXReader();
    reader.setXMLFilter(new EntityExpansionLimitFilter(100));
    return reader.read(new StringReader(xml));
  }

  @Test
  void entitiesWithinTheLimitAreExpanded() throws Exception {
    Document document = read(ENTITY + "<r a=\"&a;\">" + "&a;".repeat(5) + "</r>");
    assertEquals("0123456789".repeat(5), document.getRootElement().getText());
    assertEquals("0123456789", document.getRootElement().attributeValue("a"));
  }

  @Test
  void entityTextOverTheLimitIsRefused() {
    String xml = ENTITY + "<r>" + "&a;".repeat(11) + "</r>";
    assertThrows(DocumentException.class, () -> read(xml));
  }

  @Test
  void entityTextInAttributesOverTheLimitIsRefused() {
    String xml = ENTITY + "<r a=\"" + "&a;".repeat(11) + "\"/>";
    assertThrows(DocumentException.class, () -> read(xml));
  }

  /** Text that does not come from an entity never counts, nor do the predefined entities. */
  @Test
  void plainTextDoesNotCount() throws Exception {
    String text = "x".repeat(1000);
    Document document =
        read("<!DOCTYPE r><r a=\"" + text + "\">" + text + "&amp;".repeat(200) + "</r>");
    assertEquals(text + "&".repeat(200), document.getRootElement().getText());
  }
}
