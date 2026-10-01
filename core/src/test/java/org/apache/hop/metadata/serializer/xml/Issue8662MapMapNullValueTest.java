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
 *
 */
package org.apache.hop.metadata.serializer.xml;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.classes.WithMapMap;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/**
 * Reproduction for #8662: a {@code Map<String, Map<String, String>>} whose inner value is empty
 * round-trips into a map holding a null value, and serializing that map again throws an NPE.
 */
class Issue8662MapMapNullValueTest {

  /**
   * An attribute written with an empty value - {@code <value/>} rather than {@code
   * <value>x</value>}.
   */
  private static final String XML_WITH_EMPTY_ATTRIBUTE_VALUE =
      "<hop>"
          + "<attributes>"
          + "  <group><name>group1</name>"
          + "    <attribute><key>empty</key><value/></attribute>"
          + "  </group>"
          + "</attributes>"
          + "</hop>";

  @Test
  void emptyAttributeValueDoesNotBreakSerialization() throws Exception {
    Node node = XmlHandler.loadXmlString(XML_WITH_EMPTY_ATTRIBUTE_VALUE, "hop");

    WithMapMap deserialized =
        XmlMetadataUtil.deSerializeFromXml(node, WithMapMap.class, new MemoryMetadataProvider());

    Map<String, String> group = deserialized.getAttributesMap().get("group1");
    assertNull(group.get("empty"), "the empty value reads back as null");

    // This is the NPE from the issue: serializeMapValueToXml calls valueObject.getClass() on the
    // null entry value.
    assertDoesNotThrow(() -> XmlMetadataUtil.serializeObjectToXml(deserialized));
  }

  /** A null value put in by hand, the shape setAttribute(group, key, null) produces. */
  @Test
  void nullValueInAGroupDoesNotBreakSerialization() {
    WithMapMap mapMap = new WithMapMap();
    Map<String, String> group = new HashMap<>();
    group.put("empty", null);
    mapMap.getAttributesMap().put("group1", group);

    assertDoesNotThrow(() -> XmlMetadataUtil.serializeObjectToXml(mapMap));
  }

  /** The serializer should keep a null value as an empty tag rather than dropping the attribute. */
  @Test
  void nullValueIsWrittenAsAnEmptyValueTag() throws Exception {
    WithMapMap mapMap = new WithMapMap();
    Map<String, String> group = new HashMap<>();
    group.put("empty", null);
    mapMap.getAttributesMap().put("group1", group);

    String xml = XmlMetadataUtil.serializeObjectToXml(mapMap);

    assertEquals(
        1,
        countOccurrences(xml, "<attribute>"),
        "the attribute must survive serialization: " + xml);
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int index = haystack.indexOf(needle);
    while (index >= 0) {
      count++;
      index = haystack.indexOf(needle, index + needle.length());
    }
    return count;
  }
}
