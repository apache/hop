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

package org.apache.hop.metadata.serializer.xml;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.classes.Field;
import org.apache.hop.metadata.serializer.xml.classes.Info;
import org.apache.hop.metadata.serializer.xml.classes.MetaData;
import org.apache.hop.metadata.serializer.xml.classes.TestEnum;
import org.apache.hop.metadata.serializer.xml.classes.WithListReference;
import org.apache.hop.metadata.serializer.xml.classes.WithMap;
import org.apache.hop.metadata.serializer.xml.classes.WithMapAsList;
import org.apache.hop.metadata.serializer.xml.classes.WithMapMap;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

/** Unit test for {@link XmlMetadataUtil} */
class XmlMetadataUtilTest {
  @Test
  void testMetaXml() throws Exception {
    MetaData metaTest = new MetaData();
    metaTest.setFilename("filename.csv");
    metaTest.setGroup("\"");
    metaTest.setSeparator(",");
    metaTest.getFields().add(new Field("a", "String", 50, -1, null, TestEnum.ONE));
    metaTest.getFields().add(new Field("b", "Integer", 10, 0, "#;-#", null));
    metaTest.getFields().add(new Field("c", "Date", -1, -1, "yyyy/MM/dd HH:mm:ss", TestEnum.THREE));
    metaTest.getFields().get(1).setTestCode(2);
    metaTest.getFields().get(2).setTestCode(3);
    metaTest.setValues(Arrays.asList("v1", "v2", "v3"));
    metaTest.setTestEnum(TestEnum.TWO);
    metaTest.setInfo(new Info("aValue", "bValue"));

    String xml = XmlMetadataUtil.serializeObjectToXml(metaTest);
    Node node = XmlHandler.loadXmlString(XmlHandler.aroundTag("meta", xml), "meta");

    // validate the raw XML DOM:
    //
    validateXmlDom(node);

    // Now load that object back in from XML...
    //
    MetaData metaData =
        XmlMetadataUtil.deSerializeFromXml(node, MetaData.class, new MemoryMetadataProvider());

    assertEquals(metaTest.getFilename(), metaData.getFilename());
    assertEquals(metaTest.getGroup(), metaData.getGroup());
    assertEquals(metaTest.getSeparator(), metaData.getSeparator());
    assertEquals(metaTest.getFields().size(), metaData.getFields().size());
    for (int i = 0; i < metaTest.getFields().size(); i++) {
      Field fieldTest = metaTest.getFields().get(i);
      Field fieldData = metaData.getFields().get(i);
      assertEquals(fieldTest, fieldData);
    }
    assertEquals(metaTest.getValues().size(), metaData.getValues().size());
    for (int i = 0; i < metaTest.getValues().size(); i++) {
      String valueTest = metaTest.getValues().get(i);
      String valueData = metaData.getValues().get(i);
      assertEquals(valueTest, valueData);
    }
  }

  private static void validateXmlDom(Node node) {
    assertEquals(",", XmlHandler.getTagValue(node, "field_separator"));
    assertEquals("filename.csv", XmlHandler.getTagValue(node, "filename"));
    assertEquals("\"", XmlHandler.getTagValue(node, "grouping_symbol"));
    assertEquals("aValue", XmlHandler.getTagValue(node, "a"));
    assertEquals("bValue", XmlHandler.getTagValue(node, "b"));
    assertEquals("TWO", XmlHandler.getTagValue(node, "test_enum"));
    Node fieldsNode = XmlHandler.getSubNode(node, "fields");
    List<Node> fieldNodes = XmlHandler.getNodes(fieldsNode, "field");
    assertEquals(3, fieldNodes.size());
    Node fieldNode = fieldNodes.get(0);
    assertEquals("a", XmlHandler.getTagValue(fieldNode, "name"));
    assertEquals("String", XmlHandler.getTagValue(fieldNode, "type"));
    assertEquals("50", XmlHandler.getTagValue(fieldNode, "length"));
    assertEquals("-1", XmlHandler.getTagValue(fieldNode, "precision"));
    assertNull(XmlHandler.getTagValue(fieldNode, "format"));
    assertEquals("NONE", XmlHandler.getTagValue(fieldNode, "test_code"));
    fieldNode = fieldNodes.get(1);
    assertEquals("b", XmlHandler.getTagValue(fieldNode, "name"));
    assertEquals("Integer", XmlHandler.getTagValue(fieldNode, "type"));
    assertEquals("10", XmlHandler.getTagValue(fieldNode, "length"));
    assertEquals("0", XmlHandler.getTagValue(fieldNode, "precision"));
    assertEquals("#;-#", XmlHandler.getTagValue(fieldNode, "format"));
    assertEquals("TWO", XmlHandler.getTagValue(fieldNode, "test_code"));
    fieldNode = fieldNodes.get(2);
    assertEquals("c", XmlHandler.getTagValue(fieldNode, "name"));
    assertEquals("Date", XmlHandler.getTagValue(fieldNode, "type"));
    assertEquals("-1", XmlHandler.getTagValue(fieldNode, "length"));
    assertEquals("-1", XmlHandler.getTagValue(fieldNode, "precision"));
    assertEquals("yyyy/MM/dd HH:mm:ss", XmlHandler.getTagValue(fieldNode, "format"));
    assertEquals("THREE", XmlHandler.getTagValue(fieldNode, "test_code"));

    Node valuesNode = XmlHandler.getSubNode(node, "values");
    List<Node> valueNodes = XmlHandler.getNodes(valuesNode, "value");
    assertEquals(3, valueNodes.size());
    assertEquals("v1", XmlHandler.getNodeValue(valueNodes.get(0)));
    assertEquals("v2", XmlHandler.getNodeValue(valueNodes.get(1)));
    assertEquals("v3", XmlHandler.getNodeValue(valueNodes.get(2)));
  }

  @Test
  void testMapSerialization() throws Exception {
    WithMap withMap = new WithMap();
    WithMap.Key k1 = new WithMap.Key("k11", "k12");
    WithMap.Value v1 = new WithMap.Value("v11", "v12");
    withMap.getMappings().put(k1, v1);

    WithMap.Key k2 = new WithMap.Key("k21", "k22");
    WithMap.Value v2 = new WithMap.Value("v21", "v22");
    withMap.getMappings().put(k2, v2);

    WithMap.Key k3 = new WithMap.Key("k31", "k32");
    WithMap.Value v3 = new WithMap.Value("v31", "v32");
    withMap.getMappings().put(k3, v3);

    String xml = XmlMetadataUtil.serializeObjectToXml(withMap);
    Node node = XmlHandler.loadXmlString("<hop>" + xml + "</hop>", "hop");
    WithMap withCopy =
        XmlMetadataUtil.deSerializeFromXml(node, WithMap.class, new MemoryMetadataProvider());
    assertEquals(withCopy.getMappings().size(), withMap.getMappings().size());
    for (WithMap.Key key : withMap.getMappings().keySet()) {
      WithMap.Value value = withMap.getMappings().get(key);
      WithMap.Value valueCopy = withCopy.getMappings().get(key);
      assertTrue(withCopy.getMappings().containsKey(key));
      assertEquals(value, valueCopy);
    }
  }

  @Test
  void testMapMapSerialization() throws Exception {
    WithMapMap mapMap = new WithMapMap();

    String[] groupNames = {"group1", "group2", "group3"};
    for (String groupName : groupNames) {
      Map<String, String> groupMap = new HashMap<>();
      for (int a = 1; a <= 5; a++) {
        groupMap.put("key-" + groupName + "-" + a, "value-" + groupName + "-" + a);
      }
      mapMap.getAttributesMap().put(groupName, groupMap);
    }

    String xml = XmlMetadataUtil.serializeObjectToXml(mapMap);
    Node node = XmlHandler.loadXmlString("<hop>" + xml + "</hop>", "hop");
    WithMapMap withCopy =
        XmlMetadataUtil.deSerializeFromXml(node, WithMapMap.class, new MemoryMetadataProvider());
    assertEquals(withCopy.getAttributesMap().size(), mapMap.getAttributesMap().size());
  }

  @Test
  void testMapAsListSerialization() throws Exception {
    WithMapAsList mapList = new WithMapAsList();
    String[] groupNames = {"k1", "k2", "k3"};
    for (String groupName : groupNames) {
      WithMapAsList.Value value = new WithMapAsList.Value();
      value.setK(groupName);
      value.setV1("v1-of-" + groupName);
      value.setV2("v2-of-" + groupName);
      mapList.getMappings().put(groupName, value);
    }

    String xml = XmlMetadataUtil.serializeObjectToXml(mapList);
    Node node = XmlHandler.loadXmlString("<hop>" + xml + "</hop>", "hop");
    WithMapAsList withCopy =
        XmlMetadataUtil.deSerializeFromXml(node, WithMapAsList.class, new MemoryMetadataProvider());

    assertEquals(withCopy.getMappings().size(), mapList.getMappings().size());
  }

  @Test
  void testListRefenceSerialization() throws Exception {
    WithListReference listRef = new WithListReference();
    WithListReference.Step step1 = new WithListReference.Step("S1", "Description1", true);
    listRef.getSteps().add(step1);
    WithListReference.Step step2 = new WithListReference.Step("S2", "Description2", true);
    listRef.getSteps().add(step2);
    WithListReference.Step step3 = new WithListReference.Step("S3", "Description3", true);
    listRef.getSteps().add(step3);
    listRef.getHops().add(new WithListReference.Hop(step1, step2));
    listRef.getHops().add(new WithListReference.Hop(step2, step3));

    String xml = XmlMetadataUtil.serializeObjectToXml(listRef);
    Node node = XmlHandler.loadXmlString("<hop>" + xml + "</hop>", "hop");
    WithListReference withCopy =
        XmlMetadataUtil.deSerializeFromXml(
            node, WithListReference.class, new MemoryMetadataProvider());

    assertEquals(withCopy.getSteps().size(), listRef.getSteps().size());
    assertEquals(withCopy.getHops().size(), listRef.getHops().size());
  }

  @Test
  void testListReferenceSerializationWithEmptyReference() throws Exception {
    String xml =
        """
        <steps>
          <step><name>S1</name><description>Description1</description><enabled>Y</enabled></step>
        </steps>
        <order>
          <hop><from>S1</from><to/></hop>
        </order>
        """;
    Node node = XmlHandler.loadXmlString("<hop>" + xml + "</hop>", "hop");
    WithListReference withCopy =
        XmlMetadataUtil.deSerializeFromXml(
            node, WithListReference.class, new MemoryMetadataProvider());

    assertEquals(1, withCopy.getSteps().size());
    assertEquals(1, withCopy.getHops().size());
    assertEquals("S1", withCopy.getHops().get(0).getFrom().getName());
    assertNull(withCopy.getHops().get(0).getTo());
  }
}
