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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.classes.Field;
import org.apache.hop.metadata.serializer.xml.classes.Info;
import org.apache.hop.metadata.serializer.xml.classes.MetaData;
import org.apache.hop.metadata.serializer.xml.classes.TestEnum;
import org.apache.hop.metadata.serializer.xml.classes.WithMap;
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
    assertEquals(
        "<field_separator>,</field_separator>"
            + Const.CR
            + "<fields>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<length>50</length>"
            + Const.CR
            + "<name>a</name>"
            + Const.CR
            + "<ott>ONE</ott>"
            + Const.CR
            + "<precision>-1</precision>"
            + Const.CR
            + "<test_code>NONE</test_code>"
            + Const.CR
            + "<type>String</type>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<format>#;-#</format>"
            + Const.CR
            + "<length>10</length>"
            + Const.CR
            + "<name>b</name>"
            + Const.CR
            + "<precision>0</precision>"
            + Const.CR
            + "<test_code>TWO</test_code>"
            + Const.CR
            + "<type>Integer</type>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "<field>"
            + Const.CR
            + "<format>yyyy/MM/dd HH:mm:ss</format>"
            + Const.CR
            + "<length>-1</length>"
            + Const.CR
            + "<name>c</name>"
            + Const.CR
            + "<ott>THREE</ott>"
            + Const.CR
            + "<precision>-1</precision>"
            + Const.CR
            + "<test_code>THREE</test_code>"
            + Const.CR
            + "<type>Date</type>"
            + Const.CR
            + "</field>"
            + Const.CR
            + "</fields>"
            + Const.CR
            + "<filename>filename.csv</filename>"
            + Const.CR
            + "<grouping_symbol>&#34;</grouping_symbol>"
            + Const.CR
            + "<a>aValue</a>"
            + Const.CR
            + "<b>bValue</b>"
            + Const.CR
            + "<test_enum>TWO</test_enum>"
            + Const.CR
            + "<values>"
            + Const.CR
            + "<value>v1</value>"
            + Const.CR
            + "<value>v2</value>"
            + Const.CR
            + "<value>v3</value>"
            + Const.CR
            + "</values>"
            + Const.CR,
        xml);

    // Now load that object back in from XML...
    //
    Node node = XmlHandler.loadXmlString("<meta>" + xml + "</meta>", "meta");
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

  @Test
  void testMappingSerialization() throws Exception {
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
    Node node = XmlHandler.loadXmlString(xml);
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
}
