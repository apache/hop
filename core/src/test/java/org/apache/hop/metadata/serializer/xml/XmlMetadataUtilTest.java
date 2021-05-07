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

import junit.framework.TestCase;
import org.apache.hop.core.Const;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.classes.Field;
import org.apache.hop.metadata.serializer.xml.classes.MetaData;
import org.apache.hop.metadata.serializer.xml.classes.TestEnum;
import org.junit.Test;
import org.w3c.dom.Node;

import java.util.Arrays;

public class XmlMetadataUtilTest extends TestCase {

  @Test
  public void testMetaXml() throws Exception {
    MetaData metaTest = new MetaData();
    metaTest.setFilename("filename.csv");
    metaTest.setGroup("\"");
    metaTest.setSeparator(",");
    metaTest.getFields().add(new Field("a", "String", 50, -1, null, TestEnum.ONE));
    metaTest.getFields().add(new Field("b", "Integer", 10, 0, "#;-#", null));
    metaTest.getFields().add(new Field("c", "Date", -1, -1, "yyyy/MM/dd HH:mm:ss", TestEnum.THREE));
    metaTest.setValues(Arrays.asList("v1", "v2", "v3"));
    metaTest.setTestEnum(TestEnum.TWO);

    String xml = XmlMetadataUtil.serializeObjectToXml(metaTest);
    assertEquals(
        "<fields>"
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
            + "<field_separator>,</field_separator>"
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
    MetaData metaData = XmlMetadataUtil.deSerializeFromXml(node, MetaData.class, null);

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
}
