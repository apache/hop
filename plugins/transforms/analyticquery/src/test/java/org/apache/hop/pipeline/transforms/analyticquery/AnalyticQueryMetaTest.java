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

package org.apache.hop.pipeline.transforms.analyticquery;

import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.injection.bean.BeanInjectionInfo;
import org.apache.hop.core.injection.bean.BeanInjector;
import org.apache.hop.core.injection.bean.BeanLevelInfo;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AnalyticQueryMetaTest {

  @Test
  public void testXmlRoundTrip() throws Exception {
    String tag = TransformMeta.XML_TAG;

    Path path = Paths.get(getClass().getResource("/transform1.hpl").toURI());
    String xml = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String transformXml = XmlHandler.openTag(tag) + xml + XmlHandler.closeTag(tag);
    AnalyticQueryMeta meta = new AnalyticQueryMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, tag), AnalyticQueryMeta.class, meta, null);
    assertEquals(1, meta.getGroupFields().size());
    assertEquals(8, meta.getQueryFields().size());
    String xml2 = meta.getXml();
    assertEquals(xml, xml2);

    AnalyticQueryMeta meta2 = new AnalyticQueryMeta();
    String transformXml2 = XmlHandler.openTag(tag) + xml2 + XmlHandler.closeTag(tag);

    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml2, TransformMeta.XML_TAG),
        AnalyticQueryMeta.class,
        meta2,
        null);

    // Compare meta1 and meta2 to see if all serialization survived correctly...
    //
    assertEquals(1, meta2.getGroupFields().size());
    assertEquals(8, meta2.getQueryFields().size());
    for (int i = 0; i < meta.getGroupFields().size(); i++) {
      assertEquals(meta.getGroupFields().get(i), meta2.getGroupFields().get(i));
    }
    for (int i = 0; i < meta.getQueryFields().size(); i++) {
      assertEquals(meta.getQueryFields().get(i), meta2.getQueryFields().get(i));
    }
  }

  @Test
  public void testXmlMissingMetadata() throws Exception {
    String tag = TransformMeta.XML_TAG;

    Path path = Paths.get(getClass().getResource("/transform2.hpl").toURI());
    String xml = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String transformXml = XmlHandler.openTag(tag) + xml + XmlHandler.closeTag(tag);
    AnalyticQueryMeta meta = new AnalyticQueryMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, tag), AnalyticQueryMeta.class, meta, null);
    assertEquals(1, meta.getGroupFields().size());
    assertEquals(1, meta.getQueryFields().size());

    // See that the missing field is set to the default of 1.
    assertEquals(1, meta.getQueryFields().get(0).getValueField());
  }

  @Test
  public void testInjectionMetadata() throws Exception {
    BeanInjectionInfo<AnalyticQueryMeta> injectionInfo =
        new BeanInjectionInfo<>(AnalyticQueryMeta.class);
    assertEquals(5, injectionInfo.getProperties().size());

    BeanInjectionInfo<AnalyticQueryMeta>.Property prop =
        injectionInfo.getProperties().get("GROUP_FIELDS");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());

    BeanLevelInfo info = prop.getPath().get(1);
    assertEquals(GroupField.class, info.leafClass);
    assertEquals(BeanLevelInfo.DIMENSION.LIST, info.dim);

    info = prop.getPath().get(2);
    assertEquals(String.class, info.leafClass);
    assertNotNull(info.getter);
    assertNotNull(info.setter);

    prop = injectionInfo.getProperties().get("OUTPUT.AGGREGATE_FIELD");
    assertNotNull(prop);
    assertEquals(3, prop.getPath().size());

    info = prop.getPath().get(1);
    assertEquals(QueryField.class, info.leafClass);
    assertEquals(BeanLevelInfo.DIMENSION.LIST, info.dim);

    info = prop.getPath().get(2);
    assertEquals(String.class, info.leafClass);
    assertNotNull(info.getter);
    assertNotNull(info.setter);
  }

  @Test
  public void testInjection() throws Exception {
    BeanInjectionInfo<AnalyticQueryMeta> info = new BeanInjectionInfo<>(AnalyticQueryMeta.class);
    BeanInjector<AnalyticQueryMeta> injector = new BeanInjector<>(info);

    AnalyticQueryMeta meta = new AnalyticQueryMeta();

    IRowMeta groupMeta = new RowMetaBuilder().addString("group").build();
    List<RowMetaAndData> groupRows =
        Arrays.asList(
            new RowMetaAndData(groupMeta, "group1"), new RowMetaAndData(groupMeta, "group2"));

    injector.setProperty(meta, "GROUP_FIELDS", groupRows, "group");
    assertEquals(2, meta.getGroupFields().size());
    assertEquals("group1", meta.getGroupFields().get(0).getFieldName());
    assertEquals("group2", meta.getGroupFields().get(1).getFieldName());

    IRowMeta queryMeta =
        new RowMetaBuilder()
            .addString("fieldName")
            .addString("subject")
            .addString("type")
            .addString("offset")
            .build();
    List<RowMetaAndData> queryRows =
        Arrays.asList(
            new RowMetaAndData(queryMeta, "leadResult1", "A", "LEAD", 1),
            new RowMetaAndData(queryMeta, "leadResult2", "A", "LEAD", 2),
            new RowMetaAndData(queryMeta, "lagResult1", "B", "LAG", 1),
            new RowMetaAndData(queryMeta, "lagResult2", "B", "LAG", 2));
    injector.setProperty(meta, "OUTPUT.AGGREGATE_FIELD", queryRows, "fieldName");
    injector.setProperty(meta, "OUTPUT.SUBJECT_FIELD", queryRows, "subject");
    injector.setProperty(meta, "OUTPUT.AGGREGATE_TYPE", queryRows, "type");
    injector.setProperty(meta, "OUTPUT.VALUE_FIELD", queryRows, "offset");

    assertEquals(4, meta.getQueryFields().size());
    int index = 0;
    assertEquals("leadResult1", meta.getQueryFields().get(index).getAggregateField());
    assertEquals("LEAD", meta.getQueryFields().get(index).getAggregateType().name());
    assertEquals("A", meta.getQueryFields().get(index).getSubjectField());
    assertEquals(1, meta.getQueryFields().get(index).getValueField());

    index++;
    assertEquals("leadResult2", meta.getQueryFields().get(index).getAggregateField());
    assertEquals("LEAD", meta.getQueryFields().get(index).getAggregateType().name());
    assertEquals("A", meta.getQueryFields().get(index).getSubjectField());
    assertEquals(2, meta.getQueryFields().get(index).getValueField());

    index++;
    assertEquals("lagResult1", meta.getQueryFields().get(index).getAggregateField());
    assertEquals("LAG", meta.getQueryFields().get(index).getAggregateType().name());
    assertEquals("B", meta.getQueryFields().get(index).getSubjectField());
    assertEquals(1, meta.getQueryFields().get(index).getValueField());

    index++;
    assertEquals("lagResult2", meta.getQueryFields().get(index).getAggregateField());
    assertEquals("LAG", meta.getQueryFields().get(index).getAggregateType().name());
    assertEquals("B", meta.getQueryFields().get(index).getSubjectField());
    assertEquals(2, meta.getQueryFields().get(index).getValueField());
  }
}
