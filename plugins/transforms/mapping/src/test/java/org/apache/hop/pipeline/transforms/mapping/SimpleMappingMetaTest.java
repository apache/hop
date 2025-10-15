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
package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleMappingMetaTest {
  @BeforeEach
  void setUpLoadSave() throws Exception {
    HopEnvironment.init();
    PluginRegistry.init();
  }

  @Test
  void testSerialization() throws Exception {
    TransformSerializationTestUtil.testSerialization(
        "/simple-mapping-transform.xml", SimpleMappingMeta.class);
    /*Document document =
        XmlHandler.loadXmlFile(this.getClass().getResourceAsStream("/simple-mapping-transform.xml"));
    Node transformNode = XmlHandler.getSubNode(document, TransformMeta.XML_TAG);
    SimpleMappingMeta meta = new SimpleMappingMeta();
    XmlMetadataUtil.deSerializeFromXml(
        null, transformNode, SimpleMappingMeta.class, meta, new MemoryMetadataProvider());
    String xml =
        XmlHandler.openTag(TransformMeta.XML_TAG)
            + meta.getXml()
            + XmlHandler.closeTag(TransformMeta.XML_TAG);

    Document copyDocument = XmlHandler.loadXmlString(xml);
    Node copyNode = XmlHandler.getSubNode(copyDocument, TransformMeta.XML_TAG);
    SimpleMappingMeta copy = new SimpleMappingMeta();
    XmlMetadataUtil.deSerializeFromXml(
        null, copyNode, SimpleMappingMeta.class, copy, new MemoryMetadataProvider());
    Assert.assertEquals(meta.getXml(), copy.getXml());*/
  }
}
