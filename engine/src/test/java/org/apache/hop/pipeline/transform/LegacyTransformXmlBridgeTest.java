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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Node;

class LegacyTransformXmlBridgeTest {

  @Test
  void serializeUsesOverriddenGetXmlWhenNoMetadataProperties() throws HopException {
    LegacyOnlyTestMeta meta = new LegacyOnlyTestMeta();
    meta.secret = "external-plugin";
    String xml = XmlMetadataUtil.serializeObjectToXml(meta);
    assertTrue(xml.contains("external-plugin"));
  }

  @Test
  void deserializeInvokesLoadXmlForLegacyOnlyTransform() throws HopException {
    String fragment =
        "<transform><name>x</name><type>Dummy</type><secret>from-xml</secret></transform>";
    Node transformNode = XmlHandler.getSubNode(XmlHandler.loadXmlString(fragment), "transform");
    LegacyOnlyTestMeta meta = new LegacyOnlyTestMeta();
    XmlMetadataUtil.deSerializeFromXml(
        transformNode, LegacyOnlyTestMeta.class, meta, new MemoryMetadataProvider());
    assertEquals("from-xml", meta.getSecret());
  }
}
