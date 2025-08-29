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

package org.apache.hop.pipeline.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

public class TransformSerializationTestUtil {
  public static final <T extends ITransformMeta> T testSerialization(
      String filename, Class<T> clazz) throws Exception {
    return testSerialization(filename, clazz, TransformMeta.XML_TAG, new MemoryMetadataProvider());
  }

  public static final <T extends ITransformMeta> T testSerialization(
      String filename, Class<T> clazz, IHopMetadataProvider metadataProvider) throws Exception {
    return testSerialization(filename, clazz, TransformMeta.XML_TAG, metadataProvider);
  }

  public static final <T extends ITransformMeta> T testSerialization(
      String filename, Class<T> clazz, String xmlTag, IHopMetadataProvider metadataProvider)
      throws Exception {
    Document document = XmlHandler.loadXmlFile(clazz.getResourceAsStream(filename));
    Node node = XmlHandler.getSubNode(document, xmlTag);
    T meta = clazz.getConstructor().newInstance();
    XmlMetadataUtil.deSerializeFromXml(null, node, clazz, meta, metadataProvider);
    String xml = XmlHandler.openTag(xmlTag) + meta.getXml() + XmlHandler.closeTag(xmlTag);

    Document copyDocument = XmlHandler.loadXmlString(xml);
    Node copyNode = XmlHandler.getSubNode(copyDocument, xmlTag);
    T copy = clazz.getConstructor().newInstance();
    XmlMetadataUtil.deSerializeFromXml(null, copyNode, clazz, copy, metadataProvider);
    assertEquals(meta.getXml(), copy.getXml());

    return meta;
  }
}
