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

package org.apache.hop.neo4j.transforms.graph;

import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class GraphOutputMetaTest {
  @Test
  public void testXmlRoundTrip() throws Exception {
    String tag = TransformMeta.XML_TAG;

    Path path = Paths.get(getClass().getResource("/transform1.snippet").toURI());
    String xml = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    String transformXml = XmlHandler.openTag(tag) + xml + XmlHandler.closeTag(tag);
    GraphOutputMeta meta = new GraphOutputMeta();
    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml, tag), GraphOutputMeta.class, meta, null);
    assertEquals(6, meta.getFieldModelMappings().size());
    assertEquals(0, meta.getRelationshipMappings().size());

    // Verify the first mapping, the rest should be similarly OK or not...
    //
    FieldModelMapping mapping = meta.getFieldModelMappings().get(0);
    assertEquals("date", mapping.getField());
    assertEquals(ModelTargetType.Node, mapping.getTargetType());
    assertEquals("Item", mapping.getTargetName());
    assertEquals("date", mapping.getTargetProperty());

    String xml2 = meta.getXml();

    GraphOutputMeta meta2 = new GraphOutputMeta();
    String transformXml2 = XmlHandler.openTag(tag) + xml2 + XmlHandler.closeTag(tag);

    XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.loadXmlString(transformXml2, TransformMeta.XML_TAG),
        GraphOutputMeta.class,
        meta2,
        null);

    // Compare meta1 and meta2 to see if all serialization survived correctly...
    //
    assertEquals(6, meta2.getFieldModelMappings().size());
    for (int i = 0; i < meta.getFieldModelMappings().size(); i++) {
      assertEquals(meta.getFieldModelMappings().get(i), meta2.getFieldModelMappings().get(i));
    }
    for (int i = 0; i < meta.getRelationshipMappings().size(); i++) {
      assertEquals(meta.getRelationshipMappings().get(i), meta2.getRelationshipMappings().get(i));
    }
  }
}
