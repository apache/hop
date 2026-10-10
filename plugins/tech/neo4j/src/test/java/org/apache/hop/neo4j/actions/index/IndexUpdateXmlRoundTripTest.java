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

package org.apache.hop.neo4j.actions.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.junit.jupiter.api.Test;

/** Every setting of an index update is written to and read from the action XML. */
class IndexUpdateXmlRoundTripTest {

  @Test
  void indexUpdatesSurviveXml() throws Exception {
    IndexUpdate range =
        new IndexUpdate(UpdateType.CREATE, ObjectType.NODE, "idx", "Person", "id,name");
    range.setIndexType(IndexType.RANGE);
    IndexUpdate vector =
        IndexUpdate.vector(
            UpdateType.DROP,
            ObjectType.RELATIONSHIP,
            "vec",
            "KNOWS",
            "embedding",
            "384",
            GraphVectorSimilarity.values()[GraphVectorSimilarity.values().length - 1]);
    vector.setVectorCapacity("1000");

    Neo4jIndex action = new Neo4jIndex();
    action.setConnectionName("graph");
    action.setIndexUpdates(List.of(range, vector));

    String xml = "<action>" + XmlMetadataUtil.serializeObjectToXml(action) + "</action>";
    Neo4jIndex copy =
        XmlMetadataUtil.deSerializeFromXml(
            XmlHandler.loadXmlString(xml, "action"), Neo4jIndex.class, null);

    assertEquals("graph", copy.getConnectionName());
    assertEquals(2, copy.getIndexUpdates().size());
    assertSame(range, copy.getIndexUpdates().get(0));
    assertSame(vector, copy.getIndexUpdates().get(1));
  }

  private static void assertSame(IndexUpdate expected, IndexUpdate actual) {
    assertEquals(expected.getType(), actual.getType());
    assertEquals(expected.getObjectType(), actual.getObjectType());
    assertEquals(expected.getIndexName(), actual.getIndexName());
    assertEquals(expected.getObjectName(), actual.getObjectName());
    assertEquals(expected.getObjectProperties(), actual.getObjectProperties());
    assertEquals(expected.getIndexType(), actual.getIndexType());
    assertEquals(expected.getVectorDimensions(), actual.getVectorDimensions());
    assertEquals(expected.getVectorSimilarity(), actual.getVectorSimilarity());
    assertEquals(expected.getVectorCapacity(), actual.getVectorCapacity());
  }
}
