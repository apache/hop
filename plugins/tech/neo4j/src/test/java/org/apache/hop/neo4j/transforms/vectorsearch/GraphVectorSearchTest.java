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

package org.apache.hop.neo4j.transforms.vectorsearch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.graph.GraphObjectType;
import org.apache.hop.core.graph.GraphVectorSimilarity;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.pipeline.transform.TransformSerializationTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class GraphVectorSearchTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void testSerialization() throws Exception {
    GraphVectorSearchMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/graph-vector-search-transform.xml", GraphVectorSearchMeta.class);
    assertEquals("graph", meta.getConnection());
    // Written before the element type: nodes
    assertEquals(GraphObjectType.NODE, meta.getElementType());
    assertFalse(meta.isSearchingRelationships());
    assertEquals("doc_embeddings", meta.getIndexName());
    assertEquals("Doc", meta.getLabel());
    assertEquals("embedding", meta.getVectorProperty());
    assertEquals(GraphVectorSimilarity.EUCLIDEAN, meta.getSimilarity());
    assertEquals("query_vector", meta.getEmbeddingField());
    assertEquals("3", meta.getTopK());
    assertEquals("0.5", meta.getMinScore());
    assertTrue(meta.isEatingRowOnNoMatch());
    assertEquals("similarity", meta.getScoreField());
    assertEquals(2, meta.getReturnProperties().size());
    assertEquals("doc_id", meta.getReturnProperties().get(0).getFieldName());
    assertEquals("Integer", meta.getReturnProperties().get(0).getType());
  }

  @Test
  void testRelationshipSerialization() throws Exception {
    GraphVectorSearchMeta meta =
        TransformSerializationTestUtil.testSerialization(
            "/graph-vector-search-relationship-transform.xml", GraphVectorSearchMeta.class);
    assertEquals(GraphObjectType.RELATIONSHIP, meta.getElementType());
    assertTrue(meta.isSearchingRelationships());
    assertEquals("Doc", meta.getLabel());

    // Written with the element type, read back the same
    String xml = meta.getXml();
    assertTrue(xml.contains("<element_type>RELATIONSHIP</element_type>"), xml);
    GraphVectorSearchMeta copy = (GraphVectorSearchMeta) meta.clone();
    assertEquals(GraphObjectType.RELATIONSHIP, copy.getElementType());
  }

  @Test
  void testDefaultSearchesNodes() throws Exception {
    GraphVectorSearchMeta meta = new GraphVectorSearchMeta();
    assertEquals(GraphObjectType.NODE, meta.getElementType());
    assertTrue(meta.getXml().contains("<element_type>NODE</element_type>"), meta.getXml());
  }

  @Test
  void testFields() throws Exception {
    GraphVectorSearchMeta meta = new GraphVectorSearchMeta();
    meta.getReturnProperties().add(new GraphVectorSearchProperty("id", "doc_id", "Integer"));
    meta.getReturnProperties().add(new GraphVectorSearchProperty("text", null, null));
    meta.getReturnProperties().add(new GraphVectorSearchProperty("", "ignored", "String"));
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("embedding"));
    meta.getFields(row, "search", null, null, new Variables(), null);

    assertArrayEquals(new String[] {"embedding", "score", "doc_id", "text"}, row.getFieldNames());
    assertEquals(IValueMeta.TYPE_NUMBER, row.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(2).getType());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(3).getType());

    // A copy has its own list of properties
    GraphVectorSearchMeta copy = (GraphVectorSearchMeta) meta.clone();
    copy.getReturnProperties().clear();
    assertEquals(3, meta.getReturnProperties().size());
  }

  /** Hits become the score and the converted properties; the minimum score drops the rest. */
  @Test
  void testOutputValues() throws Exception {
    GraphVectorSearchData data = new GraphVectorSearchData();
    data.propertyValueMetas = new ArrayList<>();
    data.propertyValueMetas.add(
        GraphVectorSearchMeta.createValueMeta(
            new GraphVectorSearchProperty("id", "doc_id", "Integer")));
    data.propertyValueMetas.add(
        GraphVectorSearchMeta.createValueMeta(new GraphVectorSearchProperty("tags", null, null)));

    List<Map<String, Object>> results =
        List.of(
            Map.of("score", 0.99, "p0", 1L, "p1", List.of("a", "b")),
            Map.of("score", 0.4f, "p0", 2L, "p1", List.of()));

    List<Object[]> all = GraphVectorSearch.toOutputValues(results, null, true, data);
    assertEquals(2, all.size());
    assertArrayEquals(new Object[] {0.99, 1L, "[\"a\",\"b\"]"}, all.get(0));
    assertEquals(0.4f, ((Double) all.get(1)[0]).floatValue());

    List<Object[]> filtered = GraphVectorSearch.toOutputValues(results, 0.5, false, data);
    assertEquals(1, filtered.size());
    assertArrayEquals(new Object[] {1L, "[\"a\",\"b\"]"}, filtered.get(0));
  }
}
