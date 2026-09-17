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
package org.apache.hop.pgvector.transforms.search;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pgvector.GuiWidgetCoverage;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class PgVectorSearchMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void roundTripsEveryPropertyIncludingFiltersThroughXml() throws Exception {
    PgVectorSearchMeta original = new PgVectorSearchMeta();
    original.setConnection("pgvector-local");
    original.setSchemaName("rag");
    original.setTableName("chunks");
    original.setEmbeddingField("query_vector");
    original.setTopK("12");
    original.setDistanceMetric(VectorDistanceMetric.INNER_PRODUCT);
    original.setMinScore("0.42");
    original.setEatingRowOnNoMatch(true);
    original.setResultIdField("hit_id");
    original.setResultDocumentIdField("hit_doc");
    original.setResultChunkIndexField("hit_idx");
    original.setResultContentField("hit_content");
    original.setResultScoreField("hit_score");
    original.setFilters(List.of(new PgVectorSearchFilter("source_type", "wanted_type")));

    PgVectorSearchMeta copy = roundTrip(original);

    assertEquals(original.getConnection(), copy.getConnection());
    assertEquals(original.getSchemaName(), copy.getSchemaName());
    assertEquals(original.getTableName(), copy.getTableName());
    assertEquals(original.getEmbeddingField(), copy.getEmbeddingField());
    assertEquals(original.getTopK(), copy.getTopK());
    assertEquals(original.getDistanceMetric(), copy.getDistanceMetric());
    assertEquals(original.getMinScore(), copy.getMinScore());
    assertEquals(original.isEatingRowOnNoMatch(), copy.isEatingRowOnNoMatch());
    assertEquals(original.getResultIdField(), copy.getResultIdField());
    assertEquals(original.getResultDocumentIdField(), copy.getResultDocumentIdField());
    assertEquals(original.getResultChunkIndexField(), copy.getResultChunkIndexField());
    assertEquals(original.getResultContentField(), copy.getResultContentField());
    assertEquals(original.getResultScoreField(), copy.getResultScoreField());
    assertEquals(1, copy.getFilters().size());
    assertEquals("source_type", copy.getFilters().get(0).getColumnName());
    assertEquals("wanted_type", copy.getFilters().get(0).getStreamField());
  }

  /** The matched chunk index is a number in the table, so it must be a number in the stream too. */
  @Test
  void exposesTheChunkIndexAsAnIntegerAndTheScoreAsANumber() throws Exception {
    PgVectorSearchMeta meta = new PgVectorSearchMeta();
    meta.setDefault();

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("embedding"));
    meta.getFields(row, "search", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("match_id").getType());
    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("match_document_id").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.searchValueMeta("match_chunk_index").getType());
    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("match_content").getType());
    assertEquals(IValueMeta.TYPE_NUMBER, row.searchValueMeta("match_score").getType());
  }

  @Test
  void checkRejectsAnEmbeddingFieldMissingFromTheStream() {
    PgVectorSearchMeta meta = new PgVectorSearchMeta();
    meta.setDefault();
    meta.setConnection("pgvector-local");
    meta.setEmbeddingField("query_vector");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("something_else"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("query_vector")));
  }

  @Test
  void everyPropertyIsOnTheDialog() {
    // The filters table view is built by the dialog through registerExtraGroup.
    GuiWidgetCoverage.assertEveryPropertyHasAWidget(PgVectorSearchMeta.class, List.of("filters"));
  }

  @Test
  void everyWidgetLabelResolves() {
    GuiWidgetCoverage.assertWidgetTextResolves(PgVectorSearchMeta.class);
  }

  @Test
  void acceptsAVariableForTopK() {
    PgVectorSearchMeta meta = new PgVectorSearchMeta();
    meta.setDefault();
    meta.setConnection("pgvector");
    meta.setTableName("chunks");
    meta.setEmbeddingField("embedding");
    meta.setTopK("${TOP_K}");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("embedding"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "a variable top-k is resolved at run time, not a design-time error");
  }

  private static PgVectorSearchMeta roundTrip(PgVectorSearchMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), PgVectorSearchMeta.class, null);
  }

  /** Matches Hop's Database Lookup: rows are passed on unless the option is explicitly enabled. */
  @Test
  void defaultsToPassingRowsThroughOnNoMatch() {
    PgVectorSearchMeta meta = new PgVectorSearchMeta();
    meta.setDefault();

    assertEquals(false, meta.isEatingRowOnNoMatch());
  }
}
