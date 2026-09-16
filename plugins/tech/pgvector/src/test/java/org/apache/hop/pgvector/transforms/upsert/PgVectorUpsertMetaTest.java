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
package org.apache.hop.pgvector.transforms.upsert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pgvector.util.PgVectorColumnMapping;
import org.apache.hop.pgvector.util.VectorDistanceMetric;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class PgVectorUpsertMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void roundTripsEveryPropertyIncludingMappingsThroughXml() throws Exception {
    PgVectorUpsertMeta original = new PgVectorUpsertMeta();
    original.setConnection("pgvector-local");
    original.setSchemaName("rag");
    original.setTableName("chunks");
    original.setIdField("id");
    original.setDocumentIdField("doc");
    original.setChunkIndexField("idx");
    original.setContentField("body");
    original.setEmbeddingField("vector");
    original.setEmbeddingDimensions(1536);
    original.setCreateTableIfMissing(false);
    original.setCreateHnswIndex(false);
    original.setDeleteDocumentBeforeUpsert(true);
    original.setCommitSize(500);
    original.setIndexMetric(VectorDistanceMetric.L2);
    original.setColumnMappings(
        List.of(
            new PgVectorColumnMapping("url", "source_url"),
            new PgVectorColumnMapping("origin", "source_origin")));

    PgVectorUpsertMeta copy = roundTrip(original);

    assertEquals(original.getConnection(), copy.getConnection());
    assertEquals(original.getSchemaName(), copy.getSchemaName());
    assertEquals(original.getTableName(), copy.getTableName());
    assertEquals(original.getIdField(), copy.getIdField());
    assertEquals(original.getDocumentIdField(), copy.getDocumentIdField());
    assertEquals(original.getChunkIndexField(), copy.getChunkIndexField());
    assertEquals(original.getContentField(), copy.getContentField());
    assertEquals(original.getEmbeddingField(), copy.getEmbeddingField());
    assertEquals(original.getEmbeddingDimensions(), copy.getEmbeddingDimensions());
    assertEquals(original.isCreateTableIfMissing(), copy.isCreateTableIfMissing());
    assertEquals(original.isCreateHnswIndex(), copy.isCreateHnswIndex());
    assertEquals(original.isDeleteDocumentBeforeUpsert(), copy.isDeleteDocumentBeforeUpsert());
    assertEquals(original.getCommitSize(), copy.getCommitSize());
    assertEquals(original.getIndexMetric(), copy.getIndexMetric());
    assertEquals(2, copy.getColumnMappings().size());
    assertEquals("url", copy.getColumnMappings().get(0).getColumnName());
    assertEquals("source_url", copy.getColumnMappings().get(0).getStreamField());
    assertEquals("origin", copy.getColumnMappings().get(1).getColumnName());
  }

  /** clone() has to deep-copy the mapping list, or edits leak between the dialog and the model. */
  @Test
  void cloneDeepCopiesTheColumnMappings() {
    PgVectorUpsertMeta original = new PgVectorUpsertMeta();
    original.setColumnMappings(new ArrayList<>(List.of(new PgVectorColumnMapping("url", "src"))));

    PgVectorUpsertMeta copy = (PgVectorUpsertMeta) original.clone();
    copy.getColumnMappings().get(0).setColumnName("changed");

    assertEquals("url", original.getColumnMappings().get(0).getColumnName());
  }

  @Test
  void checkRejectsAReservedColumnMapping() {
    PgVectorUpsertMeta meta = validMeta();
    meta.setColumnMappings(List.of(new PgVectorColumnMapping("content", "body")));

    List<ICheckResult> remarks = check(meta);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("content")),
        "mapping a managed column must be rejected");
  }

  @Test
  void checkRejectsMissingIdSources() {
    PgVectorUpsertMeta meta = validMeta();
    meta.setIdField("");
    meta.setDocumentIdField("");
    meta.setChunkIndexField("");

    assertTrue(check(meta).stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR));
  }

  private static PgVectorUpsertMeta validMeta() {
    PgVectorUpsertMeta meta = new PgVectorUpsertMeta();
    meta.setDefault();
    meta.setConnection("pgvector-local");
    meta.setCreateTableIfMissing(false);
    return meta;
  }

  private static List<ICheckResult> check(PgVectorUpsertMeta meta) {
    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("chunk_text"));
    prev.addValueMeta(new ValueMetaString("embedding"));
    prev.addValueMeta(new ValueMetaString("document_id"));
    prev.addValueMeta(new ValueMetaString("chunk_index"));
    prev.addValueMeta(new ValueMetaString("body"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);
    return remarks;
  }

  private static PgVectorUpsertMeta roundTrip(PgVectorUpsertMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), PgVectorUpsertMeta.class, null);
  }
}
