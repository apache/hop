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
package org.apache.hop.pipeline.transforms.chunker;

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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.chunker.chunking.ChunkingStrategyType;
import org.apache.hop.pipeline.transforms.chunker.document.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class TextChunkerMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  /**
   * Every {@code @HopMetadataProperty} has to survive a write/read cycle. Without this, a renamed
   * or mistyped key only shows up as silently lost settings after a save and reopen.
   */
  @Test
  void roundTripsEveryPropertyThroughXml() throws Exception {
    TextChunkerMeta original = new TextChunkerMeta();
    original.setInputField("body");
    original.setOutputChunkField("piece");
    original.setChunkingStrategy(ChunkingStrategyType.STRUCTURE);
    original.setChunkSize("512");
    original.setChunkOverlap("64");
    original.setIncludeMetadata(true);
    original.setChunkIndexField("idx");
    original.setChunkStartPosField("pos");
    original.setSourceDocumentIdField("doc");
    original.setDocumentIdField("doc_out");
    original.setChunkCountField("total");
    original.setContentType(ContentType.ASCIIDOC);
    original.setContentTypeField("source_type");

    TextChunkerMeta copy = roundTrip(original);

    assertEquals(original.getInputField(), copy.getInputField());
    assertEquals(original.getOutputChunkField(), copy.getOutputChunkField());
    assertEquals(original.getChunkingStrategy(), copy.getChunkingStrategy());
    assertEquals(original.getChunkSize(), copy.getChunkSize());
    assertEquals(original.getChunkOverlap(), copy.getChunkOverlap());
    assertEquals(original.isIncludeMetadata(), copy.isIncludeMetadata());
    assertEquals(original.getChunkIndexField(), copy.getChunkIndexField());
    assertEquals(original.getChunkStartPosField(), copy.getChunkStartPosField());
    assertEquals(original.getSourceDocumentIdField(), copy.getSourceDocumentIdField());
    assertEquals(original.getDocumentIdField(), copy.getDocumentIdField());
    assertEquals(original.getChunkCountField(), copy.getChunkCountField());
    assertEquals(original.getContentType(), copy.getContentType());
    assertEquals(original.getContentTypeField(), copy.getContentTypeField());
  }

  /** Chunk index, start position and total count are numbers, not strings. */
  @Test
  void exposesChunkMetadataAsIntegers() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("text"));
    meta.getFields(row, "chunker", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("chunk_text").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.searchValueMeta("chunk_index").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.searchValueMeta("chunk_start_position").getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.searchValueMeta("total_chunks").getType());
    assertEquals(IValueMeta.TYPE_STRING, row.searchValueMeta("chunk_doc_id").getType());
  }

  @Test
  void checkReportsMissingInputField() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), null, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream().anyMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "an empty input field must be reported as an error");
  }

  @Test
  void checkReportsInputFieldMissingFromTheStream() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("something_else"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR && r.getText().contains("body")),
        "an input field absent from the stream must be reported");
  }

  @Test
  void checkAcceptsAValidConfiguration() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("body"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR),
        "a valid configuration must not produce errors");
  }

  private static TextChunkerMeta roundTrip(TextChunkerMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), TextChunkerMeta.class, null);
  }
}
