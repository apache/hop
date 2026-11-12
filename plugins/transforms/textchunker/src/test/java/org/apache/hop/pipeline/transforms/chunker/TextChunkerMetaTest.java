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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
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
    original.setParagraphSeparator("<PARA>");

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
    assertEquals(original.getParagraphSeparator(), copy.getParagraphSeparator());
  }

  /** A separator configured for a non-paragraph strategy is dead config and must be flagged. */
  @Test
  void checkWarnsWhenTheParagraphSeparatorIsSetForAnotherStrategy() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");
    meta.setParagraphSeparator("<PARA>");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("body"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().toLowerCase().contains("separator")),
        "a separator set for the Character strategy must produce a warning");
  }

  /**
   * Paragraph and Structure apply the overlap only inside a paragraph or section they split. The
   * check says so for both, as a comment: it used to warn, for Paragraph only, that the overlap was
   * ignored.
   */
  @Test
  void checkExplainsWhereParagraphAndStructureApplyTheOverlap() {
    for (ChunkingStrategyType type : ChunkingStrategyType.values()) {
      TextChunkerMeta meta = new TextChunkerMeta();
      meta.setDefault();
      meta.setInputField("body");
      meta.setChunkingStrategy(type);
      meta.setChunkOverlap("20");

      IRowMeta prev = new RowMeta();
      prev.addValueMeta(new ValueMetaString("body"));

      List<ICheckResult> remarks = new ArrayList<>();
      meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

      boolean explained =
          remarks.stream()
              .anyMatch(
                  r ->
                      r.getType() == ICheckResult.TYPE_RESULT_COMMENT
                          && r.getText().contains("overlap"));
      assertEquals(type != ChunkingStrategyType.CHARACTER, explained, type.name());
      assertTrue(
          remarks.stream().noneMatch(r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING),
          type + " with a valid overlap must not produce a warning");
    }
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

  /**
   * The transform writes Long values into an input field that has the same name as one of its
   * output fields, so that field must take the transform's type (#9285).
   */
  @Test
  void anExistingFieldTakesTheOutputType() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("text"));
    row.addValueMeta(new ValueMetaString("chunk_index"));
    row.addValueMeta(new ValueMetaString("total_chunks"));
    meta.getFields(row, "chunker", null, null, new Variables(), null);

    assertEquals(6, row.size(), "existing fields are reused, not duplicated");
    assertEquals(1, row.indexOfValue("chunk_index"), "the field keeps its position");
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(1).getType());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(2).getType());
  }

  /**
   * The transform writes plain String values, so an existing field of the same type must not keep
   * its own storage type, mask or length. With lazy conversion the input field is a binary string;
   * keeping that metadata would make later transforms read a String as byte[].
   */
  @Test
  void anExistingFieldOfTheSameTypeTakesTheOutputMetadata() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("body"));
    ValueMetaString lazy = new ValueMetaString("chunk_text", 5000, -1);
    lazy.setStorageType(IValueMeta.STORAGE_TYPE_BINARY_STRING);
    lazy.setStorageMetadata(new ValueMetaString("chunk_text"));
    lazy.setConversionMask("#");
    row.addValueMeta(lazy);
    meta.getFields(row, "chunker", null, null, new Variables(), null);

    IValueMeta chunkText = row.getValueMeta(1);
    assertEquals("chunk_text", chunkText.getName());
    assertEquals(IValueMeta.STORAGE_TYPE_NORMAL, chunkText.getStorageType());
    assertEquals(-1, chunkText.getLength());
    assertNull(chunkText.getConversionMask());
  }

  /**
   * Hop matches field names without regard to case. An input field named Chunk_Index is reused, and
   * keeps its name so later transforms that look it up by that name still find it.
   */
  @Test
  void anExistingFieldKeepsItsName() throws Exception {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("text");

    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("text"));
    row.addValueMeta(new ValueMetaString("Chunk_Index"));
    meta.getFields(row, "chunker", null, null, new Variables(), null);

    assertEquals(6, row.size(), "the existing field is reused, not duplicated");
    assertEquals("Chunk_Index", row.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(1).getType());
  }

  /** Two output fields with the same name give one warning, not two. */
  @Test
  void checkWarnsOnceForADuplicateOutputFieldName() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");
    meta.setChunkIndexField("n");
    meta.setChunkCountField("n");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("body"));
    prev.addValueMeta(new ValueMetaString("n"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertEquals(
        1,
        remarks.stream()
            .filter(
                r -> r.getType() == ICheckResult.TYPE_RESULT_WARNING && r.getText().contains("'n'"))
            .count());
  }

  /** When the output fields cannot be determined, the checks say so instead of staying silent. */
  @Test
  void checkReportsAnErrorWhenTheOutputFieldsCannotBeDetermined() {
    TextChunkerMeta meta =
        new TextChunkerMeta() {
          @Override
          public void getFields(
              IRowMeta row,
              String origin,
              IRowMeta[] info,
              TransformMeta nextTransform,
              IVariables variables,
              IHopMetadataProvider metadataProvider)
              throws HopTransformException {
            throw new HopTransformException("broken field");
          }
        };
    meta.setDefault();
    meta.setInputField("body");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("body"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("broken field")),
        "a getFields failure must be reported");
  }

  /** The checks and the transform share one set of rules for the chunk size and overlap. */
  @Test
  void validatesChunkSettings() {
    assertTrue(TextChunkerMeta.validateChunkSettings("100", "").isValid());
    assertEquals(0, TextChunkerMeta.validateChunkSettings("100", "").overlap());
    assertTrue(TextChunkerMeta.validateChunkSettings("100", "0").isValid());
    assertFalse(TextChunkerMeta.validateChunkSettings("100", "-1").isValid());
    assertFalse(TextChunkerMeta.validateChunkSettings("100", "abc").isValid());
    assertFalse(TextChunkerMeta.validateChunkSettings("0", "0").isValid());
    assertFalse(TextChunkerMeta.validateChunkSettings("", "0").isValid());
    assertEquals(2, TextChunkerMeta.validateChunkSettings("abc", "-1").errors().size());
  }

  @Test
  void checkWarnsWhenAnExistingFieldChangesType() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");

    IRowMeta prev = new RowMeta();
    prev.addValueMeta(new ValueMetaString("body"));
    prev.addValueMeta(new ValueMetaString("chunk_index"));

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), prev, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("chunk_index")),
        "a String chunk_index in the input must produce a warning");
  }

  @Test
  void checkReportsAnOverlapThatIsNotANumber() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");
    meta.setChunkOverlap("abc");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), null, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("'abc' is not a whole number")),
        "an overlap of 'abc' must be reported as not a number");
    assertFalse(
        remarks.stream().anyMatch(r -> r.getText().contains("negative")),
        "an overlap of 'abc' is not negative");
  }

  @Test
  void checkReportsANegativeOverlap() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");
    meta.setChunkOverlap("-5");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), null, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("'-5' cannot be negative")),
        "an overlap of -5 must be reported as negative");
  }

  @Test
  void checkReportsAChunkSizeThatIsNotANumber() {
    TextChunkerMeta meta = new TextChunkerMeta();
    meta.setDefault();
    meta.setInputField("body");
    meta.setChunkSize("abc");

    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(remarks, null, new TransformMeta(), null, null, null, null, new Variables(), null);

    assertTrue(
        remarks.stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_ERROR
                        && r.getText().contains("'abc' is not a whole number")),
        "a chunk size of 'abc' must be reported as not a number");
  }

  @Test
  void parsesSizeAndOverlap() {
    assertEquals(100, TextChunkerMeta.parseNumber(" 100 ", 0));
    assertEquals(0, TextChunkerMeta.parseNumber("", 0));
    assertEquals(0, TextChunkerMeta.parseNumber(null, 0));
    assertEquals(-5, TextChunkerMeta.parseNumber("-5", 0));
    assertNull(TextChunkerMeta.parseNumber("abc", 0));
    assertNull(TextChunkerMeta.parseNumber("1.5", 0));
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

  /**
   * The generated dialogs fill an enum combo with {@code toString()} and read it back with {@code
   * Enum.valueOf}. An enum that overrides {@code toString()} therefore cannot be read back, and a
   * changed selection is dropped without any error reaching the user.
   */
  @Test
  void everyEnumOnTheDialogReadsBackFromItsDisplayedText() {
    for (Enum<?>[] constants :
        new Enum<?>[][] {ChunkingStrategyType.values(), ContentType.values()}) {
      for (Enum<?> constant : constants) {
        assertEquals(
            constant.name(),
            constant.toString(),
            constant.getDeclaringClass().getSimpleName()
                + " must not override toString(): the dialog reads the combo back with"
                + " Enum.valueOf, which only accepts the constant name");
      }
    }
  }

  @Test
  void theStrategyStillParsesTheDisplayTextItUsedToShow() {
    // Pipelines saved while the combo showed "Character" have to keep working.
    assertEquals(ChunkingStrategyType.CHARACTER, ChunkingStrategyType.fromString("Character"));
    assertEquals(ChunkingStrategyType.PARAGRAPH, ChunkingStrategyType.fromString("Paragraph"));
    assertEquals(ChunkingStrategyType.STRUCTURE, ChunkingStrategyType.fromString("Structure"));
    assertEquals(ChunkingStrategyType.STRUCTURE, ChunkingStrategyType.fromString("STRUCTURE"));
  }

  private static TextChunkerMeta roundTrip(TextChunkerMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), TextChunkerMeta.class, null);
  }
}
