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
package org.apache.hop.ai.transforms.embedtext;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.serializer.xml.XmlMetadataUtil;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;

class EmbedTextMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void survivesAnXmlRoundTrip() throws Exception {
    EmbedTextMeta original = completeMeta();
    original.setOutputFormat(EmbedTextOutputFormat.VECTOR);
    original.setBatchSize("32");

    EmbedTextMeta copy = roundTrip(original);

    assertEquals("ollama-local", copy.getAiProvider());
    assertEquals(EmbedTextOutputFormat.VECTOR, copy.getOutputFormat());
    assertEquals("32", copy.getBatchSize());
    assertEquals("chunk_text", copy.getInputField());
  }

  @Test
  void addsTheEmbeddingAndItsMetadataToTheOutputRow() throws Exception {
    EmbedTextMeta meta = completeMeta();
    IRowMeta row = inputFields();

    meta.getFields(row, "Embed", null, null, new Variables(), null);

    assertEquals(4, row.size(), "one embedding field plus the two metadata fields");
    assertEquals("embedding", row.getValueMeta(1).getName());
    assertEquals("embedding_model", row.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_INTEGER, row.getValueMeta(3).getType());
  }

  @Test
  void addsOnlyTheEmbeddingWhenMetadataIsOff() throws Exception {
    EmbedTextMeta meta = completeMeta();
    meta.setIncludeModelMetadata(false);
    IRowMeta row = inputFields();

    meta.getFields(row, "Embed", null, null, new Variables(), null);

    assertEquals(2, row.size());
  }

  @Test
  void fallsBackToAStringWhenTheVectorTypeIsNotInstalled() throws Exception {
    // The Vector value type is an optional plugin, and this module does not depend on it. Without
    // it the embedding still has to reach the stream, as a JSON array.
    EmbedTextMeta meta = completeMeta();
    meta.setOutputFormat(EmbedTextOutputFormat.VECTOR);
    IRowMeta row = inputFields();

    meta.getFields(row, "Embed", null, null, new Variables(), null);

    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(1).getType());
  }

  @Test
  void acceptsAVariableForBatchSize() {
    EmbedTextMeta meta = completeMeta();
    meta.setBatchSize("${EMBED_BATCH_SIZE}");

    assertTrue(
        errorText(check(meta)).isEmpty(), "a variable is resolved at run time, not a design error");
  }

  @Test
  void stillRejectsAFixedBatchSizeBelowOne() {
    EmbedTextMeta meta = completeMeta();
    meta.setBatchSize("0");

    assertTrue(errorText(check(meta)).contains("Batch size"), errorText(check(meta)));
  }

  @Test
  void reportsAMissingProviderAndInputField() {
    EmbedTextMeta meta = new EmbedTextMeta();
    meta.setDefault();
    meta.setInputField("not_in_the_stream");

    String errors = errorText(check(meta));

    assertTrue(errors.contains("AI provider is required"), errors);
    assertTrue(errors.contains("not found"), errors);
  }

  @Test
  void supportsAnErrorHop() {
    // processRow calls putError, which Hop Gui only offers when this says so.
    assertTrue(new EmbedTextMeta().supportsErrorHandling());
  }

  @Test
  void everyPropertyIsOnTheDialog() {
    for (Field field : EmbedTextMeta.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null) {
        continue;
      }
      assertTrue(
          field.getAnnotation(GuiWidgetElement.class) != null,
          "Property "
              + field.getName()
              + " is persisted but has no @GuiWidgetElement, so it is missing from the dialog");
    }
  }

  @Test
  void everyWidgetLabelTooltipAndTabResolves() {
    for (Field field : EmbedTextMeta.class.getDeclaredFields()) {
      GuiWidgetElement widget = field.getAnnotation(GuiWidgetElement.class);
      if (widget == null) {
        continue;
      }
      assertResolves(field.getName(), "label", widget.label());
      assertResolves(field.getName(), "toolTip", widget.toolTip());
      assertResolves(field.getName(), "group", widget.group());
    }
  }

  @Test
  void everyEnumOnTheDialogReadsBackFromItsDisplayedText() {
    // Generated dialogs fill an enum combo with toString() and read it back with Enum.valueOf.
    for (EmbedTextOutputFormat format : EmbedTextOutputFormat.values()) {
      assertEquals(format.name(), format.toString());
    }
  }

  @Test
  void aVariableThatResolvesToNothingAddsNoColumn() {
    // The transform writes the metadata slots only when the resolved name is non-empty. If
    // getFields judged the raw value instead, the two would disagree and a String would land in
    // the Integer dimensions column.
    EmbedTextMeta meta = completeMeta();
    meta.setModelField("${EMPTY_MODEL_FIELD}");
    Variables variables = new Variables();
    variables.setVariable("EMPTY_MODEL_FIELD", "");
    IRowMeta row = inputFields();

    assertDoesNotThrow(() -> meta.getFields(row, "Embed", null, null, variables, null));

    assertEquals(3, row.size(), "embedding and dimensions, but no model column");
    assertEquals("embedding_dimensions", row.getValueMeta(2).getName());
  }

  @Test
  void theCategoryResolves() {
    // A dangling categoryDescription puts the transform under a raw key in the Hop Gui tree.
    Transform annotation = EmbedTextMeta.class.getAnnotation(Transform.class);
    String category =
        TranslateUtil.translate(annotation.categoryDescription(), EmbedTextMeta.class);
    assertFalse(
        category.startsWith("!") && category.endsWith("!"),
        "the transform category does not resolve: " + category);
  }

  private static void assertResolves(String fieldName, String what, String value) {
    String translated = TranslateUtil.translate(value, EmbedTextMeta.class);
    assertFalse(
        translated.startsWith("!") && translated.endsWith("!"),
        "The " + what + " of " + fieldName + " does not resolve: " + translated);
  }

  private static EmbedTextMeta completeMeta() {
    EmbedTextMeta meta = new EmbedTextMeta();
    meta.setDefault();
    meta.setAiProvider("ollama-local");
    return meta;
  }

  private static IRowMeta inputFields() {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("chunk_text"));
    return row;
  }

  private static List<ICheckResult> check(EmbedTextMeta meta) {
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks, null, new TransformMeta(), inputFields(), null, null, null, new Variables(), null);
    return remarks;
  }

  private static String errorText(List<ICheckResult> remarks) {
    return remarks.stream()
        .filter(r -> r.getType() == ICheckResult.TYPE_RESULT_ERROR)
        .map(ICheckResult::getText)
        .collect(java.util.stream.Collectors.joining(" | "));
  }

  private static EmbedTextMeta roundTrip(EmbedTextMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), EmbedTextMeta.class, null);
  }
}
