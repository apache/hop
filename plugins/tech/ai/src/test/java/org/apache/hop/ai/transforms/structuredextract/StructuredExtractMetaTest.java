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
package org.apache.hop.ai.transforms.structuredextract;

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

class StructuredExtractMetaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void survivesAnXmlRoundTripIncludingTheFieldGrid() throws Exception {
    StructuredExtractMeta original = completeMeta();
    original.getFields().get(0).setAllowedValues("low,high");

    StructuredExtractMeta copy = roundTrip(original);

    assertEquals("openai-prod", copy.getAiProvider());
    assertEquals(2, copy.getFields().size());
    assertEquals("severity", copy.getFields().get(0).getName());
    assertEquals("low,high", copy.getFields().get(0).getAllowedValues());
    assertEquals("Number", copy.getFields().get(1).getType());
    assertFalse(copy.getFields().get(1).isRequired());
  }

  @Test
  void addsOneTypedColumnPerField() throws Exception {
    IRowMeta row = inputFields();

    completeMeta().getFields(row, "Extract", null, null, new Variables(), null);

    assertEquals(3, row.size(), "the input field plus the two extracted ones");
    assertEquals("severity", row.getValueMeta(1).getName());
    assertEquals(IValueMeta.TYPE_STRING, row.getValueMeta(1).getType());
    assertEquals("total", row.getValueMeta(2).getName());
    assertEquals(IValueMeta.TYPE_NUMBER, row.getValueMeta(2).getType());
  }

  @Test
  void reportsAFieldThatWouldCollideWithTheInputStream() {
    // Two columns with one name is a confusing stream, and the extracted one would shadow.
    StructuredExtractMeta meta = completeMeta();
    meta.getFields().get(0).setName("ticket_text");

    assertTrue(
        errorText(check(meta)).contains("already in the input stream"), errorText(check(meta)));
  }

  @Test
  void reportsADuplicateFieldName() {
    StructuredExtractMeta meta = completeMeta();
    meta.getFields().get(1).setName("severity");

    assertTrue(errorText(check(meta)).contains("more than once"), errorText(check(meta)));
  }

  @Test
  void reportsATypeAModelCannotReturn() {
    StructuredExtractMeta meta = completeMeta();
    meta.getFields().get(0).setType("Binary");

    assertTrue(errorText(check(meta)).contains("cannot be extracted"), errorText(check(meta)));
  }

  @Test
  void reportsAnEmptyGrid() {
    StructuredExtractMeta meta = completeMeta();
    meta.setFields(new ArrayList<>());

    assertTrue(errorText(check(meta)).contains("At least one field"), errorText(check(meta)));
  }

  @Test
  void warnsAboutAFieldWithNoDescription() {
    // Not an error, but the description is the main lever a user has on extraction quality.
    StructuredExtractMeta meta = completeMeta();
    meta.getFields().get(0).setDescription("");

    boolean warned =
        check(meta).stream()
            .anyMatch(
                r ->
                    r.getType() == ICheckResult.TYPE_RESULT_WARNING
                        && r.getText().contains("no description"));
    assertTrue(warned);
  }

  @Test
  void acceptsAValidConfiguration() {
    assertTrue(errorText(check(completeMeta())).isEmpty(), errorText(check(completeMeta())));
  }

  @Test
  void aFieldNamedOnlyWithSpacesIsIgnoredEverywhere() throws Exception {
    // Utils.isEmpty is false for a run of spaces, so testing the raw name would add a column with
    // no name to the stream and describe it to the model.
    StructuredExtractMeta meta = completeMeta();
    meta.getFields().add(new StructuredExtractField("   ", "String", "blank row", true));
    IRowMeta row = inputFields();

    meta.getFields(row, "Extract", null, null, new Variables(), null);

    assertEquals(3, row.size(), "the blank row adds no column");
    assertTrue(errorText(check(meta)).isEmpty(), errorText(check(meta)));
  }

  @Test
  void supportsAnErrorHop() {
    // processRow calls putError, which Hop Gui only offers when this says so.
    assertTrue(new StructuredExtractMeta().supportsErrorHandling());
  }

  @Test
  void everyPropertyIsOnTheDialog() {
    for (Field field : StructuredExtractMeta.class.getDeclaredFields()) {
      if (field.getAnnotation(HopMetadataProperty.class) == null
          // The field grid is built by the dialog through registerExtraGroup.
          || "fields".equals(field.getName())) {
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
    for (Field field : StructuredExtractMeta.class.getDeclaredFields()) {
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
  void theCategoryResolves() {
    // A dangling categoryDescription puts the transform under a raw key in the Hop Gui tree.
    Transform annotation = StructuredExtractMeta.class.getAnnotation(Transform.class);
    String category =
        TranslateUtil.translate(annotation.categoryDescription(), StructuredExtractMeta.class);
    assertFalse(
        category.startsWith("!") && category.endsWith("!"),
        "the transform category does not resolve: " + category);
  }

  private static void assertResolves(String fieldName, String what, String value) {
    String translated = TranslateUtil.translate(value, StructuredExtractMeta.class);
    assertFalse(
        translated.startsWith("!") && translated.endsWith("!"),
        "The " + what + " of " + fieldName + " does not resolve: " + translated);
  }

  private static StructuredExtractMeta completeMeta() {
    StructuredExtractMeta meta = new StructuredExtractMeta();
    meta.setDefault();
    meta.setAiProvider("openai-prod");
    meta.setInputField("ticket_text");
    List<StructuredExtractField> fields = new ArrayList<>();
    fields.add(new StructuredExtractField("severity", "String", "how urgent the ticket is", true));
    fields.add(new StructuredExtractField("total", "Number", "amount in euros", false));
    meta.setFields(fields);
    return meta;
  }

  private static IRowMeta inputFields() {
    IRowMeta row = new RowMeta();
    row.addValueMeta(new ValueMetaString("ticket_text"));
    return row;
  }

  private static List<ICheckResult> check(StructuredExtractMeta meta) {
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

  private static StructuredExtractMeta roundTrip(StructuredExtractMeta original) throws Exception {
    String xml = "<transform>" + XmlMetadataUtil.serializeObjectToXml(original) + "</transform>";
    Document document = XmlHandler.loadXmlString(xml);
    return XmlMetadataUtil.deSerializeFromXml(
        XmlHandler.getSubNode(document, "transform"), StructuredExtractMeta.class, null);
  }
}
