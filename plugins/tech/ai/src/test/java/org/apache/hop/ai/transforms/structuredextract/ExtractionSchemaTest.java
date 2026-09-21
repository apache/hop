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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.langchain4j.model.chat.request.json.JsonAnyOfSchema;
import dev.langchain4j.model.chat.request.json.JsonBooleanSchema;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonIntegerSchema;
import dev.langchain4j.model.chat.request.json.JsonNullSchema;
import dev.langchain4j.model.chat.request.json.JsonNumberSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.util.List;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ExtractionSchemaTest {

  @BeforeAll
  static void setUpClass() throws Exception {
    HopClientEnvironment.init();
  }

  @Test
  void mapsEachHopTypeToItsJsonCounterpart() throws Exception {
    JsonObjectSchema root =
        root(
            ExtractionSchema.build(
                List.of(
                    field("a", "String"),
                    field("b", "Integer"),
                    field("c", "Number"),
                    field("d", "Boolean"),
                    field("e", "Date")),
                "test"));

    assertInstanceOf(JsonStringSchema.class, root.properties().get("a"));
    assertInstanceOf(JsonIntegerSchema.class, root.properties().get("b"));
    assertInstanceOf(JsonNumberSchema.class, root.properties().get("c"));
    assertInstanceOf(JsonBooleanSchema.class, root.properties().get("d"));
    // JSON has no date type, so a date is a string with the format spelled out.
    assertInstanceOf(JsonStringSchema.class, root.properties().get("e"));
  }

  @Test
  void tellsTheModelWhichDateFormatToUse() throws Exception {
    JsonObjectSchema root = root(ExtractionSchema.build(List.of(field("signed", "Date")), "test"));

    JsonStringSchema signed = (JsonStringSchema) root.properties().get("signed");
    assertTrue(signed.description().contains("yyyy-MM-dd"), signed.description());
  }

  @Test
  void aListOfAllowedValuesBecomesAnEnum() throws Exception {
    // This is what makes classification reliable: the model is constrained, not asked.
    StructuredExtractField field = field("severity", "String");
    field.setAllowedValues("low, medium , high,high");

    JsonObjectSchema root = root(ExtractionSchema.build(List.of(field), "test"));

    JsonEnumSchema severity = (JsonEnumSchema) root.properties().get("severity");
    assertEquals(List.of("low", "medium", "high"), severity.enumValues(), "trimmed and deduped");
  }

  @Test
  void onlyRequiredFieldsAreRequired() throws Exception {
    JsonObjectSchema root =
        root(
            ExtractionSchema.build(
                List.of(
                    new StructuredExtractField("must", "String", "d", true),
                    new StructuredExtractField("may", "String", "d", false)),
                "test"));

    assertEquals(List.of("must"), root.required());
  }

  @Test
  void anOptionalFieldCanAnswerNull() throws Exception {
    // Models would rather answer than say nothing. Asked for an amount the text never mentions,
    // phi3 returns 0, which reads as "nothing was lost" rather than "the text does not say".
    // Offering null as a branch of the schema makes "not present" something it can pick.
    JsonObjectSchema root =
        root(
            ExtractionSchema.build(
                List.of(new StructuredExtractField("loss", "Number", "money lost", false)),
                "test"));

    JsonAnyOfSchema loss = assertInstanceOf(JsonAnyOfSchema.class, root.properties().get("loss"));
    assertEquals(2, loss.anyOf().size());
    assertInstanceOf(JsonNumberSchema.class, loss.anyOf().get(0));
    assertInstanceOf(JsonNullSchema.class, loss.anyOf().get(1));
  }

  @Test
  void aRequiredFieldIsNotWrappedInAnyOf() throws Exception {
    JsonObjectSchema root =
        root(
            ExtractionSchema.build(
                List.of(new StructuredExtractField("loss", "Number", "money lost", true)), "test"));

    assertInstanceOf(JsonNumberSchema.class, root.properties().get("loss"));
  }

  @Test
  void theDescriptionReachesTheModel() throws Exception {
    // The description column is the main thing a user can tune, so it has to be carried through.
    JsonObjectSchema root =
        root(
            ExtractionSchema.build(
                List.of(new StructuredExtractField("total", "Number", "total in euros", true)),
                "test"));

    assertEquals("total in euros", root.properties().get("total").description());
  }

  @Test
  void rejectsATypeAModelCannotReturn() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionSchema.build(List.of(field("blob", "Binary")), "test"));

    assertTrue(e.getMessage().contains("cannot return"), e.getMessage());
  }

  @Test
  void rejectsAnUnknownType() {
    HopException e =
        assertThrows(
            HopException.class,
            () -> ExtractionSchema.build(List.of(field("x", "Elephant")), "test"));

    assertTrue(e.getMessage().contains("unknown type"), e.getMessage());
  }

  @Test
  void rejectsADuplicateFieldName() {
    HopException e =
        assertThrows(
            HopException.class,
            () ->
                ExtractionSchema.build(
                    List.of(field("total", "Number"), field("total", "String")), "test"));

    assertTrue(e.getMessage().contains("more than once"), e.getMessage());
  }

  @Test
  void rejectsAnEmptyGrid() {
    assertThrows(HopException.class, () -> ExtractionSchema.build(List.of(), "test"));
  }

  private static JsonObjectSchema root(JsonSchema schema) {
    return (JsonObjectSchema) schema.rootElement();
  }

  private static StructuredExtractField field(String name, String type) {
    return new StructuredExtractField(name, type, "the " + name, true);
  }
}
