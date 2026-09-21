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

import dev.langchain4j.model.chat.request.json.JsonAnyOfSchema;
import dev.langchain4j.model.chat.request.json.JsonBooleanSchema;
import dev.langchain4j.model.chat.request.json.JsonEnumSchema;
import dev.langchain4j.model.chat.request.json.JsonIntegerSchema;
import dev.langchain4j.model.chat.request.json.JsonNullSchema;
import dev.langchain4j.model.chat.request.json.JsonNumberSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonSchemaElement;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;

/**
 * Turns the field grid into a JSON schema the model is constrained by.
 *
 * <p>Constraining the model is the point. Asking for JSON in a prompt and hoping gets you prose,
 * markdown fences and invented fields; a schema gets you the shape you asked for, on every provider
 * that supports it.
 */
public final class ExtractionSchema {

  /** JSON has no date type, so dates travel as text and this says which text. */
  static final String DATE_FORMAT = "yyyy-MM-dd";

  private static final String DATE_HINT = " Answer with a date formatted as " + DATE_FORMAT + ".";

  private ExtractionSchema() {}

  /**
   * @param fields the grid rows, in order
   * @param schemaName a name for the schema, shown to some providers
   * @return a schema with one property per field
   * @throws HopException when a field is unusable, for example unnamed or of an unknown type
   */
  public static JsonSchema build(List<StructuredExtractField> fields, String schemaName)
      throws HopException {
    if (fields == null || fields.isEmpty()) {
      throw new HopException("At least one field is needed to extract anything");
    }

    Map<String, JsonSchemaElement> properties = new LinkedHashMap<>();
    List<String> required = new ArrayList<>();
    for (StructuredExtractField field : fields) {
      if (field == null || field.trimmedName().isEmpty()) {
        continue;
      }
      String name = field.trimmedName();
      if (properties.containsKey(name)) {
        throw new HopException("Field '" + name + "' is listed more than once");
      }
      properties.put(name, elementFor(field));
      if (field.isRequired()) {
        required.add(name);
      }
    }
    if (properties.isEmpty()) {
      throw new HopException("At least one field needs a name");
    }

    return JsonSchema.builder()
        .name(Utils.isEmpty(schemaName) ? "extraction" : schemaName)
        .rootElement(
            JsonObjectSchema.builder()
                .addProperties(properties)
                .required(required)
                .additionalProperties(false)
                .build())
        .build();
  }

  private static JsonSchemaElement elementFor(StructuredExtractField field) throws HopException {
    JsonSchemaElement element = typedElementFor(field);
    if (field.isRequired()) {
      return element;
    }
    // Leaving an optional field out of "required" allows the model to omit it, but models would
    // rather answer than say nothing: asked for a loss the text never mentions, one returns 0,
    // which reads downstream as "nothing was lost" rather than "the text does not say". Offering
    // null as a branch of the schema makes "not present" something the model can actually pick.
    return JsonAnyOfSchema.builder()
        .description(element.description())
        .anyOf(element, new JsonNullSchema())
        .build();
  }

  private static JsonSchemaElement typedElementFor(StructuredExtractField field)
      throws HopException {
    String description = Utils.isEmpty(field.getDescription()) ? null : field.getDescription();

    List<String> allowed = allowedValues(field);
    if (!allowed.isEmpty()) {
      // An enum constrains the model instead of asking it, which is what makes classification
      // reliable. It applies whatever the Hop type is, since the answer is still one of these.
      return JsonEnumSchema.builder().description(description).enumValues(allowed).build();
    }

    return switch (typeOf(field)) {
      case IValueMeta.TYPE_INTEGER -> JsonIntegerSchema.builder().description(description).build();
      case IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER ->
          JsonNumberSchema.builder().description(description).build();
      case IValueMeta.TYPE_BOOLEAN -> JsonBooleanSchema.builder().description(description).build();
      case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP ->
          JsonStringSchema.builder()
              .description((description == null ? "" : description) + DATE_HINT)
              .build();
      default -> JsonStringSchema.builder().description(description).build();
    };
  }

  /** The Hop type id for a field, rejecting anything that cannot survive a JSON round trip. */
  static int typeOf(StructuredExtractField field) throws HopException {
    String name = Utils.isEmpty(field.getType()) ? "String" : field.getType().trim();
    int type = ValueMetaFactory.getIdForValueMeta(name);
    if (type == IValueMeta.TYPE_NONE) {
      throw new HopException(
          "Field '" + field.getName() + "' has an unknown type '" + field.getType() + "'");
    }
    switch (type) {
      case IValueMeta.TYPE_BINARY, IValueMeta.TYPE_SERIALIZABLE, IValueMeta.TYPE_INET ->
          throw new HopException(
              "Field '"
                  + field.getName()
                  + "' is of type "
                  + name
                  + ", which a language model cannot return. Use String, Integer, Number, Boolean,"
                  + " Date or Timestamp.");
      default -> {
        // usable
      }
    }
    return type;
  }

  static List<String> allowedValues(StructuredExtractField field) {
    if (field == null || Utils.isEmpty(field.getAllowedValues())) {
      return List.of();
    }
    List<String> values = new ArrayList<>();
    for (String value : Arrays.asList(field.getAllowedValues().split(","))) {
      String trimmed = value.trim();
      if (!trimmed.isEmpty() && !values.contains(trimmed)) {
        values.add(trimmed);
      }
    }
    return values;
  }
}
