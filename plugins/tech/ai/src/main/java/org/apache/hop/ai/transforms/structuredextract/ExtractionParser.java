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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;

/**
 * Reads the model's answer back into typed Hop values.
 *
 * <p>Nothing here guesses. A value that will not coerce is reported with the field that caused it,
 * so the row can go to an error hop naming the problem. Silently writing null would be worse than
 * failing: downstream, an absent value and a misread value look identical, and only one of them is
 * the model's honest answer.
 */
public final class ExtractionParser {

  private ExtractionParser() {}

  /**
   * @param json the model's answer, possibly wrapped in a markdown fence
   * @param fields the grid rows, in output order
   * @return one value per field, in the same order, with null for anything the model omitted
   * @throws HopException when the answer is not an object, or a value will not coerce
   */
  public static Object[] parse(String json, List<StructuredExtractField> fields)
      throws HopException {
    JsonNode root = readTree(json);
    Object[] values = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      StructuredExtractField field = fields.get(i);
      JsonNode node = root.get(field.getName());
      values[i] = node == null || node.isNull() ? null : coerce(node, field);
    }
    return values;
  }

  /**
   * Models wrap JSON in ```json fences even when told not to, so the fence is stripped rather than
   * treated as a failure. Anything else is a genuine protocol error.
   */
  static JsonNode readTree(String json) throws HopException {
    String text = json == null ? "" : json.trim();
    if (text.startsWith("```")) {
      int firstNewline = text.indexOf('\n');
      int lastFence = text.lastIndexOf("```");
      if (firstNewline > 0 && lastFence > firstNewline) {
        text = text.substring(firstNewline + 1, lastFence).trim();
      }
    }
    if (text.isEmpty()) {
      throw new HopException("The model returned nothing to read fields from");
    }
    try {
      // Without this, Jackson parses every decimal through a double and 1.10 arrives as 1.1.
      // BigNumber is the type people choose precisely when that matters.
      ObjectMapper mapper =
          HopJson.newMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
      JsonNode root = mapper.readTree(text);
      if (root == null || !root.isObject()) {
        throw new HopException(
            "Expected a JSON object from the model but got: " + abbreviate(text));
      }
      return root;
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("The model did not return readable JSON: " + abbreviate(text), e);
    }
  }

  private static Object coerce(JsonNode node, StructuredExtractField field) throws HopException {
    int type = ExtractionSchema.typeOf(field);
    String text = node.isValueNode() ? node.asText() : node.toString();
    if (Utils.isEmpty(text)) {
      return null;
    }
    try {
      return switch (type) {
        case IValueMeta.TYPE_INTEGER -> Long.valueOf(text.trim());
        case IValueMeta.TYPE_NUMBER -> Double.valueOf(text.trim());
        case IValueMeta.TYPE_BIGNUMBER ->
            node.isNumber() ? node.decimalValue() : new BigDecimal(text.trim());
        case IValueMeta.TYPE_BOOLEAN -> toBoolean(text.trim(), field);
        case IValueMeta.TYPE_DATE, IValueMeta.TYPE_TIMESTAMP -> toDate(text.trim(), field);
        default -> text;
      };
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Field '"
              + field.getName()
              + "' came back as '"
              + abbreviate(text)
              + "', which is not a valid "
              + field.getType(),
          e);
    }
  }

  private static Boolean toBoolean(String text, StructuredExtractField field) throws HopException {
    if (text.equalsIgnoreCase("true") || text.equalsIgnoreCase("yes") || text.equals("1")) {
      return Boolean.TRUE;
    }
    if (text.equalsIgnoreCase("false") || text.equalsIgnoreCase("no") || text.equals("0")) {
      return Boolean.FALSE;
    }
    throw new HopException(
        "Field '"
            + field.getName()
            + "' came back as '"
            + abbreviate(text)
            + "', which is not a"
            + " yes or no answer");
  }

  private static java.util.Date toDate(String text, StructuredExtractField field)
      throws HopException {
    // The schema asks for yyyy-MM-dd. A model that adds a time is being helpful rather than wrong,
    // so the longer form is accepted too; anything else is reported.
    for (String pattern : new String[] {ExtractionSchema.DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ss"}) {
      SimpleDateFormat format = new SimpleDateFormat(pattern);
      format.setLenient(false);
      try {
        return format.parse(text.length() > 19 ? text.substring(0, 19) : text);
      } catch (ParseException e) {
        // try the next pattern
      }
    }
    throw new HopException(
        "Field '"
            + field.getName()
            + "' came back as '"
            + abbreviate(text)
            + "', which is not a date formatted as "
            + ExtractionSchema.DATE_FORMAT);
  }

  private static String abbreviate(String text) {
    return text.length() <= 120 ? text : text.substring(0, 117) + "...";
  }
}
