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
 *
 */

package org.apache.hop.core.row;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.TimeZone;
import org.apache.commons.lang3.LocaleUtils;
import org.apache.hop.core.row.value.ValueMetaFactory;

public class ValueMetaDeserializer extends JsonDeserializer<IValueMeta> {
  @Override
  public IValueMeta deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    ObjectMapper objectMapper = (ObjectMapper) jsonParser.getCodec();
    ObjectNode root = objectMapper.readTree(jsonParser);
    int type = root.get("type").asInt();
    String name = root.get("name").asText();
    int length = root.get("length").asInt();
    int precision = root.get("precision").asInt();

    try {
      IValueMeta valueMeta = ValueMetaFactory.createValueMeta(name, type, length, precision);
      valueMeta.setTrimType(root.get("trimType").asInt());
      valueMeta.setStorageType(root.get("storageType").asInt());
      valueMeta.setRoundingType(root.get("roundingType").asText());
      valueMeta.setConversionMask(asString(root.get("conversionMask")));
      JsonNode stringEncoding = root.get("stringEncoding");
      if (!stringEncoding.isNull()) {
        valueMeta.setStringEncoding(stringEncoding.asText());
      }
      valueMeta.setDecimalSymbol(asString(root.get("decimalSymbol")));
      valueMeta.setGroupingSymbol(asString(root.get("groupingSymbol")));
      valueMeta.setCurrencySymbol(asString(root.get("currencySymbol")));
      valueMeta.setCollatorStrength(root.get("collatorStrength").asInt());
      valueMeta.setCaseInsensitive(root.get("caseInsensitive").asBoolean());
      valueMeta.setCollatorDisabled(root.get("collatorDisabled").asBoolean());
      valueMeta.setCollatorLocale(LocaleUtils.toLocale(root.get("collatorLocale").asText()));
      valueMeta.setSortedDescending(root.get("sortedDescending").asBoolean());
      valueMeta.setOutputPaddingEnabled(root.get("outputPaddingEnabled").asBoolean());
      valueMeta.setLargeTextField(root.get("largeTextField").asBoolean());
      valueMeta.setDateFormatLocale(LocaleUtils.toLocale((root.get("dateFormatLocale").asText())));
      valueMeta.setDateFormatTimeZone(
          TimeZone.getTimeZone(root.get("dateFormatTimeZone").asText()));
      valueMeta.setDateFormatLenient(root.get("dateFormatLenient").asBoolean());
      valueMeta.setIgnoreWhitespace(root.get("ignoreWhitespace").asBoolean());

      return valueMeta;
    } catch (Exception e) {
      throw new IOException("Error creating value metadata '" + name + "' of type " + type, e);
    }
  }

  private String asString(JsonNode node) {
    if (node.isNull()) {
      return null;
    }
    return node.asText();
  }
}
