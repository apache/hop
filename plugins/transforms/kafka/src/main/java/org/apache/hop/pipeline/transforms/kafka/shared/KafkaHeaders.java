/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.kafka.shared;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Converts Kafka record headers to and from the textual form carried in a pipeline field.
 *
 * <p>A record carries an ordered list of name/value pairs and the same name may appear more than
 * once, which a flat row column cannot represent. The exchange format is therefore a JSON array of
 * {@code {"name":..,"value":..}} objects rather than a JSON object: an object would silently drop
 * repeats and lose ordering. Keeping both sides of the conversion here means the Kafka Consumer and
 * Kafka Producer cannot drift apart, so headers survive a consume-then-produce round trip.
 *
 * <p>Header values are bytes on the wire and are treated as UTF-8 text. A null value is preserved
 * as JSON null, which is distinct from an empty value.
 */
public final class KafkaHeaders {

  private static final Class<?> PKG = KafkaHeaders.class;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String NAME = "name";
  private static final String VALUE = "value";

  private KafkaHeaders() {
    // Utility class
  }

  /**
   * Renders record headers as a JSON array, preserving order and repeated names.
   *
   * @param headers the headers to render, may be null
   * @return a JSON array document, empty when there are no headers
   */
  public static String toJson(Headers headers) {
    ArrayNode array = JsonNodeFactory.instance.arrayNode();
    if (headers != null) {
      for (Header header : headers) {
        ObjectNode node = array.addObject();
        node.put(NAME, header.key());
        if (header.value() == null) {
          node.putNull(VALUE);
        } else {
          node.put(VALUE, new String(header.value(), StandardCharsets.UTF_8));
        }
      }
    }
    return array.toString();
  }

  /**
   * Parses the document produced by {@link #toJson(Headers)} back into record headers.
   *
   * @param json the headers document
   * @return the parsed headers, in document order
   * @throws HopException if the document is not a JSON array of name/value objects
   */
  public static List<Header> fromJson(String json) throws HopException {
    JsonNode root;
    try {
      root = OBJECT_MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new HopException(BaseMessages.getString(PKG, "KafkaHeaders.Error.NotJson", json), e);
    }
    if (root == null || !root.isArray()) {
      throw new HopException(BaseMessages.getString(PKG, "KafkaHeaders.Error.NotArray", json));
    }

    List<Header> headers = new ArrayList<>();
    for (JsonNode element : root) {
      JsonNode name = element.get(NAME);
      if (name == null || !name.isTextual()) {
        throw new HopException(BaseMessages.getString(PKG, "KafkaHeaders.Error.MissingName", json));
      }
      JsonNode value = element.get(VALUE);
      byte[] bytes =
          (value == null || value.isNull())
              ? null
              : value.asText().getBytes(StandardCharsets.UTF_8);
      headers.add(new RecordHeader(name.asText(), bytes));
    }
    return headers;
  }
}
