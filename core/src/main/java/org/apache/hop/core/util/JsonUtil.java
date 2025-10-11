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

package org.apache.hop.core.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;

public class JsonUtil {

  private static final ObjectReader jsonReader =
      com.fasterxml.jackson.databind.json.JsonMapper.builder()
          .enable(com.fasterxml.jackson.core.json.JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES)
          .build()
          .reader();

  /** singleton to parse Object into JsonNode */
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  private JsonUtil() {}

  public static ObjectReader jsonReader() {
    return jsonReader;
  }

  public static ObjectMapper jsonMapper() {
    return jsonMapper;
  }

  public static JsonNode parse(byte[] bytes) throws IOException {
    if (bytes == null) {
      return null;
    }
    return jsonReader.readTree(bytes);
  }

  public static JsonNode parse(CharSequence text) throws JsonProcessingException {
    if (text == null) {
      return null;
    }
    return jsonReader.readTree(text.toString());
  }

  public static JsonNode parse(InputStream in) throws IOException {
    if (in == null) {
      return null;
    }
    return jsonReader.readTree(in);
  }

  public static JsonNode parseTextValue(Object value) throws Exception {
    if (value == null) return null;
    if (value instanceof byte[] b) return parse(b);
    if (value instanceof CharSequence cs) return parse(cs);
    if (value instanceof InputStream is) return parse(is);

    return parse(value.toString());
  }

  public static JsonNode mapObjectToJson(Object object) {
    if (object == null) {
      return null;
    }

    if (object instanceof JsonNode node) {
      return node;
    }

    return jsonMapper.valueToTree(object);
  }

  public static String mapJsonToString(JsonNode jsonNode, boolean prettyPrinting)
      throws JsonProcessingException {
    if (jsonNode == null) {
      return null;
    }
    if (prettyPrinting) {
      return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
    }
    return jsonMapper.writeValueAsString(jsonNode);
  }

  public static byte[] mapJsonToBytes(JsonNode jsonNode) throws JsonProcessingException {
    if (jsonNode == null) {
      return null;
    }
    // encoded in UTF8
    return jsonMapper.writeValueAsBytes(jsonNode);
  }
}
