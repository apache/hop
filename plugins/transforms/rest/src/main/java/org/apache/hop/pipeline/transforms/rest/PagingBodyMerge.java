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

package org.apache.hop.pipeline.transforms.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.ws.rs.core.MediaType;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.util.Utils;

/** Merges paging tokens into HTTP request bodies for {@link RestPaginationType#BODY_CURSOR}. */
final class PagingBodyMerge {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private PagingBodyMerge() {}

  static String merge(
      String entityString,
      LinkedHashMap<String, String> pagingBodyParams,
      String contentTypeHeader) {
    if (pagingBodyParams == null || pagingBodyParams.isEmpty()) {
      return entityString;
    }
    if (isJsonContentType(contentTypeHeader)) {
      return mergeJsonBody(entityString, pagingBodyParams);
    }
    return mergeFormUrlEncodedBody(entityString, pagingBodyParams);
  }

  static boolean isJsonContentType(String contentTypeHeader) {
    if (Utils.isEmpty(contentTypeHeader)) {
      return false;
    }
    try {
      return MediaType.APPLICATION_JSON_TYPE.isCompatible(MediaType.valueOf(contentTypeHeader));
    } catch (IllegalArgumentException ex) {
      return contentTypeHeader.toLowerCase().contains("json");
    }
  }

  static boolean isFormUrlEncodedContentType(String contentTypeHeader) {
    if (Utils.isEmpty(contentTypeHeader)) {
      return false;
    }
    try {
      return MediaType.APPLICATION_FORM_URLENCODED_TYPE.isCompatible(
          MediaType.valueOf(contentTypeHeader));
    } catch (IllegalArgumentException ex) {
      return contentTypeHeader.toLowerCase().contains("x-www-form-urlencoded");
    }
  }

  static LinkedHashMap<String, String> parseFormUrlEncoded(String body) {
    LinkedHashMap<String, String> map = new LinkedHashMap<>();
    if (Utils.isEmpty(body)) {
      return map;
    }
    for (String pair : body.split("&")) {
      if (pair.isEmpty()) {
        continue;
      }
      int eq = pair.indexOf('=');
      if (eq < 0) {
        map.put(decodeFormComponent(pair), "");
      } else {
        map.put(
            decodeFormComponent(pair.substring(0, eq)),
            decodeFormComponent(pair.substring(eq + 1)));
      }
    }
    return map;
  }

  static String mergeFormUrlEncodedBody(
      String entityString, LinkedHashMap<String, String> pagingBodyParams) {
    LinkedHashMap<String, String> merged = parseFormUrlEncoded(entityString);
    merged.putAll(pagingBodyParams);
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> e : merged.entrySet()) {
      if (Utils.isEmpty(e.getKey())) {
        continue;
      }
      if (!sb.isEmpty()) {
        sb.append('&');
      }
      sb.append(encodeFormComponent(e.getKey()));
      sb.append('=');
      sb.append(encodeFormComponent(e.getValue() == null ? "" : e.getValue()));
    }
    return sb.toString();
  }

  static String mergeJsonBody(String entityString, LinkedHashMap<String, String> pagingBodyParams) {
    try {
      ObjectNode node;
      if (Utils.isEmpty(entityString)) {
        node = MAPPER.createObjectNode();
      } else {
        JsonNode parsed = MAPPER.readTree(entityString);
        if (parsed instanceof ObjectNode objectNode) {
          node = objectNode;
        } else {
          node = MAPPER.createObjectNode();
        }
      }
      for (Map.Entry<String, String> e : pagingBodyParams.entrySet()) {
        if (Utils.isEmpty(e.getKey())) {
          continue;
        }
        String value = e.getValue() == null ? "" : e.getValue();
        if (value.matches("-?\\d+")) {
          try {
            node.put(e.getKey(), Long.parseLong(value));
            continue;
          } catch (NumberFormatException ignored) {
            // fall through to string
          }
        }
        node.put(e.getKey(), value);
      }
      return MAPPER.writeValueAsString(node);
    } catch (Exception ex) {
      return entityString;
    }
  }

  private static String decodeFormComponent(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static String encodeFormComponent(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
