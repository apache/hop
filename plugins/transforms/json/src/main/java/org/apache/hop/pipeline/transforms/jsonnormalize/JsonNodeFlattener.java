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

package org.apache.hop.pipeline.transforms.jsonnormalize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.exception.HopException;

/**
 * Flattens a JSON object into a map of dot-separated paths (pandas json_normalize style). Arrays
 * and depth limits are handled per transform options.
 */
public final class JsonNodeFlattener {

  public static final String CODE_ARRAY_STRINGIFY = "STRINGIFY";
  public static final String CODE_ARRAY_SINGLE_ELEMENT = "SINGLE_ELEMENT";
  public static final String CODE_ARRAY_ERROR = "ERROR";

  public static final String CODE_BEYOND_STRINGIFY = "STRINGIFY";
  public static final String CODE_BEYOND_OMIT = "OMIT";
  public static final String CODE_BEYOND_ERROR = "ERROR";

  private static final JsonNodeFactory NODE_FACTORY = JsonNodeFactory.instance;

  private JsonNodeFlattener() {}

  public static Map<String, JsonNode> flatten(
      JsonNode record,
      String separator,
      int maxFlattenDepth,
      String arrayHandlingCode,
      String beyondDepthCode)
      throws HopException {
    Map<String, JsonNode> out = new LinkedHashMap<>();
    if (record == null || record.isNull()) {
      return out;
    }
    if (!record.isObject()) {
      out.put("_", record);
      return out;
    }
    flattenObject(
        record, "", 0, separator, maxFlattenDepth, arrayHandlingCode, beyondDepthCode, out);
    return out;
  }

  private static void flattenObject(
      JsonNode objectNode,
      String prefix,
      int depth,
      String separator,
      int maxFlattenDepth,
      String arrayHandlingCode,
      String beyondDepthCode,
      Map<String, JsonNode> out)
      throws HopException {
    var it = objectNode.fields();
    while (it.hasNext()) {
      var e = it.next();
      String path = concatPath(prefix, e.getKey(), separator);
      flattenValue(
          e.getValue(),
          path,
          depth,
          separator,
          maxFlattenDepth,
          arrayHandlingCode,
          beyondDepthCode,
          out);
    }
  }

  static String concatPath(String prefix, String key, String separator) {
    if (prefix == null || prefix.isEmpty()) {
      return key;
    }
    return prefix + separator + key;
  }

  private static void flattenValue(
      JsonNode node,
      String path,
      int depth,
      String separator,
      int maxFlattenDepth,
      String arrayHandlingCode,
      String beyondDepthCode,
      Map<String, JsonNode> out)
      throws HopException {
    if (node == null || node.isNull()) {
      out.put(path, node);
      return;
    }
    if (node.isArray()) {
      handleArray(
          node, path, depth, separator, maxFlattenDepth, arrayHandlingCode, beyondDepthCode, out);
      return;
    }
    if (node.isObject()) {
      if (maxFlattenDepth >= 0 && depth >= maxFlattenDepth) {
        putBeyondDepth(node, path, beyondDepthCode, out);
      } else {
        flattenObject(
            node,
            path,
            depth + 1,
            separator,
            maxFlattenDepth,
            arrayHandlingCode,
            beyondDepthCode,
            out);
      }
      return;
    }
    out.put(path, node);
  }

  private static void handleArray(
      JsonNode arrayNode,
      String path,
      int depth,
      String separator,
      int maxFlattenDepth,
      String arrayHandlingCode,
      String beyondDepthCode,
      Map<String, JsonNode> out)
      throws HopException {
    int n = arrayNode.size();
    if (CODE_ARRAY_ERROR.equals(arrayHandlingCode)) {
      if (n > 0) {
        throw new HopException(
            "Array not allowed at path '" + path + "' when array handling is ERROR.");
      }
      out.put(path, NODE_FACTORY.nullNode());
      return;
    }
    if (CODE_ARRAY_SINGLE_ELEMENT.equals(arrayHandlingCode)) {
      if (n == 0) {
        out.put(path, NODE_FACTORY.nullNode());
        return;
      }
      if (n > 1) {
        throw new HopException(
            "Array at path '"
                + path
                + "' has "
                + n
                + " elements; SINGLE_ELEMENT handling allows at most one.");
      }
      flattenValue(
          arrayNode.get(0),
          path,
          depth,
          separator,
          maxFlattenDepth,
          arrayHandlingCode,
          beyondDepthCode,
          out);
      return;
    }
    // STRINGIFY (default)
    out.put(path, arrayNode);
  }

  private static void putBeyondDepth(
      JsonNode node, String path, String beyondDepthCode, Map<String, JsonNode> out)
      throws HopException {
    if (CODE_BEYOND_OMIT.equals(beyondDepthCode)) {
      return;
    }
    if (CODE_BEYOND_ERROR.equals(beyondDepthCode)) {
      throw new HopException(
          "Nested object at path '" + path + "' exceeds max flatten depth (strict mode).");
    }
    out.put(path, node);
  }
}
