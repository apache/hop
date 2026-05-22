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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import org.apache.hop.core.exception.HopException;

/** Selects record nodes from a document using JsonPath (Jayway), similar to JSON Input. */
public final class JsonNormalizeRecords {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Configuration NODE_CONFIGURATION =
      Configuration.builder()
          .jsonProvider(new JacksonJsonNodeJsonProvider())
          .mappingProvider(new JacksonMappingProvider())
          .options(
              EnumSet.of(
                  Option.SUPPRESS_EXCEPTIONS,
                  Option.ALWAYS_RETURN_LIST,
                  Option.DEFAULT_PATH_LEAF_TO_NULL))
          .build();

  private JsonNormalizeRecords() {}

  public static List<JsonNode> extractRecordsFromStream(InputStream in, String recordPath)
      throws HopException {
    try {
      JsonNode root = MAPPER.readTree(in);
      return extractRecordsFromNode(root, recordPath);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to parse JSON document for normalize: " + e.getMessage(), e);
    }
  }

  public static List<JsonNode> extractRecordsFromNode(JsonNode root, String recordPath)
      throws HopException {
    try {
      ReadContext readContext = JsonPath.using(NODE_CONFIGURATION).parse(root);
      return readRecordNodes(readContext, recordPath);
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException("Failed to read JSON records: " + e.getMessage(), e);
    }
  }

  private static List<JsonNode> readRecordNodes(ReadContext readContext, String recordPath)
      throws HopException {
    JsonPath path = JsonPath.compile(recordPath);
    Object raw = readContext.read(path);
    if (raw == null) {
      return new ArrayList<>();
    }
    List<JsonNode> records = new ArrayList<>();
    if (raw instanceof List<?> list) {
      if (list.size() == 1 && list.get(0) instanceof ArrayNode arr) {
        for (JsonNode n : arr) {
          records.add(n);
        }
        return records;
      }
      for (Object o : list) {
        records.add(toJsonNode(o));
      }
      return records;
    }
    if (raw instanceof ArrayNode arr) {
      for (JsonNode n : arr) {
        records.add(n);
      }
      return records;
    }
    // Single value (rare with ALWAYS_RETURN_LIST but guard anyway)
    records.add(toJsonNode(raw));
    return records;
  }

  private static JsonNode toJsonNode(Object o) throws HopException {
    if (o == null) {
      return null;
    }
    if (o instanceof JsonNode j) {
      return j;
    }
    try {
      return MAPPER.valueToTree(o);
    } catch (IllegalArgumentException e) {
      throw new HopException(
          "Could not convert JSON path element of type " + o.getClass().getName() + " to JsonNode.",
          e);
    }
  }
}
