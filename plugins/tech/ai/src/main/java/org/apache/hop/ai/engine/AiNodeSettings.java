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
package org.apache.hop.ai.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.util.ReflectionUtil;

/**
 * The settings of a transform or action, as compact JSON for a prompt.
 *
 * <p>Reads the {@code @HopMetadataProperty} fields: SQL, file names, connection names, field lists
 * and so on, which is what a model needs to explain what a pipeline or workflow does. Password
 * fields are skipped. Empty values, long texts and long lists are left out or shortened so one
 * transform cannot take over the prompt.
 */
public final class AiNodeSettings {

  static final int MAX_DEPTH = 3;
  static final int MAX_LIST_ITEMS = 25;
  static final int MAX_TEXT_CHARS = 400;
  static final int MAX_NODE_CHARS = 3_000;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AiNodeSettings() {}

  /**
   * @param node the transform or action meta
   * @return a JSON object, {@code {"truncated":true}} when the settings are too large, or null when
   *     the plugin exposes no settings this way
   */
  public static String toJson(Object node) {
    if (node == null) {
      return null;
    }
    Object settings = read(node, 0);
    if (!(settings instanceof Map<?, ?> map) || map.isEmpty()) {
      return null;
    }
    try {
      String json = MAPPER.writeValueAsString(settings);
      if (json.length() > MAX_NODE_CHARS) {
        return "{\"truncated\":true}";
      }
      return AiTextUtil.redactSecrets(json);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  static Object read(Object value, int depth) {
    if (value == null) {
      return null;
    }
    if (value instanceof String text) {
      if (text.isEmpty()) {
        return null;
      }
      return text.length() > MAX_TEXT_CHARS ? text.substring(0, MAX_TEXT_CHARS) + "…" : text;
    }
    if (value instanceof Number || value instanceof Boolean) {
      return value;
    }
    if (value instanceof Enum<?> e) {
      return e.name();
    }
    if (depth >= MAX_DEPTH) {
      return null;
    }
    if (value instanceof Collection<?> collection) {
      return readList(collection, depth);
    }
    if (value.getClass().isArray() && value instanceof Object[] array) {
      return readList(List.of(array), depth);
    }
    return readObject(value, depth);
  }

  private static List<Object> readList(Collection<?> collection, int depth) {
    List<Object> list = new ArrayList<>();
    int count = 0;
    for (Object item : collection) {
      if (count == MAX_LIST_ITEMS) {
        list.add("… " + (collection.size() - MAX_LIST_ITEMS) + " more");
        break;
      }
      Object read = read(item, depth + 1);
      if (read != null) {
        list.add(read);
      }
      count++;
    }
    return list.isEmpty() ? null : list;
  }

  private static Map<String, Object> readObject(Object object, int depth) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (Field field : ReflectionUtil.findAllFields(object.getClass())) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property == null || property.password()) {
        continue;
      }
      Object fieldValue;
      try {
        fieldValue =
            ReflectionUtil.getFieldValue(object, field.getName(), isBoolean(field.getType()));
      } catch (Exception e) {
        continue;
      }
      Object read = read(fieldValue, depth + 1);
      if (read == null || Boolean.FALSE.equals(read)) {
        continue;
      }
      String key = Utils.isEmpty(property.key()) ? field.getName() : property.key();
      map.put(key, read);
    }
    return map.isEmpty() ? null : map;
  }

  private static boolean isBoolean(Class<?> type) {
    return type == boolean.class || type == Boolean.class;
  }
}
