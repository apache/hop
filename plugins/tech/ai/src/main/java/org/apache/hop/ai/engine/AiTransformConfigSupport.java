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

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.ai.advisor.AiProposal;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.json.HopJson;
import org.apache.hop.core.util.Utils;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.util.ReflectionUtil;

/**
 * Copies extra hop_proposals parameters onto transform/action metadata via {@link
 * HopMetadataProperty} keys (including dotted nested paths such as {@code file.name}).
 */
public final class AiTransformConfigSupport {

  static final Set<String> TOPOLOGY_KEYS =
      Set.of(
          "transformpluginid",
          "actionpluginid",
          "name",
          "locationx",
          "locationy",
          "xml",
          "transformname",
          "actionname",
          "newname",
          "fromtransform",
          "totransform",
          "fromaction",
          "toaction",
          "enabled",
          "unconditional",
          "evaluation",
          "text",
          "width",
          "height",
          "typekey",
          "json",
          "id",
          "config");

  private AiTransformConfigSupport() {}

  public static boolean hasConfig(AiProposal proposal) {
    return proposal != null && !configParameters(proposal).isEmpty();
  }

  public static void apply(Object target, AiProposal proposal) throws HopException {
    if (target == null || proposal == null) {
      return;
    }
    apply(target, configParameters(proposal));
  }

  public static void apply(Object target, Map<String, String> parameters) throws HopException {
    if (target == null || parameters == null || parameters.isEmpty()) {
      return;
    }
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      if (Utils.isEmpty(entry.getKey()) || Utils.isEmpty(entry.getValue())) {
        continue;
      }
      applyPath(target, canonicalPath(entry.getKey()), entry.getValue());
    }
  }

  static Map<String, String> configParameters(AiProposal proposal) {
    Map<String, String> config = new LinkedHashMap<>();
    if (proposal == null || proposal.getParameters() == null) {
      return config;
    }
    String nested = proposal.parameter("config");
    if (!Utils.isEmpty(nested) && nested.trim().startsWith("{")) {
      flattenJsonObject(nested, "", config);
    }
    for (Map.Entry<String, String> entry : proposal.getParameters().entrySet()) {
      if (isTopologyKey(entry.getKey()) || Utils.isEmpty(entry.getValue())) {
        continue;
      }
      config.put(entry.getKey(), entry.getValue());
    }
    return config;
  }

  static boolean isTopologyKey(String key) {
    return !Utils.isEmpty(key) && TOPOLOGY_KEYS.contains(key.trim().toLowerCase());
  }

  static String canonicalPath(String key) {
    if (Utils.isEmpty(key)) {
      return "";
    }
    String trimmed = key.trim();
    return switch (trimmed.toLowerCase()) {
      case "connectionname", "database" -> "connection";
      case "filename", "file" -> "file.name";
      case "sheet", "sheetname", "sheet_name" -> "file.sheetname";
      case "extension" -> "file.extension";
      case "headerenabled" -> "header";
      case "sqlfromfile" -> "sql_from_file";
      default -> trimmed;
    };
  }

  static void applyPath(Object target, String path, String value) throws HopException {
    if (target == null || Utils.isEmpty(path) || Utils.isEmpty(value)) {
      return;
    }
    String[] parts = path.split("\\.");
    Object current = target;
    for (int i = 0; i < parts.length - 1; i++) {
      Field field = findPropertyField(current.getClass(), parts[i]);
      if (field == null) {
        return;
      }
      Object nested =
          ReflectionUtil.getFieldValue(current, field.getName(), isBooleanType(field.getType()));
      if (nested == null) {
        try {
          nested = field.getType().getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          return;
        }
        ReflectionUtil.setFieldValue(current, field.getName(), field.getType(), nested);
      }
      current = nested;
    }
    Field leaf = findPropertyField(current.getClass(), parts[parts.length - 1]);
    if (leaf == null) {
      return;
    }
    Object converted = convertValue(leaf.getType(), value);
    if (converted != null) {
      ReflectionUtil.setFieldValue(current, leaf.getName(), leaf.getType(), converted);
    }
  }

  static Field findPropertyField(Class<?> clazz, String key) {
    if (clazz == null || Utils.isEmpty(key)) {
      return null;
    }
    Field byName = null;
    for (Field field : ReflectionUtil.findAllFields(clazz)) {
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property == null) {
        continue;
      }
      String propertyKey =
          StringUtils.isNotEmpty(property.key()) ? property.key() : field.getName();
      if (key.equals(propertyKey) || key.equalsIgnoreCase(propertyKey)) {
        return field;
      }
      if (key.equals(field.getName()) || key.equalsIgnoreCase(field.getName())) {
        byName = field;
      }
    }
    return byName;
  }

  static Object convertValue(Class<?> type, String value) {
    if (type == null || Utils.isEmpty(value)) {
      return null;
    }
    if (String.class.equals(type)) {
      return value;
    }
    if (boolean.class.equals(type) || Boolean.class.equals(type)) {
      return parseBoolean(value);
    }
    if (int.class.equals(type) || Integer.class.equals(type)) {
      try {
        return Integer.parseInt(value.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    if (long.class.equals(type) || Long.class.equals(type)) {
      try {
        return Long.parseLong(value.trim());
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  static boolean parseBoolean(String value) {
    String normalized = Const.NVL(value, "").trim().toUpperCase();
    return "Y".equals(normalized)
        || "YES".equals(normalized)
        || "TRUE".equals(normalized)
        || "1".equals(normalized);
  }

  static boolean isBooleanType(Class<?> type) {
    return boolean.class.equals(type) || Boolean.class.equals(type);
  }

  static void flattenJsonObject(String json, String prefix, Map<String, String> target) {
    try {
      JsonNode root = HopJson.newMapper().readTree(json);
      flattenJsonNode(root, prefix, target);
    } catch (Exception ignored) {
      // Leave the raw config string unused when it is not JSON.
    }
  }

  static void flattenJsonNode(JsonNode node, String prefix, Map<String, String> target) {
    if (node == null || !node.isObject()) {
      return;
    }
    node.fields()
        .forEachRemaining(
            entry -> {
              String key = Utils.isEmpty(prefix) ? entry.getKey() : prefix + "." + entry.getKey();
              JsonNode value = entry.getValue();
              if (value == null || value.isNull()) {
                return;
              }
              if (value.isObject()) {
                flattenJsonNode(value, key, target);
              } else if (!value.isArray()) {
                String text =
                    value.isTextual() || value.isNumber() || value.isBoolean()
                        ? value.asText("")
                        : value.toString();
                if (!Utils.isEmpty(text)) {
                  target.put(key, text);
                }
              }
            });
  }
}
