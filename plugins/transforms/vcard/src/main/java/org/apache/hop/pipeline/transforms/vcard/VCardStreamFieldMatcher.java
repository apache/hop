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
package org.apache.hop.pipeline.transforms.vcard;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;

/** Suggests vCard field mappings from incoming stream field names. */
public final class VCardStreamFieldMatcher {

  private VCardStreamFieldMatcher() {}

  public static List<VCardFieldMapping> suggestFromStream(IRowMeta row) {
    if (row == null || row.isEmpty()) {
      return List.of();
    }
    Map<String, VCardFieldMapping> unique = new LinkedHashMap<>();
    for (String fieldName : row.getFieldNames()) {
      VCardFieldMapping mapping = guessMapping(fieldName);
      if (mapping != null) {
        unique.putIfAbsent(mappingKey(mapping), mapping);
      }
    }
    return new ArrayList<>(unique.values());
  }

  public static VCardFieldMapping guessMapping(String hopField) {
    if (Utils.isEmpty(hopField)) {
      return null;
    }
    String lower = hopField.toLowerCase(Locale.ROOT);
    for (VCardPropertyType property : VCardPropertyType.values()) {
      if (property.getCode().equals(lower)) {
        return new VCardFieldMapping(property, hopField);
      }
    }
    for (VCardPropertyType property : VCardPropertyType.values()) {
      VCardFieldMapping mapping = matchDefaultHopFieldName(property, hopField);
      if (mapping != null) {
        return mapping;
      }
    }
    return null;
  }

  private static VCardFieldMapping matchDefaultHopFieldName(
      VCardPropertyType property, String hopField) {
    if (hopField.equalsIgnoreCase(VCardFieldDiscovery.defaultHopFieldName(property, null))) {
      return new VCardFieldMapping(property, hopField);
    }
    VCardPropertyType valueProperty = property;
    if (property == VCardPropertyType.EMAIL_TYPE) {
      valueProperty = VCardPropertyType.EMAIL;
    } else if (property == VCardPropertyType.TEL_TYPE) {
      valueProperty = VCardPropertyType.TEL;
    }
    String prefix = valueProperty.getCode() + "_";
    if (!hopField.toLowerCase(Locale.ROOT).startsWith(prefix)) {
      return null;
    }
    String suffix = hopField.substring(prefix.length());
    boolean typeProperty =
        property == VCardPropertyType.EMAIL_TYPE || property == VCardPropertyType.TEL_TYPE;
    if (typeProperty) {
      if (!suffix.toLowerCase(Locale.ROOT).endsWith("_type")) {
        return null;
      }
      suffix = suffix.substring(0, suffix.length() - "_type".length());
    }
    if (suffix.isEmpty()) {
      return null;
    }
    String parameterTypes = suffix.toUpperCase(Locale.ROOT).replace('_', ',');
    if (!hopField.equalsIgnoreCase(
        VCardFieldDiscovery.defaultHopFieldName(property, parameterTypes))) {
      return null;
    }
    return new VCardFieldMapping(property, hopField, parameterTypes);
  }

  private static String mappingKey(VCardFieldMapping mapping) {
    return mapping.getProperty().getCode()
        + '|'
        + (mapping.getParameterTypes() == null ? "" : mapping.getParameterTypes());
  }
}
