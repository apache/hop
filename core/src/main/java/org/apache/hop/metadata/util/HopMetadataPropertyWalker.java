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
package org.apache.hop.metadata.util;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

/**
 * Walks {@link HopMetadataProperty} fields on a metadata object, including nested objects and
 * collections, and collects string values of a given {@link HopMetadataPropertyType}.
 *
 * <p>Failures to read a field are skipped. Cycles are broken. This is a design-time helper: it must
 * not throw because of a broken plugin class.
 */
public final class HopMetadataPropertyWalker {

  private static final int MAX_DEPTH = 8;

  private HopMetadataPropertyWalker() {}

  /**
   * A string property found on a metadata object.
   *
   * @param type the annotated property type
   * @param key the serialised key, or the field name when no key is set
   * @param value the raw (unresolved) string value, never null
   */
  public record StringProperty(HopMetadataPropertyType type, String key, String value) {}

  /**
   * Collect every string field annotated with {@code type} under {@code root}.
   *
   * @param root the object to walk, may be null
   * @param type the property type to collect
   * @return the matching properties, possibly empty
   */
  public static List<StringProperty> collectStrings(Object root, HopMetadataPropertyType type) {
    List<StringProperty> collected = new ArrayList<>();
    if (root == null || type == null) {
      return collected;
    }
    walk(
        root,
        type,
        collected,
        0,
        java.util.Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>()));
    return collected;
  }

  private static void walk(
      Object node,
      HopMetadataPropertyType type,
      List<StringProperty> collected,
      int depth,
      Set<Object> visited) {
    if (node == null || depth > MAX_DEPTH || !isMetadataObject(node) || !visited.add(node)) {
      return;
    }
    for (Field field : ReflectionUtil.findAllFields(node.getClass())) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
      if (property == null) {
        continue;
      }
      Object value = readField(field, node);
      if (value == null) {
        continue;
      }
      if (property.hopMetadataPropertyType() == type && value instanceof String stringValue) {
        collected.add(new StringProperty(type, serialisedKey(property, field), stringValue));
      }
      descend(value, type, collected, depth, visited);
    }
  }

  private static void descend(
      Object value,
      HopMetadataPropertyType type,
      List<StringProperty> collected,
      int depth,
      Set<Object> visited) {
    if (value instanceof Collection<?> collection) {
      for (Object element : collection) {
        walk(element, type, collected, depth + 1, visited);
      }
      return;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object element : map.values()) {
        walk(element, type, collected, depth + 1, visited);
      }
      return;
    }
    if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      for (int i = 0; i < length; i++) {
        walk(Array.get(value, i), type, collected, depth + 1, visited);
      }
      return;
    }
    walk(value, type, collected, depth + 1, visited);
  }

  private static String serialisedKey(HopMetadataProperty property, Field field) {
    if (property.key() != null && !property.key().isEmpty()) {
      return property.key();
    }
    return field.getName();
  }

  /** Only descends into Hop's own metadata classes, never into JDK or third-party types. */
  static boolean isMetadataObject(Object value) {
    if (value == null) {
      return false;
    }
    Class<?> type = value.getClass();
    if (type.isPrimitive() || type.isEnum() || type.isArray()) {
      return false;
    }
    Package pkg = type.getPackage();
    return pkg != null && pkg.getName().toLowerCase(Locale.ROOT).startsWith("org.apache.hop");
  }

  private static Object readField(Field field, Object target) {
    try {
      field.setAccessible(true);
      return field.get(target);
    } catch (Exception e) {
      return null;
    }
  }
}
