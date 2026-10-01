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

package org.apache.hop.naming.engine;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.naming.NamingSchemeKinds;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHasName;
import org.apache.hop.naming.engine.NamingSchemeValidator.Finding;
import org.apache.hop.naming.metadata.NamingScheme;

/** Walks {@code @HopMetadataProperty} graphs and validates annotated names. */
public final class NamingSchemeWalker {

  private NamingSchemeWalker() {
    // utility
  }

  public static List<Finding> walk(
      Object root, String location, Iterable<NamingScheme> schemes, Set<String> typeFilter) {
    List<Finding> out = new ArrayList<>();
    visit(root, location, "", schemes, typeFilter, out, new IdentityHashMap<>());
    return out;
  }

  private static void visit(
      Object node,
      String location,
      String path,
      Iterable<NamingScheme> schemes,
      Set<String> typeFilter,
      List<Finding> out,
      IdentityHashMap<Object, Boolean> seen) {
    if (node == null
        || node.getClass().isPrimitive()
        || node instanceof Number
        || node instanceof Boolean) {
      return;
    }
    if (node instanceof String || node instanceof Enum || node instanceof Class) {
      return;
    }
    if (seen.put(node, Boolean.TRUE) != null) {
      return;
    }

    String identityKind = identityKind(node);
    if (StringUtils.isNotEmpty(identityKind)) {
      String identity = identityName(node);
      if (identity != null) {
        collect(location, join(path, "name"), identityKind, identity, schemes, typeFilter, out);
      }
    }

    if (node instanceof Collection<?> collection) {
      int i = 0;
      for (Object item : collection) {
        visit(item, location, path + "[" + i++ + "]", schemes, typeFilter, out, seen);
      }
      return;
    }
    if (node instanceof Map<?, ?> map) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        visit(
            entry.getValue(),
            location,
            path + "[" + entry.getKey() + "]",
            schemes,
            typeFilter,
            out,
            seen);
      }
      return;
    }
    if (node.getClass().isArray()) {
      if (node.getClass().getComponentType().isPrimitive()) {
        return;
      }
      Object[] array = (Object[]) node;
      for (int i = 0; i < array.length; i++) {
        visit(array[i], location, path + "[" + i + "]", schemes, typeFilter, out, seen);
      }
      return;
    }

    Class<?> type = node.getClass();
    while (type != null && type != Object.class) {
      for (Field field : type.getDeclaredFields()) {
        HopMetadataProperty prop = field.getAnnotation(HopMetadataProperty.class);
        if (prop == null) {
          continue;
        }
        Object value;
        try {
          field.setAccessible(true);
          value = field.get(node);
        } catch (Exception e) {
          continue;
        }
        String fieldKind = prop.namingSchemeType();
        if (StringUtils.isNotEmpty(fieldKind)
            && value instanceof String string
            && !"name".equals(field.getName())) {
          collect(
              location, join(path, field.getName()), fieldKind, string, schemes, typeFilter, out);
        }
        if (value != null && !(value instanceof String)) {
          visit(value, location, join(path, field.getName()), schemes, typeFilter, out, seen);
        }
      }
      type = type.getSuperclass();
    }
  }

  private static void collect(
      String location,
      String fieldPath,
      String typeCode,
      String value,
      Iterable<NamingScheme> schemes,
      Set<String> typeFilter,
      List<Finding> out) {
    if (typeFilter != null && !typeFilter.isEmpty() && !typeFilter.contains(typeCode)) {
      return;
    }
    for (Finding finding : NamingSchemeValidator.validate(value, typeCode, schemes)) {
      finding.setLocation(location);
      finding.setFieldPath(fieldPath);
      out.add(finding);
    }
  }

  /** Field-level {@code namingSchemeType} on {@code name} wins over {@link NamingSchemeKinds}. */
  private static String identityKind(Object node) {
    Class<?> type = node.getClass();
    while (type != null && type != Object.class) {
      try {
        java.lang.reflect.Field nameField = type.getDeclaredField("name");
        HopMetadataProperty prop = nameField.getAnnotation(HopMetadataProperty.class);
        if (prop != null && StringUtils.isNotEmpty(prop.namingSchemeType())) {
          return prop.namingSchemeType();
        }
      } catch (NoSuchFieldException ignored) {
        // look on superclass
      }
      type = type.getSuperclass();
    }
    return NamingSchemeKinds.kindOf(node.getClass());
  }

  private static String identityName(Object node) {
    if (node instanceof IHasName named) {
      return named.getName();
    }
    try {
      Object value = node.getClass().getMethod("getName").invoke(node);
      return value instanceof String string ? string : null;
    } catch (Exception e) {
      return null;
    }
  }

  private static String join(String path, String name) {
    if (StringUtils.isEmpty(path)) {
      return name;
    }
    return path + "/" + name;
  }
}
