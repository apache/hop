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

package org.apache.hop.metadata.serializer.xml;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.util.ReflectionUtil;

/**
 * Dialog-OK change detection that keeps {@code null} and {@code ""} distinct on disk.
 *
 * <p>SWT text widgets cannot hold {@code null}, so an untouched empty control round-trips as {@code
 * ""}. For String fields those two values are treated as the same content. List size still counts:
 * an extra blank table row is a real edit. Serialization is unchanged: {@code null} is omitted,
 * {@code ""} is written as an empty tag.
 */
public final class DialogOkContent {
  private DialogOkContent() {}

  /**
   * True when a widget read-back of {@code ""} should not overwrite a field that is still {@code
   * null}.
   */
  public static boolean widgetEmptyLeavesNull(Object currentValue, Object widgetValue) {
    return currentValue == null && widgetValue instanceof String text && text.isEmpty();
  }

  /**
   * True when two objects represent the same persisted content for dialog OK, treating String
   * {@code null} and {@code ""} as equal.
   */
  public static boolean same(Object left, Object right) {
    return same(left, right, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static boolean same(Object left, Object right, Set<Object> visiting) {
    if (left == right || (isNullOrEmptyString(left) && isNullOrEmptyString(right))) {
      return true;
    }
    if (left == null) {
      return isEmptyContainer(right);
    }
    if (right == null) {
      return isEmptyContainer(left);
    }
    return sameStructured(left, right, visiting);
  }

  private static boolean sameStructured(Object left, Object right, Set<Object> visiting) {
    if (left instanceof String || right instanceof String) {
      return Objects.equals(left, right);
    }
    if (left instanceof Collection<?> leftItems && right instanceof Collection<?> rightItems) {
      return sameCollections(leftItems, rightItems, visiting);
    }
    if (left instanceof Map<?, ?> leftMap && right instanceof Map<?, ?> rightMap) {
      return sameMaps(leftMap, rightMap, visiting);
    }
    if (left.getClass().isArray() && right.getClass().isArray()) {
      return sameArrays(left, right, visiting);
    }
    if (hasMetadataProperties(left) || hasMetadataProperties(right)) {
      return sameMetadataObjects(left, right, visiting);
    }
    return Objects.equals(left, right) || sameGetXml(left, right);
  }

  private static boolean hasMetadataProperties(Object value) {
    return XmlMetadataUtil.hasHopMetadataSerializableProperties(value.getClass());
  }

  private static boolean sameMetadataObjects(Object left, Object right, Set<Object> visiting) {
    if (visiting.contains(left) && visiting.contains(right)) {
      return true;
    }
    visiting.add(left);
    visiting.add(right);
    try {
      return sameMetadata(left, right, visiting);
    } finally {
      visiting.remove(left);
      visiting.remove(right);
    }
  }

  private static boolean sameCollections(
      Collection<?> leftItems, Collection<?> rightItems, Set<Object> visiting) {
    if (leftItems.size() != rightItems.size()) {
      return false;
    }
    Iterator<?> leftIterator = leftItems.iterator();
    Iterator<?> rightIterator = rightItems.iterator();
    while (leftIterator.hasNext()) {
      if (!same(leftIterator.next(), rightIterator.next(), visiting)) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameMaps(Map<?, ?> leftMap, Map<?, ?> rightMap, Set<Object> visiting) {
    if (leftMap.size() != rightMap.size()) {
      return false;
    }
    for (Map.Entry<?, ?> entry : leftMap.entrySet()) {
      if (!rightMap.containsKey(entry.getKey())) {
        return false;
      }
      if (!same(entry.getValue(), rightMap.get(entry.getKey()), visiting)) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameArrays(Object left, Object right, Set<Object> visiting) {
    int length = Array.getLength(left);
    if (length != Array.getLength(right)) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (!same(Array.get(left, i), Array.get(right, i), visiting)) {
        return false;
      }
    }
    return true;
  }

  private static boolean sameMetadata(Object left, Object right, Set<Object> visiting) {
    Class<?> leftClass = left.getClass();
    Class<?> rightClass = right.getClass();
    if (leftClass != rightClass) {
      return false;
    }
    List<Field> fields =
        ReflectionUtil.findAllFields(leftClass, new MetadataPropertyKeyFunction(), false);
    for (Field field : fields) {
      if (!isComparableProperty(field)) {
        continue;
      }
      try {
        boolean isBoolean = field.getType() == boolean.class || field.getType() == Boolean.class;
        Object leftValue = ReflectionUtil.getFieldValue(left, field.getName(), isBoolean);
        Object rightValue = ReflectionUtil.getFieldValue(right, field.getName(), isBoolean);
        if (!same(leftValue, rightValue, visiting)) {
          return false;
        }
      } catch (Exception e) {
        return false;
      }
    }
    try {
      for (Method getter : ReflectionUtil.findAllMethods(leftClass, "get")) {
        if (!isComparableGetter(getter)) {
          continue;
        }
        Object leftValue = getter.invoke(left);
        Method rightGetter = rightClass.getMethod(getter.getName());
        Object rightValue = rightGetter.invoke(right);
        if (!same(leftValue, rightValue, visiting)) {
          return false;
        }
      }
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  private static boolean isComparableProperty(Field field) {
    int modifiers = field.getModifiers();
    if (Modifier.isTransient(modifiers)
        || Modifier.isVolatile(modifiers)
        || Modifier.isStatic(modifiers)) {
      return false;
    }
    HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
    return property != null && !property.isExcludedFromSerialization();
  }

  private static boolean isComparableGetter(Method getter) {
    HopMetadataProperty property = getter.getAnnotation(HopMetadataProperty.class);
    return property != null
        && !property.isExcludedFromSerialization()
        && getter.getParameterCount() == 0;
  }

  private static boolean isNullOrEmptyString(Object value) {
    return value == null || (value instanceof String text && text.isEmpty());
  }

  private static boolean isEmptyContainer(Object value) {
    if (value instanceof Collection<?> items) {
      return items.isEmpty();
    }
    if (value instanceof Map<?, ?> map) {
      return map.isEmpty();
    }
    return value != null && value.getClass().isArray() && Array.getLength(value) == 0;
  }

  /**
   * Fallback for objects that persist with a custom {@code getXml()} and no {@code
   * HopMetadataProperty} fields. Empty-string round-trips are not rewritten here; those classes
   * typically already write null and {@code ""} the same way.
   */
  private static boolean sameGetXml(Object left, Object right) {
    try {
      Method leftXml = left.getClass().getMethod("getXml");
      Method rightXml = right.getClass().getMethod("getXml");
      if (leftXml.getParameterCount() != 0
          || rightXml.getParameterCount() != 0
          || leftXml.getReturnType() != String.class
          || rightXml.getReturnType() != String.class) {
        return false;
      }
      return Objects.equals(leftXml.invoke(left), rightXml.invoke(right));
    } catch (Exception e) {
      return false;
    }
  }
}
