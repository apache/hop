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

package org.apache.hop.metadata.util;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * Deep-copies the state that Hop persists, driven by the {@link HopMetadataProperty} annotations.
 *
 * <p>{@code Object.clone()} is shallow, so a cloned metadata object keeps sharing every list, map
 * and nested value object with the original. That is invisible most of the time, but it breaks the
 * two places where a metadata snapshot has to be independent of the live object: deciding whether a
 * dialog changed anything, and rolling an edit back through undo. See issue #8022.
 *
 * <p>The annotations describe exactly the state that ends up in the .hpl / .hwf file, which is
 * precisely the state those two features compare and restore. Everything else - live references to
 * other transforms, metadata providers, caches - is left shared, which is the deliberate behaviour
 * documented on {@code BaseTransformMeta.clone()}.
 */
public final class HopMetadataCopyUtil {

  private static final ILogChannel log = new LogChannel("HopMetadataCopyUtil");

  private HopMetadataCopyUtil() {
    // Utility class
  }

  /**
   * Deep-copies every serializable property of {@code source} into {@code target}, leaving all
   * other fields of {@code target} untouched.
   *
   * <p>Call this right after {@code super.clone()} to turn a shallow copy into one whose persisted
   * state is independent of the original.
   *
   * @param source The object to copy the properties from
   * @param target The object to copy the properties into, usually a shallow clone of the source
   */
  public static void copyMetadataProperties(Object source, Object target) {
    if (source == null || target == null) {
      return;
    }
    copyProperties(source, target, new IdentityHashMap<>());
  }

  /**
   * Creates an independent copy of a single value using the same rules as {@link
   * #copyMetadataProperties(Object, Object)}.
   *
   * @param value The value to copy, may be null
   * @return An independent copy, or the value itself when it is immutable or cannot be copied
   */
  public static Object copyValue(Object value) {
    return copyValue(value, new IdentityHashMap<>());
  }

  private static void copyProperties(
      Object source, Object target, IdentityHashMap<Object, Object> seen) {
    for (Field field : ReflectionUtil.findAllFields(source.getClass())) {
      if (!isSerializableProperty(field)) {
        continue;
      }
      try {
        field.setAccessible(true);
        Object value = field.get(source);
        // A property stored by name references a shared, named metadata object. The file only
        // holds its name, so the reference is shared rather than copied.
        HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
        field.set(target, property.storeWithName() ? value : copyValue(value, seen));
      } catch (Exception e) {
        // Leave the shallow value in place: a partial copy is still better than a failed clone.
        log.logError(
            "Unable to copy property '"
                + field.getName()
                + "' of "
                + source.getClass().getName()
                + ", it stays shared with the original",
            e);
      }
    }
  }

  private static Object copyValue(Object value, IdentityHashMap<Object, Object> seen) {
    if (value == null || isImmutable(value)) {
      return value;
    }

    Object known = seen.get(value);
    if (known != null) {
      return known;
    }

    if (value instanceof Date date) {
      return new Date(date.getTime());
    }
    if (value instanceof IHopMetadata) {
      // Named metadata lives in the metadata store, not in the object referencing it. The file
      // only carries a reference, so a snapshot shares it rather than duplicating it.
      return value;
    }
    if (value instanceof Map<?, ?> map) {
      return copyMap(map, seen);
    }
    if (value instanceof Collection<?> collection) {
      return copyCollection(collection, seen);
    }
    if (value.getClass().isArray()) {
      return copyArray(value, seen);
    }
    return copyObject(value, seen);
  }

  private static boolean isImmutable(Object value) {
    return value instanceof String
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof Character
        || value instanceof Class
        || value.getClass().isEnum();
  }

  private static Object copyMap(Map<?, ?> map, IdentityHashMap<Object, Object> seen) {
    Map<Object, Object> copy = newInstanceOr(map.getClass(), LinkedHashMap::new);
    seen.put(map, copy);
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      copy.put(copyValue(entry.getKey(), seen), copyValue(entry.getValue(), seen));
    }
    return copy;
  }

  private static Object copyCollection(
      Collection<?> collection, IdentityHashMap<Object, Object> seen) {
    Collection<Object> copy =
        collection instanceof Set
            ? newInstanceOr(collection.getClass(), LinkedHashSet::new)
            : newInstanceOr(collection.getClass(), ArrayList::new);
    seen.put(collection, copy);
    for (Object item : collection) {
      copy.add(copyValue(item, seen));
    }
    return copy;
  }

  private static Object copyArray(Object array, IdentityHashMap<Object, Object> seen) {
    int length = Array.getLength(array);
    Object copy = Array.newInstance(array.getClass().getComponentType(), length);
    seen.put(array, copy);
    for (int i = 0; i < length; i++) {
      Array.set(copy, i, copyValue(Array.get(array, i), seen));
    }
    return copy;
  }

  /**
   * Copies a value object: a new instance holding a deep copy of its serializable properties and a
   * shared reference to everything else, mirroring what {@code super.clone()} does one level up.
   */
  private static Object copyObject(Object source, IdentityHashMap<Object, Object> seen) {
    Class<?> sourceClass = source.getClass();
    Object copy;
    try {
      Constructor<?> constructor = sourceClass.getDeclaredConstructor();
      constructor.setAccessible(true);
      copy = constructor.newInstance();
    } catch (Exception e) {
      // No usable no-argument constructor: share the reference, which is the behaviour we had
      // before this method existed.
      log.logDebug(
          "No no-argument constructor on "
              + sourceClass.getName()
              + ", the value stays shared with the original");
      return source;
    }

    seen.put(source, copy);
    for (Field field : ReflectionUtil.findAllFields(sourceClass)) {
      if (Modifier.isStatic(field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
        continue;
      }
      try {
        field.setAccessible(true);
        Object value = field.get(source);
        field.set(copy, value);
      } catch (Exception e) {
        log.logDebug(
            "Unable to copy field '" + field.getName() + "' of " + sourceClass.getName(), e);
      }
    }
    copyProperties(source, copy, seen);
    return copy;
  }

  private static boolean isSerializableProperty(Field field) {
    int modifiers = field.getModifiers();
    // Transient and volatile fields are skipped by the XML serializer as well, so they are not
    // part of the persisted state and stay shared.
    if (Modifier.isStatic(modifiers)
        || Modifier.isTransient(modifiers)
        || Modifier.isVolatile(modifiers)) {
      return false;
    }
    return field.getAnnotation(HopMetadataProperty.class) != null;
  }

  @SuppressWarnings("unchecked")
  private static <T> T newInstanceOr(Class<?> clazz, java.util.function.Supplier<T> fallback) {
    try {
      return (T) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      // Immutable or otherwise non-instantiable containers such as List.of() land here.
      return fallback.get();
    }
  }
}
