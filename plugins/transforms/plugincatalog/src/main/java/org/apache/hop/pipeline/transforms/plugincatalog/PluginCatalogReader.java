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
package org.apache.hop.pipeline.transforms.plugincatalog;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.hop.core.plugins.ActionPluginType;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.plugins.TransformPluginType;
import org.apache.hop.i18n.LanguageChoice;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.plugin.MetadataPluginType;

/**
 * Reads the live {@link PluginRegistry} and reflects over each plugin's metadata class to expose a
 * structured, never-stale catalog of transforms, actions and metadata types.
 *
 * <p>Field extraction is split out as a pure static method so it can be unit-tested against fixture
 * classes without bootstrapping a Hop plugin registry.
 */
public class PluginCatalogReader {

  /** Maximum nesting depth when descending into complex {@code @HopMetadataProperty} groups. */
  private static final int MAX_NESTING_DEPTH = 1;

  private final PluginRegistry registry;

  public PluginCatalogReader() {
    this(PluginRegistry.getInstance());
  }

  public PluginCatalogReader(PluginRegistry registry) {
    this.registry = registry;
  }

  /**
   * Enumerate the requested plugin families.
   *
   * @param warnings optional sink for non-fatal problems (e.g. a metadata class that fails to
   *     load); may be {@code null}
   */
  public List<PluginRecord> readAll(
      boolean includeTransforms,
      boolean includeActions,
      boolean includeMetadataTypes,
      BiConsumer<String, Throwable> warnings) {
    List<PluginRecord> out = new ArrayList<>();
    if (includeTransforms) {
      addFamily(registry.getPlugins(TransformPluginType.class), "transform", out, warnings);
    }
    if (includeActions) {
      addFamily(registry.getPlugins(ActionPluginType.class), "action", out, warnings);
    }
    if (includeMetadataTypes) {
      addFamily(registry.getPlugins(MetadataPluginType.class), "metadata", out, warnings);
    }
    // Labels are locale-resolved, so record which locale produced them.
    String localeTag = currentLocaleTag();
    for (PluginRecord record : out) {
      record.locale = localeTag;
    }
    return out;
  }

  private void addFamily(
      List<IPlugin> plugins,
      String pluginType,
      List<PluginRecord> out,
      BiConsumer<String, Throwable> warnings) {
    if (plugins == null) {
      return;
    }
    for (IPlugin plugin : plugins) {
      String id = firstId(plugin);
      if (id.isEmpty()) {
        // Skip anonymous/unregistered plugins rather than emit a junk row with no id.
        if (warnings != null) {
          warnings.accept("Skipping " + pluginType + " plugin with no id", null);
        }
        continue;
      }
      PluginRecord record = new PluginRecord();
      record.pluginId = id;
      record.pluginType = pluginType;
      record.name = clean(plugin.getName());
      record.description = clean(plugin.getDescription());
      record.category = clean(plugin.getCategory());
      record.keywords = joinKeywords(plugin.getKeywords());
      record.englishAliases = joinKeywords(plugin.getEnglishKeywords());
      String className = resolveClassName(plugin);
      record.className = className == null ? "" : className;
      if (className != null) {
        try {
          ClassLoader classLoader = registry.getClassLoader(plugin);
          Class<?> clazz = classLoader.loadClass(className);
          record.properties = extractProperties(clazz);
        } catch (Exception | LinkageError e) {
          if (warnings != null) {
            warnings.accept("Could not reflect properties for plugin '" + record.pluginId + "'", e);
          }
        }
      }
      out.add(record);
    }
  }

  /**
   * Extract every {@code @HopMetadataProperty} field declared on {@code clazz} (including inherited
   * fields), descending one level into complex property groups.
   */
  public static List<PropertyRecord> extractProperties(Class<?> clazz) {
    List<PropertyRecord> out = new ArrayList<>();
    collect(clazz, "", out, 0);
    return out;
  }

  private static void collect(Class<?> clazz, String group, List<PropertyRecord> out, int depth) {
    if (clazz == null) {
      return;
    }
    for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        HopMetadataProperty annotation = field.getAnnotation(HopMetadataProperty.class);
        if (annotation == null) {
          continue;
        }
        if (annotation.isExcludedFromSerialization()) {
          continue;
        }
        String xmlKey = annotation.key().isEmpty() ? field.getName() : annotation.key();
        out.add(
            new PropertyRecord(
                field.getName(),
                xmlKey,
                field.getType().getSimpleName(),
                annotation.password(),
                group,
                annotation.groupKey()));
        if (depth < MAX_NESTING_DEPTH) {
          Class<?> nested = complexHopType(field, annotation);
          if (nested != null) {
            collect(nested, xmlKey, out, depth + 1);
          }
        }
      }
    }
  }

  /**
   * If {@code field} is a value class (or a collection of one) that itself carries
   * {@code @HopMetadataProperty} fields, return that class; otherwise {@code null}.
   *
   * <p>Deliberately not restricted to {@code org.apache.hop}: a third-party or Marketplace plugin
   * declaring its own group class is exactly the case this catalog exists to describe.
   */
  private static Class<?> complexHopType(Field field, HopMetadataProperty annotation) {
    Class<?> candidate;
    if (Collection.class.isAssignableFrom(field.getType())) {
      // An annotated raw List carries its element type on the annotation, as XmlMetadataUtil reads
      // it.
      candidate =
          annotation.listItemClass() != Object.class
              ? annotation.listItemClass()
              : collectionElementType(field);
    } else {
      candidate = field.getType();
    }
    if (candidate == null
        || candidate.isEnum()
        || candidate.isPrimitive()
        || candidate.isArray()
        || isPlatformClass(candidate)) {
      return null;
    }
    for (Class<?> c = candidate; c != null && c != Object.class; c = c.getSuperclass()) {
      for (Field f : c.getDeclaredFields()) {
        if (f.isAnnotationPresent(HopMetadataProperty.class)) {
          return candidate;
        }
      }
    }
    return null;
  }

  /** JDK types can never carry Hop annotations, so skip the hierarchy walk for them. */
  private static boolean isPlatformClass(Class<?> clazz) {
    String name = clazz.getName();
    return name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jakarta.");
  }

  private static Class<?> collectionElementType(Field field) {
    Type generic = field.getGenericType();
    if (generic instanceof ParameterizedType parameterized) {
      Type[] args = parameterized.getActualTypeArguments();
      if (args.length == 1 && args[0] instanceof Class<?> elementType) {
        return elementType;
      }
    }
    return null;
  }

  private String resolveClassName(IPlugin plugin) {
    Map<Class<?>, String> classMap = plugin.getClassMap();
    if (classMap == null || classMap.isEmpty()) {
      return null;
    }
    String className = plugin.getMainType() == null ? null : classMap.get(plugin.getMainType());
    if (className == null) {
      className = classMap.values().iterator().next();
    }
    return className;
  }

  /** The Hop locale that plugin labels were resolved against, as a BCP-47 tag. */
  static String currentLocaleTag() {
    Locale locale;
    try {
      locale = LanguageChoice.getInstance().getDefaultLocale();
    } catch (Exception | LinkageError e) {
      locale = Locale.getDefault();
    }
    return locale == null ? "" : locale.toLanguageTag();
  }

  private static String firstId(IPlugin plugin) {
    String[] ids = plugin.getIds();
    return (ids != null && ids.length > 0) ? ids[0] : "";
  }

  private static String joinKeywords(String[] keywords) {
    return keywords == null ? "" : String.join(",", keywords);
  }

  private static String clean(String value) {
    return value == null ? "" : value;
  }

  /** Serialize a property list to a compact JSON array string (dependency-free). */
  public static String propertiesToJson(List<PropertyRecord> properties) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < properties.size(); i++) {
      PropertyRecord p = properties.get(i);
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"field\":")
          .append(quote(p.field()))
          .append(",\"xml_key\":")
          .append(quote(p.xmlKey()))
          .append(",\"java_type\":")
          .append(quote(p.javaType()))
          .append(",\"password\":")
          .append(p.password())
          .append(",\"group\":")
          .append(quote(p.group()))
          .append(",\"group_key\":")
          .append(quote(p.groupKey()))
          .append('}');
    }
    return sb.append(']').toString();
  }

  private static String quote(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.append('"').toString();
  }
}
