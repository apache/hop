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

package org.apache.hop.lineage;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.lineage.api.RelationalLineage;
import org.apache.hop.lineage.model.RelationalIoOperation;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;

/**
 * Reads the relational lineage a transform declares on its metadata: which table it touches, on
 * which connection, and how its stream fields map to that table's columns.
 *
 * <p>This replaces per-transform emission code. A transform marked {@link RelationalLineage}
 * describes its table access once, declaratively, through the {@code RDBMS_*} property annotations
 * it already carries for search and metadata injection; the engine derives the lineage from that.
 * The alternative — every transform assembling and emitting its own event — was both repetitive and
 * silently incomplete, since nothing fails when a transform simply forgets to do it.
 *
 * <p>What is found here is the <b>static</b> half of a lineage observation. Resolving variables,
 * reading the row shape and tracing each stream field back to the transform that produced it are
 * runtime concerns, handled by the caller (see {@code LineageRelationalIoEmitter}).
 *
 * <p>Walking is defensive throughout: unreadable fields are skipped, cycles are broken, and any
 * failure yields a partial or empty result rather than an exception, because lineage must never
 * affect the transform it is observing.
 */
public final class LineageMetadataWalker {

  /** Guards against a pathological or cyclic metadata graph. */
  private static final int MAX_DEPTH = 6;

  private LineageMetadataWalker() {}

  /** One stream-field-to-column mapping declared on a metadata object. */
  public record ColumnMapping(String targetColumn, String streamField) {}

  /**
   * The relational access a transform's metadata declares. All string values are raw (unresolved)
   * metadata; the caller substitutes variables.
   */
  public record Declaration(
      RelationalIoOperation operation,
      String connectionName,
      String schemaName,
      String tableName,
      boolean overwrite,
      boolean tableNameFromField,
      List<ColumnMapping> columns) {

    /** Whether this declaration can produce a dataset identity at all. */
    public boolean isUsable() {
      return !StringUtils.isBlank(connectionName)
          && !StringUtils.isBlank(tableName)
          && !tableNameFromField;
    }
  }

  /**
   * Reads the declaration from a transform metadata instance, or returns {@code null} when the
   * class is not annotated with {@link RelationalLineage} (i.e. it does not participate in
   * relational lineage).
   */
  public static Declaration read(Object transformMeta) {
    if (transformMeta == null) {
      return null;
    }
    RelationalLineage declared = transformMeta.getClass().getAnnotation(RelationalLineage.class);
    if (declared == null) {
      return null;
    }
    Collector collector = new Collector(declared);
    try {
      collector.walk(transformMeta, 0);
    } catch (Exception e) {
      // Partial results are still useful; a broken walk must not reach the transform.
      return collector.toDeclaration();
    }
    return collector.toDeclaration();
  }

  /** Accumulates the annotated values found while descending through a metadata object graph. */
  private static final class Collector {
    private final RelationalLineage declared;
    private final Set<Object> visited =
        java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<ColumnMapping> columns = new ArrayList<>();

    private String connectionName;
    private String schemaName;
    private String tableName;
    private String tableNameFromFieldValue;
    private boolean overwrite;

    Collector(RelationalLineage declared) {
      this.declared = declared;
    }

    void walk(Object node, int depth) {
      if (node == null || depth > MAX_DEPTH || !visited.add(node)) {
        return;
      }
      // A mapping object carrying both halves of a column pair is a leaf: record the pair and do
      // not descend further, so nested strings cannot be mistaken for another mapping.
      if (collectColumnPair(node)) {
        return;
      }
      for (Class<?> c = node.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
        for (Field field : c.getDeclaredFields()) {
          HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
          if (property == null) {
            continue;
          }
          Object value = readField(field, node);
          if (value == null) {
            continue;
          }
          collectScalar(property.hopMetadataPropertyType(), field, value);
          descend(value, depth);
        }
      }
    }

    /** Follows nested metadata objects and collections, skipping plain values. */
    private void descend(Object value, int depth) {
      if (value instanceof Collection<?> collection) {
        for (Object element : collection) {
          if (isMetadataObject(element)) {
            walk(element, depth + 1);
          }
        }
      } else if (value instanceof Map<?, ?> map) {
        for (Object element : map.values()) {
          if (isMetadataObject(element)) {
            walk(element, depth + 1);
          }
        }
      } else if (isMetadataObject(value)) {
        walk(value, depth + 1);
      }
    }

    /**
     * Records a {@code target column ← stream field} pair when an object declares both halves.
     * Requiring both is what keeps generated columns — a dimension's technical key declares a
     * column but no source — out of the column lineage instead of appearing with no origin.
     *
     * <p>A mapping object is expected to declare exactly one of each. Declaring two is ambiguous —
     * a key mapping holds other stream fields, such as the second bound of a BETWEEN condition, and
     * nothing says which one feeds the column — so nothing is recorded rather than the last one
     * found silently winning.
     */
    private boolean collectColumnPair(Object node) {
      String target = null;
      String source = null;
      int targets = 0;
      int sources = 0;
      for (Class<?> c = node.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
        for (Field field : c.getDeclaredFields()) {
          HopMetadataProperty property = field.getAnnotation(HopMetadataProperty.class);
          if (property == null || field.getType() != String.class) {
            continue;
          }
          if (property.hopMetadataPropertyType() == HopMetadataPropertyType.RDBMS_COLUMN) {
            target = (String) readField(field, node);
            targets++;
          } else if (property.hopMetadataPropertyType() == HopMetadataPropertyType.STREAM_FIELD) {
            source = (String) readField(field, node);
            sources++;
          }
        }
      }
      if (targets > 1 || sources > 1) {
        return false;
      }
      if (StringUtils.isBlank(target) || StringUtils.isBlank(source)) {
        return false;
      }
      columns.add(new ColumnMapping(target, source));
      return true;
    }

    private void collectScalar(HopMetadataPropertyType type, Field field, Object value) {
      switch (type) {
        case RDBMS_CONNECTION -> connectionName = firstNonBlank(connectionName, value);
        case RDBMS_SCHEMA -> schemaName = firstNonBlank(schemaName, value);
        case RDBMS_TABLE -> {
          if (field.getName().equals(declared.tableNameFromFieldProperty())) {
            tableNameFromFieldValue = firstNonBlank(tableNameFromFieldValue, value);
          } else {
            tableName = firstNonBlank(tableName, value);
          }
        }
        case RDBMS_TRUNCATE -> overwrite = overwrite || meansOverwrite(value);
        default -> {
          // Not part of the relational dataset identity.
        }
      }
    }

    /**
     * Whether a {@code RDBMS_TRUNCATE} value marks the write as replacing the target. A boolean
     * says so directly; loaders that express it as a load action ({@code TRUNCATE}, {@code
     * REPLACE}) are matched against the values the transform declared.
     */
    private boolean meansOverwrite(Object value) {
      if (value instanceof Boolean b) {
        return b;
      }
      if (value instanceof String s) {
        for (String candidate : declared.overwriteWhen()) {
          if (candidate.equalsIgnoreCase(s.trim())) {
            return true;
          }
        }
      }
      return false;
    }

    Declaration toDeclaration() {
      boolean fromField =
          !StringUtils.isBlank(declared.tableNameFromFieldProperty())
              && !StringUtils.isBlank(tableNameFromFieldValue);
      return new Declaration(
          declared.operation(),
          connectionName,
          schemaName,
          tableName,
          overwrite,
          fromField,
          List.copyOf(columns));
    }

    private static String firstNonBlank(String current, Object candidate) {
      if (!StringUtils.isBlank(current) || !(candidate instanceof String s) || s.isBlank()) {
        return current;
      }
      return s;
    }
  }

  /** Only descends into Hop's own metadata classes, never into JDK or third-party types. */
  private static boolean isMetadataObject(Object value) {
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
      // A field the JVM will not let us read simply contributes nothing.
      return null;
    }
  }
}
