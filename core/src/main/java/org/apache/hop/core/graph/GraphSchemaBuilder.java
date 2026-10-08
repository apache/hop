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

package org.apache.hop.core.graph;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Collects what is known about the labels, relationship types and properties of a graph database
 * into a {@link GraphSchema}: either from sampled nodes and relationships, or from groups the
 * database describes, like the label combinations of Neo4j's schema procedures.
 *
 * <p>A property is mandatory for a label when every sampled node, or every group with the label,
 * has it. Type names are the same for all databases: String, Integer, Float, Boolean, Date,
 * LocalDateTime, DateTime, LocalTime, Time, Duration, Point, Vector, Map and List, with the type of
 * the elements when they all have the same one, like List&lt;String&gt;.
 */
public class GraphSchemaBuilder {

  private record Key(GraphObjectType type, String name) {}

  private static class Property {
    private long present;
    private final Set<String> mandatoryGroups = new HashSet<>();
    private final Set<String> types = new LinkedHashSet<>();
    private boolean mandatoryUnknown;
  }

  private static class Element {
    private long sampled;
    private final Set<String> groups = new HashSet<>();
    private final Set<String> startLabels = new LinkedHashSet<>();
    private final Set<String> endLabels = new LinkedHashSet<>();
    private final Map<String, Property> properties = new LinkedHashMap<>();

    long count() {
      return sampled + groups.size();
    }
  }

  private final Map<Key, Element> elements = new LinkedHashMap<>();

  private Element element(GraphObjectType type, String name) {
    return elements.computeIfAbsent(new Key(type, name), k -> new Element());
  }

  /** A label or relationship type which exists, without anything known about it. */
  public GraphSchemaBuilder addElement(GraphObjectType type, String name) {
    if (name != null) {
      element(type, name);
    }
    return this;
  }

  /** A sampled node, counted for one of its labels. */
  public GraphSchemaBuilder addSampledNode(String label, Map<String, ?> properties) {
    addSample(element(GraphObjectType.NODE, label), properties);
    return this;
  }

  /** A sampled relationship with the labels of its start and end node. */
  public GraphSchemaBuilder addSampledRelationship(
      String type,
      Collection<String> startLabels,
      Collection<String> endLabels,
      Map<String, ?> properties) {
    Element element = element(GraphObjectType.RELATIONSHIP, type);
    addAll(element.startLabels, startLabels);
    addAll(element.endLabels, endLabels);
    addSample(element, properties);
    return this;
  }

  private static void addSample(Element element, Map<String, ?> properties) {
    element.sampled++;
    if (properties == null) {
      return;
    }
    for (Map.Entry<String, ?> entry : properties.entrySet()) {
      if (entry.getValue() == null) {
        continue;
      }
      Property property = element.properties.computeIfAbsent(entry.getKey(), k -> new Property());
      property.present++;
      property.types.add(typeName(entry.getValue()));
    }
  }

  /**
   * A group of nodes or relationships the database describes, for example a combination of labels.
   * A property is mandatory for the label or type if it is mandatory in all its groups.
   */
  public GraphSchemaBuilder addGroup(
      GraphObjectType type,
      String name,
      String group,
      Collection<String> startLabels,
      Collection<String> endLabels) {
    Element element = element(type, name);
    element.groups.add(group);
    addAll(element.startLabels, startLabels);
    addAll(element.endLabels, endLabels);
    return this;
  }

  /**
   * A property of a group the database describes, added with {@link #addGroup} before.
   *
   * @param types The type names the database gives, normalized with {@link #normalizeTypeName}
   * @param mandatory True if all nodes or relationships of the group have it, null if unknown
   */
  public GraphSchemaBuilder addGroupProperty(
      GraphObjectType type,
      String name,
      String group,
      String property,
      Collection<String> types,
      Boolean mandatory) {
    Element element = element(type, name);
    element.groups.add(group);
    Property p = element.properties.computeIfAbsent(property, k -> new Property());
    if (mandatory == null) {
      p.mandatoryUnknown = true;
    } else if (mandatory) {
      p.mandatoryGroups.add(group);
    }
    if (types != null) {
      for (String typeName : types) {
        if (typeName != null && !typeName.isEmpty()) {
          p.types.add(normalizeTypeName(typeName));
        }
      }
    }
    return this;
  }

  /** Add the labels of the nodes a relationship type starts from and ends at. */
  public GraphSchemaBuilder addRelationshipEnds(
      String type, Collection<String> startLabels, Collection<String> endLabels) {
    Element element = element(GraphObjectType.RELATIONSHIP, type);
    addAll(element.startLabels, startLabels);
    addAll(element.endLabels, endLabels);
    return this;
  }

  private static void addAll(Set<String> set, Collection<String> values) {
    if (values != null) {
      values.stream().filter(Objects::nonNull).forEach(set::add);
    }
  }

  /** The names of the labels or relationship types added so far. */
  public List<String> getNames(GraphObjectType type) {
    List<String> names = new ArrayList<>();
    elements.keySet().stream().filter(k -> k.type() == type).forEach(k -> names.add(k.name()));
    return names;
  }

  /**
   * The schema: nodes before relationships, labels and types sorted by name, properties in the
   * order they were seen.
   *
   * @param indexes The indexes, null if unknown
   * @param sampled True if the schema comes from a sample
   */
  public GraphSchema build(List<GraphIndex> indexes, boolean sampled) {
    List<GraphSchemaEntry> entries = new ArrayList<>();
    for (GraphObjectType type : GraphObjectType.values()) {
      List<Map.Entry<Key, Element>> sorted =
          elements.entrySet().stream()
              .filter(e -> e.getKey().type() == type)
              .sorted(Map.Entry.comparingByKey((a, b) -> a.name().compareTo(b.name())))
              .toList();
      for (Map.Entry<Key, Element> entry : sorted) {
        Element element = entry.getValue();
        List<String> start = new ArrayList<>(element.startLabels);
        List<String> end = new ArrayList<>(element.endLabels);
        if (element.properties.isEmpty()) {
          entries.add(
              new GraphSchemaEntry(type, entry.getKey().name(), null, null, null, start, end));
          continue;
        }
        long count = element.count();
        for (Map.Entry<String, Property> property : element.properties.entrySet()) {
          Property p = property.getValue();
          Boolean mandatory =
              count == 0 || p.mandatoryUnknown
                  ? null
                  : p.present + p.mandatoryGroups.size() >= count;
          entries.add(
              new GraphSchemaEntry(
                  type,
                  entry.getKey().name(),
                  property.getKey(),
                  new ArrayList<>(p.types),
                  mandatory,
                  start,
                  end));
        }
      }
    }
    return new GraphSchema(entries, indexes, sampled);
  }

  /** The type name of a property value returned by a graph connection. */
  public static String typeName(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof CharSequence || value instanceof Character) {
      return "String";
    }
    if (value instanceof Boolean) {
      return "Boolean";
    }
    if (value instanceof Long
        || value instanceof Integer
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger) {
      return "Integer";
    }
    if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
      return "Float";
    }
    if (value instanceof LocalDate) {
      return "Date";
    }
    if (value instanceof LocalDateTime) {
      return "LocalDateTime";
    }
    if (value instanceof ZonedDateTime
        || value instanceof OffsetDateTime
        || value instanceof Instant
        || value instanceof Date) {
      return "DateTime";
    }
    if (value instanceof LocalTime) {
      return "LocalTime";
    }
    if (value instanceof OffsetTime) {
      return "Time";
    }
    if (value instanceof TemporalAmount) {
      return "Duration";
    }
    if (value instanceof float[] || value instanceof double[]) {
      return "Vector";
    }
    if (value instanceof byte[]) {
      return "ByteArray";
    }
    if (value instanceof Map<?, ?>) {
      return "Map";
    }
    if (value instanceof GraphNodeValue) {
      return "Node";
    }
    if (value instanceof GraphRelationshipValue) {
      return "Relationship";
    }
    if (value instanceof Collection<?> collection) {
      Set<String> elementTypes = new LinkedHashSet<>();
      for (Object element : collection) {
        if (element != null) {
          elementTypes.add(typeName(element));
        }
      }
      return elementTypes.size() == 1 ? "List<" + elementTypes.iterator().next() + ">" : "List";
    }
    String className = value.getClass().getSimpleName();
    if (className.contains("Vector")) {
      return "Vector";
    }
    if (className.contains("Point")) {
      return "Point";
    }
    if (className.contains("Duration")) {
      return "Duration";
    }
    return className;
  }

  /**
   * A type name as Neo4j or Memgraph give it in the same form as {@link #typeName(Object)}: Long
   * and Int become Integer, Double becomes Float, StringArray becomes List&lt;String&gt;. The GQL
   * type names of Neo4j 2025 and later, like STRING NOT NULL or LIST&lt;INTEGER NOT NULL&gt;,
   * become String and List&lt;Integer&gt;, as Neo4j 5 gives them.
   */
  public static String normalizeTypeName(String typeName) {
    if (typeName == null) {
      return null;
    }
    String name = typeName.trim();
    if (name.endsWith(" NOT NULL")) {
      name = name.substring(0, name.length() - " NOT NULL".length()).trim();
    }
    if (name.startsWith("LIST<") && name.endsWith(">")) {
      String elementType = name.substring("LIST<".length(), name.length() - 1);
      return "ANY".equals(elementType) ? "List" : "List<" + normalizeTypeName(elementType) + ">";
    }
    if (name.endsWith("Array") && name.length() > "Array".length() && !"ByteArray".equals(name)) {
      return "List<" + normalizeTypeName(name.substring(0, name.length() - "Array".length())) + ">";
    }
    if (name.startsWith("List[") && name.endsWith("]")) {
      String elementType = name.substring("List[".length(), name.length() - 1);
      return "Any".equals(elementType) ? "List" : "List<" + normalizeTypeName(elementType) + ">";
    }
    return switch (name) {
      case "Long", "Int", "Integer" -> "Integer";
      case "Double", "Float" -> "Float";
      case "Bool", "Boolean" -> "Boolean";
      case "ZonedDateTime", "ZONED DATETIME" -> "DateTime";
      case "STRING" -> "String";
      case "INTEGER" -> "Integer";
      case "FLOAT" -> "Float";
      case "BOOLEAN" -> "Boolean";
      case "DATE" -> "Date";
      case "LOCAL DATETIME" -> "LocalDateTime";
      case "ZONED TIME" -> "Time";
      case "LOCAL TIME" -> "LocalTime";
      case "DURATION" -> "Duration";
      case "POINT" -> "Point";
      case "MAP" -> "Map";
      default -> name.startsWith("VECTOR") ? "Vector" : name;
    };
  }
}
