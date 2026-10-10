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

package org.apache.hop.neo4j.core.data;

import java.time.LocalDate;
import java.time.ZoneId;
import org.apache.hop.core.exception.HopRuntimeException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;

@SuppressWarnings("java:S115")
public enum GraphPropertyDataType {
  String("string"),
  Integer("long"),
  Float("double"),
  Number("double"),
  Boolean("boolean"),
  Date("date"),
  LocalDateTime("localdatetime"),
  ByteArray(null),
  Time("time"),
  Point(null),
  Duration("duration"),
  LocalTime("localtime"),
  DateTime("datetime"),
  List("List"),
  Map("Map"),
  Node("Node"),
  Relationship("Relationship"),
  Path("Path"),
  /** An embedding: the Hop Vector value type. Imports as a float array. */
  Vector("float[]");

  private String importType;

  GraphPropertyDataType(java.lang.String importType) {
    this.importType = importType;
  }

  /**
   * Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */
  public static String getCode(GraphPropertyDataType type) {
    if (type == null) {
      return null;
    }
    return type.name();
  }

  /**
   * Default to String in case we can't recognize the code or is null
   *
   * @param code
   * @return
   */
  public static GraphPropertyDataType parseCode(String code) {
    if (code == null) {
      return String;
    }
    for (GraphPropertyDataType type : values()) {
      if (type.name().equalsIgnoreCase(code)) {
        return type;
      }
    }
    return String;
  }

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
  }

  /**
   * The type of a plain Java value as graph connections return them. Unlike {@link
   * #getTypeFromNeo4jValue(Object)} this never fails: lists, maps and other dates and times have a
   * type too, anything else is a String.
   */
  public static GraphPropertyDataType getTypeFromValue(Object object) {
    if (object instanceof java.util.List) {
      return List;
    }
    if (object instanceof java.util.Map) {
      return Map;
    }
    if (object instanceof java.time.ZonedDateTime
        || object instanceof java.time.OffsetDateTime
        || object instanceof java.util.Date) {
      return DateTime;
    }
    if (object instanceof java.time.OffsetTime) {
      return Time;
    }
    if (object instanceof byte[]) {
      return ByteArray;
    }
    try {
      return getTypeFromNeo4jValue(object);
    } catch (HopRuntimeException e) {
      return String;
    }
  }

  public static GraphPropertyDataType getTypeFromNeo4jValue(Object object) {
    if (object == null) {
      return null;
    }

    if (object instanceof Long) {
      return Integer;
    }
    if (object instanceof Double) {
      return Float;
    }
    if (object instanceof Number) {
      return Number;
    }
    if (object instanceof String) {
      return String;
    }
    if (object instanceof Boolean) {
      return Boolean;
    }
    if (object instanceof LocalDate) {
      return Date;
    }
    if (object instanceof java.time.LocalDateTime) {
      return LocalDateTime;
    }
    if (object instanceof java.time.LocalTime) {
      return LocalTime;
    }
    if (object instanceof java.time.Duration) {
      return Duration;
    }
    if (object instanceof org.neo4j.driver.types.Vector || object instanceof float[]) {
      return Vector;
    }

    throw new HopRuntimeException("Unsupported object with class: " + object.getClass().getName());
  }

  /**
   * Convert the given Hop value to a Neo4j data type
   *
   * @param valueMeta
   * @param valueData
   * @return
   */
  public Object convertFromHop(IValueMeta valueMeta, Object valueData) throws HopValueException {

    if (valueMeta.isNull(valueData)) {
      return null;
    }
    return switch (this) {
      case String -> valueMeta.getString(valueData);
      case Boolean -> valueMeta.getBoolean(valueData);
      case Float -> valueMeta.getNumber(valueData);
      case Integer -> valueMeta.getInteger(valueData);
      case Date ->
          valueMeta.getDate(valueData).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
      case LocalDateTime ->
          valueMeta.getDate(valueData).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
      case ByteArray -> valueMeta.getBinary(valueData);
      case Vector -> GraphVectors.toList(valueMeta, valueData);
      default ->
          throw new HopValueException(
              "Data conversion to Neo4j type '"
                  + name()
                  + "' from value '"
                  + valueMeta.toStringMeta()
                  + "' is not supported yet");
    };
  }

  public int getHopType() throws HopValueException {

    return switch (this) {
      case String, Map, List -> // convert to JSON
          IValueMeta.TYPE_STRING;
      case Node, Relationship, Path -> ValueMetaGraph.TYPE_GRAPH;
      case Boolean -> IValueMeta.TYPE_BOOLEAN;
      case Float -> IValueMeta.TYPE_NUMBER;
      case Integer -> IValueMeta.TYPE_INTEGER;
      case Date, LocalDateTime -> IValueMeta.TYPE_DATE;
      case ByteArray -> IValueMeta.TYPE_BINARY;
      case Vector -> IValueMeta.TYPE_VECTOR;
      default ->
          throw new HopValueException(
              "Data conversion to Neo4j type '" + name() + "' is not supported yet");
    };
  }

  public static final GraphPropertyDataType getTypeFromHop(IValueMeta valueMeta) {
    return switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING -> GraphPropertyDataType.String;
      case IValueMeta.TYPE_NUMBER -> GraphPropertyDataType.Float;
      case IValueMeta.TYPE_DATE -> GraphPropertyDataType.LocalDateTime;
      case IValueMeta.TYPE_TIMESTAMP -> GraphPropertyDataType.LocalDateTime;
      case IValueMeta.TYPE_BOOLEAN -> GraphPropertyDataType.Boolean;
      case IValueMeta.TYPE_BINARY -> GraphPropertyDataType.ByteArray;
      case IValueMeta.TYPE_BIGNUMBER -> GraphPropertyDataType.String;
      case IValueMeta.TYPE_INTEGER -> GraphPropertyDataType.Integer;
      case IValueMeta.TYPE_VECTOR -> GraphPropertyDataType.Vector;
      default -> GraphPropertyDataType.String;
    };
  }

  /**
   * Gets importType
   *
   * @return value of importType
   */
  public java.lang.String getImportType() {
    return importType;
  }

  /**
   * @param importType The importType to set
   */
  public void setImportType(java.lang.String importType) {
    this.importType = importType;
  }
}
