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
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;

@SuppressWarnings("java:S115")
public enum GraphPropertyDataType {
  String("string"),
  Integer("long"),
  Float("double"),
  Number("doubler"),
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
  Path("Path");

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

    throw new RuntimeException("Unsupported object with class: " + object.getClass().getName());
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
