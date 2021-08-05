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

import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaPlugin;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;

import java.time.LocalDate;
import java.time.ZoneId;

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
    switch (this) {
      case String:
        return valueMeta.getString(valueData);
      case Boolean:
        return valueMeta.getBoolean(valueData);
      case Float:
        return valueMeta.getNumber(valueData);
      case Integer:
        return valueMeta.getInteger(valueData);
      case Date:
        return valueMeta
            .getDate(valueData)
            .toInstant()
            .atZone(ZoneId.systemDefault())
            .toLocalDate();
      case LocalDateTime:
        return valueMeta
            .getDate(valueData)
            .toInstant()
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime();
      case ByteArray:
        return valueMeta.getBinary(valueData);
      case Duration:
      case DateTime:
      case Time:
      case Point:
      case LocalTime:
      case Map:
      case List:
      default:
        throw new HopValueException(
            "Data conversion to Neo4j type '"
                + name()
                + "' from value '"
                + valueMeta.toStringMeta()
                + "' is not supported yet");
    }
  }

  public int getHopType() throws HopValueException {

    switch (this) {
      case String:
      case Map: // convert to JSON
      case List: // convert to JSON
        return IValueMeta.TYPE_STRING;
      case Node: // Convert to Graph data type
      case Relationship:
      case Path:
        return ValueMetaGraph.TYPE_GRAPH;
      case Boolean:
        return IValueMeta.TYPE_BOOLEAN;
      case Float:
        return IValueMeta.TYPE_NUMBER;
      case Integer:
        return IValueMeta.TYPE_INTEGER;
      case Date:
      case LocalDateTime:
        return IValueMeta.TYPE_DATE;
      case ByteArray:
        return IValueMeta.TYPE_BINARY;
      case Duration:
      case DateTime:
      case Time:
      case Point:
      case LocalTime:
      default:
        throw new HopValueException(
            "Data conversion to Neo4j type '" + name() + "' is not supported yet");
    }
  }

  public static final GraphPropertyDataType getTypeFromHop(IValueMeta valueMeta) {
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        return GraphPropertyDataType.String;
      case IValueMeta.TYPE_NUMBER:
        return GraphPropertyDataType.Float;
      case IValueMeta.TYPE_DATE:
        return GraphPropertyDataType.LocalDateTime;
      case IValueMeta.TYPE_TIMESTAMP:
        return GraphPropertyDataType.LocalDateTime;
      case IValueMeta.TYPE_BOOLEAN:
        return GraphPropertyDataType.Boolean;
      case IValueMeta.TYPE_BINARY:
        return GraphPropertyDataType.ByteArray;
      case IValueMeta.TYPE_BIGNUMBER:
        return GraphPropertyDataType.String;
      case IValueMeta.TYPE_INTEGER:
        return GraphPropertyDataType.Integer;
      default:
        return GraphPropertyDataType.String;
    }
  }

  /**
   * Gets importType
   *
   * @return value of importType
   */
  public java.lang.String getImportType() {
    return importType;
  }

  /** @param importType The importType to set */
  public void setImportType(java.lang.String importType) {
    this.importType = importType;
  }
}
