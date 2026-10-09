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
 *
 */

package org.apache.hop.neo4j.shared;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.data.GraphVectors;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.json.simple.JSONValue;
import org.neo4j.driver.Value;

public class NeoHopData {

  /**
   * Convert a plain Java result value, as graph connections other than Bolt return them, to the
   * given Hop type. Lists and maps (nodes and relationships included) become JSON strings.
   */
  public static Object convertToHopValue(String name, Object value, IValueMeta targetValueMeta)
      throws HopException {
    if (value == null) {
      return null;
    }
    try {
      switch (targetValueMeta.getType()) {
        case IValueMeta.TYPE_STRING:
          if (isGraphValue(value)) {
            // The same JSON as a Neo4j node or path converted to String
            return toGraphData(value).toJson().toJSONString();
          }
          if (value instanceof java.util.Map || value instanceof java.util.List) {
            return JSONValue.toJSONString(toJsonValue(value));
          }
          return value.toString();
        case ValueMetaGraph.TYPE_GRAPH:
          return toGraphData(value);
        case IValueMeta.TYPE_VECTOR:
          return GraphVectors.toFloatArray(value);
        case IValueMeta.TYPE_INTEGER:
          return value instanceof Number number
              ? number.longValue()
              : Long.valueOf(value.toString().trim());
        case IValueMeta.TYPE_NUMBER:
          return value instanceof Number number
              ? number.doubleValue()
              : Double.valueOf(value.toString().trim());
        case IValueMeta.TYPE_BOOLEAN:
          return value instanceof Boolean bool ? bool : Boolean.valueOf(value.toString().trim());
        case IValueMeta.TYPE_BIGNUMBER:
          return new BigDecimal(value.toString().trim());
        case IValueMeta.TYPE_DATE:
          if (value instanceof Date date) {
            // Graph connections write dates and times as UTC
            return java.sql.Date.valueOf(date.toInstant().atZone(ZoneOffset.UTC).toLocalDate());
          }
          if (value instanceof LocalDate localDate) {
            return java.sql.Date.valueOf(localDate);
          }
          if (value instanceof LocalDateTime localDateTime) {
            return java.sql.Date.valueOf(localDateTime.toLocalDate());
          }
          return java.sql.Date.valueOf(LocalDate.parse(value.toString().trim().substring(0, 10)));
        case IValueMeta.TYPE_TIMESTAMP:
          if (value instanceof Date date) {
            return java.sql.Timestamp.valueOf(
                LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC));
          }
          if (value instanceof LocalDateTime localDateTime) {
            return java.sql.Timestamp.valueOf(localDateTime);
          }
          if (value instanceof LocalDate localDate) {
            return java.sql.Timestamp.valueOf(localDate.atStartOfDay());
          }
          return java.sql.Timestamp.valueOf(LocalDateTime.parse(value.toString().trim()));
        default:
          throw new HopException(
              "Unable to convert a graph database value to type " + targetValueMeta.toStringMeta());
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to convert value '" + name + "' to type : " + targetValueMeta.getTypeDesc(), e);
    }
  }

  private static boolean isGraphValue(Object value) {
    return value instanceof GraphNodeValue
        || value instanceof GraphRelationshipValue
        || value instanceof GraphPathValue;
  }

  /** The nodes, relationships and paths in a value as graph data. */
  private static GraphData toGraphData(Object value) {
    GraphData graphData = new GraphData();
    graphData.updateFromValue(value);
    return graphData;
  }

  /** A value which JSON can hold: graph values become their JSON, dates and times text. */
  private static Object toJsonValue(Object value) {
    if (isGraphValue(value)) {
      return toGraphData(value).toJson();
    }
    if (value instanceof java.util.Map<?, ?> map) {
      java.util.Map<String, Object> json = new java.util.LinkedHashMap<>();
      map.forEach((key, element) -> json.put(String.valueOf(key), toJsonValue(element)));
      return json;
    }
    if (value instanceof java.util.List<?> list) {
      java.util.List<Object> json = new java.util.ArrayList<>();
      list.forEach(element -> json.add(toJsonValue(element)));
      return json;
    }
    if (value instanceof org.neo4j.driver.types.Vector vector) {
      return GraphVectors.toList(vector);
    }
    if (value instanceof java.time.temporal.Temporal || value instanceof Date) {
      return value instanceof Date date ? date.toInstant().toString() : value.toString();
    }
    return value;
  }

  public static Object convertNeoToHopValue(
      String recordValueName,
      Value recordValue,
      GraphPropertyDataType neoType,
      IValueMeta targetValueMeta)
      throws HopException {
    if (recordValue == null || recordValue.isNull()) {
      return null;
    }
    try {
      switch (targetValueMeta.getType()) {
        case IValueMeta.TYPE_STRING:
          return convertToString(recordValue, neoType);
        case ValueMetaGraph.TYPE_GRAPH:
          // This is for Node, Path and Relationship
          return convertToGraphData(recordValue, neoType);
        case IValueMeta.TYPE_VECTOR:
          return GraphVectors.toFloatArray(recordValue.asObject());
        case IValueMeta.TYPE_INTEGER:
          return recordValue.asLong();
        case IValueMeta.TYPE_NUMBER:
          return recordValue.asDouble();
        case IValueMeta.TYPE_BOOLEAN:
          return recordValue.asBoolean();
        case IValueMeta.TYPE_BIGNUMBER:
          return new BigDecimal(recordValue.asString());
        case IValueMeta.TYPE_DATE:
          if (neoType != null) {
            // Standard...
            return switch (neoType) {
              case LocalDateTime -> {
                LocalDateTime localDateTime = recordValue.asLocalDateTime();
                yield java.sql.Date.valueOf(localDateTime.toLocalDate());
              }
              case Date -> {
                LocalDate localDate = recordValue.asLocalDate();
                yield java.sql.Date.valueOf(localDate);
              }
              case DateTime -> {
                ZonedDateTime zonedDateTime = recordValue.asZonedDateTime();
                yield Date.from(zonedDateTime.toInstant());
              }
              default ->
                  throw new HopException(
                      "Conversion from Neo4j daa type "
                          + neoType.name()
                          + " to a Hop Date isn't supported yet");
            };
          } else {
            LocalDate localDate = recordValue.asLocalDate();
            return java.sql.Date.valueOf(localDate);
          }
        case IValueMeta.TYPE_TIMESTAMP:
          LocalDateTime localDateTime = recordValue.asLocalDateTime();
          return java.sql.Timestamp.valueOf(localDateTime);
        default:
          throw new HopException(
              "Unable to convert Neo4j data to type " + targetValueMeta.toStringMeta());
      }
    } catch (Exception e) {
      throw new HopException(
          "Unable to convert Neo4j record value '"
              + recordValueName
              + "' to type : "
              + targetValueMeta.getTypeDesc(),
          e);
    }
  }

  /**
   * A value of a Bolt record which converts to JSON: native Neo4j VECTOR values, also in lists and
   * maps, become lists of numbers. Other values are left as they are.
   */
  private static Object vectorsToLists(Object value) {
    if (value instanceof org.neo4j.driver.types.Vector vector) {
      return GraphVectors.toList(vector);
    }
    if (value instanceof List<?> list) {
      List<Object> converted = new ArrayList<>(list.size());
      for (Object element : list) {
        converted.add(vectorsToLists(element));
      }
      return converted;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> converted = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        converted.put(entry.getKey(), vectorsToLists(entry.getValue()));
      }
      return converted;
    }
    return value;
  }

  /**
   * Convert the given record value to String. For complex data types it's a conversion to JSON.
   *
   * @param recordValue The record value to convert to String
   * @param sourceType The Neo4j source type
   * @return The String value of the record value
   */
  public static String convertToString(Value recordValue, GraphPropertyDataType sourceType) {
    if (recordValue == null) {
      return null;
    }
    if (sourceType == null) {
      return JSONValue.toJSONString(vectorsToLists(recordValue.asObject()));
    }
    switch (sourceType) {
      case String:
        return recordValue.asString();
      case List:
        return JSONValue.toJSONString(vectorsToLists(recordValue.asList()));
      case Map:
        return JSONValue.toJSONString(vectorsToLists(recordValue.asMap()));
      case Node:
        {
          GraphData graphData = new GraphData();
          graphData.update(recordValue.asNode());
          return graphData.toJson().toJSONString();
        }
      case Path:
        {
          GraphData graphData = new GraphData();
          graphData.update(recordValue.asPath());
          return graphData.toJson().toJSONString();
        }
      default:
        return JSONValue.toJSONString(vectorsToLists(recordValue.asObject()));
    }
  }

  /**
   * Convert the given record value to String. For complex data types it's a conversion to JSON.
   *
   * @param recordValue The record value to convert to String
   * @param sourceType The Neo4j source type
   * @return The String value of the record value
   */
  public static GraphData convertToGraphData(Value recordValue, GraphPropertyDataType sourceType)
      throws HopException {
    if (recordValue == null) {
      return null;
    }
    if (sourceType == null) {
      throw new HopException(
          "Please specify a Neo4j source data type to convert to Graph.  NODE, RELATIONSHIP and PATH are supported.");
    }
    GraphData graphData;
    switch (sourceType) {
      case Node:
        graphData = new GraphData();
        graphData.update(recordValue.asNode());
        break;

      case Path:
        graphData = new GraphData();
        graphData.update(recordValue.asPath());
        break;

      default:
        throw new HopException(
            "We can only convert NODE, PATH and RELATIONSHIP source values to a Graph data type, not "
                + sourceType.name());
    }
    return graphData;
  }
}
