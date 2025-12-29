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
import java.time.ZonedDateTime;
import java.util.Date;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.neo4j.core.data.GraphData;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.apache.hop.neo4j.core.value.ValueMetaGraph;
import org.json.simple.JSONValue;
import org.neo4j.driver.Value;

public class NeoHopData {

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
      return JSONValue.toJSONString(recordValue.asObject());
    }
    switch (sourceType) {
      case String:
        return recordValue.asString();
      case List:
        return JSONValue.toJSONString(recordValue.asList());
      case Map:
        return JSONValue.toJSONString(recordValue.asMap());
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
        return JSONValue.toJSONString(recordValue.asObject());
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
