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
 *
 */

package org.apache.hop.neo4j.model;

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaTimestamp;

@SuppressWarnings("java:S115")
public enum GraphPropertyType {
  String,
  Integer,
  Float,
  Boolean,
  Date,
  LocalDateTime,
  ByteArray,
  Time,
  Point,
  Duration,
  LocalTime,
  DateTime,
  Array;

  /**
   * Get the code for a type, handles the null case
   *
   * @param type
   * @return
   */
  public static String getCode(GraphPropertyType type) {
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
  public static GraphPropertyType parseCode(String code) {
    if (code == null) {
      return String;
    }
    try {
      return GraphPropertyType.valueOf(code);
    } catch (IllegalArgumentException e) {
      return String;
    }
  }

  public static String[] getNames() {
    String[] names = new String[values().length];
    for (int i = 0; i < names.length; i++) {
      names[i] = values()[i].name();
    }
    return names;
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
    TimeZone timeZone = valueMeta.getDateFormatTimeZone();
    if (timeZone == null) {
      timeZone = TimeZone.getDefault();
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
        return valueMeta.getDate(valueData).toInstant().atZone(timeZone.toZoneId()).toLocalDate();
      case LocalDateTime:
        return valueMeta
            .getDate(valueData)
            .toInstant()
            .atZone(timeZone.toZoneId())
            .toLocalDateTime();
      case ByteArray:
        return valueMeta.getBinary(valueData);
      case DateTime:
        {
          ZonedDateTime zonedDateTime;
          if (valueMeta instanceof ValueMetaTimestamp valueMetaTimestamp) {
            Timestamp timestamp = valueMetaTimestamp.getTimestamp(valueData);
            zonedDateTime = timestamp.toInstant().atZone(timeZone.toZoneId());
          } else {
            java.util.Date date = valueMeta.getDate(valueData);
            zonedDateTime = date.toInstant().atZone(timeZone.toZoneId());
          }
          return zonedDateTime;
        }
      case Array:
        // Array conversion requires separator and enclosure - use overloaded method
        throw new HopValueException(
            "Array conversion requires separator and enclosure parameters. Use convertFromHop(valueMeta, valueData, separator, enclosure) instead.");
      case LocalTime, Time, Point, Duration:
      default:
        throw new HopValueException(
            "Data conversion to Neo4j type '"
                + name()
                + "' from value '"
                + valueMeta.toStringMeta()
                + "' is not supported yet");
    }
  }

  /**
   * Convert the given Hop value to a Neo4j Array type
   *
   * @param valueMeta The Hop value metadata
   * @param valueData The actual value data
   * @param separator Character used to separate array elements (e.g., ",", ";", "|")
   * @param enclosure Character used to enclose each element (e.g., "\"", "'", or empty string)
   * @return List of values (typically List&lt;Double&gt; for numeric arrays)
   */
  public Object convertFromHop(
      IValueMeta valueMeta, Object valueData, String separator, String enclosure)
      throws HopValueException {
    if (this != Array) {
      // For non-Array types, use the standard conversion
      return convertFromHop(valueMeta, valueData);
    }

    if (valueMeta.isNull(valueData)) {
      return null;
    }

    String arrayString = valueMeta.getString(valueData);
    if (StringUtils.isEmpty(arrayString)) {
      return new java.util.ArrayList<>();
    }

    // Parse the array string using separator and enclosure
    java.util.List<Object> arrayList = new java.util.ArrayList<>();
    String sep = StringUtils.isEmpty(separator) ? "," : separator;
    String encl = StringUtils.isEmpty(enclosure) ? "" : enclosure;

    // Remove leading/trailing whitespace
    arrayString = arrayString.trim();

    // Split by separator
    String[] parts = arrayString.split(sep, -1); // -1 to keep trailing empty strings

    for (String part : parts) {
      part = part.trim();
      if (part.isEmpty()) {
        continue; // Skip empty elements
      }

      // Remove enclosure if present
      if (!encl.isEmpty() && part.startsWith(encl) && part.endsWith(encl)) {
        part = part.substring(encl.length(), part.length() - encl.length());
      }

      part = part.trim();

      // Try to parse as number (Double), fallback to String
      try {
        // Try parsing as double first (most common for embeddings)
        double value = Double.parseDouble(part);
        arrayList.add(value);
      } catch (NumberFormatException e) {
        // If not a number, keep as string
        arrayList.add(part);
      }
    }

    return arrayList;
  }

  public static final GraphPropertyType getTypeFromHop(IValueMeta valueMeta) {
    switch (valueMeta.getType()) {
      case IValueMeta.TYPE_STRING:
        return GraphPropertyType.String;
      case IValueMeta.TYPE_NUMBER:
        return GraphPropertyType.Float;
      case IValueMeta.TYPE_DATE:
        return GraphPropertyType.LocalDateTime;
      case IValueMeta.TYPE_TIMESTAMP:
        return GraphPropertyType.LocalDateTime;
      case IValueMeta.TYPE_BOOLEAN:
        return GraphPropertyType.Boolean;
      case IValueMeta.TYPE_BINARY:
        return GraphPropertyType.ByteArray;
      case IValueMeta.TYPE_BIGNUMBER:
        return GraphPropertyType.String;
      case IValueMeta.TYPE_INTEGER:
        return GraphPropertyType.Integer;
      default:
        return GraphPropertyType.String;
    }
  }
}
