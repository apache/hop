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

package org.apache.hop.neo4j.execution.builder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hop.core.json.HopJson;

public abstract class BaseCypherBuilder implements ICypherBuilder {
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");

  protected StringBuilder cypher;
  protected Map<String, Object> parameters;

  protected boolean firstParameter;
  protected boolean firstReturn;

  protected BaseCypherBuilder() {
    this.cypher = new StringBuilder();
    this.parameters = new HashMap<>();
    this.firstParameter = true;
    this.firstReturn = true;
  }

  protected void addParameter(String property, Object value) {
    parameters.put(property, mapTypes(value));
  }

  public void withExtraClause(String clause) {
    cypher.append(clause).append(" ");
  }

  public String cypher() {
    return cypher.toString();
  }

  public Map<String, Object> parameters() {
    return parameters;
  }

  /**
   * Convert a Hop / Java value into a type the Neo4j driver accepts as a node property.
   *
   * <p>Unsupported values are coerced to String or JSON so a single sampled field cannot abort an
   * entire execution-data transaction.
   */
  protected Object mapTypes(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Timestamp timestamp) {
      return TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime());
    }
    if (value instanceof java.sql.Date sqlDate) {
      return sqlDate.toLocalDate();
    }
    if (value instanceof java.sql.Time sqlTime) {
      return sqlTime.toLocalTime();
    }
    if (value instanceof Date date) {
      return LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
    }
    if (value instanceof BigDecimal || value instanceof BigInteger) {
      return value.toString();
    }
    if (value instanceof Float number) {
      return number.doubleValue();
    }
    if (value instanceof Integer number) {
      return number.longValue();
    }
    if (value instanceof Short number) {
      return number.longValue();
    }
    if (value instanceof Byte number) {
      return number.longValue();
    }
    if (value instanceof Character || value instanceof UUID) {
      return value.toString();
    }
    if (value instanceof JsonNode node) {
      return node.toPrettyString();
    }
    if (value instanceof Map<?, ?> map) {
      return toJsonString(map);
    }
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof List<?> list) {
      return mapList(list);
    }
    if (value.getClass().isArray()) {
      int length = Array.getLength(value);
      List<Object> list = new ArrayList<>(length);
      for (int i = 0; i < length; i++) {
        list.add(Array.get(value, i));
      }
      return mapList(list);
    }
    if (isNeo4jPropertyValue(value)) {
      return value;
    }
    return String.valueOf(value);
  }

  private Object mapList(List<?> list) {
    List<Object> mapped = new ArrayList<>(list.size());
    Class<?> elementType = null;
    for (Object element : list) {
      Object mappedElement = mapTypes(element);
      if (mappedElement == null) {
        mapped.add(null);
        continue;
      }
      if (!isNeo4jPropertyValue(mappedElement)) {
        return toJsonString(list);
      }
      if (elementType == null) {
        elementType = mappedElement.getClass();
      } else if (!elementType.equals(mappedElement.getClass())) {
        return toJsonString(list);
      }
      mapped.add(mappedElement);
    }
    return mapped;
  }

  private String toJsonString(Object value) {
    try {
      return HopJson.newMapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return String.valueOf(value);
    }
  }

  private boolean isNeo4jPropertyValue(Object value) {
    return value instanceof Boolean
        || value instanceof Long
        || value instanceof Double
        || value instanceof String
        || value instanceof byte[]
        || value instanceof LocalDate
        || value instanceof LocalDateTime
        || value instanceof LocalTime
        || value instanceof OffsetTime
        || value instanceof OffsetDateTime
        || value instanceof ZonedDateTime
        || value instanceof Duration;
  }
}
