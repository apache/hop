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

package org.apache.hop.neo4j.transforms.cypher;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.graph.GraphNodeValue;
import org.apache.hop.core.graph.GraphPathValue;
import org.apache.hop.core.graph.GraphRelationshipValue;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.core.data.GraphPropertyDataType;
import org.neo4j.driver.Value;

public class ReturnValue {
  @HopMetadataProperty(
      key = "name",
      injectionKey = "RETURN_NAME",
      injectionKeyDescription = "Cypher.Inject.RETURN_NAME")
  private String name;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "RETURN_TYPE",
      injectionKeyDescription = "Cypher.Inject.RETURN_TYPE")
  private String type;

  @HopMetadataProperty(
      key = "source_type",
      injectionKey = "RETURN_SOURCE_TYPE",
      injectionKeyDescription = "Cypher.Inject.RETURN_SOURCE_TYPE")
  private String sourceType;

  public ReturnValue() {}

  public ReturnValue(ReturnValue v) {
    this();
    this.name = v.name;
    this.type = v.type;
    this.sourceType = v.sourceType;
  }

  public ReturnValue(String name, String type, String sourceType) {
    this.name = name;
    this.type = type;
    this.sourceType = sourceType;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets type
   *
   * @return value of type
   */
  public String getType() {
    return type;
  }

  /**
   * @param type The type to set
   */
  public void setType(String type) {
    this.type = type;
  }

  /**
   * Gets sourceType
   *
   * @return value of sourceType
   */
  public String getSourceType() {
    return sourceType;
  }

  /**
   * @param sourceType The sourceType to set
   */
  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  /**
   * The return value of a column of a Bolt record, typed after the type of its value.
   *
   * @param name The column name
   * @param value The value of the column in a record
   * @return The return value
   * @throws HopValueException If the type of the value can't be converted to a Hop type
   */
  public static ReturnValue fromBoltValue(String name, Value value) throws HopValueException {
    String typeName = value.type().name().replaceAll("_", "").replace("LIST OF ANY?", "LIST");
    return of(name, GraphPropertyDataType.parseCode(typeName));
  }

  /**
   * The return value of a column of a result row of a graph connection, typed after its plain Java
   * value.
   *
   * @param name The column name
   * @param value The value of the column in a row
   * @return The return value
   * @throws HopValueException If the type of the value can't be converted to a Hop type
   */
  public static ReturnValue fromValue(String name, Object value) throws HopValueException {
    return of(name, getSourceType(value));
  }

  /** The source type of a plain Java value as graph connections return them. */
  static GraphPropertyDataType getSourceType(Object value) {
    if (value instanceof GraphNodeValue) {
      return GraphPropertyDataType.Node;
    }
    if (value instanceof GraphRelationshipValue) {
      return GraphPropertyDataType.Relationship;
    }
    if (value instanceof GraphPathValue) {
      return GraphPropertyDataType.Path;
    }
    if (value instanceof Integer
        || value instanceof Short
        || value instanceof Byte
        || value instanceof BigInteger) {
      return GraphPropertyDataType.Integer;
    }
    if (value instanceof Float || value instanceof BigDecimal) {
      return GraphPropertyDataType.Float;
    }
    GraphPropertyDataType type = GraphPropertyDataType.getTypeFromValue(value);
    return type == null ? GraphPropertyDataType.String : type;
  }

  private static ReturnValue of(String name, GraphPropertyDataType type) throws HopValueException {
    return new ReturnValue(name, ValueMetaFactory.getValueMetaName(type.getHopType()), type.name());
  }
}
