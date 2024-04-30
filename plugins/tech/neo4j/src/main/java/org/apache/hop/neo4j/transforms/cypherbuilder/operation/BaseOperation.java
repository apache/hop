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

package org.apache.hop.neo4j.transforms.cypherbuilder.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.neo4j.transforms.cypherbuilder.Parameter;
import org.apache.hop.neo4j.transforms.cypherbuilder.Property;

public class BaseOperation implements IOperation {

  /** The name of the operation makes it uniquely identifiable. */
  @HopMetadataProperty protected String name;

  /** What do we want to do? */
  @HopMetadataProperty protected OperationType operationType;

  /** The list of labels to match or apply */
  @HopMetadataProperty protected List<String> labels;

  /** This is the node or edge alias */
  @HopMetadataProperty protected String alias;

  /** What are the key properties that are needed. */
  @HopMetadataProperty protected List<Property> keys;

  /** Are there any properties to set or return? */
  @HopMetadataProperty protected List<Property> properties;

  public BaseOperation() {
    this.labels = new ArrayList<>();
    this.keys = new ArrayList<>();
    this.properties = new ArrayList<>();
  }

  public BaseOperation(BaseOperation o) {
    this();
    this.name = o.name;
    this.operationType = o.operationType;
    this.alias = o.alias;
    this.labels.addAll(o.labels);
    o.keys.forEach(k -> this.keys.add(new Property(k)));
    o.properties.forEach(p -> this.properties.add(new Property(p)));
  }

  public BaseOperation clone() {
    return new BaseOperation(this);
  }

  public BaseOperation(OperationType operationType) {
    this();
    this.operationType = operationType;
  }

  @Override
  public String getCypherClause(String unwindAlias, List<Parameter> parameters)
      throws HopException {
    throw new HopException(
        "Cypher clause generation is not implemented for this operation type: " + operationType);
  }

  @Override
  public boolean needsWriteTransaction() {
    throw new RuntimeException(
        "Write transaction information is not provided for operation type " + operationType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BaseOperation that = (BaseOperation) o;
    if (operationType != that.operationType) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(operationType, name);
  }

  protected String getLabelsClause() {
    StringBuilder cypher = new StringBuilder();
    cypher.append(alias);
    for (String label : labels) {
      cypher.append(":").append(label);
    }
    return cypher.toString();
  }

  protected String getKeysClause(String unwindAlias) {
    StringBuilder cypher = new StringBuilder();
    cypher.append(" {");
    for (int k = 0; k < keys.size(); k++) {
      Property key = keys.get(k);
      if (k > 0) {
        cypher.append(", ");
      }
      // id : {pId}
      //
      cypher.append(key.getName()).append(":").append(key.formatParameter(unwindAlias));
    }
    cypher.append("} ");
    return cypher.toString();
  }

  protected String getSetClause(String unwindAlias) {
    StringBuilder cypher = new StringBuilder();
    cypher.append("SET ");
    for (int p = 0; p < properties.size(); p++) {
      Property property = properties.get(p);
      if (p > 0) {
        cypher.append(", ");
      }
      // n.prop1={param1}
      //
      cypher
          .append(alias)
          .append(".")
          .append(property.getName())
          .append("=")
          .append(property.formatParameter(unwindAlias));
    }
    cypher.append(" ");
    return cypher.toString();
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
   * Sets name
   *
   * @param name value of name
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets operationType
   *
   * @return value of operationType
   */
  public OperationType getOperationType() {
    return operationType;
  }

  /**
   * Sets operationType
   *
   * @param operationType value of operationType
   */
  public void setOperationType(OperationType operationType) {
    this.operationType = operationType;
  }

  /**
   * Gets labels
   *
   * @return value of labels
   */
  public List<String> getLabels() {
    return labels;
  }

  /**
   * Sets labels
   *
   * @param labels value of labels
   */
  public void setLabels(List<String> labels) {
    this.labels = labels;
  }

  /**
   * Gets alias
   *
   * @return value of alias
   */
  public String getAlias() {
    return alias;
  }

  /**
   * Sets alias
   *
   * @param alias value of alias
   */
  public void setAlias(String alias) {
    this.alias = alias;
  }

  /**
   * Gets keys
   *
   * @return value of keys
   */
  public List<Property> getKeys() {
    return keys;
  }

  /**
   * Sets keys
   *
   * @param keys value of keys
   */
  public void setKeys(List<Property> keys) {
    this.keys = keys;
  }

  /**
   * Gets properties
   *
   * @return value of properties
   */
  public List<Property> getProperties() {
    return properties;
  }

  /**
   * Sets properties
   *
   * @param properties value of properties
   */
  public void setProperties(List<Property> properties) {
    this.properties = properties;
  }
}
