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

package org.apache.hop.neo4j.transforms.cypherbuilder;

import java.util.Objects;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.metadata.api.HopMetadataProperty;

public class ReturnValue implements Cloneable {

  /** The node or relationship alias */
  @HopMetadataProperty private String alias;

  /** The name of the property to return */
  @HopMetadataProperty private String property;

  /** A manual expression is returned */
  @HopMetadataProperty private String expression;

  /** If you want to return the value of a parameter you provided (pass-through) */
  @HopMetadataProperty private String parameter;

  /** Optionally you can rename the output */
  @HopMetadataProperty private String rename;

  /** The expected Neo4j data type */
  @HopMetadataProperty private String neoType;

  /** The target Hop data type */
  @HopMetadataProperty private String hopType;

  public ReturnValue() {}

  public ReturnValue(
      String alias,
      String property,
      String expression,
      String parameter,
      String rename,
      String neoType,
      String hopType) {
    this.alias = alias;
    this.property = property;
    this.expression = expression;
    this.parameter = parameter;
    this.rename = rename;
    this.neoType = neoType;
    this.hopType = hopType;
  }

  public ReturnValue(ReturnValue v) {
    this.alias = v.alias;
    this.property = v.property;
    this.expression = v.expression;
    this.rename = v.rename;
    this.neoType = v.neoType;
    this.hopType = v.hopType;
  }

  public ReturnValue clone() {
    return new ReturnValue(this);
  }

  public ReturnValue(String alias, String property) {
    this(alias, property, null, null, null, null, null);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReturnValue that = (ReturnValue) o;
    return Objects.equals(alias, that.alias)
        && Objects.equals(property, that.property)
        && Objects.equals(expression, that.expression)
        && Objects.equals(rename, that.rename)
        && Objects.equals(hopType, that.hopType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, property, expression, rename, hopType);
  }

  public String getCypherClause() {
    StringBuilder cypher = new StringBuilder();
    cypher.append(getPropertyOrExpression());
    if (StringUtils.isNotEmpty(getRename())) {
      cypher.append(" AS ").append(getRename());
    }
    return cypher.toString();
  }

  public String getPropertyOrExpression() {
    if (StringUtils.isEmpty(expression)) {
      return getAliasProperty();
    } else if (StringUtils.isEmpty(parameter)) {
      return expression;
    } else {
      return "{" + parameter + "}";
    }
  }

  public String getAliasProperty() {
    return getAlias() + "." + getProperty();
  }

  public String calculateHeader() {
    if (StringUtils.isNotEmpty(rename)) {
      return rename;
    }
    return getPropertyOrExpression();
  }

  public IValueMeta createValueMeta() throws HopTransformException {
    try {
      // The name of the output field
      String name = calculateHeader();

      // The data type
      int type = ValueMetaFactory.getIdForValueMeta(hopType);

      return ValueMetaFactory.createValueMeta(name, type);
    } catch (Exception e) {
      throw new HopTransformException("Error creating Hop output value metadata", e);
    }
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
   * Gets property
   *
   * @return value of property
   */
  public String getProperty() {
    return property;
  }

  /**
   * Sets property
   *
   * @param property value of property
   */
  public void setProperty(String property) {
    this.property = property;
  }

  /**
   * Gets expression
   *
   * @return value of expression
   */
  public String getExpression() {
    return expression;
  }

  /**
   * Gets parameter
   *
   * @return value of parameter
   */
  public String getParameter() {
    return parameter;
  }

  /**
   * Sets parameter
   *
   * @param parameter value of parameter
   */
  public void setParameter(String parameter) {
    this.parameter = parameter;
  }

  /**
   * Sets expression
   *
   * @param expression value of expression
   */
  public void setExpression(String expression) {
    this.expression = expression;
  }

  /**
   * Gets rename
   *
   * @return value of rename
   */
  public String getRename() {
    return rename;
  }

  /**
   * Sets rename
   *
   * @param rename value of rename
   */
  public void setRename(String rename) {
    this.rename = rename;
  }

  /**
   * Gets neoType
   *
   * @return value of neoType
   */
  public String getNeoType() {
    return neoType;
  }

  /**
   * Sets neoType
   *
   * @param neoType value of neoType
   */
  public void setNeoType(String neoType) {
    this.neoType = neoType;
  }

  /**
   * Gets hopType
   *
   * @return value of hopType
   */
  public String getHopType() {
    return hopType;
  }

  /**
   * Sets hopType
   *
   * @param hopType value of hopType
   */
  public void setHopType(String hopType) {
    this.hopType = hopType;
  }
}
