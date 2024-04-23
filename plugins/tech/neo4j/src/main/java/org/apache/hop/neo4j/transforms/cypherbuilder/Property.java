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
import org.apache.hop.metadata.api.HopMetadataProperty;

public class Property implements Cloneable {

  @HopMetadataProperty private String alias;
  @HopMetadataProperty private String name;
  @HopMetadataProperty private String expression;
  @HopMetadataProperty private String parameter;
  @HopMetadataProperty private String rename;
  @HopMetadataProperty private boolean descending;

  public Property() {}

  public Property(
      String alias,
      String name,
      String expression,
      String parameter,
      String rename,
      boolean descending) {
    this.alias = alias;
    this.name = name;
    this.expression = expression;
    this.parameter = parameter;
    this.rename = rename;
    this.descending = descending;
  }

  public Property(String alias, String name, String expression, String parameter, String rename) {
    this(alias, name, expression, parameter, rename, false);
  }

  public Property(String alias, String name, String parameter) {
    this(alias, name, null, parameter, null, false);
  }

  public Property(String name, String parameter) {
    this(null, name, null, parameter, null, false);
  }

  public Property(Property p) {
    this.alias = p.alias;
    this.name = p.name;
    this.expression = p.expression;
    this.parameter = p.parameter;
    this.rename = p.rename;
    this.descending = p.descending;
  }

  public Property clone() {
    return new Property(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Property property = (Property) o;
    return Objects.equals(alias, property.alias)
        && Objects.equals(name, property.name)
        && Objects.equals(expression, property.expression)
        && Objects.equals(parameter, property.parameter)
        && Objects.equals(rename, property.rename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, name, expression, parameter, rename);
  }

  public String formatParameter(String unwindAlias) {
    if (StringUtils.isEmpty(unwindAlias)) {
      return "{" + parameter + "}";
    } else {
      return unwindAlias + "." + parameter;
    }
  }

  public String getOrderByCypherClause() {
    // ORDER BY alias.name DESC
    // ORDER BY expression
    String cypher;
    if (StringUtils.isEmpty(expression)) {
      cypher = alias + "." + name;
    } else {
      cypher = expression;
    }
    if (descending) {
      cypher += " DESC";
    }
    return cypher;
  }

  /**
   * alias.property={parameter} or alias.property=expression or alias.property=unwindAlias.parameter
   *
   * @param unwindAlias unwind alias if there's any
   * @return The Cypher set clause for this property
   */
  public String getSetCypherClause(String unwindAlias) {
    String cypher = alias + "." + name + "=";
    if (StringUtils.isEmpty(expression)) {
      if (StringUtils.isEmpty(unwindAlias)) {
        cypher += "{" + parameter + "}";
      } else {
        cypher += unwindAlias + "." + parameter;
      }
    } else {
      cypher += expression;
    }
    return cypher;
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
   * Gets expression
   *
   * @return value of expression
   */
  public String getExpression() {
    return expression;
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
   * Gets descending
   *
   * @return value of descending
   */
  public boolean isDescending() {
    return descending;
  }

  /**
   * Sets descending
   *
   * @param descending value of descending
   */
  public void setDescending(boolean descending) {
    this.descending = descending;
  }
}
