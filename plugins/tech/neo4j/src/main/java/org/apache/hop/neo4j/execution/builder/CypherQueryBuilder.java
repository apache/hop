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

import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class CypherQueryBuilder extends BaseCypherBuilder {

  private CypherQueryBuilder() {
    super();
    cypher.append("MATCH");
  }

  public static CypherQueryBuilder of() {
    return new CypherQueryBuilder();
  }

  public CypherQueryBuilder withLabelAndKey(
      String nodeAlias, String label, String key, Object keyValue) {
    cypher
        .append("(")
        .append(nodeAlias)
        .append(":")
        .append(label)
        .append(" {")
        .append(key)
        .append(" : $")
        .append(key)
        .append(" }) ");
    parameters.put(key, keyValue);
    return this;
  }

  public CypherQueryBuilder withLabelAndKeys(
      String nodeAlias, String label, Map<String, Object> keyValueMap) {
    cypher.append("(").append(nodeAlias).append(":").append(label).append(" {");

    boolean firstKey = true;
    for (String key : keyValueMap.keySet()) {
      String param = nodeAlias + "_" + key;
      Object value = keyValueMap.get(key);
      if (firstKey) {
        firstKey = false;
      } else {
        cypher.append(", ");
      }
      cypher.append(key).append(" : $").append(param);
      parameters.put(param, value);
    }
    cypher.append(" }) ");

    return this;
  }

  public CypherQueryBuilder withLabelWithoutKey(String nodeAlias, String label) {
    cypher.append("(").append(nodeAlias).append(":").append(label).append(") ");
    return this;
  }

  public CypherQueryBuilder withReturnValues(String nodeAlias, String... properties) {
    for (String property : properties) {
      withReturnValue(nodeAlias, property, null);
    }
    return this;
  }

  protected CypherQueryBuilder withReturnValue(String nodeAlias, String property, String asName) {
    if (firstReturn) {
      firstReturn = false;
      cypher.append("RETURN ");
    } else {
      cypher.append(", ");
    }
    cypher.append(nodeAlias).append(".").append(property).append(" ");
    if (StringUtils.isNotEmpty(asName)) {
      cypher.append("as ").append(asName).append(" ");
    }
    return this;
  }

  public CypherQueryBuilder withWhereIsNull(String nodeAlias, String property) {
    cypher.append("WHERE ").append(nodeAlias).append(".").append(property).append(" IS NULL ");
    return this;
  }

  public CypherQueryBuilder withLimit(int limit) {
    if (limit > 0) {
      cypher.append("LIMIT ").append(limit).append(" ");
    }
    return this;
  }

  public CypherQueryBuilder withOrderBy(String nodeAlias, String property, boolean ascending) {
    cypher
        .append("ORDER BY ")
        .append(nodeAlias)
        .append(".")
        .append(property)
        .append(" ")
        .append(ascending ? " " : "DESC ");
    return this;
  }

  public CypherQueryBuilder withMatch(
      String nodeAlias, String nodeLabel, Map<String, Object> nodeKeys) {
    cypher.append("MATCH(").append(nodeAlias).append(":").append(nodeLabel).append(" {");
    boolean firstKey = true;
    for (String otherKey : nodeKeys.keySet()) {
      if (firstKey) {
        firstKey = false;
      } else {
        cypher.append(", ");
      }
      String parameter = nodeAlias + "_" + otherKey;
      Object value = nodeKeys.get(otherKey);
      cypher.append(otherKey).append(" : $").append(parameter);
      parameters.put(parameter, value);
    }
    cypher.append(" }) ");
    return this;
  }

  public CypherQueryBuilder withRelationship(
      String alias1, String alias2, String relationshipLabel) {
    cypher
        .append("MATCH(")
        .append(alias1)
        .append(")-[rel:")
        .append(relationshipLabel)
        .append("]->(")
        .append(alias2)
        .append(") ");
    return this;
  }
}
