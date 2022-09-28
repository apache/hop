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

public class CypherDeleteBuilder extends CypherMatchBuilder {

  private CypherDeleteBuilder() {
    super();
  }

  public static CypherDeleteBuilder of() {
    return new CypherDeleteBuilder();
  }

  @Override
  public CypherDeleteBuilder withMatch(String label, String alias, String id, Object value) {
    return (CypherDeleteBuilder) super.withMatch(label, alias, id, value);
  }

  @Override
  public CypherDeleteBuilder withMatch(
      String label, String alias, Map<String, Object> keyValueMap) {
    return (CypherDeleteBuilder) super.withMatch(label, alias, keyValueMap);
  }

  public CypherDeleteBuilder withDelete(String... aliases) {
    cypher.append("DELETE ");
    boolean firstAlias = true;
    for (String alias : aliases) {
      if (firstAlias) {
        firstAlias = false;
      } else {
        cypher.append(", ");
      }
      cypher.append(alias).append(" ");
    }
    return this;
  }

  public CypherDeleteBuilder withDetachDelete(String... aliases) {
    cypher.append("DETACH ");
    return withDelete(aliases);
  }

  public CypherDeleteBuilder withRelationshipMatch(
      String edgeLabel, String edgeAlias, String sourceNodeAlias, String targetNodeAlias) {
    cypher
        .append("MATCH(")
        .append(sourceNodeAlias)
        .append(")-[")
        .append(edgeAlias)
        .append(":")
        .append(edgeLabel)
        .append("]->(")
        .append(targetNodeAlias)
        .append(") ");
    return this;
  }
}
