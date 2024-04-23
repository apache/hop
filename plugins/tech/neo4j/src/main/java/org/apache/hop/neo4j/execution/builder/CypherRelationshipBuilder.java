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

public class CypherRelationshipBuilder extends CypherMatchBuilder {

  private CypherRelationshipBuilder() {
    super();
  }

  public static CypherRelationshipBuilder of() {
    return new CypherRelationshipBuilder();
  }

  @Override
  public CypherRelationshipBuilder withMatch(String label, String alias, String id, Object value) {
    return (CypherRelationshipBuilder) super.withMatch(label, alias, id, value);
  }

  @Override
  public CypherRelationshipBuilder withMatch(
      String label, String alias, Map<String, Object> keyValueMap) {
    return (CypherRelationshipBuilder) super.withMatch(label, alias, keyValueMap);
  }

  public CypherRelationshipBuilder withMerge(
      String alias1, String alias2, String relationshipLabel) {

    cypher
        .append("MERGE(")
        .append(alias1)
        .append(")-[rel:")
        .append(relationshipLabel)
        .append("]->(")
        .append(alias2)
        .append(")");
    return this;
  }

  public CypherRelationshipBuilder withCreate(
      String alias1, String alias2, String relationshipLabel) {

    cypher
        .append("CREATE(")
        .append(alias1)
        .append(")-[rel:")
        .append(relationshipLabel)
        .append("]->(")
        .append(alias2)
        .append(")");
    return this;
  }
}
