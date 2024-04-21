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

public class CypherMatchBuilder extends BaseCypherBuilder {

  protected CypherMatchBuilder() {
    super();
  }

  public static CypherMatchBuilder of() {
    return new CypherMatchBuilder();
  }

  public CypherMatchBuilder withMatch(String label, String alias, String id1, Object value1) {
    String key1 = alias + "_" + id1;
    cypher
        .append("MATCH(")
        .append(alias)
        .append(":")
        .append(label)
        .append("{ ")
        .append(id1)
        .append(" : $")
        .append(key1)
        .append(" }) ");
    addParameter(key1, value1);
    return this;
  }

  public CypherMatchBuilder withMatch(
      String nodeLabel, String nodeAlias, Map<String, Object> keyValueMap) {
    cypher.append("MATCH(").append(nodeAlias).append(":").append(nodeLabel).append(" {");

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
}
