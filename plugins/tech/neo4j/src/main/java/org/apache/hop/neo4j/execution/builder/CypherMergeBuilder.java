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
import java.util.Map;
import org.apache.hop.core.json.HopJson;

public class CypherMergeBuilder extends BaseCypherBuilder {
  private CypherMergeBuilder() {
    cypher.append("MERGE");
  }

  public static CypherMergeBuilder of() {
    return new CypherMergeBuilder();
  }

  public CypherMergeBuilder withLabelAndKey(String label, String key, Object value) {
    cypher
        .append("(n:")
        .append(label)
        .append(" {")
        .append(key)
        .append(" : $")
        .append(key)
        .append("}) ");
    parameters.put(key, value);
    return this;
  }

  public CypherMergeBuilder withLabelAndKeys(String label, Map<String, Object> keyValueMap) {
    cypher.append("(n:").append(label).append(" {");
    boolean firstKey = true;
    for (String key : keyValueMap.keySet()) {
      Object value = keyValueMap.get(key);
      if (firstKey) {
        firstKey = false;
      } else {
        cypher.append(", ");
      }
      cypher.append(key).append(" : $").append(key);
      parameters.put(key, value);
    }
    cypher.append(" }) ");

    return this;
  }

  public CypherMergeBuilder withValue(String property, Object value) {
    if (firstParameter) {
      firstParameter = false;
      cypher.append("SET ");
    } else {
      cypher.append(", ");
    }
    cypher.append("n.").append(property).append("=$").append(property).append(" ");

    if (value instanceof Map) {
      String jsonString;
      try {
        jsonString = HopJson.newMapper().writeValueAsString(value);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Error converting Map to a JSON String", e);
      }
      addParameter(property, jsonString);
    } else {
      addParameter(property, value);
    }

    return this;
  }
}
