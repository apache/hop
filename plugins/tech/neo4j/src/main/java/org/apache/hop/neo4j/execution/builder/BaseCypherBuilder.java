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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseCypherBuilder implements ICypherBuilder {
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
    if (value != null) {
      if (value instanceof Date date) {
        // Convert to LocalDateTime
        parameters.put(property, LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault()));
      } else {
        parameters.put(property, value);
      }
    } else {
      parameters.put(property, null);
    }
  }

  public String cypher() {
    return cypher.toString();
  }

  public Map<String, Object> parameters() {
    return parameters;
  }
}
