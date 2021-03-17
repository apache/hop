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

package org.apache.hop.neo4j.transforms.graph;

import java.util.ArrayList;
import java.util.List;

/**
 * The class contains a cypher statement and the mapping between each parameter name and the input
 * field
 */
public class CypherParameters {
  private String cypher;
  private List<TargetParameter> targetParameters;

  public CypherParameters() {
    targetParameters = new ArrayList<>();
  }

  public CypherParameters(String cypher, List<TargetParameter> targetParameters) {
    this.cypher = cypher;
    this.targetParameters = targetParameters;
  }

  /**
   * Gets cypher
   *
   * @return value of cypher
   */
  public String getCypher() {
    return cypher;
  }

  /** @param cypher The cypher to set */
  public void setCypher(String cypher) {
    this.cypher = cypher;
  }

  /**
   * Gets targetParameters
   *
   * @return value of targetParameters
   */
  public List<TargetParameter> getTargetParameters() {
    return targetParameters;
  }

  /** @param targetParameters The targetParameters to set */
  public void setTargetParameters(List<TargetParameter> targetParameters) {
    this.targetParameters = targetParameters;
  }
}
