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

package org.apache.hop.neo4j.transforms.cypher;

import org.apache.hop.core.injection.Injection;

public class ParameterMapping {

  @Injection(name = "PARAMETER_NAME", group = "PARAMETERS")
  private String parameter;

  @Injection(name = "PARAMETER_FIELD", group = "PARAMETERS")
  private String field;

  @Injection(name = "PARAMETER_NEO4J_TYPE", group = "PARAMETERS")
  private String neoType;

  public ParameterMapping() {}

  public ParameterMapping(String parameter, String field, String neoType) {
    this.parameter = parameter;
    this.field = field;
    this.neoType = neoType;
  }

  public String getParameter() {
    return parameter;
  }

  public void setParameter(String parameter) {
    this.parameter = parameter;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getNeoType() {
    return neoType;
  }

  public void setNeoType(String neoType) {
    this.neoType = neoType;
  }
}
