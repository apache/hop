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

import org.apache.hop.metadata.api.HopMetadataProperty;

public class ParameterMapping {

  @HopMetadataProperty(
      key = "parameter",
      injectionKey = "PARAMETER_NAME",
      injectionKeyDescription = "Cypher.Injection.PARAMETER_NAME")
  private String parameter;

  @HopMetadataProperty(
      key = "field",
      injectionKey = "PARAMETER_FIELD",
      injectionKeyDescription = "Cypher.Injection.PARAMETER_FIELD")
  private String field;

  @HopMetadataProperty(
      key = "type",
      injectionKey = "PARAMETER_NEO4J_TYPE",
      injectionKeyDescription = "Cypher.Injection.PARAMETER_NEO4J_TYPE")
  private String neoType;

  public ParameterMapping() {}

  public ParameterMapping(ParameterMapping m) {
    this();
    this.parameter = m.parameter;
    this.field = m.field;
    this.neoType = m.neoType;
  }

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
