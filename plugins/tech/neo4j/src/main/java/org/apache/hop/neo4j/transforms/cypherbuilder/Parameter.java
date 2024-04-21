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
import org.apache.hop.metadata.api.HopMetadataProperty;

/** A parameter is set using an input field */
public class Parameter implements Cloneable {

  @HopMetadataProperty private String name;
  @HopMetadataProperty private String inputFieldName;
  @HopMetadataProperty private String neoType;

  public Parameter() {}

  public Parameter(String name, String inputFieldName, String neoType) {
    this.name = name;
    this.inputFieldName = inputFieldName;
    this.neoType = neoType;
  }

  public Parameter(Parameter p) {
    this.name = p.name;
    this.inputFieldName = p.inputFieldName;
    this.neoType = p.neoType;
  }

  public Parameter clone() {
    return new Parameter(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Parameter parameter = (Parameter) o;
    return Objects.equals(name, parameter.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
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
   * Gets inputFieldName
   *
   * @return value of inputFieldName
   */
  public String getInputFieldName() {
    return inputFieldName;
  }

  /**
   * Sets inputFieldName
   *
   * @param inputFieldName value of inputFieldName
   */
  public void setInputFieldName(String inputFieldName) {
    this.inputFieldName = inputFieldName;
  }

  /**
   * Gets neoType
   *
   * @return value of neoType
   */
  public String getNeoType() {
    return neoType;
  }

  /**
   * Sets neoType
   *
   * @param neoType value of neoType
   */
  public void setNeoType(String neoType) {
    this.neoType = neoType;
  }
}
