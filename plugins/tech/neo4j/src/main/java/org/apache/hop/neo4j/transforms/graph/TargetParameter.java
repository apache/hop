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

import org.apache.hop.neo4j.model.GraphPropertyType;

public class TargetParameter {
  private String inputField;
  private int inputFieldIndex;

  private String parameterName;
  private GraphPropertyType parameterType;

  public TargetParameter() {}

  public TargetParameter(
      String inputField,
      int inputFieldIndex,
      String parameterName,
      GraphPropertyType parameterType) {
    this.inputField = inputField;
    this.inputFieldIndex = inputFieldIndex;
    this.parameterName = parameterName;
    this.parameterType = parameterType;
  }

  /**
   * Gets inputField
   *
   * @return value of inputField
   */
  public String getInputField() {
    return inputField;
  }

  /** @param inputField The inputField to set */
  public void setInputField(String inputField) {
    this.inputField = inputField;
  }

  /**
   * Gets inputFieldIndex
   *
   * @return value of inputFieldIndex
   */
  public int getInputFieldIndex() {
    return inputFieldIndex;
  }

  /** @param inputFieldIndex The inputFieldIndex to set */
  public void setInputFieldIndex(int inputFieldIndex) {
    this.inputFieldIndex = inputFieldIndex;
  }

  /**
   * Gets parameterName
   *
   * @return value of parameterName
   */
  public String getParameterName() {
    return parameterName;
  }

  /** @param parameterName The parameterName to set */
  public void setParameterName(String parameterName) {
    this.parameterName = parameterName;
  }

  /**
   * Gets parameterType
   *
   * @return value of parameterType
   */
  public GraphPropertyType getParameterType() {
    return parameterType;
  }

  /** @param parameterType The parameterType to set */
  public void setParameterType(GraphPropertyType parameterType) {
    this.parameterType = parameterType;
  }
}
