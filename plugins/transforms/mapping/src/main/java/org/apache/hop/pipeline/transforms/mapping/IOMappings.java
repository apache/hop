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

package org.apache.hop.pipeline.transforms.mapping;

import org.apache.hop.metadata.api.HopMetadataProperty;

public class IOMappings implements Cloneable {
  @HopMetadataProperty(key = "input")
  private MappingIODefinition inputMapping;

  @HopMetadataProperty(key = "output")
  private MappingIODefinition outputMapping;

  @HopMetadataProperty(key = "parameters")
  private MappingParameters mappingParameters;

  public IOMappings() {
    inputMapping = new MappingIODefinition();
    outputMapping = new MappingIODefinition();
    mappingParameters = new MappingParameters();
  }

  public IOMappings(IOMappings m) {
    this();
    this.inputMapping = m.inputMapping != null ? new MappingIODefinition(m.inputMapping) : null;
    this.outputMapping = m.outputMapping != null ? new MappingIODefinition(m.outputMapping) : null;
    this.mappingParameters =
        m.mappingParameters != null ? new MappingParameters(m.mappingParameters) : null;
  }

  @Override
  protected IOMappings clone() {
    return new IOMappings(this);
  }

  /**
   * Gets inputMapping
   *
   * @return value of inputMapping
   */
  public MappingIODefinition getInputMapping() {
    return inputMapping;
  }

  /**
   * Sets inputMapping
   *
   * @param inputMapping value of inputMapping
   */
  public void setInputMapping(MappingIODefinition inputMapping) {
    this.inputMapping = inputMapping;
  }

  /**
   * Gets outputMapping
   *
   * @return value of outputMapping
   */
  public MappingIODefinition getOutputMapping() {
    return outputMapping;
  }

  /**
   * Sets outputMapping
   *
   * @param outputMapping value of outputMapping
   */
  public void setOutputMapping(MappingIODefinition outputMapping) {
    this.outputMapping = outputMapping;
  }

  /**
   * Gets mappingParameters
   *
   * @return value of mappingParameters
   */
  public MappingParameters getMappingParameters() {
    return mappingParameters;
  }

  /**
   * Sets mappingParameters
   *
   * @param mappingParameters value of mappingParameters
   */
  public void setMappingParameters(MappingParameters mappingParameters) {
    this.mappingParameters = mappingParameters;
  }
}
