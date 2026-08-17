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
 */

package org.apache.hop.pipeline.transforms.multimapping;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingParameters;

@Getter
@Setter
public class MultiIOMappings implements Cloneable {

  @HopMetadataProperty(
      key = "mapping",
      groupKey = "input",
      injectionGroupKey = "INPUT_MAPPINGS",
      injectionGroupDescription = "MultiMappingMeta.Injection.INPUT_MAPPINGS")
  private List<MultiMappingInputDefinition> inputMappings;

  @HopMetadataProperty(
      key = "mapping",
      groupKey = "output",
      injectionGroupKey = "OUTPUT_MAPPINGS",
      injectionGroupDescription = "MultiMappingMeta.Injection.OUTPUT_MAPPINGS")
  private List<MultiMappingOutputDefinition> outputMappings;

  @HopMetadataProperty(key = "parameters")
  private MappingParameters mappingParameters;

  public MultiIOMappings() {
    this.inputMappings = new ArrayList<>();
    this.outputMappings = new ArrayList<>();
    this.mappingParameters = new MappingParameters();
  }

  public MultiIOMappings(MultiIOMappings m) {
    this();
    if (m.inputMappings != null) {
      for (MultiMappingInputDefinition definition : m.inputMappings) {
        this.inputMappings.add(new MultiMappingInputDefinition(definition));
      }
    }
    if (m.outputMappings != null) {
      for (MultiMappingOutputDefinition definition : m.outputMappings) {
        this.outputMappings.add(new MultiMappingOutputDefinition(definition));
      }
    }
    this.mappingParameters =
        m.mappingParameters != null
            ? new MappingParameters(m.mappingParameters)
            : new MappingParameters();
  }

  @Override
  public MultiIOMappings clone() {
    return new MultiIOMappings(this);
  }

  public List<MappingIODefinition> getInputIoDefinitions() {
    List<MappingIODefinition> list = new ArrayList<>();
    for (MultiMappingInputDefinition definition : inputMappings) {
      list.add(definition.toIoDefinition());
    }
    return list;
  }

  public List<MappingIODefinition> getOutputIoDefinitions() {
    List<MappingIODefinition> list = new ArrayList<>();
    for (MultiMappingOutputDefinition definition : outputMappings) {
      list.add(definition.toIoDefinition());
    }
    return list;
  }
}
