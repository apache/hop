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
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mapping.MappingIODefinition;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

/** Parent source → child Mapping Input. */
@Getter
@Setter
public class MultiMappingInputDefinition implements Cloneable {

  @SuppressWarnings("java:S2065")
  private transient TransformMeta inputTransform;

  @HopMetadataProperty(
      key = "input_transform",
      injectionKey = "INPUT_SOURCE_TRANSFORM",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_SOURCE_TRANSFORM")
  private String inputTransformName;

  @HopMetadataProperty(
      key = "output_transform",
      injectionKey = "INPUT_MAPPING_TRANSFORM",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_MAPPING_TRANSFORM")
  private String outputTransformName;

  @HopMetadataProperty(
      key = "description",
      injectionKey = "INPUT_DESCRIPTION",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_DESCRIPTION")
  private String description;

  @HopMetadataProperty(
      key = "connector",
      injectionGroupKey = "INPUT_RENAMES",
      injectionGroupDescription = "MultiMappingMeta.Injection.INPUT_RENAMES")
  private List<MultiMappingInputRename> valueRenames;

  @HopMetadataProperty(
      key = "main_path",
      injectionKey = "INPUT_MAIN_PATH",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_MAIN_PATH")
  private boolean mainDataPath;

  @HopMetadataProperty(
      key = "rename_on_output",
      injectionKey = "INPUT_RENAME_ON_OUTPUT",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_RENAME_ON_OUTPUT")
  private boolean renamingOnOutput;

  public MultiMappingInputDefinition() {
    this.valueRenames = new ArrayList<>();
  }

  public MultiMappingInputDefinition(String inputTransformName, String outputTransformName) {
    this();
    this.inputTransformName = inputTransformName;
    this.outputTransformName = outputTransformName;
  }

  public MultiMappingInputDefinition(MultiMappingInputDefinition d) {
    this();
    copyFrom(d);
  }

  public void copyFrom(MultiMappingInputDefinition d) {
    this.inputTransformName = d.inputTransformName;
    this.outputTransformName = d.outputTransformName;
    this.description = d.description;
    this.mainDataPath = d.mainDataPath;
    this.renamingOnOutput = d.renamingOnOutput;
    this.inputTransform = d.inputTransform;
    this.valueRenames.clear();
    for (MultiMappingInputRename rename : d.valueRenames) {
      this.valueRenames.add(new MultiMappingInputRename(rename));
    }
  }

  @Override
  public MultiMappingInputDefinition clone() {
    return new MultiMappingInputDefinition(this);
  }

  public MappingIODefinition toIoDefinition() {
    MappingIODefinition definition =
        new MappingIODefinition(inputTransformName, outputTransformName);
    definition.setDescription(description);
    definition.setMainDataPath(mainDataPath);
    definition.setRenamingOnOutput(renamingOnOutput);
    definition.setInputTransform(inputTransform);
    List<MappingValueRename> renames = new ArrayList<>();
    for (MultiMappingInputRename rename : valueRenames) {
      renames.add(rename.toValueRename());
    }
    definition.setValueRenames(renames);
    return definition;
  }

  public static MultiMappingInputDefinition fromIoDefinition(MappingIODefinition d) {
    MultiMappingInputDefinition definition = new MultiMappingInputDefinition();
    if (d == null) {
      return definition;
    }
    definition.inputTransformName = d.getInputTransformName();
    definition.outputTransformName = d.getOutputTransformName();
    definition.description = d.getDescription();
    definition.mainDataPath = d.isMainDataPath();
    definition.renamingOnOutput = d.isRenamingOnOutput();
    definition.inputTransform = d.getInputTransform();
    if (d.getValueRenames() != null) {
      for (MappingValueRename rename : d.getValueRenames()) {
        definition.valueRenames.add(MultiMappingInputRename.fromValueRename(rename));
      }
    }
    return definition;
  }
}
