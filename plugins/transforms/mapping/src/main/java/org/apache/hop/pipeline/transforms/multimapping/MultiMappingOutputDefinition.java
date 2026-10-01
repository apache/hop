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
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

/** Child Mapping Output → parent target. */
@Getter
@Setter
public class MultiMappingOutputDefinition implements Cloneable {

  @HopMetadataProperty(
      key = "input_transform",
      injectionKey = "OUTPUT_MAPPING_TRANSFORM",
      injectionKeyDescription = "MultiMappingMeta.Injection.OUTPUT_MAPPING_TRANSFORM")
  private String inputTransformName;

  @HopMetadataProperty(
      key = "output_transform",
      injectionKey = "OUTPUT_TARGET_TRANSFORM",
      injectionKeyDescription = "MultiMappingMeta.Injection.OUTPUT_TARGET_TRANSFORM")
  private String outputTransformName;

  @HopMetadataProperty(
      key = "description",
      injectionKey = "OUTPUT_DESCRIPTION",
      injectionKeyDescription = "MultiMappingMeta.Injection.OUTPUT_DESCRIPTION")
  private String description;

  @HopMetadataProperty(
      key = "connector",
      injectionGroupKey = "OUTPUT_RENAMES",
      injectionGroupDescription = "MultiMappingMeta.Injection.OUTPUT_RENAMES")
  private List<MultiMappingOutputRename> valueRenames;

  @HopMetadataProperty(
      key = "main_path",
      injectionKey = "OUTPUT_MAIN_PATH",
      injectionKeyDescription = "MultiMappingMeta.Injection.OUTPUT_MAIN_PATH")
  private boolean mainDataPath;

  @HopMetadataProperty(
      key = "rename_on_output",
      injectionKey = "OUTPUT_RENAME_ON_OUTPUT",
      injectionKeyDescription = "MultiMappingMeta.Injection.OUTPUT_RENAME_ON_OUTPUT")
  private boolean renamingOnOutput;

  public MultiMappingOutputDefinition() {
    this.valueRenames = new ArrayList<>();
  }

  public MultiMappingOutputDefinition(String inputTransformName, String outputTransformName) {
    this();
    this.inputTransformName = inputTransformName;
    this.outputTransformName = outputTransformName;
  }

  public MultiMappingOutputDefinition(MultiMappingOutputDefinition d) {
    this();
    copyFrom(d);
  }

  public void copyFrom(MultiMappingOutputDefinition d) {
    this.inputTransformName = d.inputTransformName;
    this.outputTransformName = d.outputTransformName;
    this.description = d.description;
    this.mainDataPath = d.mainDataPath;
    this.renamingOnOutput = d.renamingOnOutput;
    this.valueRenames.clear();
    for (MultiMappingOutputRename rename : d.valueRenames) {
      this.valueRenames.add(new MultiMappingOutputRename(rename));
    }
  }

  @Override
  public MultiMappingOutputDefinition clone() {
    return new MultiMappingOutputDefinition(this);
  }

  public MappingIODefinition toIoDefinition() {
    MappingIODefinition definition =
        new MappingIODefinition(inputTransformName, outputTransformName);
    definition.setDescription(description);
    definition.setMainDataPath(mainDataPath);
    definition.setRenamingOnOutput(renamingOnOutput);
    List<MappingValueRename> renames = new ArrayList<>();
    for (MultiMappingOutputRename rename : valueRenames) {
      renames.add(rename.toValueRename());
    }
    definition.setValueRenames(renames);
    return definition;
  }

  public static MultiMappingOutputDefinition fromIoDefinition(MappingIODefinition d) {
    MultiMappingOutputDefinition definition = new MultiMappingOutputDefinition();
    if (d == null) {
      return definition;
    }
    definition.inputTransformName = d.getInputTransformName();
    definition.outputTransformName = d.getOutputTransformName();
    definition.description = d.getDescription();
    definition.mainDataPath = d.isMainDataPath();
    definition.renamingOnOutput = d.isRenamingOnOutput();
    if (d.getValueRenames() != null) {
      for (MappingValueRename rename : d.getValueRenames()) {
        definition.valueRenames.add(MultiMappingOutputRename.fromValueRename(rename));
      }
    }
    return definition;
  }
}
