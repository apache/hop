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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;

@Getter
@Setter
public class MultiMappingInputRename implements Cloneable {

  @HopMetadataProperty(
      key = "parent",
      injectionKey = "INPUT_RENAME_SOURCE",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_RENAME_SOURCE")
  private String sourceValueName;

  @HopMetadataProperty(
      key = "child",
      injectionKey = "INPUT_RENAME_TARGET",
      injectionKeyDescription = "MultiMappingMeta.Injection.INPUT_RENAME_TARGET")
  private String targetValueName;

  public MultiMappingInputRename() {}

  public MultiMappingInputRename(String sourceValueName, String targetValueName) {
    this.sourceValueName = sourceValueName == null ? "" : sourceValueName;
    this.targetValueName = targetValueName == null ? "" : targetValueName;
  }

  public MultiMappingInputRename(MultiMappingInputRename rename) {
    this.sourceValueName = rename.sourceValueName;
    this.targetValueName = rename.targetValueName;
  }

  @Override
  public MultiMappingInputRename clone() {
    return new MultiMappingInputRename(this);
  }

  public MappingValueRename toValueRename() {
    return new MappingValueRename(sourceValueName, targetValueName);
  }

  public static MultiMappingInputRename fromValueRename(MappingValueRename rename) {
    if (rename == null) {
      return new MultiMappingInputRename();
    }
    return new MultiMappingInputRename(rename.getSourceValueName(), rename.getTargetValueName());
  }
}
