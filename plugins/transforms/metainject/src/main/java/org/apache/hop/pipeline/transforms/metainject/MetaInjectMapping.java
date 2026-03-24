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

package org.apache.hop.pipeline.transforms.metainject;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class MetaInjectMapping {
  @HopMetadataProperty(
      key = "source_transform",
      injectionKey = "MAPPING_SOURCE_TRANSFORM",
      injectionKeyDescription = "MetaInject.Injection.MAPPING_SOURCE_TRANSFORM")
  private String sourceTransformName;

  @HopMetadataProperty(
      key = "source_field",
      injectionKey = "MAPPING_SOURCE_FIELD",
      injectionKeyDescription = "MetaInject.Injection.MAPPING_SOURCE_FIELD")
  private String sourceField;

  @HopMetadataProperty(
      key = "target_transform_name",
      injectionKey = "MAPPING_TARGET_TRANSFORM",
      injectionKeyDescription = "MetaInject.Injection.MAPPING_TARGET_TRANSFORM")
  private String targetTransformName;

  @HopMetadataProperty(
      key = "target_attribute_key",
      injectionKey = "MAPPING_TARGET_FIELD",
      injectionKeyDescription = "MetaInject.Injection.MAPPING_TARGET_FIELD")
  private String targetAttributeKey;

  @HopMetadataProperty(
      key = "target_detail",
      injectionKey = "MAPPING_TARGET_DETAIL",
      injectionKeyDescription = "MetaInject.Injection.MAPPING_TARGET_DETAIL")
  private boolean targetDetail;

  public MetaInjectMapping() {}

  public MetaInjectMapping(MetaInjectMapping m) {
    this();
    this.sourceField = m.sourceField;
    this.sourceTransformName = m.sourceTransformName;
    this.targetAttributeKey = m.targetAttributeKey;
    this.targetDetail = m.targetDetail;
    this.targetTransformName = m.targetTransformName;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetaInjectMapping mapping)) {
      return false;
    }
    return targetDetail == mapping.targetDetail
        && Const.NVL(targetTransformName, "")
            .equalsIgnoreCase(Const.NVL(mapping.targetTransformName, ""))
        && Const.NVL(targetAttributeKey, "")
            .equalsIgnoreCase(Const.NVL(mapping.targetAttributeKey, ""));
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetTransformName, targetAttributeKey, targetDetail);
  }
}
