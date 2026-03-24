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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformMeta;

@Getter
@Setter
public class InfoTransformDefinition extends TransformDefinition {
  @HopMetadataProperty(
      key = "transform_tag",
      injectionKey = "INFO_TAG",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.INFO_TAG")
  private String tag = super.tag;

  @HopMetadataProperty(
      key = "transform_name",
      injectionKey = "INFO_TRANSFORM_NAME",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.INFO_TRANSFORM_NAME")
  private String transformName = super.transformName;

  @HopMetadataProperty(
      key = "transform_description",
      injectionKey = "INFO_DESCRIPTION",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.INFO_DESCRIPTION")
  private String description = super.description;

  public TransformMeta transformMeta = super.transformMeta;

  public InfoTransformDefinition() {
    super();
  }

  public InfoTransformDefinition(InfoTransformDefinition d) {
    super(d);
    this.tag = d.tag;
    this.transformName = d.transformName;
    this.description = d.description;
    if (d.transformMeta != null) {
      this.transformMeta = (TransformMeta) d.transformMeta.clone();
    }
  }

  @Override
  public Object clone() {
    return new InfoTransformDefinition(this);
  }
}
