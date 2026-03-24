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

import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.transform.TransformMeta;

public class TargetTransformDefinition extends TransformDefinition {
  @HopMetadataProperty(
      key = "transform_tag",
      injectionKey = "TARGET_TAG",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.TARGET_TAG")
  public String tag = super.tag;

  @HopMetadataProperty(
      key = "transform_name",
      injectionKey = "TARGET_TRANSFORM_NAME",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.TARGET_TRANSFORM_NAME")
  public String transformName = super.transformName;

  public TransformMeta transformMeta = super.transformMeta;

  @HopMetadataProperty(
      key = "transform_description",
      injectionKey = "TARGET_DESCRIPTION",
      injectionKeyDescription = "UserDefinedJavaClass.Injection.TARGET_DESCRIPTION")
  public String description = super.description;

  public TargetTransformDefinition() {
    super();
  }

  public TargetTransformDefinition(TargetTransformDefinition d) {
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
    return new TargetTransformDefinition(this);
  }
}
