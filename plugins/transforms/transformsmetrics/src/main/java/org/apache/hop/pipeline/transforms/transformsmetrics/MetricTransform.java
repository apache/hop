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
 */

package org.apache.hop.pipeline.transforms.transformsmetrics;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

@Getter
@Setter
public class MetricTransform {

  @HopMetadataProperty(
      injectionKey = "TRANSFORM_NAME",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.TRANSFORM_NAME")
  private String name;

  @HopMetadataProperty(
      key = "copyNr",
      injectionKey = "TRANSFORM_COPY_NR",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.TRANSFORM_COPY_NR")
  private String copyNr;

  @HopMetadataProperty(
      key = "transformRequired",
      injectionKey = "TRANSFORM_REQUIRED",
      injectionKeyDescription = "TransformsMetricsMeta.Injection.TRANSFORM_REQUIRED")
  private boolean required;

  public MetricTransform() {}

  public MetricTransform(String name, String copyNr, boolean required) {
    this.name = name;
    this.copyNr = copyNr;
    this.required = required;
  }

  public MetricTransform(MetricTransform other) {
    this.name = other.name;
    this.copyNr = other.copyNr;
    this.required = other.required;
  }
}
