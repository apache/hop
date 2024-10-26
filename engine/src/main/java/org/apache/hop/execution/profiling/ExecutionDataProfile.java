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

package org.apache.hop.execution.profiling;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * This data profile makes it easy to specify what kind of information you want to capture about the
 * output of a transform during its execution.
 */
@HopMetadata(
    key = "execution-data-profile",
    name = "i18n::ExecutionDataProfile.name",
    description = "i18n::ExecutionDataProfile.description",
    image = "ui/images/analyzer.svg",
    documentationUrl = "/metadata-types/execution-data-profile.html",
    hopMetadataPropertyType = HopMetadataPropertyType.EXEC_INFO_DATA_PROFILE)
public class ExecutionDataProfile extends HopMetadataBase implements IHopMetadata, Cloneable {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "ExecutionDataSamplerParent";
  @HopMetadataProperty private String description;

  @HopMetadataProperty(groupKey = "samplers", key = "sampler")
  private List<IExecutionDataSampler> samplers;

  public ExecutionDataProfile() {
    this.samplers = new ArrayList<>();
  }

  public ExecutionDataProfile(String name) {
    super(name);
    this.samplers = new ArrayList<>();
  }

  public ExecutionDataProfile(
      String name, String description, List<IExecutionDataSampler> samplers) {
    super(name);
    this.description = description;
    this.samplers = samplers;
  }

  public ExecutionDataProfile(ExecutionDataProfile profile) {
    this(profile.name);
    this.description = profile.description;
    for (IExecutionDataSampler<?> sampler : profile.samplers) {
      this.samplers.add(sampler.clone());
    }
  }

  @Override
  protected ExecutionDataProfile clone() {
    return new ExecutionDataProfile(this);
  }

  /**
   * Gets description
   *
   * @return value of description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Sets description
   *
   * @param description value of description
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Gets samplers
   *
   * @return value of samplers
   */
  public List<IExecutionDataSampler> getSamplers() {
    return samplers;
  }

  /**
   * Sets samplers
   *
   * @param samplers value of samplers
   */
  public void setSamplers(List<IExecutionDataSampler> samplers) {
    this.samplers = samplers;
  }
}
