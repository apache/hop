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
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.gui.plugin.GuiWidgetGroupType;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.SampledValueLimits;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

/**
 * This data profile makes it easy to specify what kind of information you want to capture about the
 * output of a transform during its execution.
 */
@GuiPlugin(description = "Execution data profile widgets")
@HopMetadata(
    key = "execution-data-profile",
    name = "i18n::ExecutionDataProfile.name",
    description = "i18n::ExecutionDataProfile.description",
    image = "ui/images/analyzer.svg",
    category = HopMetadataCategory.EXECUTION,
    documentationUrl = "/metadata-types/execution-data-profile.html",
    hopMetadataPropertyType = HopMetadataPropertyType.EXEC_INFO_DATA_PROFILE,
    supportsGlobalReplace = true)
public class ExecutionDataProfile extends HopMetadataBase implements IHopMetadata, Cloneable {

  public static final String GUI_PLUGIN_ELEMENT_PARENT_ID = "ExecutionDataSamplerParent";
  public static final String GUI_PLUGIN_LIMITS_PARENT_ID = "ExecutionDataProfileLimits";

  private static final String LARGE_VALUES_GROUP = "i18n::ExecutionDataProfile.Group.LargeValues";

  @HopMetadataProperty private String description;

  @GuiWidgetElement(
      id = "stringValueLimit",
      order = "100",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_LIMITS_PARENT_ID,
      label = "i18n::ExecutionDataProfile.Label.StringValueLimit",
      toolTip = "i18n::ExecutionDataProfile.Tooltip.StringValueLimit",
      groupType = GuiWidgetGroupType.BOXES,
      group = LARGE_VALUES_GROUP)
  @HopMetadataProperty
  private String stringValueLimit;

  @GuiWidgetElement(
      id = "jsonValueLimit",
      order = "110",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_LIMITS_PARENT_ID,
      label = "i18n::ExecutionDataProfile.Label.JsonValueLimit",
      toolTip = "i18n::ExecutionDataProfile.Tooltip.JsonValueLimit",
      groupType = GuiWidgetGroupType.BOXES,
      group = LARGE_VALUES_GROUP)
  @HopMetadataProperty
  private String jsonValueLimit;

  @GuiWidgetElement(
      id = "binaryValueLimit",
      order = "120",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_LIMITS_PARENT_ID,
      label = "i18n::ExecutionDataProfile.Label.BinaryValueLimit",
      toolTip = "i18n::ExecutionDataProfile.Tooltip.BinaryValueLimit",
      groupType = GuiWidgetGroupType.BOXES,
      group = LARGE_VALUES_GROUP)
  @HopMetadataProperty
  private String binaryValueLimit;

  @GuiWidgetElement(
      id = "avroValueLimit",
      order = "130",
      type = GuiElementType.TEXT,
      parentId = GUI_PLUGIN_LIMITS_PARENT_ID,
      label = "i18n::ExecutionDataProfile.Label.AvroValueLimit",
      toolTip = "i18n::ExecutionDataProfile.Tooltip.AvroValueLimit",
      groupType = GuiWidgetGroupType.BOXES,
      group = LARGE_VALUES_GROUP)
  @HopMetadataProperty
  private String avroValueLimit;

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
    this.stringValueLimit = profile.stringValueLimit;
    this.jsonValueLimit = profile.jsonValueLimit;
    this.binaryValueLimit = profile.binaryValueLimit;
    this.avroValueLimit = profile.avroValueLimit;
    for (IExecutionDataSampler<?> sampler : profile.samplers) {
      this.samplers.add(sampler.clone());
    }
  }

  /**
   * Resolve this profile's value limits and attach them to every sampler that will store rows.
   *
   * @param samplers Profile samplers and any extra samplers added by the engine
   * @param variables Pipeline variables used to resolve the limit fields
   */
  public void applyLimits(List<IExecutionDataSampler> samplers, IVariables variables) {
    if (samplers == null) {
      return;
    }
    SampledValueLimits limits = SampledValueLimits.from(this, variables);
    for (IExecutionDataSampler sampler : samplers) {
      if (sampler != null) {
        sampler.setSampledValueLimits(limits);
      }
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
   * Gets stringValueLimit
   *
   * @return value of stringValueLimit
   */
  public String getStringValueLimit() {
    return stringValueLimit;
  }

  /**
   * Sets stringValueLimit
   *
   * @param stringValueLimit value of stringValueLimit
   */
  public void setStringValueLimit(String stringValueLimit) {
    this.stringValueLimit = stringValueLimit;
  }

  /**
   * Gets jsonValueLimit
   *
   * @return value of jsonValueLimit
   */
  public String getJsonValueLimit() {
    return jsonValueLimit;
  }

  /**
   * Sets jsonValueLimit
   *
   * @param jsonValueLimit value of jsonValueLimit
   */
  public void setJsonValueLimit(String jsonValueLimit) {
    this.jsonValueLimit = jsonValueLimit;
  }

  /**
   * Gets binaryValueLimit
   *
   * @return value of binaryValueLimit
   */
  public String getBinaryValueLimit() {
    return binaryValueLimit;
  }

  /**
   * Sets binaryValueLimit
   *
   * @param binaryValueLimit value of binaryValueLimit
   */
  public void setBinaryValueLimit(String binaryValueLimit) {
    this.binaryValueLimit = binaryValueLimit;
  }

  /**
   * Gets avroValueLimit
   *
   * @return value of avroValueLimit
   */
  public String getAvroValueLimit() {
    return avroValueLimit;
  }

  /**
   * Sets avroValueLimit
   *
   * @param avroValueLimit value of avroValueLimit
   */
  public void setAvroValueLimit(String avroValueLimit) {
    this.avroValueLimit = avroValueLimit;
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
