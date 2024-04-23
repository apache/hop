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

package org.apache.hop.execution.sampler.plugins;

import java.util.Objects;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.execution.profiling.ExecutionDataProfile;
import org.apache.hop.execution.sampler.ExecutionDataSamplerMeta;
import org.apache.hop.execution.sampler.IExecutionDataSampler;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.metadata.api.HopMetadataProperty;

public abstract class ExecutionDataSamplerBase<Store extends IExecutionDataSamplerStore>
    implements IExecutionDataSampler<Store> {

  @GuiWidgetElement(
      order = "100",
      type = GuiElementType.TEXT,
      parentId = ExecutionDataProfile.GUI_PLUGIN_ELEMENT_PARENT_ID,
      label = "i18n::ExecutionDataSamplerBase.Label.SampleSize",
      toolTip = "i18n::ExecutionDataSamplerBase.Tooltip.SampleSize")
  @HopMetadataProperty
  protected String sampleSize;

  protected String pluginId;

  protected String pluginName;

  public ExecutionDataSamplerBase() {
    this.sampleSize = "100";
  }

  public ExecutionDataSamplerBase(String sampleSize, String pluginId, String pluginName) {
    this.sampleSize = sampleSize;
    this.pluginId = pluginId;
    this.pluginName = pluginName;
  }

  public ExecutionDataSamplerBase(ExecutionDataSamplerBase<Store> base) {
    this.sampleSize = base.sampleSize;
    this.pluginId = base.pluginId;
    this.pluginName = base.pluginName;
  }

  @Override
  public abstract ExecutionDataSamplerBase<Store> clone();

  @Override
  public abstract Store createSamplerStore(ExecutionDataSamplerMeta samplerMeta);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExecutionDataSamplerBase that = (ExecutionDataSamplerBase) o;
    return Objects.equals(pluginId, that.pluginId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pluginId);
  }

  /**
   * Gets sampleSize
   *
   * @return value of sampleSize
   */
  public String getSampleSize() {
    return sampleSize;
  }

  /**
   * Sets sampleSize
   *
   * @param sampleSize value of sampleSize
   */
  public void setSampleSize(String sampleSize) {
    this.sampleSize = sampleSize;
  }

  /**
   * Gets pluginId
   *
   * @return value of pluginId
   */
  @Override
  public String getPluginId() {
    return pluginId;
  }

  /**
   * Sets pluginId
   *
   * @param pluginId value of pluginId
   */
  public void setPluginId(String pluginId) {
    this.pluginId = pluginId;
  }

  /**
   * Gets pluginName
   *
   * @return value of pluginName
   */
  @Override
  public String getPluginName() {
    return pluginName;
  }

  /**
   * Sets pluginName
   *
   * @param pluginName value of pluginName
   */
  @Override
  public void setPluginName(String pluginName) {
    this.pluginName = pluginName;
  }
}
