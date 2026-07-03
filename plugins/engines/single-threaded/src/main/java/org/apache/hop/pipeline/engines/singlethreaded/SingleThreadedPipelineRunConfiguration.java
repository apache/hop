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

package org.apache.hop.pipeline.engines.singlethreaded;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.pipeline.engines.IMeasuringLocalPipelineRunConfiguration;
import org.apache.hop.pipeline.engines.local.LocalPipelineRunConfiguration;

@GuiPlugin
@Getter
@Setter
public class SingleThreadedPipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements IPipelineEngineRunConfiguration, IMeasuringLocalPipelineRunConfiguration {
  /** The feedback size. */
  @GuiWidgetElement(
      id = "sampleTypeInGui",
      order = "080",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.COMBO,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SampleTypeInGui.Label",
      comboValuesMethod = "getSampleTypes")
  @HopMetadataProperty(key = "sample_type_in_gui")
  protected String sampleTypeInGui;

  /** The feedback size. */
  @GuiWidgetElement(
      id = "sampleSize",
      order = "090",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SampleSize.Label")
  @HopMetadataProperty(key = "sample_size")
  protected String sampleSize;

  public SingleThreadedPipelineRunConfiguration() {
    super();
    this.sampleTypeInGui = LocalPipelineRunConfiguration.SampleType.Last.name();
    this.sampleSize = "100";
  }

  public SingleThreadedPipelineRunConfiguration(
      String pluginId, String pluginName, String rowSetSize) {
    super(pluginId, pluginName);
  }

  public SingleThreadedPipelineRunConfiguration(SingleThreadedPipelineRunConfiguration config) {
    super(config);
    this.sampleTypeInGui = config.sampleTypeInGui;
    this.sampleSize = config.sampleSize;
  }

  @Override
  public SingleThreadedPipelineRunConfiguration clone() {
    return new SingleThreadedPipelineRunConfiguration(this);
  }

  public List<String> getSampleTypes(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> list = new ArrayList<>();
    for (LocalPipelineRunConfiguration.SampleType type :
        LocalPipelineRunConfiguration.SampleType.values()) {
      list.add(type.name());
    }
    return list;
  }
}
