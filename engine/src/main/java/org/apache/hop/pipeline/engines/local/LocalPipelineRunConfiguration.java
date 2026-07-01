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

package org.apache.hop.pipeline.engines.local;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;

@GuiPlugin(description = "Local pipeline run configuration widgets")
@Getter
@Setter
public class LocalPipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements IPipelineEngineRunConfiguration,
        org.apache.hop.pipeline.engines.IMeasuringLocalPipelineRunConfiguration {

  @GuiWidgetElement(
      id = "rowSetSize",
      order = "010",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.RowSetSize.Label")
  @HopMetadataProperty(key = "rowset_size")
  protected String rowSetSize;

  @GuiWidgetElement(
      id = "safeModeEnabled",
      order = "020",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SafeModeEnabled.Label")
  @HopMetadataProperty(key = "safe_mode")
  protected boolean safeModeEnabled;

  @GuiWidgetElement(
      id = "gatheringMetrics",
      order = "030",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.GatheringMetrics.Label")
  @HopMetadataProperty(key = "gather_metrics")
  protected boolean gatheringMetrics;

  @GuiWidgetElement(
      id = "sortTransformsTopologically",
      order = "040",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SortTransformsTopologically.Label")
  @HopMetadataProperty(key = "topo_sort")
  protected boolean sortingTransformsTopologically;

  /** Whether the feedback is shown. */
  @GuiWidgetElement(
      id = "feedbackShown",
      order = "050",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.FeedbackShown.Label")
  @HopMetadataProperty(key = "show_feedback")
  protected boolean feedbackShown;

  /** The feedback size. */
  @GuiWidgetElement(
      id = "feedbackSize",
      order = "060",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.FeedbackSize.Label")
  @HopMetadataProperty(key = "feedback_size")
  protected String feedbackSize;

  /** The feedback size. */
  @GuiWidgetElement(
      id = "waitTime",
      order = "070",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.WaitTime.Label")
  @HopMetadataProperty(key = "wait_time")
  protected String waitTime;

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

  @GuiWidgetElement(
      id = "transactional",
      order = "100",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.Transactional.Label",
      toolTip =
          "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.Transactional.ToolTip")
  @HopMetadataProperty(key = "transactional")
  protected boolean transactional;

  @SuppressWarnings("java:S115")
  public enum SampleType {
    None,
    First,
    Last,
    Random;
  }

  public LocalPipelineRunConfiguration() {
    super();
    this.rowSetSize = Integer.toString(Const.ROWS_IN_ROWSET);
    this.feedbackShown = false;
    this.feedbackSize = Integer.toString(Const.ROWS_UPDATE);
    this.waitTime = EnvUtil.getSystemProperty(Const.HOP_DEFAULT_BUFFER_POLLING_WAITTIME, "20");
    this.sampleTypeInGui = SampleType.Last.name();
    this.sampleSize = "100";
    this.transactional = false;
  }

  public LocalPipelineRunConfiguration(LocalPipelineRunConfiguration config) {
    super(config);
    this.rowSetSize = config.rowSetSize;
    this.feedbackShown = config.feedbackShown;
    this.feedbackSize = config.feedbackSize;
    this.waitTime = config.waitTime;
    this.safeModeEnabled = config.safeModeEnabled;
    this.gatheringMetrics = config.gatheringMetrics;
    this.sortingTransformsTopologically = config.sortingTransformsTopologically;
    this.sampleTypeInGui = config.sampleTypeInGui;
    this.sampleSize = config.sampleSize;
    this.transactional = config.transactional;
  }

  @Override
  public LocalPipelineRunConfiguration clone() {
    return new LocalPipelineRunConfiguration(this);
  }

  public List<String> getSampleTypes(ILogChannel log, IHopMetadataProvider metadataProvider) {
    List<String> list = new ArrayList<>();
    for (SampleType type : SampleType.values()) {
      list.add(type.name());
    }
    return list;
  }
}
