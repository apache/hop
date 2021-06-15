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

package org.apache.hop.pipeline.engines.local;

import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;

import java.util.ArrayList;
import java.util.List;

@GuiPlugin(description = "Local pipeline run configuration widgets")
public class LocalPipelineRunConfiguration extends EmptyPipelineRunConfiguration implements IPipelineEngineRunConfiguration {

  @GuiWidgetElement(
    id = "rowSetSize",
    order = "10",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.RowSetSize.Label"
  )
  @HopMetadataProperty(key="rowset_size")
  protected String rowSetSize;

  @GuiWidgetElement(
    id = "safeModeEnabled",
    order = "20",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SafeModeEnabled.Label"
  )
  @HopMetadataProperty(key="safe_mode")
  protected boolean safeModeEnabled;

  @GuiWidgetElement(
    id = "gatheringMetrics",
    order = "30",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.GatheringMetrics.Label"
  )
  @HopMetadataProperty(key="gather_metrics")
  protected boolean gatheringMetrics;

  @GuiWidgetElement(
    id = "sortTransformsTopologically",
    order = "40",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SortTransformsTopologically.Label"
  )
  @HopMetadataProperty(key="topo_sort")
  protected boolean sortingTransformsTopologically;

  /**
   * Whether the feedback is shown.
   */
  @GuiWidgetElement(
    id = "feedbackShown",
    order = "50",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.CHECKBOX,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.FeedbackShown.Label"
  )
  @HopMetadataProperty(key="show_feedback")
  protected boolean feedbackShown;

  /**
   * The feedback size.
   */
  @GuiWidgetElement(
    id = "feedbackSize",
    order = "60",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.FeedbackSize.Label"
  )
  @HopMetadataProperty(key="feedback_size")
  protected String feedbackSize;

  /**
   * The feedback size.
   */
  @GuiWidgetElement(
    id = "sampleTypeInGui",
    order = "70",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.COMBO,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SampleTypeInGui.Label",
    comboValuesMethod = "getSampleTypes"
  )
  @HopMetadataProperty(key="sample_type_in_gui")
  protected String sampleTypeInGui;

  /**
   * The feedback size.
   */
  @GuiWidgetElement(
    id = "sampleSize",
    order = "80",
    parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
    type = GuiElementType.TEXT,
    label = "i18n:org.apache.hop.ui.pipeline.config:PipelineRunConfigurationDialog.SampleSize.Label"
  )
  @HopMetadataProperty(key="sample_size")
  protected String sampleSize;

  public enum SampleType {
    None, First, Last, Random;
  }

  public LocalPipelineRunConfiguration() {
    super();
    this.rowSetSize = Integer.toString( Const.ROWS_IN_ROWSET );
    this.feedbackShown = false;
    this.feedbackSize = Integer.toString( Const.ROWS_UPDATE );
    this.sampleTypeInGui = SampleType.Last.name();
    this.sampleSize = "100";
  }

  public LocalPipelineRunConfiguration( LocalPipelineRunConfiguration config ) {
    super( config );
    this.rowSetSize = config.rowSetSize;
    this.feedbackShown = config.feedbackShown;
    this.feedbackSize = config.feedbackSize;
    this.safeModeEnabled = config.safeModeEnabled;
    this.gatheringMetrics = config.gatheringMetrics;
    this.sortingTransformsTopologically = config.sortingTransformsTopologically;
    this.sampleTypeInGui = config.sampleTypeInGui;
    this.sampleSize = config.sampleSize;
  }

  public LocalPipelineRunConfiguration clone() {
    return new LocalPipelineRunConfiguration( this );
  }

  public List<String> getSampleTypes( ILogChannel log, IHopMetadataProvider metadataProvider ) {
    List<String> list = new ArrayList<>();
    for (SampleType type : SampleType.values()) {
      list.add(type.name());
    }
    return list;
  }
  /**
   * Gets rowSetSize
   *
   * @return value of rowSetSize
   */
  public String getRowSetSize() {
    return rowSetSize;
  }

  /**
   * @param rowSetSize The rowSetSize to set
   */
  public void setRowSetSize( String rowSetSize ) {
    this.rowSetSize = rowSetSize;
  }

  /**
   * Gets safeModeEnabled
   *
   * @return value of safeModeEnabled
   */
  public boolean isSafeModeEnabled() {
    return safeModeEnabled;
  }

  /**
   * @param safeModeEnabled The safeModeEnabled to set
   */
  public void setSafeModeEnabled( boolean safeModeEnabled ) {
    this.safeModeEnabled = safeModeEnabled;
  }

  /**
   * Gets gatheringMetrics
   *
   * @return value of gatheringMetrics
   */
  public boolean isGatheringMetrics() {
    return gatheringMetrics;
  }

  /**
   * @param gatheringMetrics The gatheringMetrics to set
   */
  public void setGatheringMetrics( boolean gatheringMetrics ) {
    this.gatheringMetrics = gatheringMetrics;
  }

  /**
   * Gets sortingTransformsTopologically
   *
   * @return value of sortingTransformsTopologically
   */
  public boolean isSortingTransformsTopologically() {
    return sortingTransformsTopologically;
  }

  /**
   * @param sortingTransformsTopologically The sortingTransformsTopologically to set
   */
  public void setSortingTransformsTopologically( boolean sortingTransformsTopologically ) {
    this.sortingTransformsTopologically = sortingTransformsTopologically;
  }

  /**
   * Gets feedbackShown
   *
   * @return value of feedbackShown
   */
  public boolean isFeedbackShown() {
    return feedbackShown;
  }

  /**
   * @param feedbackShown The feedbackShown to set
   */
  public void setFeedbackShown( boolean feedbackShown ) {
    this.feedbackShown = feedbackShown;
  }

  /**
   * Gets feedbackSize
   *
   * @return value of feedbackSize
   */
  public String getFeedbackSize() {
    return feedbackSize;
  }

  /**
   * @param feedbackSize The feedbackSize to set
   */
  public void setFeedbackSize( String feedbackSize ) {
    this.feedbackSize = feedbackSize;
  }

  /**
   * Gets sampleTypeInGui
   *
   * @return value of sampleTypeInGui
   */
  public String getSampleTypeInGui() {
    return sampleTypeInGui;
  }

  /**
   * @param sampleTypeInGui The sampleTypeInGui to set
   */
  public void setSampleTypeInGui( String sampleTypeInGui ) {
    this.sampleTypeInGui = sampleTypeInGui;
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
   * @param sampleSize The sampleSize to set
   */
  public void setSampleSize( String sampleSize ) {
    this.sampleSize = sampleSize;
  }
}
