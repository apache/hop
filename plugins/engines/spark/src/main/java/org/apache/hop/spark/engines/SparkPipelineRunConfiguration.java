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

package org.apache.hop.spark.engines;

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engines.EmptyPipelineRunConfiguration;
import org.apache.hop.spark.util.SparkConst;

@GuiPlugin
@Getter
@Setter
public class SparkPipelineRunConfiguration extends EmptyPipelineRunConfiguration
    implements ISparkPipelineEngineRunConfiguration, Cloneable {

  @GuiWidgetElement(
      order = "20000-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsMaster.Label",
      toolTip = "i18n::SparkEngine.OptionsMaster.ToolTip")
  @HopMetadataProperty
  private String sparkMaster;

  @GuiWidgetElement(
      order = "20010-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsAppName.Label",
      toolTip = "i18n::SparkEngine.OptionsAppName.ToolTip")
  @HopMetadataProperty
  private String sparkAppName;

  @GuiWidgetElement(
      order = "20020-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FILENAME,
      label = "i18n::SparkEngine.OptionsFatJar.Label",
      toolTip = "i18n::SparkEngine.OptionsFatJar.ToolTip")
  @HopMetadataProperty
  private String fatJar;

  @GuiWidgetElement(
      order = "20030-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.MULTI_LINE_TEXT,
      multiLineTextHeight = 5,
      label = "i18n::SparkEngine.OptionsConfigs.Label",
      toolTip = "i18n::SparkEngine.OptionsConfigs.ToolTip")
  @HopMetadataProperty
  private String sparkConfigs;

  @GuiWidgetElement(
      order = "20040-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsDriverMemory.Label",
      toolTip = "i18n::SparkEngine.OptionsDriverMemory.ToolTip")
  @HopMetadataProperty
  private String driverMemory;

  @GuiWidgetElement(
      order = "20050-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsExecutorMemory.Label",
      toolTip = "i18n::SparkEngine.OptionsExecutorMemory.ToolTip")
  @HopMetadataProperty
  private String executorMemory;

  @GuiWidgetElement(
      order = "20060-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsExecutorCores.Label",
      toolTip = "i18n::SparkEngine.OptionsExecutorCores.ToolTip")
  @HopMetadataProperty
  private String executorCores;

  @GuiWidgetElement(
      order = "20070-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::SparkEngine.OptionsTempLocation.Label",
      toolTip = "i18n::SparkEngine.OptionsTempLocation.ToolTip")
  @HopMetadataProperty
  private String tempLocation;

  @GuiWidgetElement(
      order = "20080-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::SparkEngine.OptionsPluginsToStage.Label",
      toolTip = "i18n::SparkEngine.OptionsPluginsToStage.ToolTip")
  @HopMetadataProperty
  private String pluginsToStage;

  public SparkPipelineRunConfiguration() {
    super();
    this.sparkMaster = "local[*]";
    this.sparkAppName = "Apache Hop";
    this.tempLocation = System.getProperty("java.io.tmpdir");
    setEnginePluginId(SparkConst.PLUGIN_ID);
    setEnginePluginName(SparkConst.PLUGIN_NAME);
  }

  public SparkPipelineRunConfiguration(SparkPipelineRunConfiguration config) {
    super(config);
    this.sparkMaster = config.sparkMaster;
    this.sparkAppName = config.sparkAppName;
    this.fatJar = config.fatJar;
    this.sparkConfigs = config.sparkConfigs;
    this.driverMemory = config.driverMemory;
    this.executorMemory = config.executorMemory;
    this.executorCores = config.executorCores;
    this.tempLocation = config.tempLocation;
    this.pluginsToStage = config.pluginsToStage;
  }

  @Override
  public SparkPipelineRunConfiguration clone() {
    return new SparkPipelineRunConfiguration(this);
  }
}
