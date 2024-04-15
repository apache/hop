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

package org.apache.hop.beam.engines.spark;

import java.util.Arrays;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.engines.BeamPipelineRunConfiguration;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.RunnerType;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiElementType;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiWidgetElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;

@GuiPlugin
public class BeamSparkPipelineRunConfiguration extends BeamPipelineRunConfiguration
    implements IBeamPipelineEngineRunConfiguration, IVariables, Cloneable {

  @GuiWidgetElement(
      order = "20000-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsMaster.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsMaster.ToolTip")
  @HopMetadataProperty
  private String sparkMaster;

  @GuiWidgetElement(
      order = "20010-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsBatchIntervalMillis.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsBatchIntervalMillis.ToolTip")
  @HopMetadataProperty
  private String sparkBatchIntervalMillis;

  @GuiWidgetElement(
      order = "20020-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.FOLDER,
      label = "i18n::BeamEnginesSpark.OptionsCheckpointDir.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsCheckpointDir.ToolTip")
  @HopMetadataProperty
  private String sparkCheckpointDir;

  @GuiWidgetElement(
      order = "20030-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsCheckpointDurationMillis.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsCheckpointDurationMillis.ToolTip")
  @HopMetadataProperty
  private String sparkCheckpointDurationMillis;

  @GuiWidgetElement(
      order = "20040-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "i18n::BeamEnginesSpark.OptionsEnableSparkMetricSinks.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsEnableSparkMetricSinks.ToolTip")
  @HopMetadataProperty
  private boolean sparkEnableSparkMetricSinks;

  @GuiWidgetElement(
      order = "20050-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsMaxRecordsPerBatch.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsMaxRecordsPerBatch.ToolTip")
  @HopMetadataProperty
  private String sparkMaxRecordsPerBatch;

  @GuiWidgetElement(
      order = "20060-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsMinReadTimeMillis.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsMinReadTimeMillis.ToolTip")
  @HopMetadataProperty
  private String sparkMinReadTimeMillis;

  @GuiWidgetElement(
      order = "20070-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsReadTimePercentage.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsReadTimePercentage.ToolTip")
  @HopMetadataProperty
  private String sparkReadTimePercentage;

  @GuiWidgetElement(
      order = "20080-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsBundleSize.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsBundleSize.ToolTip")
  @HopMetadataProperty
  private String sparkBundleSize;

  @GuiWidgetElement(
      order = "20090-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsAppName.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsAppName.ToolTip")
  @HopMetadataProperty
  private String sparkAppName;

  @GuiWidgetElement(
      order = "30010-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsBlobStorageEndpoint.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsBlobStorageEndpoint.ToolTip")
  @HopMetadataProperty
  private String azureBlobStorageEndpoint;

  @GuiWidgetElement(
      order = "300020-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsBlobStorageAccount.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsBlobStorageAccount.ToolTip")
  @HopMetadataProperty
  private String azureBlobStorageAccount;

  @GuiWidgetElement(
      order = "300030-spark-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "i18n::BeamEnginesSpark.OptionsBlobStorageKey.Label",
      toolTip = "i18n::BeamEnginesSpark.OptionsBlobStorageKey.ToolTip")
  @HopMetadataProperty
  private String azureBlobStorageKey;

  @HopMetadataProperty private String sparkStorageLevel;

  public BeamSparkPipelineRunConfiguration() {
    super();
    this.tempLocation = System.getProperty("java.io.tmpdir");
  }

  public BeamSparkPipelineRunConfiguration(BeamSparkPipelineRunConfiguration config) {
    super(config);
    this.sparkMaster = config.sparkMaster;
    this.sparkBatchIntervalMillis = config.sparkBatchIntervalMillis;
    this.sparkCheckpointDir = config.sparkCheckpointDir;
    this.sparkCheckpointDurationMillis = config.sparkCheckpointDurationMillis;
    this.sparkEnableSparkMetricSinks = config.sparkEnableSparkMetricSinks;
    this.sparkMaxRecordsPerBatch = config.sparkMaxRecordsPerBatch;
    this.sparkMinReadTimeMillis = config.sparkMinReadTimeMillis;
    this.sparkReadTimePercentage = config.sparkReadTimePercentage;
    this.sparkBundleSize = config.sparkBundleSize;
    this.sparkStorageLevel = config.sparkStorageLevel;
    this.sparkAppName = config.sparkAppName;
    this.azureBlobStorageAccount = config.azureBlobStorageAccount;
    this.azureBlobStorageKey = config.azureBlobStorageKey;
    this.azureBlobStorageEndpoint = config.azureBlobStorageEndpoint;
  }

  @Override
  public BeamSparkPipelineRunConfiguration clone() {
    return new BeamSparkPipelineRunConfiguration(this);
  }

  @Override
  public RunnerType getRunnerType() {
    return RunnerType.Spark;
  }

  @Override
  public PipelineOptions getPipelineOptions() throws HopException {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);

    if (StringUtils.isNotEmpty(getSparkMaster())) {
      options.setSparkMaster(resolve(getSparkMaster()));
    }
    if (StringUtils.isNotEmpty(getSparkBatchIntervalMillis())) {
      long interval = Const.toLong(resolve(getSparkBatchIntervalMillis()), -1L);
      if (interval >= 0) {
        options.setBatchIntervalMillis(interval);
      }
    }
    if (StringUtils.isNotEmpty(getSparkCheckpointDir())) {
      options.setCheckpointDir(resolve(getSparkCheckpointDir()));
    }
    if (StringUtils.isNotEmpty(getSparkCheckpointDurationMillis())) {
      long duration = Const.toLong(resolve(getSparkCheckpointDurationMillis()), -1L);
      if (duration >= 0) {
        options.setCheckpointDurationMillis(duration);
      }
    }
    if (StringUtils.isNotEmpty(getSparkMaxRecordsPerBatch())) {
      long records = Const.toLong(resolve(getSparkMaxRecordsPerBatch()), -1L);
      if (records >= 0) {
        options.setMaxRecordsPerBatch(records);
      }
    }
    if (StringUtils.isNotEmpty(getSparkMinReadTimeMillis())) {
      long readTime = Const.toLong(resolve(getSparkMinReadTimeMillis()), -1L);
      if (readTime >= 0) {
        options.setMinReadTimeMillis(readTime);
      }
    }
    if (StringUtils.isNotEmpty(getSparkReadTimePercentage())) {
      double percentage = Const.toDouble(resolve(getSparkReadTimePercentage()), -1.0);
      if (percentage >= 0) {
        options.setReadTimePercentage(percentage / 100);
      }
    }
    if (StringUtils.isNotEmpty(getSparkBundleSize())) {
      long bundleSize = Const.toLong(resolve(getSparkBundleSize()), -1L);
      if (bundleSize >= 0) {
        options.setBundleSize(bundleSize);
      }
    }
    if (StringUtils.isNotEmpty(getSparkStorageLevel())) {
      options.setStorageLevel(resolve(getSparkStorageLevel()));
    }

    if (StringUtils.isNotEmpty(getFatJar())) {
      options.setFilesToStage(Arrays.asList(resolve(fatJar)));
    }

    options.setEnableSparkMetricSinks(isSparkEnableSparkMetricSinks());

    if (StringUtils.isNotEmpty(getSparkAppName())) {
      options.setAppName(resolve(getSparkAppName()));
    }

    if (StringUtils.isNotEmpty(getAzureBlobStorageAccount())) {
      options
          .as(BlobstoreOptions.class)
          .setBlobServiceEndpoint(resolve(getAzureBlobStorageEndpoint()));
    }

    if (StringUtils.isNotEmpty(getAzureBlobStorageAccount())) {
      options.as(BlobstoreOptions.class).setAccountName(resolve(getAzureBlobStorageAccount()));
    }

    if (StringUtils.isNotEmpty(getAzureBlobStorageKey())) {
      options.as(BlobstoreOptions.class).setAccessKey(resolve(getAzureBlobStorageKey()));
    }

    return options;
  }

  @Override
  public boolean isRunningAsynchronous() {
    return true;
  }

  /**
   * Gets sparkMaster
   *
   * @return value of sparkMaster
   */
  public String getSparkMaster() {
    return sparkMaster;
  }

  /**
   * @param sparkMaster The sparkMaster to set
   */
  public void setSparkMaster(String sparkMaster) {
    this.sparkMaster = sparkMaster;
  }

  /**
   * Gets sparkBatchIntervalMillis
   *
   * @return value of sparkBatchIntervalMillis
   */
  public String getSparkBatchIntervalMillis() {
    return sparkBatchIntervalMillis;
  }

  /**
   * @param sparkBatchIntervalMillis The sparkBatchIntervalMillis to set
   */
  public void setSparkBatchIntervalMillis(String sparkBatchIntervalMillis) {
    this.sparkBatchIntervalMillis = sparkBatchIntervalMillis;
  }

  /**
   * Gets sparkCheckpointDir
   *
   * @return value of sparkCheckpointDir
   */
  public String getSparkCheckpointDir() {
    return sparkCheckpointDir;
  }

  /**
   * @param sparkCheckpointDir The sparkCheckpointDir to set
   */
  public void setSparkCheckpointDir(String sparkCheckpointDir) {
    this.sparkCheckpointDir = sparkCheckpointDir;
  }

  /**
   * Gets sparkCheckpointDurationMillis
   *
   * @return value of sparkCheckpointDurationMillis
   */
  public String getSparkCheckpointDurationMillis() {
    return sparkCheckpointDurationMillis;
  }

  /**
   * @param sparkCheckpointDurationMillis The sparkCheckpointDurationMillis to set
   */
  public void setSparkCheckpointDurationMillis(String sparkCheckpointDurationMillis) {
    this.sparkCheckpointDurationMillis = sparkCheckpointDurationMillis;
  }

  /**
   * Gets sparkEnableSparkMetricSinks
   *
   * @return value of sparkEnableSparkMetricSinks
   */
  public boolean isSparkEnableSparkMetricSinks() {
    return sparkEnableSparkMetricSinks;
  }

  /**
   * @param sparkEnableSparkMetricSinks The sparkEnableSparkMetricSinks to set
   */
  public void setSparkEnableSparkMetricSinks(boolean sparkEnableSparkMetricSinks) {
    this.sparkEnableSparkMetricSinks = sparkEnableSparkMetricSinks;
  }

  /**
   * Gets sparkMaxRecordsPerBatch
   *
   * @return value of sparkMaxRecordsPerBatch
   */
  public String getSparkMaxRecordsPerBatch() {
    return sparkMaxRecordsPerBatch;
  }

  /**
   * @param sparkMaxRecordsPerBatch The sparkMaxRecordsPerBatch to set
   */
  public void setSparkMaxRecordsPerBatch(String sparkMaxRecordsPerBatch) {
    this.sparkMaxRecordsPerBatch = sparkMaxRecordsPerBatch;
  }

  /**
   * Gets sparkMinReadTimeMillis
   *
   * @return value of sparkMinReadTimeMillis
   */
  public String getSparkMinReadTimeMillis() {
    return sparkMinReadTimeMillis;
  }

  /**
   * @param sparkMinReadTimeMillis The sparkMinReadTimeMillis to set
   */
  public void setSparkMinReadTimeMillis(String sparkMinReadTimeMillis) {
    this.sparkMinReadTimeMillis = sparkMinReadTimeMillis;
  }

  /**
   * Gets sparkReadTimePercentage
   *
   * @return value of sparkReadTimePercentage
   */
  public String getSparkReadTimePercentage() {
    return sparkReadTimePercentage;
  }

  /**
   * @param sparkReadTimePercentage The sparkReadTimePercentage to set
   */
  public void setSparkReadTimePercentage(String sparkReadTimePercentage) {
    this.sparkReadTimePercentage = sparkReadTimePercentage;
  }

  /**
   * Gets sparkBundleSize
   *
   * @return value of sparkBundleSize
   */
  public String getSparkBundleSize() {
    return sparkBundleSize;
  }

  /**
   * @param sparkBundleSize The sparkBundleSize to set
   */
  public void setSparkBundleSize(String sparkBundleSize) {
    this.sparkBundleSize = sparkBundleSize;
  }

  /**
   * Gets sparkStorageLevel
   *
   * @return value of sparkStorageLevel
   */
  public String getSparkStorageLevel() {
    return sparkStorageLevel;
  }

  /**
   * @param sparkStorageLevel The sparkStorageLevel to set
   */
  public void setSparkStorageLevel(String sparkStorageLevel) {
    this.sparkStorageLevel = sparkStorageLevel;
  }

  /**
   * Gets sparkAppName
   *
   * @return value of sparkAppName
   */
  public String getSparkAppName() {
    return sparkAppName;
  }

  /**
   * Sets sparkAppName
   *
   * @param sparkAppName value of sparkAppName
   */
  public void setSparkAppName(String sparkAppName) {
    this.sparkAppName = sparkAppName;
  }

  public String getAzureBlobStorageEndpoint() {
    return azureBlobStorageEndpoint;
  }

  public void setAzureBlobStorageEndpoint(String azureBlobStorageEndpoint) {
    this.azureBlobStorageEndpoint = azureBlobStorageEndpoint;
  }

  public String getAzureBlobStorageAccount() {
    return azureBlobStorageAccount;
  }

  public void setAzureBlobStorageAccount(String azureBlobStorageAccount) {
    this.azureBlobStorageAccount = azureBlobStorageAccount;
  }

  public String getAzureBlobStorageKey() {
    return azureBlobStorageKey;
  }

  public void setAzureBlobStorageKey(String azureBlobStorageKey) {
    this.azureBlobStorageKey = azureBlobStorageKey;
  }
}
