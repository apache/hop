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

package org.apache.hop.beam.engines.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
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

import java.util.Arrays;
import java.util.Collections;

@GuiPlugin
public class BeamDataFlowPipelineRunConfiguration extends BeamPipelineRunConfiguration
    implements IBeamPipelineEngineRunConfiguration, IVariables, Cloneable {

  @GuiWidgetElement(
      order = "20000-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Project ID")
  @HopMetadataProperty
  private String gcpProjectId;

  @GuiWidgetElement(
      order = "20005-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      toolTip = "Run the job as a specific service account, instead of the default GCE robot",
      label = "Service account")
  @HopMetadataProperty
  private String gcpServiceAccount;

  @GuiWidgetElement(
      order = "20010-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Application name")
  @HopMetadataProperty
  private String gcpAppName;

  @GuiWidgetElement(
      order = "20020-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Staging location")
  @HopMetadataProperty
  private String gcpStagingLocation;

  @GuiWidgetElement(
      order = "20030-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Initial number of workers")
  @HopMetadataProperty
  private String gcpInitialNumberOfWorkers;

  @GuiWidgetElement(
      order = "20040-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Maximum number of workers",
      toolTip =
          "The maximum number of workers to use for the workerpool. This options limits the "
              + "size of the workerpool for the lifetime of the job, including pipeline updates. "
              + "If left unspecified, the Dataflow service will compute a ceiling.")
  @HopMetadataProperty
  private String gcpMaximumNumberOfWorkers;

  @GuiWidgetElement(
      order = "20050-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Auto scaling algorithm")
  @HopMetadataProperty
  private String gcpAutoScalingAlgorithm;

  @GuiWidgetElement(
      order = "20060-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Worker machine type")
  @HopMetadataProperty
  private String gcpWorkerMachineType;

  @GuiWidgetElement(
      order = "20070-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Worker disk type")
  @HopMetadataProperty
  private String gcpWorkerDiskType;

  @GuiWidgetElement(
      order = "20080-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Disk size in GB")
  @HopMetadataProperty
  private String gcpDiskSizeGb;

  @GuiWidgetElement(
      order = "20090-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Region")
  @HopMetadataProperty
  private String gcpRegion;

  @GuiWidgetElement(
      order = "20100-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Zone")
  @HopMetadataProperty
  private String gcpZone;

  @GuiWidgetElement(
      order = "20110-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Network",
      toolTip =
          "GCE network for launching workers. For more information, see the reference documentation "
              + "https://cloud.google.com/compute/docs/networking. Default is up to the Dataflow service.")
  @HopMetadataProperty
  private String gcpNetwork;

  @GuiWidgetElement(
      order = "20120-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.TEXT,
      label = "Subnetwork",
      toolTip =
          "GCE subnetwork for launching workers. For more information, see the reference documentation "
              + "https://cloud.google.com/compute/docs/networking. Default is up to the Dataflow service.")
  @HopMetadataProperty
  private String gcpSubnetwork;

  @GuiWidgetElement(
      order = "20130-dataflow-options",
      parentId = PipelineRunConfiguration.GUI_PLUGIN_ELEMENT_PARENT_ID,
      type = GuiElementType.CHECKBOX,
      label = "Use public IPs?",
      toolTip =
          "Specifies whether worker pools should be started with public IP addresses."
              + "  WARNING: This feature is experimental. You must be allowlisted to use it.")
  @HopMetadataProperty
  private boolean gcpUsingPublicIps;

  public BeamDataFlowPipelineRunConfiguration() {
    super();
    this.gcpAppName = "Hop";
    this.gcpUsingPublicIps = true;
  }

  // Clone
  public BeamDataFlowPipelineRunConfiguration(BeamDataFlowPipelineRunConfiguration config) {
    super(config);
    this.gcpProjectId = config.gcpProjectId;
    this.gcpServiceAccount = config.gcpServiceAccount;
    this.gcpAppName = config.gcpAppName;
    this.gcpStagingLocation = config.gcpStagingLocation;
    this.gcpInitialNumberOfWorkers = config.gcpInitialNumberOfWorkers;
    this.gcpMaximumNumberOfWorkers = config.gcpMaximumNumberOfWorkers;
    this.gcpAutoScalingAlgorithm = config.gcpAutoScalingAlgorithm;
    this.gcpWorkerMachineType = config.gcpWorkerMachineType;
    this.gcpWorkerDiskType = config.gcpWorkerDiskType;
    this.gcpDiskSizeGb = config.gcpDiskSizeGb;
    this.gcpRegion = config.gcpRegion;
    this.gcpZone = config.gcpZone;
    this.gcpNetwork = config.gcpNetwork;
    this.gcpSubnetwork = config.gcpSubnetwork;
    this.gcpUsingPublicIps = config.gcpUsingPublicIps;
  }

  @Override
  public BeamDataFlowPipelineRunConfiguration clone() {
    return new BeamDataFlowPipelineRunConfiguration(this);
  }

  @Override
  public RunnerType getRunnerType() {
    return RunnerType.DataFlow;
  }

  @Override
  public PipelineOptions getPipelineOptions() throws HopException {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

    options.setProject(resolve(getGcpProjectId()));
    options.setAppName(resolve(getGcpAppName()));
    options.setStagingLocation(resolve(getGcpStagingLocation()));

    if (StringUtils.isNotEmpty(getGcpServiceAccount())) {
      options.setServiceAccount(resolve(getGcpServiceAccount()));
    }

    if (StringUtils.isNotEmpty(getGcpInitialNumberOfWorkers())) {
      int numWorkers = Const.toInt(resolve(getGcpInitialNumberOfWorkers()), -1);
      if (numWorkers >= 0) {
        options.setNumWorkers(numWorkers);
      }
    }

    if (StringUtils.isNotEmpty(getGcpMaximumNumberOfWorkers())) {
      int numWorkers = Const.toInt(resolve(getGcpMaximumNumberOfWorkers()), -1);
      if (numWorkers >= 0) {
        options.setMaxNumWorkers(numWorkers);
      }
    }
    if (StringUtils.isNotEmpty(getGcpWorkerMachineType())) {
      String machineType = resolve(getGcpWorkerMachineType());
      options.setWorkerMachineType(machineType);
    }
    if (StringUtils.isNotEmpty(getGcpWorkerDiskType())) {
      String diskType = resolve(getGcpWorkerDiskType());
      options.setWorkerDiskType(diskType);
    }
    if (StringUtils.isNotEmpty(getGcpDiskSizeGb())) {
      int diskSize = Const.toInt(resolve(getGcpDiskSizeGb()), -1);
      if (diskSize >= 0) {
        options.setDiskSizeGb(diskSize);
      }
    }
    if (StringUtils.isNotEmpty(getGcpZone())) {
      String zone = resolve(getGcpZone());
      options.setWorkerZone(zone);
    }
    if (StringUtils.isNotEmpty(getGcpRegion())) {
      String region = resolve(getGcpRegion());
      options.setRegion(region);
    }
    if (StringUtils.isNotEmpty(getGcpAutoScalingAlgorithm())) {
      String algorithmCode = resolve(getGcpAutoScalingAlgorithm());
      try {

        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType algorithm =
            DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.valueOf(algorithmCode);
        options.setAutoscalingAlgorithm(algorithm);
      } catch (Exception e) {
        throw new HopException(
            "Unknown autoscaling algorithm for GCP DataFlow: " + algorithmCode, e);
      }
    }

    if (StringUtils.isNotEmpty(getGcpNetwork())) {
      String network = resolve(getGcpNetwork());
      options.setNetwork(network);
    }
    if (StringUtils.isNotEmpty(getGcpSubnetwork())) {
      String subnetwork = resolve(getGcpSubnetwork());
      options.setSubnetwork(subnetwork);
    }

    // Experimental feature...
    //
    options.setUsePublicIps(isGcpUsingPublicIps());

    if (StringUtils.isNotEmpty(getFatJar())) {
      options.setFilesToStage(Collections.singletonList(resolve(fatJar)));
    }

    return options;
  }

  @Override
  public boolean isRunningAsynchronous() {
    return true;
  }

  /**
   * Gets gcpProjectId
   *
   * @return value of gcpProjectId
   */
  public String getGcpProjectId() {
    return gcpProjectId;
  }

  /** @param gcpProjectId The gcpProjectId to set */
  public void setGcpProjectId(String gcpProjectId) {
    this.gcpProjectId = gcpProjectId;
  }


  /**
   * Gets the GCP service account
   * @return value of gcpServiceAccount
   */
  public String getGcpServiceAccount() {
    return gcpServiceAccount;
  }

  /**
   * @param gcpServiceAccount The GCP service account to set
   */
  public void setGcpServiceAccount(String gcpServiceAccount) {
    this.gcpServiceAccount = gcpServiceAccount;
  }

  /**
   * Gets gcpAppName
   *
   * @return value of gcpAppName
   */
  public String getGcpAppName() {
    return gcpAppName;
  }

  /** @param gcpAppName The gcpAppName to set */
  public void setGcpAppName(String gcpAppName) {
    this.gcpAppName = gcpAppName;
  }

  /**
   * Gets gcpStagingLocation
   *
   * @return value of gcpStagingLocation
   */
  public String getGcpStagingLocation() {
    return gcpStagingLocation;
  }

  /** @param gcpStagingLocation The gcpStagingLocation to set */
  public void setGcpStagingLocation(String gcpStagingLocation) {
    this.gcpStagingLocation = gcpStagingLocation;
  }

  /**
   * Gets gcpInitialNumberOfWorkers
   *
   * @return value of gcpInitialNumberOfWorkers
   */
  public String getGcpInitialNumberOfWorkers() {
    return gcpInitialNumberOfWorkers;
  }

  /** @param gcpInitialNumberOfWorkers The gcpInitialNumberOfWorkers to set */
  public void setGcpInitialNumberOfWorkers(String gcpInitialNumberOfWorkers) {
    this.gcpInitialNumberOfWorkers = gcpInitialNumberOfWorkers;
  }

  /**
   * Gets gcpMaximumNumberOfWorkers
   *
   * @return value of gcpMaximumNumberOfWorkers
   */
  public String getGcpMaximumNumberOfWorkers() {
    return gcpMaximumNumberOfWorkers;
  }

  /** @param gcpMaximumNumberOfWorkers The gcpMaximumNumberOfWorkers to set */
  public void setGcpMaximumNumberOfWorkers(String gcpMaximumNumberOfWorkers) {
    this.gcpMaximumNumberOfWorkers = gcpMaximumNumberOfWorkers;
  }

  /**
   * Gets gcpAutoScalingAlgorithm
   *
   * @return value of gcpAutoScalingAlgorithm
   */
  public String getGcpAutoScalingAlgorithm() {
    return gcpAutoScalingAlgorithm;
  }

  /** @param gcpAutoScalingAlgorithm The gcpAutoScalingAlgorithm to set */
  public void setGcpAutoScalingAlgorithm(String gcpAutoScalingAlgorithm) {
    this.gcpAutoScalingAlgorithm = gcpAutoScalingAlgorithm;
  }

  /**
   * Gets gcpWorkerMachineType
   *
   * @return value of gcpWorkerMachineType
   */
  public String getGcpWorkerMachineType() {
    return gcpWorkerMachineType;
  }

  /** @param gcpWorkerMachineType The gcpWorkerMachineType to set */
  public void setGcpWorkerMachineType(String gcpWorkerMachineType) {
    this.gcpWorkerMachineType = gcpWorkerMachineType;
  }

  /**
   * Gets gcpWorkerDiskType
   *
   * @return value of gcpWorkerDiskType
   */
  public String getGcpWorkerDiskType() {
    return gcpWorkerDiskType;
  }

  /** @param gcpWorkerDiskType The gcpWorkerDiskType to set */
  public void setGcpWorkerDiskType(String gcpWorkerDiskType) {
    this.gcpWorkerDiskType = gcpWorkerDiskType;
  }

  /**
   * Gets gcpDiskSizeGb
   *
   * @return value of gcpDiskSizeGb
   */
  public String getGcpDiskSizeGb() {
    return gcpDiskSizeGb;
  }

  /** @param gcpDiskSizeGb The gcpDiskSizeGb to set */
  public void setGcpDiskSizeGb(String gcpDiskSizeGb) {
    this.gcpDiskSizeGb = gcpDiskSizeGb;
  }

  /**
   * Gets gcpRegion
   *
   * @return value of gcpRegion
   */
  public String getGcpRegion() {
    return gcpRegion;
  }

  /** @param gcpRegion The gcpRegion to set */
  public void setGcpRegion(String gcpRegion) {
    this.gcpRegion = gcpRegion;
  }

  /**
   * Gets gcpZone
   *
   * @return value of gcpZone
   */
  public String getGcpZone() {
    return gcpZone;
  }

  /** @param gcpZone The gcpZone to set */
  public void setGcpZone(String gcpZone) {
    this.gcpZone = gcpZone;
  }

  /**
   * Gets gcpNetwork
   *
   * @return value of gcpNetwork
   */
  public String getGcpNetwork() {
    return gcpNetwork;
  }

  /** @param gcpNetwork The gcpNetwork to set */
  public void setGcpNetwork(String gcpNetwork) {
    this.gcpNetwork = gcpNetwork;
  }

  /**
   * Gets gcpSubnetwork
   *
   * @return value of gcpSubnetwork
   */
  public String getGcpSubnetwork() {
    return gcpSubnetwork;
  }

  /** @param gcpSubnetwork The gcpSubnetwork to set */
  public void setGcpSubnetwork(String gcpSubnetwork) {
    this.gcpSubnetwork = gcpSubnetwork;
  }

  /**
   * Gets gcpUsingPublicIps
   *
   * @return value of gcpUsingPublicIps
   */
  public boolean isGcpUsingPublicIps() {
    return gcpUsingPublicIps;
  }

  /** @param gcpUsingPublicIps The gcpUsingPublicIps to set */
  public void setGcpUsingPublicIps(boolean gcpUsingPublicIps) {
    this.gcpUsingPublicIps = gcpUsingPublicIps;
  }
}
