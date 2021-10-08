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

package org.apache.hop.pipeline.transforms.serverstatus;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaBoolean;
import org.apache.hop.core.row.value.ValueMetaInteger;
import org.apache.hop.core.row.value.ValueMetaNumber;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

@Transform(
    id = "GetServerStatus",
    name = "i18n::GetServerStatus.Transform.Name",
    description = "i18n::GetServerStatus.Transform.Description",
    image = "ui/images/server.svg",
    categoryDescription = "Lookup",
    documentationUrl = "/pipeline/transforms/serverstatus.html",
    keywords = "i18n::GetServerStatus.Transform.Keywords")
public class GetServerStatusMeta extends BaseTransformMeta
    implements ITransformMeta<GetServerStatus, GetServerStatusData> {

  @HopMetadataProperty(key = "server_field")
  private String serverField;

  @HopMetadataProperty(key = "error_message_field")
  private String errorMessageField;

  @HopMetadataProperty(key = "status_description_field")
  private String statusDescriptionField;

  @HopMetadataProperty(key = "server_load_field")
  private String serverLoadField;

  @HopMetadataProperty(key = "memory_free_field")
  private String memoryFreeField;

  @HopMetadataProperty(key = "memory_total_field")
  private String memoryTotalField;

  @HopMetadataProperty(key = "cpu_cores_field")
  private String cpuCoresField;

  @HopMetadataProperty(key = "cpu_process_time_field")
  private String cpuProcessTimeField;

  @HopMetadataProperty(key = "os_name_field")
  private String osNameField;

  @HopMetadataProperty(key = "os_version_field")
  private String osVersionField;

  @HopMetadataProperty(key = "os_architecture_field")
  private String osArchitectureField;

  @HopMetadataProperty(key = "active_pipelines_field")
  private String activePipelinesField;

  @HopMetadataProperty(key = "active_workflows_field")
  private String activeWorkflowsField;

  @HopMetadataProperty(key = "available_field")
  private String availableField;

  @HopMetadataProperty(key = "response_ns_field")
  private String responseNsField;

  public GetServerStatusMeta() {
    serverField = "";
    errorMessageField = "errorMessage";
    statusDescriptionField = "statusDescription";
    serverLoadField = "serverLoad";
    memoryFreeField = "memoryFree";
    memoryTotalField = "memoryTotal";
    cpuCoresField = "cpuCores";
    cpuProcessTimeField = "cpuProcessTime";
    osNameField = "osName";
    osVersionField = "osVersion";
    osArchitectureField = "osArchitecture";
    activePipelinesField = "activePipelines";
    activeWorkflowsField = "activeWorkflows";
    availableField = "available";
    responseNsField = "responseNs";
  }

  public GetServerStatusMeta(GetServerStatusMeta meta) {
    this.serverField = meta.serverField;
    this.errorMessageField = meta.errorMessageField;
    this.statusDescriptionField = meta.statusDescriptionField;
    this.serverLoadField = meta.serverLoadField;
    this.memoryFreeField = meta.memoryFreeField;
    this.memoryTotalField = meta.memoryTotalField;
    this.cpuCoresField = meta.cpuCoresField;
    this.cpuProcessTimeField = meta.cpuProcessTimeField;
    this.osNameField = meta.osNameField;
    this.osVersionField = meta.osVersionField;
    this.osArchitectureField = meta.osArchitectureField;
    this.activePipelinesField = meta.activePipelinesField;
    this.activeWorkflowsField = meta.activeWorkflowsField;
    this.availableField = meta.availableField;
    this.responseNsField = meta.responseNsField;
  }

  public GetServerStatusMeta clone() {
    return new GetServerStatusMeta(this);
  }

  @Override
  public void getFields(
      IRowMeta inputRowMeta,
      String name,
      IRowMeta[] info,
      TransformMeta nextTransform,
      IVariables variables,
      IHopMetadataProvider metadataProvider)
      throws HopTransformException {
    if (StringUtils.isNotEmpty(errorMessageField)) {
      inputRowMeta.addValueMeta(new ValueMetaString(errorMessageField));
    }
    if (StringUtils.isNotEmpty(statusDescriptionField)) {
      inputRowMeta.addValueMeta(new ValueMetaString(statusDescriptionField));
    }
    if (StringUtils.isNotEmpty(serverLoadField)) {
      inputRowMeta.addValueMeta(new ValueMetaNumber(serverLoadField));
    }
    if (StringUtils.isNotEmpty(memoryFreeField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(memoryFreeField));
    }
    if (StringUtils.isNotEmpty(memoryTotalField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(memoryTotalField));
    }
    if (StringUtils.isNotEmpty(cpuCoresField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(cpuCoresField));
    }
    if (StringUtils.isNotEmpty(cpuProcessTimeField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(cpuProcessTimeField));
    }
    if (StringUtils.isNotEmpty(osNameField)) {
      inputRowMeta.addValueMeta(new ValueMetaString(osNameField));
    }
    if (StringUtils.isNotEmpty(osVersionField)) {
      inputRowMeta.addValueMeta(new ValueMetaString(osVersionField));
    }
    if (StringUtils.isNotEmpty(osArchitectureField)) {
      inputRowMeta.addValueMeta(new ValueMetaString(osArchitectureField));
    }
    if (StringUtils.isNotEmpty(activePipelinesField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(activePipelinesField));
    }
    if (StringUtils.isNotEmpty(activeWorkflowsField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(activeWorkflowsField));
    }
    if (StringUtils.isNotEmpty(availableField)) {
      inputRowMeta.addValueMeta(new ValueMetaBoolean(availableField));
    }
    if (StringUtils.isNotEmpty(responseNsField)) {
      inputRowMeta.addValueMeta(new ValueMetaInteger(responseNsField));
    }
  }

  @Override
  public GetServerStatus createTransform(
      TransformMeta transformMeta,
      GetServerStatusData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    return new GetServerStatus(transformMeta, this, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public GetServerStatusData getTransformData() {
    return new GetServerStatusData();
  }

  /**
   * Gets serverField
   *
   * @return value of serverField
   */
  public String getServerField() {
    return serverField;
  }

  /** @param serverField The serverField to set */
  public void setServerField(String serverField) {
    this.serverField = serverField;
  }

  /**
   * Gets errorMessageField
   *
   * @return value of errorMessageField
   */
  public String getErrorMessageField() {
    return errorMessageField;
  }

  /** @param errorMessageField The errorMessageField to set */
  public void setErrorMessageField(String errorMessageField) {
    this.errorMessageField = errorMessageField;
  }

  /**
   * Gets statusDescriptionField
   *
   * @return value of statusDescriptionField
   */
  public String getStatusDescriptionField() {
    return statusDescriptionField;
  }

  /** @param statusDescriptionField The statusDescriptionField to set */
  public void setStatusDescriptionField(String statusDescriptionField) {
    this.statusDescriptionField = statusDescriptionField;
  }

  /**
   * Gets serverLoadField
   *
   * @return value of serverLoadField
   */
  public String getServerLoadField() {
    return serverLoadField;
  }

  /** @param serverLoadField The serverLoadField to set */
  public void setServerLoadField(String serverLoadField) {
    this.serverLoadField = serverLoadField;
  }

  /**
   * Gets memoryFreeField
   *
   * @return value of memoryFreeField
   */
  public String getMemoryFreeField() {
    return memoryFreeField;
  }

  /** @param memoryFreeField The memoryFreeField to set */
  public void setMemoryFreeField(String memoryFreeField) {
    this.memoryFreeField = memoryFreeField;
  }

  /**
   * Gets memoryTotalField
   *
   * @return value of memoryTotalField
   */
  public String getMemoryTotalField() {
    return memoryTotalField;
  }

  /** @param memoryTotalField The memoryTotalField to set */
  public void setMemoryTotalField(String memoryTotalField) {
    this.memoryTotalField = memoryTotalField;
  }

  /**
   * Gets cpuCoresField
   *
   * @return value of cpuCoresField
   */
  public String getCpuCoresField() {
    return cpuCoresField;
  }

  /** @param cpuCoresField The cpuCoresField to set */
  public void setCpuCoresField(String cpuCoresField) {
    this.cpuCoresField = cpuCoresField;
  }

  /**
   * Gets cpuProcessTimeField
   *
   * @return value of cpuProcessTimeField
   */
  public String getCpuProcessTimeField() {
    return cpuProcessTimeField;
  }

  /** @param cpuProcessTimeField The cpuProcessTimeField to set */
  public void setCpuProcessTimeField(String cpuProcessTimeField) {
    this.cpuProcessTimeField = cpuProcessTimeField;
  }

  /**
   * Gets osNameField
   *
   * @return value of osNameField
   */
  public String getOsNameField() {
    return osNameField;
  }

  /** @param osNameField The osNameField to set */
  public void setOsNameField(String osNameField) {
    this.osNameField = osNameField;
  }

  /**
   * Gets osVersionField
   *
   * @return value of osVersionField
   */
  public String getOsVersionField() {
    return osVersionField;
  }

  /** @param osVersionField The osVersionField to set */
  public void setOsVersionField(String osVersionField) {
    this.osVersionField = osVersionField;
  }

  /**
   * Gets osArchitectureField
   *
   * @return value of osArchitectureField
   */
  public String getOsArchitectureField() {
    return osArchitectureField;
  }

  /** @param osArchitectureField The osArchitectureField to set */
  public void setOsArchitectureField(String osArchitectureField) {
    this.osArchitectureField = osArchitectureField;
  }

  /**
   * Gets activePipelinesField
   *
   * @return value of activePipelinesField
   */
  public String getActivePipelinesField() {
    return activePipelinesField;
  }

  /** @param activePipelinesField The activePipelinesField to set */
  public void setActivePipelinesField(String activePipelinesField) {
    this.activePipelinesField = activePipelinesField;
  }

  /**
   * Gets activeWorkflowsField
   *
   * @return value of activeWorkflowsField
   */
  public String getActiveWorkflowsField() {
    return activeWorkflowsField;
  }

  /** @param activeWorkflowsField The activeWorkflowsField to set */
  public void setActiveWorkflowsField(String activeWorkflowsField) {
    this.activeWorkflowsField = activeWorkflowsField;
  }

  /**
   * Gets availableField
   *
   * @return value of availableField
   */
  public String getAvailableField() {
    return availableField;
  }

  /** @param availableField The availableField to set */
  public void setAvailableField(String availableField) {
    this.availableField = availableField;
  }

  /**
   * Gets responseNsField
   *
   * @return value of responseNsField
   */
  public String getResponseNsField() {
    return responseNsField;
  }

  /** @param responseNsField The responseNsField to set */
  public void setResponseNsField(String responseNsField) {
    this.responseNsField = responseNsField;
  }
}
