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
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.server.HopServer;
import org.apache.hop.www.HopServerPipelineStatus;
import org.apache.hop.www.HopServerStatus;
import org.apache.hop.www.HopServerWorkflowStatus;

public class GetServerStatus extends BaseTransform<GetServerStatusMeta, GetServerStatusData>
    implements ITransform<GetServerStatusMeta, GetServerStatusData> {
  public GetServerStatus(
      TransformMeta transformMeta,
      GetServerStatusMeta meta,
      GetServerStatusData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();
    if (row == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // Get the output row layout
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      data.serverFieldIndex = getInputRowMeta().indexOfValue(meta.getServerField());
      if (data.serverFieldIndex < 0) {
        throw new HopException("Unable to find hop server field '" + meta.getServerField());
      }
    }

    String serverName = getInputRowMeta().getString(row, data.serverFieldIndex);
    HopServer hopServer = metadataProvider.getSerializer(HopServer.class).load(serverName);
    if (hopServer == null) {
      throw new HopException("Hop server '" + serverName + "' couldn't be found");
    }

    String errorMessage;
    String statusDescription = null;
    Double serverLoad = null;
    Double memoryFree = null;
    Double memoryTotal = null;
    Long cpuCores = null;
    Long cpuProcessTime = null;
    String osName = null;
    String osVersion = null;
    String osArchitecture = null;
    Long activePipelines = null;
    Long activeWorkflows = null;
    Boolean available = null;
    Long responseNs = null;
    long startTime = System.nanoTime();
    long endTime;

    try {
      errorMessage = null;
      HopServerStatus status = hopServer.getStatus(this);
      statusDescription = status.getStatusDescription();
      serverLoad = status.getLoadAvg();
      memoryFree = status.getMemoryFree();
      memoryTotal = status.getMemoryTotal();
      cpuCores = (long) status.getCpuCores();
      cpuProcessTime = status.getCpuProcessTime();
      osName = status.getOsName();
      osVersion = status.getOsVersion();
      osArchitecture = status.getOsArchitecture();
      activePipelines = 0L;
      for (HopServerPipelineStatus pipelineStatus : status.getPipelineStatusList()) {
        if (pipelineStatus.isRunning()) {
          activePipelines++;
        }
      }
      activeWorkflows = 0L;
      for (HopServerWorkflowStatus workflowStatus : status.getWorkflowStatusList()) {
        if (workflowStatus.isRunning()) {
          activeWorkflows++;
        }
      }

      available = true;
    } catch (Exception e) {
      errorMessage = "Error querying Hop server : " + e.getMessage();
    } finally {
      endTime = System.nanoTime();
    }
    responseNs = endTime - startTime;

    // Add the fields to the output row
    //
    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    int outIndex = getInputRowMeta().size();
    if (StringUtils.isNotEmpty(meta.getErrorMessageField())) {
      outputRow[outIndex++] = errorMessage;
    }
    if (StringUtils.isNotEmpty(meta.getStatusDescriptionField())) {
      outputRow[outIndex++] = statusDescription;
    }
    if (StringUtils.isNotEmpty(meta.getServerLoadField())) {
      outputRow[outIndex++] = serverLoad;
    }
    if (StringUtils.isNotEmpty(meta.getMemoryFreeField())) {
      outputRow[outIndex++] = memoryFree;
    }
    if (StringUtils.isNotEmpty(meta.getMemoryTotalField())) {
      outputRow[outIndex++] = memoryTotal;
    }
    if (StringUtils.isNotEmpty(meta.getCpuCoresField())) {
      outputRow[outIndex++] = cpuCores;
    }
    if (StringUtils.isNotEmpty(meta.getCpuProcessTimeField())) {
      outputRow[outIndex++] = cpuProcessTime;
    }
    if (StringUtils.isNotEmpty(meta.getOsNameField())) {
      outputRow[outIndex++] = osName;
    }
    if (StringUtils.isNotEmpty(meta.getOsVersionField())) {
      outputRow[outIndex++] = osVersion;
    }
    if (StringUtils.isNotEmpty(meta.getOsArchitectureField())) {
      outputRow[outIndex++] = osArchitecture;
    }
    if (StringUtils.isNotEmpty(meta.getActivePipelinesField())) {
      outputRow[outIndex++] = activePipelines;
    }
    if (StringUtils.isNotEmpty(meta.getActiveWorkflowsField())) {
      outputRow[outIndex++] = activeWorkflows;
    }
    if (StringUtils.isNotEmpty(meta.getAvailableField())) {
      outputRow[outIndex++] = available;
    }
    if (StringUtils.isNotEmpty(meta.getResponseNsField())) {
      outputRow[outIndex++] = responseNs;
    }

    putRow(data.outputRowMeta, outputRow);

    return true;
  }
}
