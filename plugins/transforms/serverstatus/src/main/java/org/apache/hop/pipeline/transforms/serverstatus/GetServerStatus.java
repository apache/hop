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

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.server.HopServerMeta;
import org.apache.hop.server.loadbalance.HopServerProbe;
import org.apache.hop.server.loadbalance.ServerHealthSnapshot;

public class GetServerStatus extends BaseTransform<GetServerStatusMeta, GetServerStatusData> {
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
    HopServerMeta serverMeta = metadataProvider.getSerializer(HopServerMeta.class).load(serverName);
    if (serverMeta == null) {
      throw new HopException("Hop server '" + serverName + "' couldn't be found");
    }
    ServerHealthSnapshot snapshot = HopServerProbe.probe(serverMeta, this, true, 0);
    String errorMessage = snapshot.getErrorMessage();
    String statusDescription = snapshot.getStatusDescription();
    Double serverLoad = snapshot.getLoadAvg();
    Long memoryFree = snapshot.getMemoryFree();
    Long memoryTotal = snapshot.getMemoryTotal();
    Long cpuCores = snapshot.getCpuCores() == null ? null : snapshot.getCpuCores().longValue();
    Long cpuProcessTime = snapshot.getCpuProcessTime();
    String osName = snapshot.getOsName();
    String osVersion = snapshot.getOsVersion();
    String osArchitecture = snapshot.getOsArchitecture();
    Long activePipelines = snapshot.isAvailable() ? (long) snapshot.getRunningPipelines() : null;
    Long finishedPipelines = snapshot.isAvailable() ? (long) snapshot.getFinishedPipelines() : null;
    Long activeWorkflows = snapshot.isAvailable() ? (long) snapshot.getRunningWorkflows() : null;
    Long finishedWorkflows = snapshot.isAvailable() ? (long) snapshot.getFinishedWorkflows() : null;
    Boolean available = snapshot.isAvailable() ? Boolean.TRUE : null;
    Long responseNs = snapshot.getResponseNs();

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
    if (StringUtils.isNotEmpty(meta.getFinishedPipelinesField())) {
      outputRow[outIndex++] = finishedPipelines;
    }
    if (StringUtils.isNotEmpty(meta.getActiveWorkflowsField())) {
      outputRow[outIndex++] = activeWorkflows;
    }
    if (StringUtils.isNotEmpty(meta.getFinishedWorkflowsField())) {
      outputRow[outIndex++] = finishedWorkflows;
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
