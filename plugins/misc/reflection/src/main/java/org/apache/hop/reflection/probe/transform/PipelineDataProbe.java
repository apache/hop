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
 *
 */

package org.apache.hop.reflection.probe.transform;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class PipelineDataProbe extends BaseTransform<PipelineDataProbeMeta, PipelineDataProbeData>
    implements ITransform<PipelineDataProbeMeta, PipelineDataProbeData> {

  private String sourcePipelineName;
  private String sourceTransformLogChannelId;
  private String sourceTransformName;
  private long sourceTransformCopy;

  public PipelineDataProbe(
      TransformMeta transformMeta,
      PipelineDataProbeMeta meta,
      PipelineDataProbeData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {

    // Get a row from the probe
    //
    Object[] inputRow = getRow();
    if (inputRow == null) {
      setOutputDone();
      return false;
    }

    if (first) {
      first = false;

      // Calculate the output fields of this transform
      //
      data.outputRowMeta = new RowMeta();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);
    }

    // Normalize this data...
    //
    for (int v = 0; v < getInputRowMeta().size(); v++) {
      IValueMeta valueMeta = getInputRowMeta().getValueMeta(v);
      Object valueData = inputRow[v];

      // Output a row for every one of these input rows...
      //
      Object[] outputRow = RowDataUtil.allocateRowData(data.outputRowMeta.size());
      int index = -1;
      outputRow[++index] = sourcePipelineName;
      outputRow[++index] = sourceTransformLogChannelId;
      outputRow[++index] = sourceTransformName;
      outputRow[++index] = sourceTransformCopy;
      outputRow[++index] = getLinesRead();
      outputRow[++index] = valueMeta.getName();
      outputRow[++index] = valueMeta.getTypeDesc();
      outputRow[++index] = valueMeta.getFormatMask();
      outputRow[++index] = (long) valueMeta.getLength();
      outputRow[++index] = (long) valueMeta.getPrecision();
      outputRow[++index] = valueMeta.getString(valueData);

      putRow(data.outputRowMeta, outputRow);
    }

    return true;
  }

  /**
   * Gets sourcePipelineName
   *
   * @return value of sourcePipelineName
   */
  public String getSourcePipelineName() {
    return sourcePipelineName;
  }

  /** @param sourcePipelineName The sourcePipelineName to set */
  public void setSourcePipelineName(String sourcePipelineName) {
    this.sourcePipelineName = sourcePipelineName;
  }

  /**
   * Gets sourceTransformLogChannelId
   *
   * @return value of sourceTransformLogChannelId
   */
  public String getSourceTransformLogChannelId() {
    return sourceTransformLogChannelId;
  }

  /** @param sourceTransformLogChannelId The sourceTransformLogChannelId to set */
  public void setSourceTransformLogChannelId(String sourceTransformLogChannelId) {
    this.sourceTransformLogChannelId = sourceTransformLogChannelId;
  }

  /**
   * Gets sourceTransformName
   *
   * @return value of sourceTransformName
   */
  public String getSourceTransformName() {
    return sourceTransformName;
  }

  /** @param sourceTransformName The sourceTransformName to set */
  public void setSourceTransformName(String sourceTransformName) {
    this.sourceTransformName = sourceTransformName;
  }

  /**
   * Gets sourceTransformCopy
   *
   * @return value of sourceTransformCopy
   */
  public long getSourceTransformCopy() {
    return sourceTransformCopy;
  }

  /** @param sourceTransformCopy The sourceTransformCopy to set */
  public void setSourceTransformCopy(long sourceTransformCopy) {
    this.sourceTransformCopy = sourceTransformCopy;
  }
}
