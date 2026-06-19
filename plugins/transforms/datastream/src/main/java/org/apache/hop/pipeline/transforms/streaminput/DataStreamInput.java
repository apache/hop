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
 *
 */

package org.apache.hop.pipeline.transforms.streaminput;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;

public class DataStreamInput extends BaseTransform<DataStreamInputMeta, DataStreamInputData> {
  /**
   * This is the base transform that forms that basis for all transforms. You can derive from this
   * class to implement your own transforms.
   *
   * @param transformMeta The TransformMeta object to run.
   * @param meta The data stream input metadata
   * @param data the data object to store temporary data, database connections, caches, result sets,
   *     hash tables etc.
   * @param copyNr The copy number for this transform.
   * @param pipelineMeta The PipelineMeta of which the transform transformMeta is part of.
   * @param pipeline The (running) pipeline to obtain information shared among the transforms.
   */
  public DataStreamInput(
      TransformMeta transformMeta,
      DataStreamInputMeta meta,
      DataStreamInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (!super.init()) {
      return false;
    }
    try {
      data.dataStreamName = resolve(meta.getDataStreamName());
      data.dataStreamMeta =
          DataStreamInputMeta.getAndValidateDataStream(metadataProvider, data.dataStreamName);
      data.dataStream = data.dataStreamMeta.getDataStream();
      data.dataStream.initialize(variables, metadataProvider, false, data.dataStreamMeta);
      data.streamRowMeta = data.dataStream.getRowMeta();
    } catch (Exception e) {
      getLogChannel().logError("Error initializing data stream", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean processRow() throws HopException {
    try {
      Object[] row = data.dataStream.readRow();
      if (row == null) {
        setOutputDone();
        return false;
      }
      incrementLinesInput();
      putRow(data.streamRowMeta, row);
      return true;
    } catch (Exception e) {
      throw new HopException("Error reading row from data stream: " + data.dataStreamName, e);
    }
  }

  @Override
  public void dispose() {
    try {
      data.dataStream.close();
    } catch (Exception e) {
      getLogChannel().logError("Error closing data stream", e);
    }
    super.dispose();
  }
}
