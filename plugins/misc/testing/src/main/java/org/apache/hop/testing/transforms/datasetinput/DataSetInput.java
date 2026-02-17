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

package org.apache.hop.testing.transforms.datasetinput;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;

public class DataSetInput extends BaseTransform<DataSetInputMeta, DataSetInputData> {

  public DataSetInput(
      TransformMeta transformMeta,
      DataSetInputMeta meta,
      DataSetInputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    try {
      if (StringUtils.isEmpty(meta.getDataSetName())) {
        logError("Please specify the name of the data set to read from");
        setErrors(1);
        return false;
      }
      data.realDataSetName = resolve(meta.getDataSetName());
      IHopMetadataSerializer<DataSet> serializer = metadataProvider.getSerializer(DataSet.class);
      data.dataSet = serializer.load(data.realDataSetName);
    } catch (Exception e) {
      logError("Error initializing", e);
      setErrors(1);
      return false;
    }

    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {

    if (first) {
      first = false;

      data.outputRowMeta = new RowMeta();
      data.outputRowMeta.addRowMeta(data.dataSet.getSetRowMeta());
    }

    List<Object[]> rows = data.dataSet.getAllRows(this, getLogChannel());
    for (Object[] row : rows) {
      putRow(data.outputRowMeta, row);
    }
    setOutputDone();

    return false;
  }
}
