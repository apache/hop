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

package org.apache.hop.testing.transforms.datasetoutput;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.testing.DataSet;
import org.apache.hop.testing.DataSetCsvWriter;

public class DataSetOutput extends BaseTransform<DataSetOutputMeta, DataSetOutputData> {
  private static final Class<?> PKG = DataSetOutputMeta.class;

  public DataSetOutput(
      TransformMeta transformMeta,
      DataSetOutputMeta meta,
      DataSetOutputData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean init() {
    if (getTransformMeta().getCopies(this) > 1) {
      logError(BaseMessages.getString(PKG, "DataSetOutput.Log.OnlyOneCopy"));
      return false;
    }
    if (StringUtils.isEmpty(meta.getDataSetName())) {
      logError(BaseMessages.getString(PKG, "DataSetOutput.Log.DataSetNameMissing"));
      return false;
    }
    data.realDataSetName = resolve(meta.getDataSetName());
    if (StringUtils.isEmpty(data.realDataSetName)) {
      logError(BaseMessages.getString(PKG, "DataSetOutput.Log.DataSetNameMissing"));
      return false;
    }
    data.realFolderName = Const.NVL(resolve(Const.NVL(meta.getFolderName(), "")), "");
    data.realCsvFilename = Const.NVL(resolve(Const.NVL(meta.getCsvFilename(), "")), "");
    return super.init();
  }

  @Override
  public boolean processRow() throws HopException {
    Object[] row = getRow();

    if (first) {
      first = false;
      prepareDataSet(getInputRowMeta());
    }

    if (row == null) {
      closeWriter();
      setOutputDone();
      return false;
    }

    writeMappedRow(row);
    putRow(getInputRowMeta(), row);
    return true;
  }

  private void prepareDataSet(IRowMeta inputRowMeta) throws HopException {
    IRowMeta rowMeta = inputRowMeta;
    if (rowMeta == null) {
      rowMeta = getPipelineMeta().getPrevTransformFields(this, getTransformMeta());
    }
    if (rowMeta == null || rowMeta.isEmpty()) {
      throw new HopException(BaseMessages.getString(PKG, "DataSetOutput.Log.NoInputRowMeta"));
    }

    IHopMetadataSerializer<DataSet> serializer = metadataProvider.getSerializer(DataSet.class);

    if (meta.isRecreateDataSet()) {
      data.dataSet = serializer.load(data.realDataSetName);
      if (data.dataSet == null) {
        data.dataSet = new DataSet();
        data.dataSet.setName(data.realDataSetName);
      }
      if (StringUtils.isNotEmpty(data.realFolderName)) {
        data.dataSet.setFolderName(data.realFolderName);
      }
      if (StringUtils.isNotEmpty(data.realCsvFilename)) {
        data.dataSet.setBaseFilename(data.realCsvFilename);
      } else if (StringUtils.isEmpty(data.dataSet.getBaseFilename())) {
        data.dataSet.setBaseFilename(data.realDataSetName + ".csv");
      }
      data.dataSet.setFields(DataSet.createFieldsFromRowMeta(rowMeta));
      serializer.save(data.dataSet);
      data.setRowMeta = data.dataSet.getSetRowMeta();
      data.fieldIndexes = identityIndexes(rowMeta.size());
    } else {
      DataSet existing = serializer.load(data.realDataSetName);
      if (existing == null) {
        throw new HopException(
            BaseMessages.getString(PKG, "DataSetOutput.Log.DataSetNotFound", data.realDataSetName));
      }
      if (meta.isValidateDataSet()) {
        existing.validateRowMeta(rowMeta);
      }
      String folder =
          StringUtils.isNotEmpty(data.realFolderName)
              ? data.realFolderName
              : existing.getFolderName();
      String filename =
          StringUtils.isNotEmpty(data.realCsvFilename)
              ? data.realCsvFilename
              : existing.getBaseFilename();
      data.dataSet =
          new DataSet(
              existing.getName(),
              existing.getDescription(),
              folder,
              filename,
              existing.getFields());
      data.setRowMeta = existing.getSetRowMeta();
      data.fieldIndexes = mapInputToDataSet(rowMeta, data.setRowMeta);
    }

    data.writer = new DataSetCsvWriter(this, data.dataSet, data.setRowMeta);
  }

  private int[] identityIndexes(int size) {
    int[] indexes = new int[size];
    for (int i = 0; i < size; i++) {
      indexes[i] = i;
    }
    return indexes;
  }

  private int[] mapInputToDataSet(IRowMeta inputRowMeta, IRowMeta setRowMeta) {
    int[] indexes = new int[setRowMeta.size()];
    for (int i = 0; i < setRowMeta.size(); i++) {
      indexes[i] = inputRowMeta.indexOfValue(setRowMeta.getValueMeta(i).getName());
    }
    return indexes;
  }

  private void writeMappedRow(Object[] row) throws HopException {
    Object[] setRow = RowDataUtil.allocateRowData(data.setRowMeta.size());
    for (int i = 0; i < data.fieldIndexes.length; i++) {
      int index = data.fieldIndexes[i];
      if (index >= 0 && index < row.length) {
        setRow[i] = row[index];
      }
    }
    data.writer.writeRow(setRow);
  }

  private void closeWriter() throws HopException {
    if (data.writer != null) {
      data.writer.close();
      data.writer = null;
    }
  }

  @Override
  public void dispose() {
    try {
      closeWriter();
    } catch (HopException e) {
      logError("Error closing data set CSV file", e);
    }
    super.dispose();
  }
}
