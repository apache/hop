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

package org.apache.hop.pipeline.transforms.execinfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.JsonRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.execution.Execution;
import org.apache.hop.execution.ExecutionData;
import org.apache.hop.execution.ExecutionDataSetMeta;
import org.apache.hop.execution.ExecutionInfoLocation;
import org.apache.hop.execution.ExecutionState;
import org.apache.hop.execution.ExecutionType;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.json.simple.JSONObject;

/** Execute a process * */
public class ExecInfo extends BaseTransform<ExecInfoMeta, ExecInfoData> {

  private static final Class<?> PKG = ExecInfoMeta.class;
  private boolean killing;
  private CountDownLatch waitForLatch;

  public ExecInfo(
      TransformMeta transformMeta,
      ExecInfoMeta meta,
      ExecInfoData data,
      int copyNr,
      PipelineMeta pipelineMeta,
      Pipeline pipeline) {
    super(transformMeta, meta, data, copyNr, pipelineMeta, pipeline);
  }

  @Override
  public boolean processRow() throws HopException {
    // Get a row until we get null signaling the end of the stream
    //
    Object[] inputRow = getRow();
    if (inputRow == null) {
      setOutputDone();
      return false;
    }

    ExecInfoMeta.OperationType type = meta.getOperationType();

    if (first) {
      first = false;
      // get the RowMeta
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields(data.outputRowMeta, getTransformName(), null, null, this, metadataProvider);

      // Verify that we have the input fields we expect.
      //
      verifyField(resolve(meta.getIdFieldName()), type.isAcceptingExecutionId());
      verifyField(resolve(meta.getParentIdFieldName()), type.isAcceptingParentExecutionId());
      verifyField(resolve(meta.getTypeFieldName()), type.isAcceptingExecutionType());
      verifyField(resolve(meta.getNameFieldName()), type.isAcceptingName());
      verifyField(resolve(meta.getIncludeChildrenFieldName()), type.isAcceptingIncludeChildren());
      verifyField(resolve(meta.getLimitFieldName()), type.isAcceptingLimit());
    }

    switch (type) {
      case GetExecutionIds:
        getExecutionIds(getInputRowMeta(), inputRow);
        break;
      case GetExecutionAndState:
        getExecutionAndState(getInputRowMeta(), inputRow);
        break;
      case FindExecutions:
        findExecutions(getInputRowMeta(), inputRow);
        break;
      case DeleteExecution:
        deleteExecution(getInputRowMeta(), inputRow);
        break;
      case FindPreviousSuccessfulExecution:
        findPreviousSuccessfulExecution(getInputRowMeta(), inputRow);
        break;
      case FindChildIds:
        findChildIds(getInputRowMeta(), inputRow);
        break;
      case FindLastExecution:
        findLastExecution(getInputRowMeta(), inputRow);
        break;
      case FindParentId:
        findParentId(getInputRowMeta(), inputRow);
        break;
      case GetExecutionData:
        getExecutionData(getInputRowMeta(), inputRow);
      default:
        break;
    }

    return true;
  }

  /** Using the children flag and limit we can execute the call to the location. */
  private void getExecutionIds(IRowMeta rowMeta, Object[] row) throws HopException {
    boolean includeChildren = getValueIncludeChildren(rowMeta, row);
    int limit = getValueLimit(rowMeta, row);
    List<String> executionIds =
        data.location.getExecutionInfoLocation().getExecutionIds(includeChildren, limit);
    for (String executionId : executionIds) {
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = rowMeta.size();
      outputRow[index] = executionId;
      putRow(data.outputRowMeta, outputRow);
    }
  }

  private void getExecutionAndState(IRowMeta rowMeta, Object[] row) throws HopException {
    outputExecutionAndState(rowMeta, row, getValueExecutionId(rowMeta, row));
  }

  private void findExecutions(IRowMeta rowMeta, Object[] row) throws HopException {
    String parentId = getValueParentId(rowMeta, row);
    List<Execution> executions = data.location.getExecutionInfoLocation().findExecutions(parentId);
    for (Execution execution : executions) {
      outputExecutionAndState(rowMeta, row, execution.getId());
    }
  }

  private void deleteExecution(IRowMeta rowMeta, Object[] row) throws HopException {
    String executionId = getValueExecutionId(rowMeta, row);
    boolean deleted = data.location.getExecutionInfoLocation().deleteExecution(executionId);
    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    int index = rowMeta.size();
    outputRow[index] = deleted;
    putRow(data.outputRowMeta, outputRow);
  }

  private void findPreviousSuccessfulExecution(IRowMeta rowMeta, Object[] row) throws HopException {
    ExecutionType executionType = getValueExecutionType(rowMeta, row);
    String name = getValueName(rowMeta, row);
    Execution execution =
        data.location
            .getExecutionInfoLocation()
            .findPreviousSuccessfulExecution(executionType, name);
    outputExecutionAndState(rowMeta, row, execution);
  }

  private void findChildIds(IRowMeta rowMeta, Object[] row) throws HopException {
    String parentExecutionId = getValueParentId(rowMeta, row);
    ExecutionType parentExecutionType = getValueExecutionType(rowMeta, row);
    List<String> executionIds =
        data.location
            .getExecutionInfoLocation()
            .findChildIds(parentExecutionType, parentExecutionId);
    for (String executionId : executionIds) {
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = rowMeta.size();
      outputRow[index] = executionId;
      putRow(data.outputRowMeta, outputRow);
    }
  }

  private void findLastExecution(IRowMeta rowMeta, Object[] row) throws HopException {
    ExecutionType executionType = getValueExecutionType(rowMeta, row);
    String name = getValueName(rowMeta, row);
    Execution execution =
        data.location.getExecutionInfoLocation().findLastExecution(executionType, name);
    outputExecutionAndState(rowMeta, row, execution);
  }

  private void findParentId(IRowMeta rowMeta, Object[] row) throws HopException {
    String executionId = getValueExecutionId(rowMeta, row);
    String parentId = data.location.getExecutionInfoLocation().findParentId(executionId);

    if (parentId != null) {
      Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
      int index = rowMeta.size();
      outputRow[index] = parentId;
      putRow(data.outputRowMeta, outputRow);
    }
  }

  private void getExecutionData(IRowMeta rowMeta, Object[] row) throws HopException {
    String parentId = getValueParentId(rowMeta, row);
    String executionId = getValueExecutionId(rowMeta, row);
    ExecutionData executionData =
        this.data.location.getExecutionInfoLocation().getExecutionData(parentId, executionId);
    if (executionData != null) {
      // Output all the data set rows...
      //
      for (String setKey : executionData.getDataSets().keySet()) {
        ExecutionDataSetMeta setMeta = executionData.getSetMetaData().get(setKey);
        RowBuffer rowBuffer = executionData.getDataSets().get(setKey);
        if (setMeta != null && rowBuffer != null) {
          Object[] outputBaseRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
          int baseIndex = rowMeta.size();

          outputBaseRow[baseIndex++] = parentId;
          outputBaseRow[baseIndex++] = executionId;
          outputBaseRow[baseIndex++] = executionData.getCollectionDate();
          outputBaseRow[baseIndex++] = executionData.isFinished();
          outputBaseRow[baseIndex++] = setKey;
          outputBaseRow[baseIndex++] = setMeta.getName();
          outputBaseRow[baseIndex++] = setMeta.getCopyNr();
          outputBaseRow[baseIndex++] = setMeta.getDescription();
          outputBaseRow[baseIndex++] = setMeta.getLogChannelId();
          outputBaseRow[baseIndex++] = JsonRowMeta.toJson(rowBuffer.getRowMeta());

          // Now output all the data set rows. Encode metadata and data in JSON Strings
          //
          long rowNr = 0;
          for (Object[] bufferRow : rowBuffer.getBuffer()) {
            Object[] outputRow =
                RowDataUtil.createResizedCopy(outputBaseRow, data.outputRowMeta.size());
            int index = baseIndex;
            outputRow[index++] = ++rowNr;
            JSONObject jsonRow = new JSONObject();
            for (int v = 0; v < rowBuffer.getRowMeta().size(); v++) {
              IValueMeta valueMeta = rowBuffer.getRowMeta().getValueMeta(v);
              jsonRow.put(valueMeta.getName(), bufferRow[v]);
            }
            outputRow[index] = jsonRow.toJSONString();

            putRow(data.outputRowMeta, outputRow);
          }
        }
      }
    }
  }

  private void outputExecutionAndState(IRowMeta rowMeta, Object[] row, String executionId)
      throws HopException {
    Execution execution = data.location.getExecutionInfoLocation().getExecution(executionId);
    if (execution != null) {
      outputExecutionAndState(rowMeta, row, execution);
    }
  }

  private void outputExecutionAndState(IRowMeta rowMeta, Object[] row, Execution execution)
      throws HopException {
    if (execution == null) {
      return;
    }
    ExecutionState executionState =
        data.location.getExecutionInfoLocation().getExecutionState(execution.getId());

    Object[] outputRow = RowDataUtil.createResizedCopy(row, data.outputRowMeta.size());
    int index = rowMeta.size();

    outputRow[index++] = execution.getId();
    outputRow[index++] = execution.getParentId();
    outputRow[index++] = execution.getName();
    outputRow[index++] = execution.getExecutionType().name();
    outputRow[index++] = execution.getFilename();
    outputRow[index++] = execution.getExecutorXml();
    outputRow[index++] = execution.getMetadataJson();
    outputRow[index++] = execution.getRegistrationDate();
    outputRow[index++] = execution.getExecutionStartDate();
    outputRow[index++] = execution.getRunConfigurationName();
    outputRow[index++] = execution.getLogLevel() == null ? null : execution.getLogLevel().getCode();
    if (executionState != null) {
      outputRow[index++] = executionState.getUpdateTime();
      outputRow[index++] = executionState.getLoggingText();
      outputRow[index++] = executionState.isFailed();
      outputRow[index++] = executionState.getStatusDescription();
      outputRow[index] = executionState.getExecutionEndDate();
    }
    putRow(data.outputRowMeta, outputRow);
  }

  private String getValueExecutionId(IRowMeta rowMeta, Object[] row) throws HopException {
    String idField = resolve(meta.getIdFieldName());
    String executionId = rowMeta.getString(row, idField, "");
    if (StringUtils.isEmpty(executionId)) {
      throw new HopException(
          BaseMessages.getString(PKG, "ExecInfo.Error.PleaseProvideExecutionId", idField));
    }
    return executionId;
  }

  private String getValueParentId(IRowMeta rowMeta, Object[] row) throws HopException {
    String parentIdField = resolve(meta.getParentIdFieldName());
    String parentId = rowMeta.getString(row, parentIdField, "");
    if (StringUtils.isEmpty(parentId)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "ExecInfo.Error.PleaseProvideParentExecutionId", parentIdField));
    }
    return parentId;
  }

  private int getValueLimit(IRowMeta rowMeta, Object[] row) throws HopException {
    long limit = rowMeta.getInteger(row, resolve(meta.getLimitFieldName()), 0L);
    return (int) limit;
  }

  private String getValueName(IRowMeta rowMeta, Object[] row) throws HopException {
    String nameField = resolve(meta.getIdFieldName());
    String name = rowMeta.getString(row, nameField, "");
    if (StringUtils.isEmpty(name)) {
      throw new HopException(
          BaseMessages.getString(PKG, "ExecInfo.Error.PleaseProvideName", nameField));
    }
    return name;
  }

  private ExecutionType getValueExecutionType(IRowMeta rowMeta, Object[] row) throws HopException {
    String executionTypeField = resolve(meta.getTypeFieldName());
    String executionType = rowMeta.getString(row, executionTypeField, "");
    if (StringUtils.isEmpty(executionType)) {
      throw new HopException(
          BaseMessages.getString(
              PKG, "ExecInfo.Error.PleaseProvideExecutionType", executionTypeField));
    }
    return ExecutionType.valueOf(executionType);
  }

  private boolean getValueIncludeChildren(IRowMeta rowMeta, Object[] row) throws HopException {
    return rowMeta.getBoolean(row, resolve(meta.getIncludeChildrenFieldName()), Boolean.FALSE);
  }

  private void verifyField(String fieldName, boolean required) throws HopException {
    if (required && getInputRowMeta().indexOfValue(fieldName) < 0) {
      throw new HopException(
          BaseMessages.getString("ExecInfoMeta.Error.FieldDoesNotExist", fieldName));
    }
  }

  @Override
  public boolean init() {
    // Get a list of remarks.
    // Any negative remark is an error
    //
    List<ICheckResult> remarks = new ArrayList<>();
    meta.check(
        remarks,
        getPipelineMeta(),
        getTransformMeta(),
        null,
        null,
        null,
        null,
        this,
        metadataProvider);
    boolean noRemarks = true;
    for (ICheckResult remark : remarks) {
      if (remark.getType() == ICheckResult.TYPE_RESULT_ERROR) {
        logError(remark.getText());
        noRemarks = false;
      }
    }
    if (!noRemarks) {
      return false;
    }

    // Load the location and initialize it.
    //
    try {
      data.location =
          metadataProvider
              .getSerializer(ExecutionInfoLocation.class)
              .load(resolve(meta.getLocation()));
      data.location.getExecutionInfoLocation().initialize(this, metadataProvider);
    } catch (HopException e) {
      logError("Error initializing execution information location " + meta.getLocation(), e);
      return false;
    }

    return super.init();
  }

  @Override
  public void dispose() {
    if (data.location != null) {
      try {
        data.location.getExecutionInfoLocation().close();
      } catch (Exception e) {
        logError("Error closing location " + data.location.getName(), e);
      }
    }
    super.dispose();
  }
}
