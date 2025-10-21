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

package org.apache.hop.execution;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowBuffer;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.execution.sampler.IExecutionDataSamplerStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

public final class ExecutionDataBuilder {
  public static final String ALL_TRANSFORMS = "all-transforms";
  public static final String KEY_RESULT = "result";
  public static final String KEY_ROWS = "rows";
  public static final String KEY_FILES = "files";
  public static final String KEY_VARIABLES_BEFORE = "variables_before";
  public static final String KEY_VARIABLES_AFTER = "variables_after";
  public static final String RESULT_KEY_RESULT = "result?";
  public static final String RESULT_KEY_ERRORS = "errors";
  public static final String RESULT_KEY_STOPPED = "stopped?";
  public static final String CONST_VALUE = "value";

  private ExecutionType executionType;
  private ExecutionDataSetMeta dataSetMeta;
  private boolean finished;
  private Date collectionDate;
  private String parentId;
  private String ownerId;
  private Map<String, RowBuffer> dataSets;
  private Map<String, ExecutionDataSetMeta> setMetaData;

  private ExecutionDataBuilder() {
    this.collectionDate = new Date();
    this.dataSets = Collections.synchronizedMap(new HashMap<>());
    this.setMetaData = Collections.synchronizedMap(new HashMap<>());
  }

  public static ExecutionDataBuilder of() {
    return new ExecutionDataBuilder();
  }

  public static ExecutionDataBuilder fromAllTransformData(
      IPipelineEngine<PipelineMeta> pipeline,
      Map<String, List<IExecutionDataSamplerStore>> samplerStoresMap,
      boolean finished) {
    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.of()
            .withExecutionType(ExecutionType.Transform)
            .withParentId(pipeline.getLogChannelId())
            .withOwnerId(ALL_TRANSFORMS)
            .withFinished(finished);

    if (samplerStoresMap != null) {
      for (String transformName : samplerStoresMap.keySet()) {
        List<IExecutionDataSamplerStore> samplerStores = samplerStoresMap.get(transformName);
        for (IExecutionDataSamplerStore samplerStore : samplerStores) {
          dataBuilder =
              dataBuilder
                  .addDataSets(samplerStore.getSamples())
                  .addSetMeta(samplerStore.getSamplesMetadata());
        }
      }
    }
    dataBuilder = dataBuilder.withCollectionDate(new Date());
    return dataBuilder;
  }

  public static ExecutionDataBuilder beforeActionExecution(
      IWorkflowEngine<WorkflowMeta> workflow,
      ActionMeta actionMeta,
      IAction action,
      IVariables referenceVariables) {
    String logChannelId = action.getLogChannel().getLogChannelId();

    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.of()
            .withExecutionType(ExecutionType.Action)
            .withParentId(workflow.getLogChannelId())
            .withOwnerId(logChannelId)
            .withDataSetMeta(
                new ExecutionDataSetMeta(
                    logChannelId, logChannelId, actionMeta.getName(), "", "action"))
            .withFinished(false);

    // Add the state of the variables before executing the action
    //
    addBeforeVariables(
        dataBuilder,
        referenceVariables,
        workflow,
        action.getLogChannel().getLogChannelId(),
        action.getName());

    return dataBuilder;
  }

  private static void addBeforeVariables(
      ExecutionDataBuilder dataBuilder,
      IVariables referenceVariables,
      IVariables variables,
      String logChannelId,
      String actionName) {
    IRowMeta rowMeta = new RowMetaBuilder().addString("variable").addString(CONST_VALUE).build();
    RowBuffer rowBuffer = new RowBuffer(rowMeta);

    // Get all the variables but avoid system properties.
    //
    String[] variableNames = referenceVariables.getVariableNames();

    for (String variableName : variables.getVariableNames()) {
      if (Const.indexOfString(variableName, variableNames) >= 0) {
        continue;
      }
      String valueValue = variables.getVariable(variableName);
      rowBuffer.addRow(variableName, valueValue);
    }

    if (!rowBuffer.isEmpty()) {
      String resultKey = KEY_VARIABLES_BEFORE;
      ExecutionDataSetMeta dataSetMeta =
          new ExecutionDataSetMeta(
              resultKey, logChannelId, actionName, "", "Variables before execution");
      dataBuilder.addSetMeta(resultKey, dataSetMeta);
      dataBuilder.addDataSet(resultKey, rowBuffer);
    }
  }

  /**
   * We log to a new file after every executed action. Information about result rows, variables and
   * so on is stored in row buffers.
   *
   * @param workflow The workflow
   * @param actionMeta the action metadata
   * @param action The finished action
   * @param result The result of the action
   * @param referenceVariables The set of reference variables. We can filter out interesting changes
   *     with this and avoid logging a lot of useless information.
   * @return The builder with the information
   */
  public static ExecutionDataBuilder afterActionExecution(
      IWorkflowEngine<WorkflowMeta> workflow,
      ActionMeta actionMeta,
      IAction action,
      Result result,
      IVariables referenceVariables,
      IVariables beforeVariables) {

    String logChannelId = action.getLogChannel().getLogChannelId();

    ExecutionDataBuilder dataBuilder =
        ExecutionDataBuilder.of()
            .withExecutionType(ExecutionType.Action)
            .withParentId(workflow.getLogChannelId())
            .withOwnerId(logChannelId)
            .withDataSetMeta(
                new ExecutionDataSetMeta(
                    logChannelId,
                    logChannelId,
                    actionMeta.getName(),
                    Long.toString(result.getEntryNr()),
                    "action"))
            .withFinished(true);

    // Store all sorts of results : simple key/value pairs
    //
    // The result data
    //
    {
      IRowMeta resultRowMeta = new RowMetaBuilder().addString("key").addString(CONST_VALUE).build();
      RowBuffer resultBuffer = new RowBuffer(resultRowMeta);
      resultBuffer.addRow(RESULT_KEY_RESULT, result.isResult() ? "true" : "false");
      resultBuffer.addRow(RESULT_KEY_ERRORS, Long.toString(result.getNrErrors()));
      resultBuffer.addRow(RESULT_KEY_STOPPED, result.isStopped() ? "true" : "false");

      String resultKey = KEY_RESULT;
      ExecutionDataSetMeta dataSetMeta =
          new ExecutionDataSetMeta(
              resultKey,
              action.getLogChannel().getLogChannelId(),
              actionMeta.getName(),
              Long.toString(result.getEntryNr()),
              "Result details of action");
      dataBuilder.addSetMeta(resultKey, dataSetMeta);
      dataBuilder.addDataSet(resultKey, resultBuffer);
    }

    // Store the result rows...
    //
    if (!Utils.isEmpty(result.getRows())) {
      IRowMeta rowsMeta = result.getRows().get(0).getRowMeta();
      RowBuffer rowsBuffer = new RowBuffer(rowsMeta);
      for (RowMetaAndData rowMetaAndData : result.getRows()) {
        rowsBuffer.addRow(rowMetaAndData.getData());
      }
      String resultKey = KEY_ROWS;
      ExecutionDataSetMeta dataSetMeta =
          new ExecutionDataSetMeta(
              resultKey,
              action.getLogChannel().getLogChannelId(),
              actionMeta.getName(),
              Long.toString(result.getEntryNr()),
              "Result rows of action");
      dataBuilder.addSetMeta(resultKey, dataSetMeta);
      dataBuilder.addDataSet(resultKey, rowsBuffer);
    }

    // The result files
    //
    if (!Utils.isEmpty(result.getResultFiles())) {
      IRowMeta filesMeta =
          new RowMetaBuilder().addString("filename").addString("type").addString("origin").build();
      RowBuffer filesBuffer = new RowBuffer(filesMeta);
      for (ResultFile resultFile : result.getResultFilesList()) {
        filesBuffer.addRow(
            resultFile.getFile().toString(), resultFile.getTypeDesc(), resultFile.getOrigin());
      }
      String resultKey = KEY_FILES;
      ExecutionDataSetMeta dataSetMeta =
          new ExecutionDataSetMeta(
              resultKey,
              action.getLogChannel().getLogChannelId(),
              actionMeta.getName(),
              Long.toString(result.getEntryNr()),
              "Result files of action");
      dataBuilder.addSetMeta(resultKey, dataSetMeta);
      dataBuilder.addDataSet(resultKey, filesBuffer);
    }

    // The variables as well...
    //
    {
      IRowMeta rowMeta = new RowMetaBuilder().addString("variable").addString(CONST_VALUE).build();
      RowBuffer rowBuffer = new RowBuffer(rowMeta);

      // Get all the variables but avoid system properties.
      //
      String[] variableNames = referenceVariables.getVariableNames();

      for (String variableName : workflow.getVariableNames()) {
        if (Const.indexOfString(variableName, variableNames) >= 0) {
          continue;
        }
        String valueValue = workflow.getVariable(variableName);
        rowBuffer.addRow(variableName, valueValue);
      }

      if (!rowBuffer.isEmpty()) {
        String resultKey = KEY_VARIABLES_AFTER;
        ExecutionDataSetMeta dataSetMeta =
            new ExecutionDataSetMeta(
                resultKey,
                action.getLogChannel().getLogChannelId(),
                actionMeta.getName(),
                Long.toString(result.getEntryNr()),
                "Variables after execution");
        dataBuilder.addSetMeta(resultKey, dataSetMeta);
        dataBuilder.addDataSet(resultKey, rowBuffer);
      }
    }

    if (beforeVariables != null) {
      addBeforeVariables(
          dataBuilder,
          referenceVariables,
          beforeVariables,
          action.getLogChannel().getLogChannelId(),
          action.getName());
    }

    return dataBuilder;
  }

  public ExecutionDataBuilder withExecutionType(ExecutionType executionType) {
    this.executionType = executionType;
    return this;
  }

  public ExecutionDataBuilder withDataSetMeta(ExecutionDataSetMeta dataSetMeta) {
    this.dataSetMeta = dataSetMeta;
    return this;
  }

  public ExecutionDataBuilder withFinished(boolean finished) {
    this.finished = finished;
    return this;
  }

  public ExecutionDataBuilder withCollectionDate(Date collectionDate) {
    this.collectionDate = collectionDate;
    return this;
  }

  public ExecutionDataBuilder withParentId(String parentId) {
    this.parentId = parentId;
    return this;
  }

  public ExecutionDataBuilder withOwnerId(String ownerId) {
    this.ownerId = ownerId;
    return this;
  }

  public ExecutionDataBuilder withDataSets(Map<String, RowBuffer> dataSets) {
    this.dataSets = dataSets;
    return this;
  }

  public ExecutionDataBuilder withSetDescriptions(
      Map<String, ExecutionDataSetMeta> setDescriptions) {
    synchronized (setDescriptions) {
      this.setMetaData = setDescriptions;
    }
    return this;
  }

  public ExecutionDataBuilder addDataSets(Map<String, RowBuffer> dataSets) {
    synchronized (dataSets) {
      this.dataSets.putAll(dataSets);
    }
    return this;
  }

  public ExecutionDataBuilder addDataSet(String key, RowBuffer buffer) {
    return addDataSets(Map.of(key, buffer));
  }

  public ExecutionDataBuilder addSetMeta(Map<String, ExecutionDataSetMeta> setDescriptions) {
    this.setMetaData.putAll(setDescriptions);
    return this;
  }

  public ExecutionDataBuilder addSetMeta(String key, ExecutionDataSetMeta setMeta) {
    return addSetMeta(Map.of(key, setMeta));
  }

  public ExecutionData build() {
    ExecutionData executionData = new ExecutionData();
    executionData.setExecutionType(executionType);
    executionData.setDataSetMeta(dataSetMeta);
    executionData.setFinished(finished);
    executionData.setCollectionDate(collectionDate);
    executionData.setParentId(parentId);
    executionData.setOwnerId(ownerId);
    executionData.setDataSets(dataSets);
    executionData.setSetMetaData(setMetaData);
    return executionData;
  }
}
