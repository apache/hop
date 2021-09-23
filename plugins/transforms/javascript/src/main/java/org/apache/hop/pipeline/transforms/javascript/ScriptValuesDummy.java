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

package org.apache.hop.pipeline.transforms.javascript;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent;
import org.apache.hop.pipeline.transform.*;
import java.awt.event.ComponentListener;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/** Dummy class used for test(). */
public class ScriptValuesDummy implements ITransform {
  private IRowMeta inputRowMeta;
  private IRowMeta outputRowMeta;

  public ScriptValuesDummy(IRowMeta inputRowMeta, IRowMeta outputRowMeta) {
    this.inputRowMeta = inputRowMeta;
    this.outputRowMeta = outputRowMeta;
  }

  @Override
  public boolean processRow() throws HopException {
    return false;
  }

  @Override
  public void addRowListener(IRowListener rowListener) {}

  @Override
  public void dispose() {}

  @Override
  public long getErrors() {
    return 0;
  }

  @Override
  public List<IRowSet> getInputRowSets() {
    return null;
  }

  @Override
  public long getLinesInput() {
    return 0;
  }

  @Override
  public long getLinesOutput() {
    return 0;
  }

  @Override
  public long getLinesRead() {
    return 0;
  }

  @Override
  public long getLinesUpdated() {
    return 0;
  }

  @Override
  public String getStatusDescription() {
    return null;
  }

  @Override
  public long getLinesWritten() {
    return 0;
  }

  @Override
  public long getLinesRejected() {
    return 0;
  }

  @Override
  public List<IRowSet> getOutputRowSets() {
    return null;
  }

  public String getPartitionID() {
    return null;
  }

  @Override
  public Object[] getRow() throws HopException {
    return null;
  }

  @Override
  public List<IRowListener> getRowListeners() {
    return null;
  }

  @Override
  public String getTransformPluginId() {
    return null;
  }

  @Override
  public String getTransformName() {
    return null;
  }

  public boolean init(ITransform transformMetaInterface, ITransformData iTransformData) {
    return false;
  }

  public boolean isAlive() {
    return false;
  }

  @Override
  public boolean isPartitioned() {
    return false;
  }

  @Override
  public void setPartitionId(String partitionId) {}

  @Override
  public String getPartitionId() {
    return null;
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void markStart() {}

  @Override
  public void markStop() {}

  @Override
  public void stopRunning() throws HopException {}

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopException {}

  @Override
  public void removeRowListener(IRowListener rowListener) {}

  public void run() {}

  @Override
  public void setErrors(long errors) {}

  @Override
  public void setOutputDone() {}

  public void setPartitionID(String partitionID) {}

  public void start() {}

  @Override
  public void stopAll() {}

  public void stopRunning(ITransform transformMetaInterface, ITransformData iTransformData)
      throws HopException {}

  @Override
  public void cleanup() {}

  @Override
  public void pauseRunning() {}

  @Override
  public void resumeRunning() {}

  @Override
  public void copyFrom(IVariables variables) {}

  @Override
  public String resolve(String aString) {
    return null;
  }

  @Override
  public String[] resolve(String[] string) {
    return null;
  }

  @Override
  public String resolve(String aString, IRowMeta rowMeta, Object[] rowData)
      throws HopValueException {
    return null;
  }

  @Override
  public boolean getVariableBoolean(String variableName, boolean defaultValue) {
    return false;
  }

  @Override
  public IVariables getParentVariables() {
    return null;
  }

  @Override
  public void setParentVariables(IVariables parent) {}

  @Override
  public String getVariable(String variableName, String defaultValue) {
    return defaultValue;
  }

  @Override
  public String getVariable(String variableName) {
    return null;
  }

  @Override
  public void initializeFrom(IVariables parent) {}

  @Override
  public void setVariables(Map<String, String> map) {}

  @Override
  public String[] getVariableNames() {
    return null;
  }

  @Override
  public void setVariable(String variableName, String variableValue) {}

  @Override
  public void shareWith(IVariables variables) {}

  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  @Override
  public void initBeforeStart() throws HopTransformException {}

  @Override
  public void addTransformFinishedListener(ITransformFinishedListener transformListener) {}

  @Override
  public void addTransformStartedListener(ITransformStartedListener transformListener) {}

  @Override
  public void setLinesRejected(long linesRejected) {}

  @Override
  public int getCopy() {
    return 0;
  }

  public void addTransformListener(ComponentListener transformListener) {}

  @Override
  public boolean isMapping() {
    return false;
  }

  @Override
  public TransformMeta getTransformMeta() {
    return null;
  }

  @Override
  public Pipeline getPipeline() {
    return null;
  }

  public PipelineMeta getPipelineMeta() {
    return null;
  }

  @Override
  public ILogChannel getLogChannel() {
    return null;
  }

  @Override
  public String getLogText() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public int getCopyNr() {
    return 0;
  }

  @Override
  public LogLevel getLogLevel() {
    return null;
  }

  @Override
  public void setLogLevel(LogLevel logLevel) {}

  @Override
  public String getLogChannelId() {
    return null;
  }

  @Override
  public boolean isSelected() {
    return false;
  }

  @Override
  public boolean isRunning() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isUsingThreadPriorityManagment() {
    // TODO Auto-generated method stub
    return false;
  }

  public void setUsingThreadPriorityManagment(boolean usingThreadPriorityManagment) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRunning(boolean running) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setStopped(boolean stopped) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setSafeStopped(boolean stopped) {
    // TODO Auto-generated method stub
  }

  @Override
  public int rowsetInputSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int rowsetOutputSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getProcessed() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<String, ResultFile> getResultFiles() {
    // TODO Auto-generated method stub
    return null;
  }

  public long getRuntime() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public EngineComponent.ComponentExecutionStatus getStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getExecutionDuration() {
    return 0;
  }

  @Override
  public long getInputBufferSize() {
    return 0;
  }

  @Override
  public long getOutputBufferSize() {
    return 0;
  }

  @Override
  public boolean isPaused() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void identifyErrorOutput() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setPartitioned(boolean partitioned) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRepartitioning(int partitioningMethod) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean canProcessOneRow() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean init() {
    return false;
  }

  public boolean isWaitingForData() {
    // TODO Auto-generated method stub
    return false;
  }

  public void setWaitingForData(boolean waitingForData) {
    // TODO Auto-generated method stub
  }

  public boolean isIdle() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isPassingData() {
    // TODO Auto-generated method stub
    return false;
  }

  public void setPassingData(boolean passingData) {
    // TODO Auto-generated method stub

  }

  @Override
  public void batchComplete() throws HopException {
    // TODO Auto-generated method stub
  }

  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    // TODO Auto-generated method stub

  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getCurrentInputRowSetNr() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCurrentOutputRowSetNr(int index) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getCurrentOutputRowSetNr() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCurrentInputRowSetNr(int index) {
    // TODO Auto-generated method stub

  }

  @Override
  public ITransformMeta getMeta() {
    return null;
  }

  @Override
  public ITransformData getData() {
    return null;
  }

  @Override
  public Date getInitStartDate() {
    return null;
  }

  @Override
  public void setInitStartDate(Date initStartDate) {}

  @Override
  public Date getExecutionStartDate() {
    return null;
  }

  @Override
  public void setExecutionStartDate(Date executionStartDate) {}

  @Override
  public Date getFirstRowReadDate() {
    return null;
  }

  @Override
  public void setFirstRowReadDate(Date firstRowReadDate) {}

  @Override
  public Date getLastRowWrittenDate() {
    return null;
  }

  @Override
  public void setLastRowWrittenDate(Date lastRowWrittenDate) {}

  @Override
  public Date getExecutionEndDate() {
    return null;
  }

  @Override
  public void setExecutionEndDate(Date executionEndDate) {}

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return Collections.emptyMap();
  }
}
