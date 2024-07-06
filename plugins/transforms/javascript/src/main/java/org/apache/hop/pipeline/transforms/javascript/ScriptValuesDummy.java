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

import java.awt.event.ComponentListener;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformFinishedListener;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.ITransformStartedListener;
import org.apache.hop.pipeline.transform.TransformMeta;

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
  public void addRowListener(IRowListener rowListener) {
    // Do nothing
  }

  @Override
  public void dispose() {
    // Do nothing
  }

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
  public void setPartitionId(String partitionId) {
    // Do nothing
  }

  @Override
  public String getPartitionId() {
    return null;
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public void markStart() {
    // Do nothing
  }

  @Override
  public void markStop() {
    // Do nothing
  }

  @Override
  public void stopRunning() throws HopException {
    // Do nothing
  }

  @Override
  public void putRow(IRowMeta rowMeta, Object[] row) throws HopException {
    // Do nothing
  }

  @Override
  public void removeRowListener(IRowListener rowListener) {
    // Do nothing
  }

  public void run() {
    // Do nothing
  }

  @Override
  public void setErrors(long errors) {
    // Do nothing
  }

  @Override
  public void setOutputDone() {
    // Do nothing
  }

  public void setPartitionID(String partitionID) {
    // Do nothing
  }

  public void start() {
    // Do nothing
  }

  @Override
  public void stopAll() {
    // Do nothing
  }

  public void stopRunning(ITransform transformMetaInterface, ITransformData iTransformData)
      throws HopException {
    // Do nothing
  }

  @Override
  public void cleanup() {
    // Do nothing
  }

  @Override
  public void pauseRunning() {
    // Do nothing
  }

  @Override
  public void resumeRunning() {
    // Do nothing
  }

  @Override
  public void copyFrom(IVariables variables) {
    // Do nothing
  }

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
  public void setParentVariables(IVariables parent) {
    // Do nothing
  }

  @Override
  public String getVariable(String variableName, String defaultValue) {
    return defaultValue;
  }

  @Override
  public String getVariable(String variableName) {
    return null;
  }

  @Override
  public void initializeFrom(IVariables parent) {
    // Do nothing
  }

  @Override
  public void setVariables(Map<String, String> map) {
    // Do nothing
  }

  @Override
  public String[] getVariableNames() {
    return null;
  }

  @Override
  public void setVariable(String variableName, String variableValue) {
    // Do nothing
  }

  @Override
  public void shareWith(IVariables variables) {
    // Do nothing
  }

  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  @Override
  public void initBeforeStart() throws HopTransformException {
    // Do nothing
  }

  @Override
  public void addTransformFinishedListener(ITransformFinishedListener transformListener) {
    // Do nothing
  }

  @Override
  public void addTransformStartedListener(ITransformStartedListener transformListener) {
    // Do nothing
  }

  @Override
  public void setLinesRejected(long linesRejected) {
    // Do nothing
  }

  @Override
  public int getCopy() {
    return 0;
  }

  public void addTransformListener(ComponentListener transformListener) {
    // Do nothing
  }

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
  public void setLogLevel(LogLevel logLevel) {
    // Do nothing
  }

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
    return false;
  }

  public boolean isUsingThreadPriorityManagment() {
    return false;
  }

  public void setUsingThreadPriorityManagment(boolean usingThreadPriorityManagment) {
    // Do nothing
  }

  @Override
  public void setRunning(boolean running) {
    // Do nothing
  }

  @Override
  public void setStopped(boolean stopped) {
    // Do nothing
  }

  @Override
  public void setSafeStopped(boolean stopped) {
    // Do nothing
  }

  @Override
  public int rowsetInputSize() {
    return 0;
  }

  @Override
  public int rowsetOutputSize() {
    return 0;
  }

  @Override
  public long getProcessed() {
    return 0;
  }

  @Override
  public Map<String, ResultFile> getResultFiles() {
    return null;
  }

  public long getRuntime() {
    return 0;
  }

  @Override
  public EngineComponent.ComponentExecutionStatus getStatus() {
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
    return false;
  }

  @Override
  public void identifyErrorOutput() {
    // Do nothing
  }

  @Override
  public void setPartitioned(boolean partitioned) {
    // Do nothing
  }

  @Override
  public void setRepartitioning(int partitioningMethod) {
    // Do nothing
  }

  @Override
  public boolean canProcessOneRow() {
    return false;
  }

  @Override
  public boolean init() {
    return false;
  }

  public boolean isWaitingForData() {
    return false;
  }

  public void setWaitingForData(boolean waitingForData) {
    // Do nothing
  }

  public boolean isIdle() {
    return false;
  }

  public boolean isPassingData() {
    return false;
  }

  public void setPassingData(boolean passingData) {
    // Do nothing
  }

  @Override
  public void batchComplete() throws HopException {
    // Do nothing
  }

  @Override
  public void startBundle() throws HopException {
    // Do nothing
  }

  @Override
  public void finishBundle() throws HopException {
    // Do nothing
  }

  @Override
  public void setMetadataProvider(IHopMetadataProvider metadataProvider) {
    // Do nothing
  }

  @Override
  public IHopMetadataProvider getMetadataProvider() {
    return null;
  }

  @Override
  public int getCurrentInputRowSetNr() {
    return 0;
  }

  @Override
  public void setCurrentOutputRowSetNr(int index) {
    // Do nothing
  }

  @Override
  public int getCurrentOutputRowSetNr() {
    return 0;
  }

  @Override
  public void setCurrentInputRowSetNr(int index) {
    // Do nothing
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
  public void setInitStartDate(Date initStartDate) {
    // Do nothing
  }

  @Override
  public Date getExecutionStartDate() {
    return null;
  }

  @Override
  public void setExecutionStartDate(Date executionStartDate) {
    // Do nothing
  }

  @Override
  public Date getFirstRowReadDate() {
    return null;
  }

  @Override
  public void setFirstRowReadDate(Date firstRowReadDate) {
    // Do nothing
  }

  @Override
  public Date getLastRowWrittenDate() {
    return null;
  }

  @Override
  public void setLastRowWrittenDate(Date lastRowWrittenDate) {
    // Do nothing
  }

  @Override
  public Date getExecutionEndDate() {
    return null;
  }

  @Override
  public void setExecutionEndDate(Date executionEndDate) {
    // Do nothing
  }

  @Override
  public Map<String, Object> getExtensionDataMap() {
    return Collections.emptyMap();
  }
}
