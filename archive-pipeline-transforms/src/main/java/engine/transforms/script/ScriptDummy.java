/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.pipeline.transforms.script;

import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.iVariables;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.transform.BaseTransformData.TransformExecutionStatus;
import org.apache.hop.pipeline.transform.RowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformListener;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaInterface;

import java.util.List;
import java.util.Map;

/**
 * Dummy class used for test().
 */
public class ScriptDummy implements ITransform {
  private IRowMeta inputRowMeta;
  private IRowMeta outputRowMeta;

  public ScriptDummy( IRowMeta inputRowMeta, IRowMeta outputRowMeta ) {
    this.inputRowMeta = inputRowMeta;
    this.outputRowMeta = outputRowMeta;
  }

  public boolean processRow( TransformMetaInterface smi, ITransformData sdi ) throws HopException {
    return false;
  }

  public void addRowListener( RowListener rowListener ) {
  }

  public void dispose( TransformMetaInterface sii, ITransformData sdi ) {
  }

  public long getErrors() {
    return 0;
  }

  public List<RowSet> getInputRowSets() {
    return null;
  }

  public long getLinesInput() {
    return 0;
  }

  public long getLinesOutput() {
    return 0;
  }

  public long getLinesRead() {
    return 0;
  }

  public long getLinesUpdated() {
    return 0;
  }

  public long getLinesWritten() {
    return 0;
  }

  public long getLinesRejected() {
    return 0;
  }

  public List<RowSet> getOutputRowSets() {
    return null;
  }

  public String getPartitionID() {
    return null;
  }

  public Object[] getRow() throws HopException {
    return null;
  }

  public List<RowListener> getRowListeners() {
    return null;
  }

  public String getTransformPluginId() {
    return null;
  }

  public String getTransformName() {
    return null;
  }

  public boolean init( TransformMetaInterface transformMetaInterface, ITransformData iTransformData ) {
    return false;
  }

  public boolean isAlive() {
    return false;
  }

  public boolean isPartitioned() {
    return false;
  }

  public boolean isStopped() {
    return false;
  }

  public void markStart() {
  }

  public void markStop() {
  }

  public void putRow( IRowMeta rowMeta, Object[] row ) throws HopException {
  }

  public void removeRowListener( RowListener rowListener ) {
  }

  public void run() {
  }

  public void setErrors( long errors ) {
  }

  public void setOutputDone() {
  }

  public void setPartitionID( String partitionID ) {
  }

  public void start() {
  }

  public void stopAll() {
  }

  public void stopRunning( TransformMetaInterface transformMetaInterface, ITransformData iTransformData ) throws HopException {
  }

  public void cleanup() {
  }

  public void pauseRunning() {
  }

  public void resumeRunning() {
  }

  public void copyVariablesFrom( iVariables variables ) {
  }

  public String environmentSubstitute( String aString ) {
    return null;
  }

  public String[] environmentSubstitute( String[] string ) {
    return null;
  }

  public String fieldSubstitute( String aString, IRowMeta rowMeta, Object[] rowData ) throws HopValueException {
    return null;
  }

  public boolean getBooleanValueOfVariable( String variableName, boolean defaultValue ) {
    return false;
  }

  public iVariables getParentVariableSpace() {
    return null;
  }

  public void setParentVariableSpace( iVariables parent ) {
  }

  public String getVariable( String variableName, String defaultValue ) {
    return defaultValue;
  }

  public String getVariable( String variableName ) {
    return null;
  }

  public void initializeVariablesFrom( iVariables parent ) {
  }

  public void injectVariables( Map<String, String> prop ) {
  }

  public String[] listVariables() {
    return null;
  }

  public void setVariable( String variableName, String variableValue ) {
  }

  public void shareVariablesWith( iVariables variables ) {
  }

  public IRowMeta getInputRowMeta() {
    return inputRowMeta;
  }

  public IRowMeta getOutputRowMeta() {
    return outputRowMeta;
  }

  public void initBeforeStart() throws HopTransformException {
  }

  public void setLinesRejected( long linesRejected ) {
  }

  public int getCopy() {
    return 0;
  }

  public void addTransformListener( TransformListener transformListener ) {
  }

  public boolean isMapping() {
    return false;
  }

  public TransformMeta getTransformMeta() {
    return null;
  }

  @Override public LogChannelInterface getLogChannel() {
    return null;
  }

  @Override public boolean isSelected() {
    return false;
  }

  public Pipeline getPipeline() {
    return null;
  }

  @Override public String getLogText() {
    return null;
  }

  @Override public String getName() {
    return null;
  }

  @Override public int getCopyNr() {
    return 0;
  }

  @Override public String getLogChannelId() {
    return null;
  }

  public boolean isUsingThreadPriorityManagment() {
    return false;
  }

  public void setUsingThreadPriorityManagment( boolean usingThreadPriorityManagment ) {
  }

  public boolean isRunning() {
    return false;
  }

  public void setRunning( boolean running ) {
    // TODO Auto-generated method stub

  }

  public void setStopped( boolean stopped ) {
    // TODO Auto-generated method stub

  }

  @Override public void setSafeStopped( boolean stopped ) {
    // TODO Auto-generated method stub
  }

  public int rowsetInputSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  public int rowsetOutputSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  public long getProcessed() {
    // TODO Auto-generated method stub
    return 0;
  }

  public Map<String, ResultFile> getResultFiles() {
    // TODO Auto-generated method stub
    return null;
  }

  public long getRuntime() {
    // TODO Auto-generated method stub
    return 0;
  }

  public TransformExecutionStatus getStatus() {
    // TODO Auto-generated method stub
    return null;
  }

  public boolean isPaused() {
    // TODO Auto-generated method stub
    return false;
  }

  public void identifyErrorOutput() {
    // TODO Auto-generated method stub

  }

  public void setPartitioned( boolean partitioned ) {
    // TODO Auto-generated method stub

  }

  public void setRepartitioning( int partitioningMethod ) {
    // TODO Auto-generated method stub

  }

  public boolean canProcessOneRow() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isWaitingForData() {
    // TODO Auto-generated method stub
    return false;
  }

  public void setWaitingForData( boolean waitingForData ) {
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

  public void setPassingData( boolean passingData ) {
    // TODO Auto-generated method stub
  }

  public void batchComplete() throws HopException {
    // TODO Auto-generated method stub
  }

  @Override
  public void setMetaStore( IMetaStore metaStore ) {
    // TODO Auto-generated method stub

  }

  @Override
  public IMetaStore getMetaStore() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getCurrentInputRowSetNr() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCurrentOutputRowSetNr( int index ) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getCurrentOutputRowSetNr() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setCurrentInputRowSetNr( int index ) {
    // TODO Auto-generated method stub

  }
}
