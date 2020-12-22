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
package org.apache.hop.pipeline.transforms.userdefinedjavaclass;

import org.apache.hop.core.BlockingRowSet;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
//import org.apache.hop.pipeline.transform.TransformListener;
import org.apache.hop.pipeline.transform.ITransformFinishedListener;
import org.apache.hop.pipeline.transform.ITransformStartedListener;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;

import java.util.List;
import java.util.Map;

public class UserDefinedJavaClass extends BaseTransform<UserDefinedJavaClassMeta, UserDefinedJavaClassData> implements ITransform<UserDefinedJavaClassMeta, UserDefinedJavaClassData> {
  private TransformClassBase child;
  public static final String HOP_DEFAULT_CLASS_CACHE_SIZE = "HOP_DEFAULT_CLASS_CACHE_SIZE";

  public UserDefinedJavaClass( TransformMeta transformMeta, UserDefinedJavaClassMeta meta, UserDefinedJavaClassData data, int copyNr,
                               PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    if ( copyNr == 0 ) {
      meta.cookClasses();
    }

    child = meta.newChildInstance( this, meta, data );

    if ( meta.cookErrors.size() > 0 ) {
      for ( Exception e : meta.cookErrors ) {
        logErrorImpl( "Error initializing UserDefinedJavaClass:", e );
      }
      setErrorsImpl( meta.cookErrors.size() );
      stopAllImpl();
    }
  }

  public void addResultFile( ResultFile resultFile ) {
    if ( child == null ) {
      addResultFileImpl( resultFile );
    } else {
      child.addResultFile( resultFile );
    }
  }

  public void addResultFileImpl( ResultFile resultFile ) {
    super.addResultFile( resultFile );
  }

  public void addRowListener( IRowListener rowListener ) {
    if ( child == null ) {
      addRowListenerImpl( rowListener );
    } else {
      child.addRowListener( rowListener );
    }
  }

  public void addRowListenerImpl( IRowListener rowListener ) {
    super.addRowListener( rowListener );
  }

//  public void addTransformListener( TransformListener transformListener ) {
//    if ( child == null ) {
//      addTransformListenerImpl( transformListener );
//    } else {
//      child.addTransformListener( transformListener );
//    }
//  }

//  public void addTransformListenerImpl( TransformListener transformListener ) {
//    super.addTransformListener( transformListener );
//  }

  public boolean checkFeedback( long lines ) {
    if ( child == null ) {
      return checkFeedbackImpl( lines );
    } else {
      return child.checkFeedback( lines );
    }
  }

  public boolean checkFeedbackImpl( long lines ) {
    return super.checkFeedback( lines );
  }

  public void cleanup() {
    if ( child == null ) {
      cleanupImpl();
    } else {
      child.cleanup();
    }
  }

  public void cleanupImpl() {
    super.cleanup();
  }

  public long decrementLinesRead() {
    if ( child == null ) {
      return decrementLinesReadImpl();
    } else {
      return child.decrementLinesRead();
    }
  }

  public long decrementLinesReadImpl() {
    return super.decrementLinesRead();
  }

  public long decrementLinesWritten() {
    if ( child == null ) {
      return decrementLinesWrittenImpl();
    } else {
      return child.decrementLinesWritten();
    }
  }

  public long decrementLinesWrittenImpl() {
    return super.decrementLinesWritten();
  }


  public void disposeImpl( ITransform smi, ITransformData sdi ) {
    super.dispose();
  }

  public IRowSet findInputRowSet( String sourceTransform ) throws HopTransformException {
    if ( child == null ) {
      return findInputRowSetImpl( sourceTransform );
    } else {
      return child.findInputRowSet( sourceTransform );
    }
  }

  public IRowSet findInputRowSet( String from, int fromcopy, String to, int tocopy ) {
    if ( child == null ) {
      return findInputRowSetImpl( from, fromcopy, to, tocopy );
    } else {
      return child.findInputRowSet( from, fromcopy, to, tocopy );
    }
  }

  public IRowSet findInputRowSetImpl( String sourceTransform ) throws HopTransformException {
    return super.findInputRowSet( sourceTransform );
  }

  public IRowSet findInputRowSetImpl( String from, int fromcopy, String to, int tocopy ) {
    return super.findInputRowSet( from, fromcopy, to, tocopy );
  }

  public IRowSet findOutputRowSet( String targetTransform ) throws HopTransformException {
    if ( child == null ) {
      return findOutputRowSetImpl( targetTransform );
    } else {
      return child.findOutputRowSet( targetTransform );
    }
  }

  public IRowSet findOutputRowSet( String from, int fromcopy, String to, int tocopy ) {
    if ( child == null ) {
      return findOutputRowSetImpl( from, fromcopy, to, tocopy );
    } else {
      return child.findOutputRowSet( from, fromcopy, to, tocopy );
    }
  }

  public IRowSet findOutputRowSetImpl( String targetTransform ) throws HopTransformException {
    return super.findOutputRowSet( targetTransform );
  }

  public IRowSet findOutputRowSetImpl( String from, int fromcopy, String to, int tocopy ) {
    return super.findOutputRowSet( from, fromcopy, to, tocopy );
  }

//  public int getClusterSize() {
//    if ( child == null ) {
//      return getClusterSizeImpl();
//    } else {
//      return child.getClusterSize();
//    }
//  }


  public int getCopyImpl() {
    return super.getCopy();
  }

  public IRowMeta getErrorRowMeta() {
    if ( child == null ) {
      return getErrorRowMetaImpl();
    } else {
      return child.getErrorRowMeta();
    }
  }

  public IRowMeta getErrorRowMetaImpl() {
    return super.getErrorRowMeta();
  }

  public long getErrors() {
    if ( child == null ) {
      return getErrorsImpl();
    } else {
      return child.getErrors();
    }
  }

  public long getErrorsImpl() {
    return super.getErrors();
  }

  public IRowMeta getInputRowMeta() {
    if ( child == null ) {
      return getInputRowMetaImpl();
    } else {
      return child.getInputRowMeta();
    }
  }

  public IRowMeta getInputRowMetaImpl() {
    return super.getInputRowMeta();
  }

  public List<IRowSet> getInputRowSets() {
    return child==null ? getInputRowSetsImpl() : child.getInputRowSets();
  }

  public List<IRowSet> getInputRowSetsImpl() {
    return super.getInputRowSets();
  }

  public long getLinesInput() {
    if ( child == null ) {
      return getLinesInputImpl();
    } else {
      return child.getLinesInput();
    }
  }

  public long getLinesInputImpl() {
    return super.getLinesInput();
  }

  public long getLinesOutput() {
    if ( child == null ) {
      return getLinesOutputImpl();
    } else {
      return child.getLinesOutput();
    }
  }

  public long getLinesOutputImpl() {
    return super.getLinesOutput();
  }

  public long getLinesRead() {
    if ( child == null ) {
      return getLinesReadImpl();
    } else {
      return child.getLinesRead();
    }
  }

  public long getLinesReadImpl() {
    return super.getLinesRead();
  }

  public long getLinesRejected() {
    if ( child == null ) {
      return getLinesRejectedImpl();
    } else {
      return child.getLinesRejected();
    }
  }

  public long getLinesRejectedImpl() {
    return super.getLinesRejected();
  }

  public long getLinesSkipped() {
    if ( child == null ) {
      return getLinesSkippedImpl();
    } else {
      return child.getLinesSkipped();
    }
  }

  public long getLinesSkippedImpl() {
    return super.getLinesSkipped();
  }

  public long getLinesUpdated() {
    if ( child == null ) {
      return getLinesUpdatedImpl();
    } else {
      return child.getLinesUpdated();
    }
  }

  public long getLinesUpdatedImpl() {
    return super.getLinesUpdated();
  }

  public long getLinesWritten() {
    if ( child == null ) {
      return getLinesWrittenImpl();
    } else {
      return child.getLinesWritten();
    }
  }

  public long getLinesWrittenImpl() {
    return super.getLinesWritten();
  }

  public List<IRowSet> getOutputRowSets() {
    if ( child == null ) {
      return getOutputRowSetsImpl();
    } else {
      return child.getOutputRowSets();
    }
  }

  public List<IRowSet> getOutputRowSetsImpl() {
    return super.getOutputRowSets();
  }

  public String getPartitionId() {
    if ( child == null ) {
      return getPartitionIdImpl();
    } else {
      return child.getPartitionId();
    }
  }

  public String getPartitionIdImpl() {
    return super.getPartitionId();
  }


  public Map<String, BlockingRowSet> getPartitionTargets() {
    if ( child == null ) {
      return getPartitionTargetsImpl();
    } else {
      return child.getPartitionTargets();
    }
  }

  public Map<String, BlockingRowSet> getPartitionTargetsImpl() {
    return super.getPartitionTargets();
  }

  public long getProcessed() {
    if ( child == null ) {
      return getProcessedImpl();
    } else {
      return child.getProcessed();
    }
  }

  public long getProcessedImpl() {
    return super.getProcessed();
  }

  public int getRepartitioning() {
    if ( child == null ) {
      return getRepartitioningImpl();
    } else {
      return child.getRepartitioning();
    }
  }

  public int getRepartitioningImpl() {
    return super.getRepartitioning();
  }

  public Map<String, ResultFile> getResultFiles() {
    if ( child == null ) {
      return getResultFilesImpl();
    } else {
      return child.getResultFiles();
    }
  }

  public Map<String, ResultFile> getResultFilesImpl() {
    return super.getResultFiles();
  }

  public Object[] getRow() throws HopException {
    if ( child == null ) {
      return getRowImpl();
    } else {
      return child.getRow();
    }
  }

  public Object[] getRowFrom( IRowSet rowSet ) throws HopTransformException {
    if ( child == null ) {
      return getRowFromImpl( rowSet );
    } else {
      return child.getRowFrom( rowSet );
    }
  }

  public Object[] getRowFromImpl( IRowSet rowSet ) throws HopTransformException {
    return super.getRowFrom( rowSet );
  }

  public Object[] getRowImpl() throws HopException {
    return super.getRow();
  }

  public List<IRowListener> getRowListeners() {
    if ( child == null ) {
      return getRowListenersImpl();
    } else {
      return child.getRowListeners();
    }
  }

  public List<IRowListener> getRowListenersImpl() {
    return super.getRowListeners();
  }

//  public long getRuntime() {
//    if ( child == null ) {
//      return getRuntimeImpl();
//    } else {
//      return child.getRuntime();
//    }
//  }

//  public int getServerNr() {
//    if ( child == null ) {
//      return getServerNrImpl();
//    } else {
//      return child.getServerNr();
//    }
//  }

//  public int getServerNrImpl() {
//    if ( child == null ) {
//      return getServerNrImpl();
//    } else {
//      return super.getServerNr();
//    }
//  }

  public ComponentExecutionStatus getStatus() {
    if ( child == null ) {
      return getStatusImpl();
    } else {
      return child.getStatus();
    }
  }

  public String getStatusDescription() {
    if ( child == null ) {
      return getStatusDescriptionImpl();
    } else {
      return child.getStatusDescription();
    }
  }

  public String getStatusDescriptionImpl() {
    return super.getStatusDescription();
  }

  public ComponentExecutionStatus getStatusImpl() {
    return super.getStatus();
  }

  public String getTransformPluginId() {
    if ( child == null ) {
      return getTransformPluginIdImpl();
    } else {
      return child.getTransformPluginId();
    }
  }

  public String getTransformPluginIdImpl() {
    return super.getTransformPluginId();
  }

  public TransformMeta getTransformMeta() {
    if ( child == null ) {
      return getTransformMetaImpl();
    } else {
      return child.getTransformMeta();
    }
  }

  public TransformMeta getTransformMetaImpl() {
    return super.getTransformMeta();
  }

  public String getTransformName() {
    if ( child == null ) {
      return getTransformNameImpl();
    } else {
      return child.getTransformName();
    }
  }

  public String getTransformNameImpl() {
    return super.getTransformName();
  }

  public IPipelineEngine getPipelineImpl() {
    return super.getPipeline();
  }

  public PipelineMeta getPipelineMeta() {
    if ( child == null ) {
      return getPipelineMetaImpl();
    } else {
      return child.getPipelineMeta();
    }
  }

  public PipelineMeta getPipelineMetaImpl() {
    return super.getPipelineMeta();
  }

  public String getVariable( String variableName ) {
    if ( child == null ) {
      return getVariableImpl( variableName );
    } else {
      return child.getVariable( variableName );
    }
  }

  public String getVariable( String variableName, String defaultValue ) {
    if ( child == null ) {
      return getVariableImpl( variableName, defaultValue );
    } else {
      return child.getVariable( variableName, defaultValue );
    }
  }

  public String getVariableImpl( String variableName ) {
    return super.getVariable( variableName );
  }

  public String getVariableImpl( String variableName, String defaultValue ) {
    return super.getVariable( variableName, defaultValue );
  }

  public long incrementLinesInput() {
    if ( child == null ) {
      return incrementLinesInputImpl();
    } else {
      return child.incrementLinesInput();
    }
  }

  public long incrementLinesInputImpl() {
    return super.incrementLinesInput();
  }

  public long incrementLinesOutput() {
    if ( child == null ) {
      return incrementLinesOutputImpl();
    } else {
      return child.incrementLinesOutput();
    }
  }

  public long incrementLinesOutputImpl() {
    return super.incrementLinesOutput();
  }

  public long incrementLinesRead() {
    if ( child == null ) {
      return incrementLinesReadImpl();
    } else {
      return child.incrementLinesRead();
    }
  }

  public long incrementLinesReadImpl() {
    return super.incrementLinesRead();
  }

  public long incrementLinesRejected() {
    if ( child == null ) {
      return incrementLinesRejectedImpl();
    } else {
      return child.incrementLinesRejected();
    }
  }

  public long incrementLinesRejectedImpl() {
    return super.incrementLinesRejected();
  }

  public long incrementLinesSkipped() {
    if ( child == null ) {
      return incrementLinesSkippedImpl();
    } else {
      return child.incrementLinesSkipped();
    }
  }

  public long incrementLinesSkippedImpl() {
    return super.incrementLinesSkipped();
  }

  public long incrementLinesUpdated() {
    if ( child == null ) {
      return incrementLinesUpdatedImpl();
    } else {
      return child.incrementLinesUpdated();
    }
  }

  public long incrementLinesUpdatedImpl() {
    return super.incrementLinesUpdated();
  }

  public long incrementLinesWritten() {
    if ( child == null ) {
      return incrementLinesWrittenImpl();
    } else {
      return child.incrementLinesWritten();
    }
  }

  public long incrementLinesWrittenImpl() {
    return super.incrementLinesWritten();
  }

  public boolean init( ITransform transformMetaInterface, ITransformData iTransformData ) {
    if ( meta.cookErrors.size() > 0 ) {
      return false;
    }

    if ( meta.cookedTransformClass == null ) {
      logError( "No UDFC marked as Pipeline class" );
      return false;
    }

    if ( child == null ) {
      return initImpl( transformMetaInterface, data );
    } else {
      return child.init(transformMetaInterface, data);
    }
  }

  public void initBeforeStart() throws HopTransformException {
    if ( child == null ) {
      initBeforeStartImpl();
    } else {
      child.initBeforeStart();
    }
  }

  public void initBeforeStartImpl() throws HopTransformException {
    super.initBeforeStart();
  }

  public boolean initImpl( ITransform transformMetaInterface, ITransformData iTransformData ) {
    return super.init();
  }

  public boolean isDistributed() {
    if ( child == null ) {
      return isDistributedImpl();
    } else {
      return child.isDistributed();
    }
  }

  public boolean isDistributedImpl() {
    return super.isDistributed();
  }

  public boolean isInitialising() {
    if ( child == null ) {
      return isInitialisingImpl();
    } else {
      return child.isInitialising();
    }
  }

  public boolean isInitialisingImpl() {
    return super.isInitialising();
  }

  public boolean isPartitioned() {
    if ( child == null ) {
      return isPartitionedImpl();
    } else {
      return child.isPartitioned();
    }
  }

  public boolean isPartitionedImpl() {
    return super.isPartitioned();
  }

  public boolean isSafeModeEnabled() {
    if ( child == null ) {
      return isSafeModeEnabledImpl();
    } else {
      return child.isSafeModeEnabled();
    }
  }

  public boolean isSafeModeEnabledImpl() {
    return getPipeline().isSafeModeEnabled();
  }

  public boolean isStopped() {
    if ( child == null ) {
      return isStoppedImpl();
    } else {
      return child.isStopped();
    }
  }

  public boolean isStoppedImpl() {
    return super.isStopped();
  }

//  public boolean isUsingThreadPriorityManagment() {
//    if ( child == null ) {
//      return isUsingThreadPriorityManagmentImpl();
//    } else {
//      return child.isUsingThreadPriorityManagment();
//    }
//  }

//  public boolean isUsingThreadPriorityManagmentImpl() {
//    return super.isUsingThreadPriorityManagment();
//  }

  public void logBasic( String s ) {
    if ( child == null ) {
      logBasicImpl( s );
    } else {
      child.logBasic( s );
    }
  }

  public void logBasicImpl( String s ) {
    super.logBasic( s );
  }

  public void logDebug( String s ) {
    if ( child == null ) {
      logDebugImpl( s );
    } else {
      child.logDebug( s );
    }
  }

  public void logDebugImpl( String s ) {
    super.logDebug( s );
  }

  public void logDetailed( String s ) {
    if ( child == null ) {
      logDetailedImpl( s );
    } else {
      child.logDetailed( s );
    }
  }

  public void logDetailedImpl( String s ) {
    super.logDetailed( s );
  }

  public void logError( String s ) {
    if ( child == null ) {
      logErrorImpl( s );
    } else {
      child.logError( s );
    }
  }

  public void logError( String s, Throwable e ) {
    if ( child == null ) {
      logErrorImpl( s, e );
    } else {
      child.logError( s, e );
    }
  }

  public void logErrorImpl( String s ) {
    super.logError( s );
  }

  public void logErrorImpl( String s, Throwable e ) {
    super.logError( s, e );
  }

  public void logMinimal( String s ) {
    if ( child == null ) {
      logMinimalImpl( s );
    } else {
      child.logMinimal( s );
    }
  }

  public void logMinimalImpl( String s ) {
    super.logMinimal( s );
  }

  public void logRowlevel( String s ) {
    if ( child == null ) {
      logRowlevelImpl( s );
    } else {
      child.logRowlevel( s );
    }
  }

  public void logRowlevelImpl( String s ) {
    super.logRowlevel( s );
  }

  public void logSummary() {
    if ( child == null ) {
      logSummaryImpl();
    } else {
      child.logSummary();
    }
  }

  public void logSummaryImpl() {
    super.logSummary();
  }

  public void markStart() {
    if ( child == null ) {
      markStartImpl();
    } else {
      child.markStart();
    }
  }

  public void markStartImpl() {
    super.markStart();
  }

  public void markStop() {
    if ( child == null ) {
      markStopImpl();
    } else {
      child.markStop();
    }
  }

  public void markStopImpl() {
    super.markStop();
  }

//  public void openRemoteInputTransformSocketsOnce() throws HopTransformException {
//    if ( child == null ) {
//      openRemoteInputTransformSocketsOnceImpl();
//    } else {
//      child.openRemoteInputTransformSocketsOnce();
//    }
//  }

//  public void openRemoteInputTransformSocketsOnceImpl() throws HopTransformException {
//    super.openRemoteInputTransformSocketsOnce();
//  }

//  public void openRemoteOutputTransformSocketsOnce() throws HopTransformException {
//    if ( child == null ) {
//      openRemoteOutputTransformSocketsOnceImpl();
//    } else {
//      child.openRemoteOutputTransformSocketsOnce();
//    }
//  }

//  public void openRemoteOutputTransformSocketsOnceImpl() throws HopTransformException {
//    super.openRemoteOutputTransformSocketsOnce();
//  }

  public boolean outputIsDone() {
    if ( child == null ) {
      return outputIsDoneImpl();
    } else {
      return child.outputIsDone();
    }
  }

  public boolean outputIsDoneImpl() {
    return super.outputIsDone();
  }

  public boolean processRow() throws HopException {
    if ( child == null ) {
      return false;
    } else {
      return child.processRow();
    }
  }

  public void putError( IRowMeta rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                        String fieldNames, String errorCodes ) throws HopTransformException {
    if ( child == null ) {
      putErrorImpl( rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
    } else {
      child.putError( rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
    }
  }

  public void putErrorImpl( IRowMeta rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                            String fieldNames, String errorCodes ) throws HopTransformException {
    super.putError( rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
  }

  public void putRow( IRowMeta row, Object[] data ) throws HopTransformException {
    if ( child == null ) {
      putRowImpl( row, data );
    } else {
      child.putRow( row, data );
    }
  }

  public void putRowImpl( IRowMeta row, Object[] data ) throws HopTransformException {
    super.putRow( row, data );
  }

  public void putRowTo( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {
    if ( child == null ) {
      putRowToImpl( rowMeta, row, rowSet );
    } else {
      child.putRowTo( rowMeta, row, rowSet );
    }
  }

  public void putRowToImpl( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {
    super.putRowTo( rowMeta, row, rowSet );
  }

  public void removeRowListener( IRowListener rowListener ) {
    if ( child == null ) {
      removeRowListenerImpl( rowListener );
    } else {
      child.removeRowListener( rowListener );
    }
  }

  public void removeRowListenerImpl( IRowListener rowListener ) {
    super.removeRowListener( rowListener );
  }

  public int rowsetInputSize() {
    if ( child == null ) {
      return rowsetInputSizeImpl();
    } else {
      return child.rowsetInputSize();
    }
  }

  public int rowsetInputSizeImpl() {
    return super.rowsetInputSize();
  }

  public int rowsetOutputSize() {
    if ( child == null ) {
      return rowsetOutputSizeImpl();
    } else {
      return child.rowsetOutputSize();
    }
  }

  public int rowsetOutputSizeImpl() {
    return super.rowsetOutputSize();
  }

  public void safeModeChecking( IRowMeta row ) throws HopRowException {
    if ( child == null ) {
      safeModeCheckingImpl( row );
    } else {
      child.safeModeChecking( row );
    }
  }

  public void safeModeCheckingImpl( IRowMeta row ) throws HopRowException {
    super.safeModeChecking( row );
  }

  public void setErrors( long errors ) {
    if ( child == null ) {
      setErrorsImpl( errors );
    } else {
      child.setErrors( errors );
    }
  }

  public void setErrorsImpl( long errors ) {
    super.setErrors( errors );
  }

  public void setInputRowMeta( IRowMeta rowMeta ) {
    if ( child == null ) {
      setInputRowMetaImpl( rowMeta );
    } else {
      child.setInputRowMeta( rowMeta );
    }
  }

  public void setInputRowMetaImpl( IRowMeta rowMeta ) {
    super.setInputRowMeta( rowMeta );
  }

  public void setInputRowSets( List<IRowSet> inputRowSets ) {
    if ( child == null ) {
      setInputRowSetsImpl( inputRowSets );
    } else {
      child.setInputRowSets( inputRowSets );
    }
  }

  public void setInputRowSetsImpl( List<IRowSet> inputRowSets ) {
    super.setInputRowSets( inputRowSets );
  }

  public void setLinesInput( long newLinesInputValue ) {
    if ( child == null ) {
      setLinesInputImpl( newLinesInputValue );
    } else {
      child.setLinesInput( newLinesInputValue );
    }
  }

  public void setLinesInputImpl( long newLinesInputValue ) {
    super.setLinesInput( newLinesInputValue );
  }

  public void setLinesOutput( long newLinesOutputValue ) {
    if ( child == null ) {
      setLinesOutputImpl( newLinesOutputValue );
    } else {
      child.setLinesOutput( newLinesOutputValue );
    }
  }

  public void setLinesOutputImpl( long newLinesOutputValue ) {
    super.setLinesOutput( newLinesOutputValue );
  }

  public void setLinesRead( long newLinesReadValue ) {
    if ( child == null ) {
      setLinesReadImpl( newLinesReadValue );
    } else {
      child.setLinesRead( newLinesReadValue );
    }
  }

  public void setLinesReadImpl( long newLinesReadValue ) {
    super.setLinesRead( newLinesReadValue );
  }

  public void setLinesRejected( long linesRejected ) {
    if ( child == null ) {
      setLinesRejectedImpl( linesRejected );
    } else {
      child.setLinesRejected( linesRejected );
    }
  }

  public void setLinesRejectedImpl( long linesRejected ) {
    super.setLinesRejected( linesRejected );
  }

  public void setLinesSkipped( long newLinesSkippedValue ) {
    if ( child == null ) {
      setLinesSkippedImpl( newLinesSkippedValue );
    } else {
      child.setLinesSkipped( newLinesSkippedValue );
    }
  }

  public void setLinesSkippedImpl( long newLinesSkippedValue ) {
    super.setLinesSkipped( newLinesSkippedValue );
  }

  public void setLinesUpdated( long newLinesUpdatedValue ) {
    if ( child == null ) {
      setLinesUpdatedImpl( newLinesUpdatedValue );
    } else {
      child.setLinesUpdated( newLinesUpdatedValue );
    }
  }

  public void setLinesUpdatedImpl( long newLinesUpdatedValue ) {
    super.setLinesUpdated( newLinesUpdatedValue );
  }

  public void setLinesWritten( long newLinesWrittenValue ) {
    if ( child == null ) {
      setLinesWrittenImpl( newLinesWrittenValue );
    } else {
      child.setLinesWritten( newLinesWrittenValue );
    }
  }

  public void setLinesWrittenImpl( long newLinesWrittenValue ) {
    super.setLinesWritten( newLinesWrittenValue );
  }

  public void setOutputDone() {
    if ( child == null ) {
      setOutputDoneImpl();
    } else {
      child.setOutputDone();
    }
  }

  public void setOutputDoneImpl() {
    super.setOutputDone();
  }

  public void setOutputRowSets( List<IRowSet> outputRowSets ) {
    if ( child == null ) {
      setOutputRowSetsImpl( outputRowSets );
    } else {
      child.setOutputRowSets( outputRowSets );
    }
  }

  public void setOutputRowSetsImpl( List<IRowSet> outputRowSets ) {
    super.setOutputRowSets( outputRowSets );
  }

//  public void setTransformListeners( List<TransformListener> transformListeners ) {
//    if ( child == null ) {
//      setTransformListenersImpl( transformListeners );
//    } else {
//      child.setTransformListeners( transformListeners );
//    }
//  }

//  public void setTransformListenersImpl( List<TransformListener> transformListeners ) {
//    super.setTransformListeners( transformListeners );
//  }

  public void setVariable( String variableName, String variableValue ) {
    if ( child == null ) {
      setVariableImpl( variableName, variableValue );
    } else {
      child.setVariable( variableName, variableValue );
    }
  }

  public void setVariableImpl( String variableName, String variableValue ) {
    super.setVariable( variableName, variableValue );
  }

  public void stopAll() {
    if ( child == null ) {
      stopAllImpl();
    } else {
      child.stopAll();
    }
  }

  public void stopAllImpl() {
    super.stopAll();
  }

  public void stopRunning( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException {
    if ( child == null ) {
      stopRunningImpl( transformMetaInterface, data );
    } else {
      child.stopRunning( transformMetaInterface, data );
    }
  }

  public void stopRunningImpl( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException {
    super.stopRunning();
  }

  public String toString() {
    if ( child == null ) {
      return toStringImpl();
    } else {
      return child.toString();
    }
  }

  public String toStringImpl() {
    return super.toString();
  }

}
