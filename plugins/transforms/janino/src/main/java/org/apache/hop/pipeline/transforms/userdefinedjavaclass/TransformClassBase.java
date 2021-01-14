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
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.transform.IRowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.TransformIOMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
//import org.apache.hop.pipeline.transform.ITransformListener;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transforms.userdefinedjavaclass.UserDefinedJavaClassMeta.FieldInfo;
import org.apache.hop.pipeline.engine.EngineComponent.ComponentExecutionStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class TransformClassBase {
  private static final Class<?> PKG = UserDefinedJavaClassMeta.class; // For Translator

  protected boolean first = true;
  protected boolean updateRowMeta = true;
  protected UserDefinedJavaClass parent;
  protected UserDefinedJavaClassMeta meta;
  protected UserDefinedJavaClassData data;

  public TransformClassBase( UserDefinedJavaClass parent, UserDefinedJavaClassMeta meta,
                             UserDefinedJavaClassData data ) throws HopTransformException {
    this.parent = parent;
    this.meta = meta;
    this.data = data;

    try {
      data.inputRowMeta = getPipelineMeta().getPrevTransformFields( parent, getTransformMeta() ).clone();
      data.outputRowMeta = getPipelineMeta().getThisTransformFields( parent, getTransformMeta(), null, data.inputRowMeta.clone() );

      data.parameterMap = new HashMap<>();
      for ( UsageParameter par : meta.getUsageParameters() ) {
        if ( par.tag != null && par.value != null ) {
          data.parameterMap.put( par.tag, par.value );
        }
      }

      data.infoMap = new HashMap<>();
      for ( InfoTransformDefinition transformDefinition : meta.getInfoTransformDefinitions() ) {
        if ( transformDefinition.tag != null
          && transformDefinition.transformMeta != null && transformDefinition.transformMeta.getName() != null ) {
          data.infoMap.put( transformDefinition.tag, transformDefinition.transformMeta.getName() );
        }
      }

      data.targetMap = new HashMap<>();
      for ( TargetTransformDefinition transformDefinition : meta.getTargetTransformDefinitions() ) {
        if ( transformDefinition.tag != null
          && transformDefinition.transformMeta != null && transformDefinition.transformMeta.getName() != null ) {
          data.targetMap.put( transformDefinition.tag, transformDefinition.transformMeta.getName() );
        }
      }
    } catch ( HopTransformException e ) {
      e.printStackTrace();
      throw e;
    }
  }

  public void addResultFile( ResultFile resultFile ) {
    parent.addResultFileImpl( resultFile );
  }

  public void addRowListener( IRowListener rowListener ) {
    parent.addRowListenerImpl( rowListener );
  }

//  public void addTransformListener( ITransformListener transformListener ) {
//    parent.addTransformListenerImpl( transformListener );
//  }

  public boolean checkFeedback( long lines ) {
    return parent.checkFeedbackImpl( lines );
  }

  public void cleanup() {
    parent.cleanupImpl();
  }

  public long decrementLinesRead() {
    return parent.decrementLinesReadImpl();
  }

  public long decrementLinesWritten() {
    return parent.decrementLinesWrittenImpl();
  }


  public IRowSet findInputRowSet( String sourceTransform ) throws HopTransformException {
    return parent.findInputRowSetImpl( sourceTransform );
  }

  public IRowSet findInputRowSet( String from, int fromcopy, String to, int tocopy ) {
    return parent.findInputRowSetImpl( from, fromcopy, to, tocopy );
  }

  public IRowSet findOutputRowSet( String targetTransform ) throws HopTransformException {
    return parent.findOutputRowSetImpl( targetTransform );
  }

  public IRowSet findOutputRowSet( String from, int fromcopy, String to, int tocopy ) {
    return parent.findOutputRowSetImpl( from, fromcopy, to, tocopy );
  }

//  public int getClusterSize() {
//    return parent.getClusterSizeImpl();
//  }ITransformListener

  public int getCopy() {
    return parent.getCopyImpl();
  }

  public IRowMeta getErrorRowMeta() {
    return parent.getErrorRowMetaImpl();
  }

  public long getErrors() {
    return parent.getErrorsImpl();
  }

  public IRowMeta getInputRowMeta() {
    return parent.getInputRowMetaImpl();
  }

  public List<IRowSet> getInputRowSets() {
    return parent.getInputRowSetsImpl();
  }

  public long getLinesInput() {
    return parent.getLinesInputImpl();
  }

  public long getLinesOutput() {
    return parent.getLinesOutputImpl();
  }

  public long getLinesRead() {
    return parent.getLinesReadImpl();
  }

  public long getLinesRejected() {
    return parent.getLinesRejectedImpl();
  }

  public long getLinesSkipped() {
    return parent.getLinesSkippedImpl();
  }

  public long getLinesUpdated() {
    return parent.getLinesUpdatedImpl();
  }

  public long getLinesWritten() {
    return parent.getLinesWrittenImpl();
  }

  public List<IRowSet> getOutputRowSets() {
    return parent.getOutputRowSetsImpl();
  }

  public String getPartitionId() {
    return parent.getPartitionId();
  }

  public Map<String, BlockingRowSet> getPartitionTargets() {
    return parent.getPartitionTargetsImpl();
  }

  public long getProcessed() {
    return parent.getProcessedImpl();
  }

  public int getRepartitioning() {
    return parent.getRepartitioningImpl();
  }

  public Map<String, ResultFile> getResultFiles() {
    return parent.getResultFilesImpl();
  }

  public Object[] getRow() throws HopException {
    Object[] row = parent.getRowImpl();

    if ( updateRowMeta ) {
      // Update data.inputRowMeta and data.outputRowMeta
      IRowMeta inputRowMeta = parent.getInputRowMeta();
      data.inputRowMeta = inputRowMeta;
      data.outputRowMeta =
        inputRowMeta == null ? null : getPipelineMeta().getThisTransformFields(
          parent, getTransformMeta(), null, inputRowMeta.clone() );
      updateRowMeta = false;
    }

    return row;
  }

  public Object[] getRowFrom( IRowSet rowSet ) throws HopTransformException {
    return parent.getRowFromImpl( rowSet );
  }

  public List<IRowListener> getRowListeners() {
    return parent.getRowListenersImpl();
  }

  public ComponentExecutionStatus getStatus() {
    return parent.getStatusImpl();
  }

  public String getStatusDescription() {
    return parent.getStatusDescriptionImpl();
  }

  public String getTransformPluginId() {
    return parent.getTransformPluginIdImpl();
  }

  public TransformMeta getTransformMeta() {
    return parent.getTransformMetaImpl();
  }

  public String getTransformName() {
    return parent.getTransformNameImpl();
  }

  public IPipelineEngine getPipeline() {
    return parent.getPipelineImpl();
  }

  public PipelineMeta getPipelineMeta() {
    return parent.getPipelineMetaImpl();
  }

  public String getVariable( String variableName ) {
    return parent.getVariableImpl( variableName );
  }

  public String getVariable( String variableName, String defaultValue ) {
    return parent.getVariableImpl( variableName, defaultValue );
  }

  public long incrementLinesInput() {
    return parent.incrementLinesInputImpl();
  }

  public long incrementLinesOutput() {
    return parent.incrementLinesOutputImpl();
  }

  public long incrementLinesRead() {
    return parent.incrementLinesReadImpl();
  }

  public long incrementLinesRejected() {
    return parent.incrementLinesRejectedImpl();
  }

  public long incrementLinesSkipped() {
    return parent.incrementLinesSkippedImpl();
  }

  public long incrementLinesUpdated() {
    return parent.incrementLinesUpdatedImpl();
  }

  public long incrementLinesWritten() {
    return parent.incrementLinesWrittenImpl();
  }

  public boolean init( ITransform transformMetaInterface, ITransformData iTransformData ) {
    return parent.initImpl( transformMetaInterface, data );
  }

  public void initBeforeStart() throws HopTransformException {
    parent.initBeforeStartImpl();
  }

  public boolean isDistributed() {
    return parent.isDistributedImpl();
  }

  public boolean isInitialising() {
    return parent.isInitialisingImpl();
  }

  public boolean isPartitioned() {
    return parent.isPartitionedImpl();
  }

  public boolean isSafeModeEnabled() {
    return parent.isSafeModeEnabledImpl();
  }

  public boolean isStopped() {
    return parent.isStoppedImpl();
  }

//  public boolean isUsingThreadPriorityManagment() {
//    return parent.isUsingThreadPriorityManagmentImpl();
//  }

  public void logBasic( String s ) {
    parent.logBasicImpl( s );
  }

  public void logDebug( String s ) {
    parent.logDebugImpl( s );
  }

  public void logDetailed( String s ) {
    parent.logDetailedImpl( s );
  }

  public void logError( String s ) {
    parent.logErrorImpl( s );
  }

  public void logError( String s, Throwable e ) {
    parent.logErrorImpl( s, e );
  }

  public void logMinimal( String s ) {
    parent.logMinimalImpl( s );
  }

  public void logRowlevel( String s ) {
    parent.logRowlevelImpl( s );
  }

  public void logSummary() {
    parent.logSummaryImpl();
  }

  public void markStart() {
    parent.markStartImpl();
  }

  public void markStop() {
    parent.markStopImpl();
  }

  public boolean outputIsDone() {
    return parent.outputIsDoneImpl();
  }

  public abstract boolean processRow() throws HopException;

  public void putError( IRowMeta rowMeta, Object[] row, long nrErrors, String errorDescriptions,
                        String fieldNames, String errorCodes ) throws HopTransformException {
    parent.putErrorImpl( rowMeta, row, nrErrors, errorDescriptions, fieldNames, errorCodes );
  }

  public void putRow( IRowMeta row, Object[] data ) throws HopTransformException {
    parent.putRowImpl( row, data );
  }

  public void putRowTo( IRowMeta rowMeta, Object[] row, IRowSet rowSet ) throws HopTransformException {
    parent.putRowToImpl( rowMeta, row, rowSet );
  }

  public void removeRowListener( IRowListener rowListener ) {
    parent.removeRowListenerImpl( rowListener );
  }

  public int rowsetInputSize() {
    return parent.rowsetInputSizeImpl();
  }

  public int rowsetOutputSize() {
    return parent.rowsetOutputSizeImpl();
  }

  public void safeModeChecking( IRowMeta row ) throws HopRowException {
    parent.safeModeCheckingImpl( row );
  }

  public void setErrors( long errors ) {
    parent.setErrorsImpl( errors );
  }

  public void setInputRowMeta( IRowMeta rowMeta ) {
    parent.setInputRowMetaImpl( rowMeta );
  }

  public void setInputRowSets( List<IRowSet> inputRowSets ) {
    parent.setInputRowSetsImpl( inputRowSets );
  }

  public void setLinesInput( long newLinesInputValue ) {
    parent.setLinesInputImpl( newLinesInputValue );
  }

  public void setLinesOutput( long newLinesOutputValue ) {
    parent.setLinesOutputImpl( newLinesOutputValue );
  }

  public void setLinesRead( long newLinesReadValue ) {
    parent.setLinesReadImpl( newLinesReadValue );
  }

  public void setLinesRejected( long linesRejected ) {
    parent.setLinesRejectedImpl( linesRejected );
  }

  public void setLinesSkipped( long newLinesSkippedValue ) {
    parent.setLinesSkippedImpl( newLinesSkippedValue );
  }

  public void setLinesUpdated( long newLinesUpdatedValue ) {
    parent.setLinesUpdatedImpl( newLinesUpdatedValue );
  }

  public void setLinesWritten( long newLinesWrittenValue ) {
    parent.setLinesWrittenImpl( newLinesWrittenValue );
  }

  public void setOutputDone() {
    parent.setOutputDoneImpl();
  }

  public void setOutputRowSets( List<IRowSet> outputRowSets ) {
    parent.setOutputRowSetsImpl( outputRowSets );
  }

  public void setVariable( String variableName, String variableValue ) {
    parent.setVariableImpl( variableName, variableValue );
  }

  public void stopAll() {
    parent.stopAllImpl();
  }

  public void stopRunning( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException {
    parent.stopRunningImpl( transformMetaInterface, data );
  }

  public String toString() {
    return parent.toStringImpl();
  }

  public static String[] getInfoTransforms() {
    return null;
  }

  @SuppressWarnings( "unchecked" )
  public static void getFields( boolean clearResultFields, IRowMeta row, String originTransformName,
                                IRowMeta[] info, TransformMeta nextTransform, IVariables variables, List<?> fields ) throws HopTransformException {
    if ( clearResultFields ) {
      row.clear();
    }
    for ( FieldInfo fi : (List<FieldInfo>) fields ) {
      try {
        IValueMeta v = ValueMetaFactory.createValueMeta( fi.name, fi.type );
        v.setLength( fi.length );
        v.setPrecision( fi.precision );
        v.setOrigin( originTransformName );
        row.addValueMeta( v );
      } catch ( Exception e ) {
        throw new HopTransformException( e );
      }
    }
  }

  public static ITransformIOMeta getTransformIOMeta( UserDefinedJavaClassMeta meta ) {
    ITransformIOMeta ioMeta = new TransformIOMeta( true, true, true, false, true, true );

    for ( InfoTransformDefinition transformDefinition : meta.getInfoTransformDefinitions() ) {
      ioMeta.addStream( new Stream(
        StreamType.INFO, transformDefinition.transformMeta, transformDefinition.description, StreamIcon.INFO, null ) );
    }
    for ( TargetTransformDefinition transformDefinition : meta.getTargetTransformDefinitions() ) {
      ioMeta.addStream( new Stream(
        StreamType.TARGET, transformDefinition.transformMeta, transformDefinition.description, StreamIcon.TARGET, null ) );
    }

    return ioMeta;
  }

  public String getParameter( String tag ) {
    if ( tag == null ) {
      return null;
    }
    return parent.resolve( data.parameterMap.get( tag ) );
  }

  public IRowSet findInfoRowSet( String tag ) throws HopException {
    if ( tag == null ) {
      return null;
    }
    String transformName = data.infoMap.get( tag );
    if ( Utils.isEmpty( transformName ) ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransformClassBase.Exception.UnableToFindInfoTransformNameForTag", tag ) );
    }
    IRowSet rowSet = findInputRowSet( transformName );
    if ( rowSet == null ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransformClassBase.Exception.UnableToFindInfoRowSetForTransform", transformName ) );
    }
    return rowSet;
  }

  public IRowSet findTargetRowSet( String tag ) throws HopException {
    if ( tag == null ) {
      return null;
    }
    String transformName = data.targetMap.get( tag );
    if ( Utils.isEmpty( transformName ) ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransformClassBase.Exception.UnableToFindTargetTransformNameForTag", tag ) );
    }
    IRowSet rowSet = findOutputRowSet( transformName );
    if ( rowSet == null ) {
      throw new HopException( BaseMessages.getString(
        PKG, "TransformClassBase.Exception.UnableToFindTargetRowSetForTransform", transformName ) );
    }
    return rowSet;
  }

  private final Map<String, FieldHelper> inFieldHelpers = new HashMap<String, FieldHelper>();
  private final Map<String, FieldHelper> infoFieldHelpers = new HashMap<String, FieldHelper>();
  private final Map<String, FieldHelper> outFieldHelpers = new HashMap<String, FieldHelper>();

  public enum Fields {
    In, Out, Info;
  }

  public FieldHelper get( Fields type, String name ) throws HopTransformException {
    FieldHelper fh;
    switch ( type ) {
      case In:
        fh = inFieldHelpers.get( name );
        if ( fh == null ) {
          try {
            fh = new FieldHelper( data.inputRowMeta, name );
          } catch ( IllegalArgumentException e ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TransformClassBase.Exception.UnableToFindFieldHelper", type.name(), name ) );
          }
          inFieldHelpers.put( name, fh );
        }
        break;
      case Out:
        fh = outFieldHelpers.get( name );
        if ( fh == null ) {
          try {
            fh = new FieldHelper( data.outputRowMeta, name );
          } catch ( IllegalArgumentException e ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TransformClassBase.Exception.UnableToFindFieldHelper", type.name(), name ) );
          }
          outFieldHelpers.put( name, fh );
        }
        break;
      case Info:
        fh = infoFieldHelpers.get( name );
        if ( fh == null ) {
          IRowMeta rmi = getPipelineMeta().getPrevInfoFields( parent, getTransformName() );
          try {
            fh = new FieldHelper( rmi, name );
          } catch ( IllegalArgumentException e ) {
            throw new HopTransformException( BaseMessages.getString(
              PKG, "TransformClassBase.Exception.UnableToFindFieldHelper", type.name(), name ) );
          }
          infoFieldHelpers.put( name, fh );
        }
        break;
      default:
        throw new HopTransformException( BaseMessages.getString(
          PKG, "TransformClassBase.Exception.InvalidFieldsType", type.name(), name ) );
    }
    return fh;
  }

  public Object[] createOutputRow( Object[] inputRow, int outputRowSize ) {
    if ( meta.isClearingResultFields() ) {
      return RowDataUtil.allocateRowData( outputRowSize );
    } else {
      return RowDataUtil.createResizedCopy( inputRow, outputRowSize );
    }
  }

  /**
   * Gets parent
   *
   * @return value of parent
   */
  public UserDefinedJavaClass getParent() {
    return parent;
  }

  /**
   * Gets meta
   *
   * @return value of meta
   */
  public UserDefinedJavaClassMeta getMeta() {
    return meta;
  }

  /**
   * Gets data
   *
   * @return value of data
   */
  public UserDefinedJavaClassData getData() {
    return data;
  }
}
