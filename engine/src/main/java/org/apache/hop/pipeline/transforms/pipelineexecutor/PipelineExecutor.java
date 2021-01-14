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

package org.apache.hop.pipeline.transforms.pipelineexecutor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.pipeline.engine.PipelineEngineFactory;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.PipelineTransformUtil;
import org.apache.hop.pipeline.transforms.workflowexecutor.WorkflowExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Execute a pipeline for every input row, set parameters.
 * <p>
 * <b>Note:</b><br/>
 * Be aware, logic of the classes methods is very similar to corresponding methods of
 * {@link WorkflowExecutor WorkflowExecutor}.
 * If you change something in this class, consider copying your changes to WorkflowExecutor as well.
 * </p>
 *
 * @author Matt
 * @since 18-mar-2013
 */
public class PipelineExecutor extends BaseTransform<PipelineExecutorMeta, PipelineExecutorData> implements ITransform<PipelineExecutorMeta, PipelineExecutorData> {

  private static final Class<?> PKG = PipelineExecutorMeta.class; // For Translator

  public PipelineExecutor( TransformMeta transformMeta, PipelineExecutorMeta meta, PipelineExecutorData data, int copyNr, PipelineMeta pipelineMeta,
                           Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the pipeline, we
   * look up the MappingInput transform to send our rows to it. As a consequence, for the time being, there can only be one
   * MappingInput and one MappingOutput transform in the PipelineExecutor.
   */
  public boolean processRow() throws HopException {
    try {
      PipelineExecutorData pipelineExecutorData = getData();
      // Wait for a row...
      Object[] row = getRow();

      if ( row == null ) {
        executePipeline( null );
        setOutputDone();
        return false;
      }

      List<String> incomingFieldValues = new ArrayList<>();
      if ( getInputRowMeta() != null ) {
        for ( int i = 0; i < getInputRowMeta().size(); i++ ) {
          String fieldvalue = getInputRowMeta().getString( row, i );
          incomingFieldValues.add( fieldvalue );
        }
      }

      if ( first ) {
        first = false;
        initOnFirstProcessingIteration();
      }

      IRowSet executorTransformOutputRowSet = pipelineExecutorData.getExecutorTransformOutputRowSet();
      if ( pipelineExecutorData.getExecutorTransformOutputRowMeta() != null && executorTransformOutputRowSet != null ) {
        putRowTo( pipelineExecutorData.getExecutorTransformOutputRowMeta(), row, executorTransformOutputRowSet );
      }

      // Grouping by field and execution time works ONLY if grouping by size is disabled.
      if ( pipelineExecutorData.groupSize < 0 ) {
        if ( pipelineExecutorData.groupFieldIndex >= 0 ) { // grouping by field
          Object groupFieldData = row[ pipelineExecutorData.groupFieldIndex ];
          if ( pipelineExecutorData.prevGroupFieldData != null ) {
            if ( pipelineExecutorData.groupFieldMeta.compare( pipelineExecutorData.prevGroupFieldData, groupFieldData ) != 0 ) {
              executePipeline( getLastIncomingFieldValues() );
            }
          }
          pipelineExecutorData.prevGroupFieldData = groupFieldData;
        } else if ( pipelineExecutorData.groupTime > 0 ) { // grouping by execution time
          long now = System.currentTimeMillis();
          if ( now - pipelineExecutorData.groupTimeStart >= pipelineExecutorData.groupTime ) {
            executePipeline( incomingFieldValues );
          }
        }
      }

      // Add next value AFTER pipeline execution, in case we are grouping by field (see PDI-14958),
      // and BEFORE checking size of a group, in case we are grouping by size (see PDI-14121).
      pipelineExecutorData.groupBuffer.add( new RowMetaAndData( getInputRowMeta(), row ) ); // should we clone for safety?

      // Grouping by size.
      // If group buffer size exceeds specified limit, then execute pipeline and flush group buffer.
      if ( pipelineExecutorData.groupSize > 0 ) {
        if ( pipelineExecutorData.groupBuffer.size() >= pipelineExecutorData.groupSize ) {
          executePipeline( incomingFieldValues );
        }
      }

      return true;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "PipelineExecutor.UnexpectedError" ), e );
    }
  }

  private void initOnFirstProcessingIteration() throws HopException {
    PipelineExecutorData pipelineExecutorData = getData();
    // internal pipeline's first transform has exactly the same input
    pipelineExecutorData.setInputRowMeta( getInputRowMeta() );

    // internal pipeline's execution results
    pipelineExecutorData.setExecutionResultsOutputRowMeta( new RowMeta() );
    if ( meta.getExecutionResultTargetTransformMeta() != null ) {
      meta.prepareExecutionResultsFields( pipelineExecutorData.getExecutionResultsOutputRowMeta(),
        meta.getExecutionResultTargetTransformMeta() );
      pipelineExecutorData
        .setExecutionResultRowSet( findOutputRowSet( meta.getExecutionResultTargetTransformMeta().getName() ) );
    }
    // internal pipeline's execution result's file
    pipelineExecutorData.setResultFilesOutputRowMeta( new RowMeta() );
    if ( meta.getResultFilesTargetTransformMeta() != null ) {
      meta.prepareExecutionResultsFileFields( pipelineExecutorData.getResultFilesOutputRowMeta(),
        meta.getResultFilesTargetTransformMeta() );
      pipelineExecutorData.setResultFilesRowSet( findOutputRowSet( meta.getResultFilesTargetTransformMeta().getName() ) );
    }
    // internal pipeline's execution output
    pipelineExecutorData.setResultRowsOutputRowMeta( new RowMeta() );
    if ( meta.getOutputRowsSourceTransformMeta() != null ) {
      meta.prepareResultsRowsFields( pipelineExecutorData.getResultRowsOutputRowMeta() );
      pipelineExecutorData.setResultRowsRowSet( findOutputRowSet( meta.getOutputRowsSourceTransformMeta().getName() ) );
    }

    // executor's self output is exactly its input
    if ( meta.getExecutorsOutputTransformMeta() != null ) {
      pipelineExecutorData.setExecutorTransformOutputRowMeta( getInputRowMeta().clone() );
      pipelineExecutorData.setExecutorTransformOutputRowSet( findOutputRowSet( meta.getExecutorsOutputTransformMeta().getName() ) );
    }

    // Remember which column to group on, if any...
    pipelineExecutorData.groupFieldIndex = -1;
    if ( !Utils.isEmpty( pipelineExecutorData.groupField ) ) {
      pipelineExecutorData.groupFieldIndex = getInputRowMeta().indexOfValue( pipelineExecutorData.groupField );
      if ( pipelineExecutorData.groupFieldIndex < 0 ) {
        throw new HopException( BaseMessages.getString(
          PKG, "PipelineExecutor.Exception.GroupFieldNotFound", pipelineExecutorData.groupField ) );
      }
      pipelineExecutorData.groupFieldMeta = getInputRowMeta().getValueMeta( pipelineExecutorData.groupFieldIndex );
    }
  }

  private void executePipeline( List<String> incomingFieldValues ) throws HopException {
    PipelineExecutorData pipelineExecutorData = getData();
    // If we got 0 rows on input we don't really want to execute the pipeline
    if ( pipelineExecutorData.groupBuffer.isEmpty() ) {
      return;
    }
    pipelineExecutorData.groupTimeStart = System.currentTimeMillis();

    if ( first ) {
      discardLogLines( pipelineExecutorData );
    }

    IPipelineEngine<PipelineMeta> executorPipeline = createInternalPipeline();
    pipelineExecutorData.setExecutorPipeline( executorPipeline );
    if ( incomingFieldValues != null ) {
      // Pass parameter values
      passParametersToPipeline( incomingFieldValues );
    } else {
      List<String> lastIncomingFieldValues = getLastIncomingFieldValues();
      // incomingFieldValues == null-  There are no more rows - Last Case - pass previous values if exists
      // If not still pass the null parameter values
      passParametersToPipeline( lastIncomingFieldValues != null && !lastIncomingFieldValues.isEmpty() ? lastIncomingFieldValues : incomingFieldValues );
    }

    // keep track for drill down in HopGui...
    getPipeline().addActiveSubPipeline( getTransformName(), executorPipeline );

    Result result = new Result();
    result.setRows( pipelineExecutorData.groupBuffer );
    executorPipeline.setPreviousResult( result );

    try {
      executorPipeline.prepareExecution();

      // run pipeline
      executorPipeline.startThreads();

      // Wait a while until we're done with the pipeline
      executorPipeline.waitUntilFinished();

      result = executorPipeline.getResult();
    } catch ( HopException e ) {
      log.logError( "An error occurred executing the pipeline: ", e );
      result.setResult( false );
      result.setNrErrors( 1 );
    }

    collectPipelineResults( result );
    collectExecutionResults( result );
    collectExecutionResultFiles( result );

    pipelineExecutorData.groupBuffer.clear();
  }

  @VisibleForTesting
  void discardLogLines( PipelineExecutorData pipelineExecutorData ) {
    // Keep the strain on the logging back-end conservative.
    // TODO: make this optional/user-defined later
    IPipelineEngine<PipelineMeta> executorPipeline = pipelineExecutorData.getExecutorPipeline();
    if ( executorPipeline != null ) {
      HopLogStore.discardLines( executorPipeline.getLogChannelId(), false );
      LoggingRegistry.getInstance().removeIncludingChildren( executorPipeline.getLogChannelId() );
    }
  }

  @VisibleForTesting
  IPipelineEngine<PipelineMeta> createInternalPipeline() throws HopException {

    String runConfigurationName = resolve( meta.getRunConfigurationName() );
    IPipelineEngine<PipelineMeta> executorPipeline = PipelineEngineFactory.createPipelineEngine( this, runConfigurationName, metadataProvider, getData().getExecutorPipelineMeta() );
    executorPipeline.setParentPipeline( getPipeline() );
    executorPipeline.setParent(this);
    executorPipeline.setLogLevel( getLogLevel() );
    executorPipeline.setInternalHopVariables( this );
    executorPipeline.setPreview( getPipeline().isPreview() );

    PipelineTransformUtil.initServletConfig( getPipeline(), executorPipeline );

    return executorPipeline;
  }

  @VisibleForTesting
  void passParametersToPipeline( List<String> incomingFieldValues ) throws HopException {
    //The values of the incoming fields from the previous transform.
    if ( incomingFieldValues == null ) {
      incomingFieldValues = new ArrayList<>();
    }

    // Set parameters, when fields are used take the first row in the set.
    PipelineExecutorParameters parameters = meta.getParameters();

    // A map where the final parameters and values are stored.
    Map<String, String> resolvingValuesMap = new LinkedHashMap<>();
    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      resolvingValuesMap.put( parameters.getVariable()[ i ], null );
    }

    //The names of the "Fields to use".
    List<String> fieldsToUse = new ArrayList<>();
    if ( parameters.getField() != null ) {
      fieldsToUse = Arrays.asList( parameters.getField() );
    }

    //The names of the incoming fields from the previous transform.
    List<String> incomingFields = new ArrayList<>();
    if ( data.getInputRowMeta() != null ) {
      incomingFields = Arrays.asList( data.getInputRowMeta().getFieldNames() );
    }

    //The values of the "Static input value".
    List<String> staticInputs = Arrays.asList( parameters.getInput() );

    /////////////////////////////////////////////
    // For all parameters declared in pipelineExecutor
    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      String currentVariableToUpdate = (String) resolvingValuesMap.keySet().toArray()[ i ];
      boolean hasIncomingFieldValues = incomingFieldValues != null && !incomingFieldValues.isEmpty();
      try {
        if ( i < fieldsToUse.size() && incomingFields.contains( fieldsToUse.get( i ) ) && hasIncomingFieldValues
          && ( !Utils.isEmpty( Const.trim( incomingFieldValues.get( incomingFields.indexOf( fieldsToUse.get( i ) ) ) ) ) ) ) {
          // if field to use is defined on previous transforms ( incomingFields ) and is not empty - put that value
          resolvingValuesMap.put( currentVariableToUpdate, incomingFieldValues.get( incomingFields.indexOf( fieldsToUse.get( i ) ) ) );
        } else {
          if ( i < staticInputs.size() && !Utils.isEmpty( Const.trim( staticInputs.get( i ) ) ) ) {
            // if we do not have a field to use then check for static input values - if not empty - put that value
            resolvingValuesMap.put( currentVariableToUpdate, staticInputs.get( i ) );
          } else {
            if ( !Utils.isEmpty( Const.trim( fieldsToUse.get( i ) ) ) ) {
              // if both -field to use- and -static values- are empty, then check if it is in fact an empty field cell
              // if not an empty cell then it is a declared variable that was resolved as null by previous transforms
              // put "" value ( not null) and also set pipelineExecutor variable - to force create this variable
              resolvingValuesMap.put( currentVariableToUpdate, "" );
              this.setVariable( parameters.getVariable()[ i ], resolvingValuesMap.get( parameters.getVariable()[ i ] ) );
            } else {
              if ( !Utils.isEmpty( Const.trim( this.getVariable( parameters.getVariable()[ i ] ) ) ) && meta.getParameters().isInheritingAllVariables() ) {
                // if everything is empty, then check for last option - parent variables if isInheriting is checked - if exists - put that value
                resolvingValuesMap.put( currentVariableToUpdate, this.getVariable( parameters.getVariable()[ i ] ) );
              } else {
                // last case - if no variables defined - put "" value ( not null)
                // and also set pipelineExecutor variable - to force create this variable
                resolvingValuesMap.put( currentVariableToUpdate, "" );
                this.setVariable( parameters.getVariable()[ i ], resolvingValuesMap.get( parameters.getVariable()[ i ] ) );
              }
            }
          }
        }
      } catch ( Exception e ) {
        //Set the value to the first parameter in the resolvingValuesMap.
        resolvingValuesMap.put( (String) resolvingValuesMap.keySet().toArray()[ i ], "" );
        this.setVariable( parameters.getVariable()[ i ], resolvingValuesMap.get( parameters.getVariable()[ i ] ) );
      }
    }
    /////////////////////////////////////////////

    //Transform the values of the resolvingValuesMap into a String array "inputFieldValues" to be passed as parameter..
    String[] inputFieldValues = new String[ parameters.getVariable().length ];
    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      inputFieldValues[ i ] = resolvingValuesMap.get( parameters.getVariable()[ i ] );
    }

    IPipelineEngine<PipelineMeta> pipeline = getExecutorPipeline();
    TransformWithMappingMeta.activateParams(
      pipeline,
      pipeline,
      this,
      pipeline.listParameters(),
      parameters.getVariable(),
      inputFieldValues,
      meta.getParameters().isInheritingAllVariables()
    );
  }

  @VisibleForTesting
  void collectPipelineResults( Result result ) throws HopException {
    IRowSet pipelineResultsRowSet = getData().getResultRowsRowSet();
    if ( meta.getOutputRowsSourceTransformMeta() != null && pipelineResultsRowSet != null ) {
      for ( RowMetaAndData metaAndData : result.getRows() ) {
        putRowTo( metaAndData.getRowMeta(), metaAndData.getData(), pipelineResultsRowSet );
      }
    }
  }

  @VisibleForTesting
  void collectExecutionResults( Result result ) throws HopException {
    IRowSet executionResultsRowSet = getData().getExecutionResultRowSet();
    if ( meta.getExecutionResultTargetTransformMeta() != null && executionResultsRowSet != null ) {
      Object[] outputRow = RowDataUtil.allocateRowData( getData().getExecutionResultsOutputRowMeta().size() );
      int idx = 0;

      if ( !Utils.isEmpty( meta.getExecutionTimeField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( System.currentTimeMillis() - getData().groupTimeStart );
      }
      if ( !Utils.isEmpty( meta.getExecutionResultField() ) ) {
        outputRow[ idx++ ] = Boolean.valueOf( result.getResult() );
      }
      if ( !Utils.isEmpty( meta.getExecutionNrErrorsField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrErrors() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesReadField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesRead() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesWrittenField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesWritten() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesInputField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesInput() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesOutputField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesOutput() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesRejectedField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesRejected() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesUpdatedField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesUpdated() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLinesDeletedField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrLinesDeleted() );
      }
      if ( !Utils.isEmpty( meta.getExecutionFilesRetrievedField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getNrFilesRetrieved() );
      }
      if ( !Utils.isEmpty( meta.getExecutionExitStatusField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( result.getExitStatus() );
      }
      if ( !Utils.isEmpty( meta.getExecutionLogTextField() ) ) {
        String channelId = getData().getExecutorPipeline().getLogChannelId();
        String logText = HopLogStore.getAppender().getBuffer( channelId, false ).toString();
        outputRow[ idx++ ] = logText;
      }
      if ( !Utils.isEmpty( meta.getExecutionLogChannelIdField() ) ) {
        outputRow[ idx++ ] = getData().getExecutorPipeline().getLogChannelId();
      }

      putRowTo( getData().getExecutionResultsOutputRowMeta(), outputRow, executionResultsRowSet );
    }
  }

  @VisibleForTesting
  void collectExecutionResultFiles( Result result ) throws HopException {
    IRowSet resultFilesRowSet = getData().getResultFilesRowSet();
    if ( meta.getResultFilesTargetTransformMeta() != null && result.getResultFilesList() != null && resultFilesRowSet != null ) {
      for ( ResultFile resultFile : result.getResultFilesList() ) {
        Object[] targetRow = RowDataUtil.allocateRowData( getData().getResultFilesOutputRowMeta().size() );
        int idx = 0;
        targetRow[ idx++ ] = resultFile.getFile().getName().toString();

        // TODO: time, origin, ...

        putRowTo( getData().getResultFilesOutputRowMeta(), targetRow, resultFilesRowSet );
      }
    }
  }


  public boolean init() {

    PipelineExecutorData pipelineExecutorData = getData();
    if ( super.init() ) {
      // First we need to load the mapping (pipeline)
      try {
        pipelineExecutorData.setExecutorPipelineMeta( loadExecutorPipelineMeta() );

        // Do we have a pipeline at all?
        if ( pipelineExecutorData.getExecutorPipelineMeta() != null ) {
          pipelineExecutorData.groupBuffer = new ArrayList<>();

          // How many rows do we group together for the pipeline?
          if ( !Utils.isEmpty( meta.getGroupSize() ) ) {
            pipelineExecutorData.groupSize = Const.toInt( resolve( meta.getGroupSize() ), -1 );
          } else {
            pipelineExecutorData.groupSize = -1;
          }
          // Is there a grouping time set?
          if ( !Utils.isEmpty( meta.getGroupTime() ) ) {
            pipelineExecutorData.groupTime = Const.toInt( resolve( meta.getGroupTime() ), -1 );
          } else {
            pipelineExecutorData.groupTime = -1;
          }
          pipelineExecutorData.groupTimeStart = System.currentTimeMillis();

          // Is there a grouping field set?
          if ( !Utils.isEmpty( meta.getGroupField() ) ) {
            pipelineExecutorData.groupField = resolve( meta.getGroupField() );
          }
          // That's all for now...
          return true;
        } else {
          logError( "No valid pipeline was specified nor loaded!" );
          return false;
        }
      } catch ( Exception e ) {
        logError( "Unable to load the pipeline executor because of an error : ", e );
      }

    }
    return false;
  }

  @VisibleForTesting
  PipelineMeta loadExecutorPipelineMeta() throws HopException {
    return PipelineExecutorMeta.loadMappingMeta( meta, metadataProvider, this );
  }

  public void dispose(){
    PipelineExecutorData pipelineExecutorData = getData();
    pipelineExecutorData.groupBuffer = null;
    super.dispose();
  }

  public void stopRunning() throws HopException {
    if ( getData().getExecutorPipeline() != null ) {
      getData().getExecutorPipeline().stopAll();
    }
  }

  public void stopAll() {
    // Stop the pipeline execution.
    if ( getData().getExecutorPipeline() != null ) {
      getData().getExecutorPipeline().stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  public IPipelineEngine<PipelineMeta> getExecutorPipeline() {
    return getData().getExecutorPipeline();
  }

  protected List<String> getLastIncomingFieldValues() {
    PipelineExecutorData pipelineExecutorData = getData();
    List<String> lastIncomingFieldValues = new ArrayList<>();
    if ( pipelineExecutorData == null || pipelineExecutorData.groupBuffer.isEmpty() ) {
      return null;
    }

    int lastIncomingFieldIndex = pipelineExecutorData.groupBuffer.size() - 1;
    ArrayList lastGroupBufferData = new ArrayList( Arrays.asList( pipelineExecutorData.groupBuffer.get( lastIncomingFieldIndex ).getData() ) );
    lastGroupBufferData.removeAll( Collections.singleton( null ) );

    for ( int i = 0; i < lastGroupBufferData.size(); i++ ) {
      lastIncomingFieldValues.add( lastGroupBufferData.get( i ).toString() );
    }
    return lastIncomingFieldValues;
  }


}
