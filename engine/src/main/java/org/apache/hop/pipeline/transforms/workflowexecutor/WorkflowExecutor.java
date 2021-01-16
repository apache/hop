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

package org.apache.hop.pipeline.transforms.workflowexecutor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingRegistry;
import org.apache.hop.core.parameters.UnknownParamException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutor;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engine.WorkflowEngineFactory;

import java.util.ArrayList;

/**
 * Execute a workflow for every input row.
 * <p>
 * <b>Note:</b><br/>
 * Be aware, logic of the classes methods is very similar to corresponding methods of
 * {@link PipelineExecutor PipelineExecutor}.
 * If you change something in this class, consider copying your changes to PipelineExecutor as well.
 * </p>
 *
 * @author Matt
 * @since 22-nov-2005
 */
public class WorkflowExecutor extends BaseTransform<WorkflowExecutorMeta, WorkflowExecutorData> implements ITransform<WorkflowExecutorMeta, WorkflowExecutorData> {

  private static final Class<?> PKG = WorkflowExecutorMeta.class; // For Translator

  public WorkflowExecutor( TransformMeta transformMeta, WorkflowExecutorMeta meta, WorkflowExecutorData data, int copyNr, PipelineMeta pipelineMeta,
                           Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the pipeline, we
   * look up the MappingInput transform to send our rows to it. As a consequence, for the time being, there can only be one
   * MappingInput and one MappingOutput transform in the WorkflowExecutor.
   */
  @Override
  public boolean processRow() throws HopException {
    try {
      // Wait for a row...
      //
      Object[] row = getRow();

      if ( row == null ) {
        if ( !data.groupBuffer.isEmpty() ) {
          executeWorkflow();
        }
        setOutputDone();
        return false;
      }

      if ( first ) {
        first = false;

        // calculate the various output row layouts first...
        //
        data.inputRowMeta = getInputRowMeta();
        data.executionResultsOutputRowMeta = data.inputRowMeta.clone();
        data.resultRowsOutputRowMeta = data.inputRowMeta.clone();
        data.resultFilesOutputRowMeta = data.inputRowMeta.clone();

        if ( meta.getExecutionResultTargetTransformMeta() != null ) {
          meta.getFields( data.executionResultsOutputRowMeta, getTransformName(), null, meta
            .getExecutionResultTargetTransformMeta(), this, metadataProvider );
          data.executionResultRowSet = findOutputRowSet( meta.getExecutionResultTargetTransformMeta().getName() );
        }
        if ( meta.getResultRowsTargetTransformMeta() != null ) {
          meta.getFields(
            data.resultRowsOutputRowMeta, getTransformName(), null, meta.getResultRowsTargetTransformMeta(), this,
            metadataProvider );
          data.resultRowsRowSet = findOutputRowSet( meta.getResultRowsTargetTransformMeta().getName() );
        }
        if ( meta.getResultFilesTargetTransformMeta() != null ) {
          meta.getFields(
            data.resultFilesOutputRowMeta, getTransformName(), null, meta.getResultFilesTargetTransformMeta(), this,
            metadataProvider );
          data.resultFilesRowSet = findOutputRowSet( meta.getResultFilesTargetTransformMeta().getName() );
        }

        // Remember which column to group on, if any...
        //
        data.groupFieldIndex = -1;
        if ( !Utils.isEmpty( data.groupField ) ) {
          data.groupFieldIndex = getInputRowMeta().indexOfValue( data.groupField );
          if ( data.groupFieldIndex < 0 ) {
            throw new HopException( BaseMessages.getString(
              PKG, "JobExecutor.Exception.GroupFieldNotFound", data.groupField ) );
          }
          data.groupFieldMeta = getInputRowMeta().getValueMeta( data.groupFieldIndex );
        }
      }

      // Grouping by field and execution time works ONLY if grouping by size is disabled.
      if ( data.groupSize < 0 ) {
        if ( data.groupFieldIndex >= 0 ) { // grouping by field
          Object groupFieldData = row[ data.groupFieldIndex ];
          if ( data.prevGroupFieldData != null ) {
            if ( data.groupFieldMeta.compare( data.prevGroupFieldData, groupFieldData ) != 0 ) {
              executeWorkflow();
            }
          }
          data.prevGroupFieldData = groupFieldData;
        } else if ( data.groupTime > 0 ) { // grouping by execution time
          long now = System.currentTimeMillis();
          if ( now - data.groupTimeStart >= data.groupTime ) {
            executeWorkflow();
          }
        }
      }

      // Add next value AFTER workflow execution, in case we are grouping by field (see PDI-14958),
      // and BEFORE checking size of a group, in case we are grouping by size (see PDI-14121).
      data.groupBuffer.add( new RowMetaAndData( getInputRowMeta(), row ) ); // should we clone for safety?

      // Grouping by size.
      // If group buffer size exceeds specified limit, then execute workflow and flush group buffer.
      if ( data.groupSize > 0 ) {
        // Pass all input rows...
        if ( data.groupBuffer.size() >= data.groupSize ) {
          executeWorkflow();
        }
      }

      return true;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobExecutor.UnexpectedError" ), e );
    }
  }

  private void executeWorkflow() throws HopException {

    // If we got 0 rows on input we don't really want to execute the workflow
    //
    if ( data.groupBuffer.isEmpty() ) {
      return;
    }

    data.groupTimeStart = System.currentTimeMillis();

    if ( first ) {
      discardLogLines( data );
    }

    data.executorWorkflow = createWorkflow( data.executorWorkflowMeta, this );

    data.executorWorkflow.initializeFrom( this );
    data.executorWorkflow.setParentPipeline( getPipeline() );
    data.executorWorkflow.setLogLevel( getLogLevel() );
    data.executorWorkflow.setInternalHopVariables();

    // Copy the parameters
    data.executorWorkflow.copyParametersFromDefinitions( data.executorWorkflowMeta );

    // data.executorWorkflow.setInteractive(); TODO: pass interactivity through the pipeline too for drill-down.

    // TODO
    /*
     * if (data.executorWorkflow.isInteractive()) {
     * data.executorWorkflow.getJobEntryListeners().addAll(parentWorkflow.getJobEntryListeners()); }
     */

    // Pass the accumulated rows
    //
    data.executorWorkflow.setSourceRows( data.groupBuffer );

    // Pass parameter values
    //
    passParametersToWorkflow();

    // keep track for drill down in HopGui...
    //
    getPipeline().addActiveSubWorkflow( getTransformName(), data.executorWorkflow );

    ExtensionPointHandler.callExtensionPoint( log, this, HopExtensionPoint.WorkflowStart.id, data.executorWorkflow );

    Result result = data.executorWorkflow.startExecution();

    // First the natural output...
    //
    if ( meta.getExecutionResultTargetTransformMeta() != null ) {
      Object[] outputRow = RowDataUtil.allocateRowData( data.executionResultsOutputRowMeta.size() );
      int idx = 0;

      if ( !Utils.isEmpty( meta.getExecutionTimeField() ) ) {
        outputRow[ idx++ ] = Long.valueOf( System.currentTimeMillis() - data.groupTimeStart );
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
        String channelId = data.executorWorkflow.getLogChannelId();
        String logText = HopLogStore.getAppender().getBuffer( channelId, false ).toString();
        outputRow[ idx++ ] = logText;
      }
      if ( !Utils.isEmpty( meta.getExecutionLogChannelIdField() ) ) {
        outputRow[ idx++ ] = data.executorWorkflow.getLogChannelId();
      }

      putRowTo( data.executionResultsOutputRowMeta, outputRow, data.executionResultRowSet );
    }

    // Optionally also send the result rows to a specified target transform...
    //
    if ( meta.getResultRowsTargetTransformMeta() != null && result.getRows() != null ) {
      for ( RowMetaAndData row : result.getRows() ) {

        Object[] targetRow = RowDataUtil.allocateRowData( data.resultRowsOutputRowMeta.size() );

        for ( int i = 0; i < meta.getResultRowsField().length; i++ ) {
          IValueMeta valueMeta = row.getRowMeta().getValueMeta( i );
          if ( valueMeta.getType() != meta.getResultRowsType()[ i ] ) {
            throw new HopException( BaseMessages.getString(
              PKG, "JobExecutor.IncorrectDataTypePassed", valueMeta.getTypeDesc(),
              ValueMetaFactory.getValueMetaName( meta.getResultRowsType()[ i ] ) ) );
          }

          targetRow[ i ] = row.getData()[ i ];
        }
        putRowTo( data.resultRowsOutputRowMeta, targetRow, data.resultRowsRowSet );
      }
    }

    if ( meta.getResultFilesTargetTransformMeta() != null && result.getResultFilesList() != null ) {
      for ( ResultFile resultFile : result.getResultFilesList() ) {
        Object[] targetRow = RowDataUtil.allocateRowData( data.resultFilesOutputRowMeta.size() );
        int idx = 0;
        targetRow[ idx++ ] = resultFile.getFile().getName().toString();

        // TODO: time, origin, ...

        putRowTo( data.resultFilesOutputRowMeta, targetRow, data.resultFilesRowSet );
      }
    }

    data.groupBuffer.clear();
  }

  @VisibleForTesting
  IWorkflowEngine<WorkflowMeta> createWorkflow( WorkflowMeta workflowMeta, ILoggingObject parentLogging ) throws HopException {

    return WorkflowEngineFactory.createWorkflowEngine( this, resolve(meta.getRunConfigurationName()), metadataProvider, workflowMeta, parentLogging );
  }

  @VisibleForTesting
  void discardLogLines( WorkflowExecutorData data ) {
    // Keep the strain on the logging back-end conservative.
    // TODO: make this optional/user-defined later
    if ( data.executorWorkflow != null ) {
      HopLogStore.discardLines( data.executorWorkflow.getLogChannelId(), false );
      LoggingRegistry.getInstance().removeIncludingChildren( data.executorWorkflow.getLogChannelId() );
    }
  }

  private void passParametersToWorkflow() throws HopException {
    // Set parameters, when fields are used take the first row in the set.
    //
    WorkflowExecutorParameters parameters = meta.getParameters();

    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      String variableName = parameters.getVariable()[i];
      String variableInput = parameters.getInput()[i];
      String fieldName = parameters.getField()[ i ];
      String variableValue = null;
      if (StringUtils.isNotEmpty( variableName )) {
        // The value is provided by a field in an input row
        //
        if ( StringUtils.isNotEmpty( fieldName ) ) {
          int idx = getInputRowMeta().indexOfValue( fieldName );
          if ( idx < 0 ) {
            throw new HopException( BaseMessages.getString(
              PKG, "JobExecutor.Exception.UnableToFindField", fieldName ) );
          }
          variableValue = data.groupBuffer.get( 0 ).getString( idx, "" );
        } else {
          // The value is provided by a static String or variable expression as an Input value
          //
          if (StringUtils.isNotEmpty( variableInput )) {
            variableValue = this.resolve( variableInput );
          }
        }

        try {
          data.executorWorkflow.setParameterValue( variableName, Const.NVL(variableValue, "") );
        } catch( UnknownParamException e ) {
          data.executorWorkflow.setVariable( variableName, Const.NVL(variableValue, "") );
        }
      }
    }
    data.executorWorkflow.activateParameters(data.executorWorkflow);
  }

  @Override
  public boolean init() {

    if ( super.init() ) {
      // First we need to load the mapping (pipeline)
      try {

        data.executorWorkflowMeta = WorkflowExecutorMeta.loadWorkflowMeta( meta, metadataProvider, this );

        // Do we have a workflow at all?
        //
        if ( data.executorWorkflowMeta != null ) {
          data.groupBuffer = new ArrayList<>();

          // How many rows do we group together for the workflow?
          //
          data.groupSize = -1;
          if ( !Utils.isEmpty( meta.getGroupSize() ) ) {
            data.groupSize = Const.toInt( resolve( meta.getGroupSize() ), -1 );
          }

          // Is there a grouping time set?
          //
          data.groupTime = -1;
          if ( !Utils.isEmpty( meta.getGroupTime() ) ) {
            data.groupTime = Const.toInt( resolve( meta.getGroupTime() ), -1 );
          }
          data.groupTimeStart = System.currentTimeMillis();

          // Is there a grouping field set?
          //
          data.groupField = null;
          if ( !Utils.isEmpty( meta.getGroupField() ) ) {
            data.groupField = resolve( meta.getGroupField() );
          }

          // That's all for now...
          return true;
        } else {
          logError( "No valid workflow was specified nor loaded!" );
          return false;
        }
      } catch ( Exception e ) {
        logError( "Unable to load the executor workflow because of an error : ", e );
      }

    }
    return false;
  }

  public void dispose() {
    data.groupBuffer = null;

    super.dispose();
  }

  public void stopRunning() throws HopException {
    if ( data.executorWorkflow != null ) {
      data.executorWorkflow.stopExecution();
    }
  }

  public void stopAll() {
    // Stop the workflow execution.
    if ( data.executorWorkflow != null ) {
      data.executorWorkflow.stopExecution();
    }

    // Also stop this transform
    super.stopAll();
  }
}
