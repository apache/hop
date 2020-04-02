/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.pipeline.transforms.jobexecutor;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.IDelegationListener;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.pipelineexecutor.PipelineExecutor;

import java.util.ArrayList;

/**
 * Execute a job for every input row.
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
public class JobExecutor extends BaseTransform implements ITransform {
  private static Class<?> PKG = JobExecutorMeta.class; // for i18n purposes, needed by Translator!!

  private JobExecutorMeta meta;
  private JobExecutorData data;

  public JobExecutor( TransformMeta transformMeta, ITransformData iTransformData, int copyNr, PipelineMeta pipelineMeta,
                      Pipeline pipeline ) {
    super( transformMeta, iTransformData, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the pipeline, we
   * look up the MappingInput transform to send our rows to it. As a consequence, for the time being, there can only be one
   * MappingInput and one MappingOutput transform in the JobExecutor.
   */
  public boolean processRow( ITransformMeta smi, ITransformData sdi ) throws HopException {
    try {
      meta = (JobExecutorMeta) smi;
      data = (JobExecutorData) sdi;

      // Wait for a row...
      //
      Object[] row = getRow();

      if ( row == null ) {
        if ( !data.groupBuffer.isEmpty() ) {
          executeJob();
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
            .getExecutionResultTargetTransformMeta(), this, metaStore );
          data.executionResultRowSet = findOutputRowSet( meta.getExecutionResultTargetTransformMeta().getName() );
        }
        if ( meta.getResultRowsTargetTransformMeta() != null ) {
          meta.getFields(
            data.resultRowsOutputRowMeta, getTransformName(), null, meta.getResultRowsTargetTransformMeta(), this,
            metaStore );
          data.resultRowsRowSet = findOutputRowSet( meta.getResultRowsTargetTransformMeta().getName() );
        }
        if ( meta.getResultFilesTargetTransformMeta() != null ) {
          meta.getFields(
            data.resultFilesOutputRowMeta, getTransformName(), null, meta.getResultFilesTargetTransformMeta(), this,
            metaStore );
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
              executeJob();
            }
          }
          data.prevGroupFieldData = groupFieldData;
        } else if ( data.groupTime > 0 ) { // grouping by execution time
          long now = System.currentTimeMillis();
          if ( now - data.groupTimeStart >= data.groupTime ) {
            executeJob();
          }
        }
      }

      // Add next value AFTER job execution, in case we are grouping by field (see PDI-14958),
      // and BEFORE checking size of a group, in case we are grouping by size (see PDI-14121).
      data.groupBuffer.add( new RowMetaAndData( getInputRowMeta(), row ) ); // should we clone for safety?

      // Grouping by size.
      // If group buffer size exceeds specified limit, then execute job and flush group buffer.
      if ( data.groupSize > 0 ) {
        // Pass all input rows...
        if ( data.groupBuffer.size() >= data.groupSize ) {
          executeJob();
        }
      }

      return true;
    } catch ( Exception e ) {
      throw new HopException( BaseMessages.getString( PKG, "JobExecutor.UnexpectedError" ), e );
    }
  }

  private void executeJob() throws HopException {

    // If we got 0 rows on input we don't really want to execute the job
    //
    if ( data.groupBuffer.isEmpty() ) {
      return;
    }

    data.groupTimeStart = System.currentTimeMillis();

    if ( first ) {
      discardLogLines( data );
    }

    data.executorJob = createJob( data.executorJobMeta, this );

    data.executorJob.shareVariablesWith( data.executorJobMeta );
    data.executorJob.setParentPipeline( getPipeline() );
    data.executorJob.setLogLevel( getLogLevel() );
    data.executorJob.setInternalHopVariables( this );
    data.executorJob.copyParametersFrom( data.executorJobMeta );

    // data.executorJob.setInteractive(); TODO: pass interactivity through the pipeline too for drill-down.

    // TODO
    /*
     * if (data.executorJob.isInteractive()) {
     * data.executorJob.getJobEntryListeners().addAll(parentJob.getJobEntryListeners()); }
     */

    // Pass the accumulated rows
    //
    data.executorJob.setSourceRows( data.groupBuffer );

    // Pass parameter values
    //
    passParametersToJob();

    // keep track for drill down in HopGui...
    //
    getPipeline().getActiveSubjobs().put( getTransformName(), data.executorJob );

    ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobStart.id, data.executorJob );

    data.executorJob.beginProcessing();

    Result result = new Result();

    // Inform the parent pipeline we delegated work here...
    //
    for ( IDelegationListener delegationListener : getPipeline().getDelegationListeners() ) {
      // TODO: copy some settings in the job execution configuration, not strictly needed
      // but the execution configuration information is useful in case of a job re-start on HopServer
      //
      delegationListener.jobDelegationStarted( data.executorJob, new JobExecutionConfiguration() );
    }

    // Now go execute this job
    //
    try {
      result = data.executorJob.execute( 0, result );
    } catch ( HopException e ) {
      log.logError( "An error occurred executing the job: ", e );
      result.setResult( false );
      result.setNrErrors( 1 );
    } finally {
      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobFinish.id, data.executorJob );
        data.executorJob.fireJobFinishListeners();
      } catch ( HopException e ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        log.logError( BaseMessages.getString( PKG, "JobExecutor.Log.ErrorExecJob", e.getMessage() ), e );
      }
    }

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
        String channelId = data.executorJob.getLogChannelId();
        String logText = HopLogStore.getAppender().getBuffer( channelId, false ).toString();
        outputRow[ idx++ ] = logText;
      }
      if ( !Utils.isEmpty( meta.getExecutionLogChannelIdField() ) ) {
        outputRow[ idx++ ] = data.executorJob.getLogChannelId();
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
  Job createJob( JobMeta jobMeta, ILoggingObject parentLogging ) {
    return new Job( jobMeta, parentLogging );
  }

  @VisibleForTesting
  void discardLogLines( JobExecutorData data ) {
    // Keep the strain on the logging back-end conservative.
    // TODO: make this optional/user-defined later
    if ( data.executorJob != null ) {
      HopLogStore.discardLines( data.executorJob.getLogChannelId(), false );
      LoggingRegistry.getInstance().removeIncludingChildren( data.executorJob.getLogChannelId() );
    }
  }

  private void passParametersToJob() throws HopException {
    // Set parameters, when fields are used take the first row in the set.
    //
    JobExecutorParameters parameters = meta.getParameters();

    String value;

    for ( int i = 0; i < parameters.getVariable().length; i++ ) {
      String fieldName = parameters.getField()[ i ];
      if ( !Utils.isEmpty( fieldName ) ) {
        int idx = getInputRowMeta().indexOfValue( fieldName );
        if ( idx < 0 ) {
          throw new HopException( BaseMessages.getString(
            PKG, "JobExecutor.Exception.UnableToFindField", fieldName ) );
        }
        value = data.groupBuffer.get( 0 ).getString( idx, "" );
        this.setVariable( parameters.getVariable()[ i ], value );
      }
    }

    TransformWithMappingMeta.activateParams( data.executorJob, data.executorJob, this, data.executorJob.listParameters(),
      parameters.getVariable(), parameters.getInput(), meta.getParameters().isInheritingAllVariables() );
  }

  public boolean init( ITransformMeta smi, ITransformData sdi ) {
    meta = (JobExecutorMeta) smi;
    data = (JobExecutorData) sdi;

    if ( super.init( smi, sdi ) ) {
      // First we need to load the mapping (pipeline)
      try {

        data.executorJobMeta = JobExecutorMeta.loadJobMeta( meta, this );

        // Do we have a job at all?
        //
        if ( data.executorJobMeta != null ) {
          data.groupBuffer = new ArrayList<RowMetaAndData>();

          // How many rows do we group together for the job?
          //
          data.groupSize = -1;
          if ( !Utils.isEmpty( meta.getGroupSize() ) ) {
            data.groupSize = Const.toInt( environmentSubstitute( meta.getGroupSize() ), -1 );
          }

          // Is there a grouping time set?
          //
          data.groupTime = -1;
          if ( !Utils.isEmpty( meta.getGroupTime() ) ) {
            data.groupTime = Const.toInt( environmentSubstitute( meta.getGroupTime() ), -1 );
          }
          data.groupTimeStart = System.currentTimeMillis();

          // Is there a grouping field set?
          //
          data.groupField = null;
          if ( !Utils.isEmpty( meta.getGroupField() ) ) {
            data.groupField = environmentSubstitute( meta.getGroupField() );
          }

          // That's all for now...
          return true;
        } else {
          logError( "No valid job was specified nor loaded!" );
          return false;
        }
      } catch ( Exception e ) {
        logError( "Unable to load the executor job because of an error : ", e );
      }

    }
    return false;
  }

  public void dispose( ITransformMeta smi, ITransformData sdi ) {
    data.groupBuffer = null;

    super.dispose( smi, sdi );
  }

  public void stopRunning( ITransformMeta transformMetaInterface, ITransformData iTransformData ) throws HopException {
    if ( data.executorJob != null ) {
      data.executorJob.stopAll();
    }
  }

  public void stopAll() {
    // Stop the job execution.
    if ( data.executorJob != null ) {
      data.executorJob.stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  /*
   *
   * @Override public long getLinesInput() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesInput(); else return 0; }
   *
   * @Override public long getLinesOutput() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesOutput(); else return 0; }
   *
   * @Override public long getLinesRead() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesRead(); else return 0; }
   *
   * @Override public long getLinesRejected() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesRejected(); else return 0; }
   *
   * @Override public long getLinesUpdated() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesUpdated(); else return 0; }
   *
   * @Override public long getLinesWritten() { if (data!=null && data.executorJob != null &&
   * data.executorJob.getResult()!=null) return data.executorJob.getResult().getNrLinesWritten(); else return 0; }
   */

  public Job getExecutorJob() {
    return data.executorJob;
  }
}
