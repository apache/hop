/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
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

package org.apache.hop.pipeline.transforms.singlethreader;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.PipelineTransformUtil;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mappinginput.MappingInputData;

import java.util.ArrayList;

/**
 * Execute a mapping: a re-usuable pipeline
 *
 * @author Matt
 * @since 22-nov-2005
 */
public class SingleThreader extends BaseTransform<SingleThreaderMeta, SingleThreaderData> implements ITransform<SingleThreaderMeta, SingleThreaderData> {

  private static Class<?> PKG = SingleThreaderMeta.class; // for i18n purposes, needed by Translator!!
  
  public SingleThreader( TransformMeta transformMeta, SingleThreaderMeta meta, SingleThreaderData data, int copyNr, PipelineMeta pipelineMeta,
                         Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process rows in batches of N rows. The sub- pipeline will execute in a single thread.
   */
  public boolean processRow() throws HopException {

    Object[] row = getRow();
    if ( row == null ) {
      if ( data.batchCount > 0 ) {
        data.batchCount = 0;
        return execOneIteration();
      }

      setOutputDone();
      return false;
    }
    if ( first ) {
      first = false;
      data.startTime = System.currentTimeMillis();
    }

    // Add the row to the producer...
    //
    data.rowProducer.putRow( getInputRowMeta(), row );
    data.batchCount++;

    if ( getTransformMeta().isDoingErrorHandling() ) {
      data.errorBuffer.add( row );
    }

    boolean countWindow = data.batchSize > 0 && data.batchCount >= data.batchSize;
    boolean timeWindow = data.batchTime > 0 && ( System.currentTimeMillis() - data.startTime ) > data.batchTime;

    if ( countWindow || timeWindow ) {
      data.batchCount = 0;
      boolean more = execOneIteration();
      if ( !more ) {
        setOutputDone();
        return false;
      }
      data.startTime = System.currentTimeMillis();
    }
    return true;
  }

  private boolean execOneIteration() {
    boolean more = false;
    try {
      more = data.executor.oneIteration();
      if ( data.executor.isStopped() || data.executor.getErrors() > 0 ) {
        return handleError();
      }
    } catch ( Exception e ) {
      setErrors( 1L );
      stopAll();
      logError( BaseMessages.getString( PKG, "SingleThreader.Log.ErrorOccurredInSubPipeline" ) );
      return false;
    } finally {
      if ( getTransformMeta().isDoingErrorHandling() ) {
        data.errorBuffer.clear();
      }
    }
    return more;
  }

  private boolean handleError() throws HopTransformException {
    if ( getTransformMeta().isDoingErrorHandling() ) {
      int lastLogLine = HopLogStore.getLastBufferLineNr();
      StringBuffer logText =
        HopLogStore.getAppender().getBuffer( data.mappingPipeline.getLogChannelId(), false, data.lastLogLine );
      data.lastLogLine = lastLogLine;

      for ( Object[] row : data.errorBuffer ) {
        putError( getInputRowMeta(), row, 1L, logText.toString(), null, "STR-001" );
      }

      data.executor.clearError();

      return true; // continue
    } else {
      setErrors( 1 );
      stopAll();
      logError( BaseMessages.getString( PKG, "SingleThreader.Log.ErrorOccurredInSubPipeline" ) );
      return false; // stop running
    }
  }

  public void prepareMappingExecution() throws HopException {
    // Set the type to single threaded in case the user forgot...
    //
    data.mappingPipelineMeta.setPipelineType( PipelineType.SingleThreaded );

    // Create the pipeline from meta-data...
    data.mappingPipeline = new Pipeline( data.mappingPipelineMeta, getPipeline() );

    // Pass the parameters down to the sub-pipeline.
    //
    TransformWithMappingMeta.activateParams( data.mappingPipeline, data.mappingPipeline, this, data.mappingPipeline.listParameters(),
        meta.getParameters(), meta.getParameterValues(), meta.isPassingAllParameters() );
    data.mappingPipeline.activateParameters();

    // Disable thread priority managment as it will slow things down needlessly.
    // The single threaded engine doesn't use threads and doesn't need row locking.
    //
    data.mappingPipeline.getPipelineMeta().setUsingThreadPriorityManagment( false );

    // Leave a path up so that we can set variables in sub-pipelines...
    //
    data.mappingPipeline.setParentPipeline( getPipeline() );

    // Pass down the safe mode flag to the mapping...
    //
    data.mappingPipeline.setSafeModeEnabled( getPipeline().isSafeModeEnabled() );

    // Pass down the metrics gathering flag to the mapping...
    //
    data.mappingPipeline.setGatheringMetrics( getPipeline().isGatheringMetrics() );

    // Also set the name of this transform in the mapping pipeline for logging purposes
    //
    data.mappingPipeline.setMappingTransformName( getTransformName() );

    initServletConfig();

    // prepare the execution
    //
    data.mappingPipeline.prepareExecution();

    // If the inject transform is a mapping input transform, tell it all is OK...
    //
    if ( data.injectTransformMeta.isMappingInput() ) {
      MappingInputData mappingInputData =
        (MappingInputData) data.mappingPipeline.findDataInterface( data.injectTransformMeta.getName() );
      mappingInputData.sourceTransforms = new ITransform[ 0 ];
      mappingInputData.valueRenames = new ArrayList<MappingValueRename>();
    }

    // Add row producer & row listener
    data.rowProducer = data.mappingPipeline.addRowProducer( meta.getInjectTransform(), 0 );

    ITransform retrieveTransform = data.mappingPipeline.getTransformInterface( meta.getRetrieveTransform(), 0 );
    retrieveTransform.addRowListener( new RowAdapter() {
      @Override
      public void rowWrittenEvent( IRowMeta rowMeta, Object[] row ) throws HopTransformException {
        // Simply pass it along to the next transforms after the SingleThreader
        //
        SingleThreader.this.putRow( rowMeta, row );
      }
    } );

    data.mappingPipeline.startThreads();

    // Create the executor...
    data.executor = new SingleThreadedPipelineExecutor( data.mappingPipeline );

    // We launch the pipeline in the processRow when the first row is received.
    // This will allow the correct variables to be passed.
    // Otherwise the parent is the init() thread which will be gone once the init is done.
    //
    try {
      boolean ok = data.executor.init();
      if ( !ok ) {
        throw new HopException( BaseMessages.getString(
          PKG, "SingleThreader.Exception.UnableToInitSingleThreadedPipeline" ) );
      }
    } catch ( HopException e ) {
      throw new HopException( BaseMessages.getString(
        PKG, "SingleThreader.Exception.UnableToPrepareExecutionOfMapping" ), e );
    }

    // Add the mapping pipeline to the active sub-pipelines map in the parent pipeline
    //
    getPipeline().addActiveSubPipeline( getTransformName(), data.mappingPipeline );
  }

  void initServletConfig() {
    PipelineTransformUtil.initServletConfig( getPipeline(), data.getMappingPipeline() );
  }

  public boolean init() {
    if ( super.init() ) {
      // First we need to load the mapping (pipeline)
      try {
        // The batch size...
        data.batchSize = Const.toInt( environmentSubstitute( meta.getBatchSize() ), 0 );
        data.batchTime = Const.toInt( environmentSubstitute( meta.getBatchTime() ), 0 );

        // TODO: Pass the MetaStore down to the metadata object?...
        //
        data.mappingPipelineMeta = SingleThreaderMeta.loadSingleThreadedPipelineMeta( meta, this, meta.isPassingAllParameters() );
        if ( data.mappingPipelineMeta != null ) { // Do we have a mapping at all?

          // Validate the inject and retrieve transform names
          //
          String injectTransformName = environmentSubstitute( meta.getInjectTransform() );
          data.injectTransformMeta = data.mappingPipelineMeta.findTransform( injectTransformName );
          if ( data.injectTransformMeta == null ) {
            logError( "The inject transform with name '"
              + injectTransformName + "' couldn't be found in the sub-pipeline" );
          }

          String retrieveTransformName = environmentSubstitute( meta.getRetrieveTransform() );
          if ( !Utils.isEmpty( retrieveTransformName ) ) {
            data.retrieveTransformMeta = data.mappingPipelineMeta.findTransform( retrieveTransformName );
            if ( data.retrieveTransformMeta == null ) {
              logError( "The retrieve transform with name '"
                + retrieveTransformName + "' couldn't be found in the sub-pipeline" );
            }
          }

          // OK, now prepare the execution of the mapping.
          // This includes the allocation of IRowSet buffers, the creation of the
          // sub- pipeline threads, etc.
          //
          prepareMappingExecution();

          if ( getTransformMeta().isDoingErrorHandling() ) {
            data.errorBuffer = new ArrayList<Object[]>();
          }

          // That's all for now...
          //
          return true;
        } else {
          logError( "No valid mapping was specified!" );
          return false;
        }
      } catch ( Exception e ) {
        logError( "Unable to load the mapping pipeline because of an error : " + e.toString() );
        logError( Const.getStackTracker( e ) );
      }

    }
    return false;
  }

  public void dispose(){
    // dispose of the single threading execution engine
    //
    try {
      if ( data.executor != null ) {
        data.executor.dispose();
      }
    } catch ( HopException e ) {
      log.logError( "Error disposing of sub-pipeline: ", e );
    }

    super.dispose();
  }

  public void stopRunning() throws HopException {
    if ( data.mappingPipeline != null ) {
      data.mappingPipeline.stopAll();
    }
  }

  public void stopAll() {
    // Stop the mapping transform.
    if ( data.mappingPipeline != null ) {
      data.mappingPipeline.stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  public Pipeline getMappingPipeline() {
    return data.mappingPipeline;
  }
}
