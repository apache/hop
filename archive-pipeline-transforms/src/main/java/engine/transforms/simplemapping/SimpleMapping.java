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

package org.apache.hop.pipeline.transforms.simplemapping;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.RowListener;
import org.apache.hop.pipeline.transform.ITransformData;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transforms.PipelineTransformUtil;
import org.apache.hop.pipeline.transforms.mapping.MappingValueRename;
import org.apache.hop.pipeline.transforms.mappinginput.MappingInput;
import org.apache.hop.pipeline.transforms.mappingoutput.MappingOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * Execute a mapping: a re-usuable pipeline
 *
 * @author Matt
 * @since 22-nov-2005
 */
public class SimpleMapping extends BaseTransform implements ITransform {
  private static Class<?> PKG = SimpleMappingMeta.class; // for i18n purposes, needed by Translator!!

  private SimpleMappingMeta meta;
  private SimpleMappingData data;

  public SimpleMapping( TransformMeta transformMeta, ITransformData data, int copyNr, PipelineMeta pipelineMeta,
                        Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the pipeline, we
   * look up the MappingInput transform to send our rows to it. As a consequence, for the time being, there can only be one
   * MappingInput and one MappingOutput transform in the Mapping.
   */
  public boolean processRow() throws HopException {
    meta = (SimpleMappingMeta) smi;
    setData( (SimpleMappingData) sdi );
    SimpleMappingData simpleMappingData = getData();
    try {
      if ( first ) {
        first = false;
        simpleMappingData.wasStarted = true;

        // Rows read are injected into the one available Mapping Input transform
        //
        String mappingInputTransformName = simpleMappingData.mappingInput.getTransformName();
        RowProducer rowProducer = simpleMappingData.mappingPipeline.addRowProducer( mappingInputTransformName, 0 );
        simpleMappingData.rowDataInputMapper = new RowDataInputMapper( meta.getInputMapping(), rowProducer );

        // Rows produced by the mapping are read and passed on.
        //
        String mappingOutputTransformName = simpleMappingData.mappingOutput.getTransformName();
        ITransform outputTransformInterface = simpleMappingData.mappingPipeline.findTransformInterface( mappingOutputTransformName, 0 );
        RowOutputDataMapper outputDataMapper =
          new RowOutputDataMapper( meta.getInputMapping(), meta.getOutputMapping(), new PutRowInterface() {

            @Override
            public void putRow( IRowMeta rowMeta, Object[] rowData ) throws HopTransformException {
              SimpleMapping.this.putRow( rowMeta, rowData );
            }
          } );
        outputTransformInterface.addRowListener( outputDataMapper );

        // Start the mapping/sub- pipeline threads
        //
        simpleMappingData.mappingPipeline.startThreads();
      }

      // The data we read we pass to the mapping
      //
      Object[] row = getRow();
      boolean rowWasPut = false;
      if ( row != null ) {
        while ( !( data.mappingPipeline.isFinishedOrStopped() || rowWasPut ) ) {
          rowWasPut = data.rowDataInputMapper.putRow( getInputRowMeta(), row );
        }
      }

      if ( !rowWasPut ) {
        simpleMappingData.rowDataInputMapper.finished();
        simpleMappingData.mappingPipeline.waitUntilFinished();
        setOutputDone();
        return false;
      }

      return true;
    } catch ( Throwable t ) {
      // Some unexpected situation occurred.
      // Better to stop the mapping pipeline.
      //
      if ( simpleMappingData.mappingPipeline != null ) {
        simpleMappingData.mappingPipeline.stopAll();
      }

      // Forward the exception...
      //
      throw new HopException( t );
    }
  }

  public void prepareMappingExecution() throws HopException {

    SimpleMappingData simpleMappingData = getData();
    // Create the pipeline from meta-data...
    simpleMappingData.mappingPipeline = new LocalPipelineEngine( simpleMappingData.mappingPipelineMeta, this );

    // Set the parameters values in the mapping.
    //
    TransformWithMappingMeta.activateParams( simpleMappingData.mappingPipeline, simpleMappingData.mappingPipeline, this, simpleMappingData.mappingPipelineMeta.listParameters(),
      meta.getMappingParameters().getVariable(), meta.getMappingParameters().getInputField(), meta.getMappingParameters().isInheritingAllVariables() );
    if ( simpleMappingData.mappingPipelineMeta.getPipelineType() != PipelineType.Normal ) {
      simpleMappingData.mappingPipeline.getPipelineMeta().setUsingThreadPriorityManagment( false );
    }

    // Leave a path up so that we can set variables in sub-pipelines...
    //
    simpleMappingData.mappingPipeline.setParentPipeline( getPipeline() );

    // Pass down the safe mode flag to the mapping...
    //
    simpleMappingData.mappingPipeline.setSafeModeEnabled( getPipeline().isSafeModeEnabled() );

    // Pass down the metrics gathering flag:
    //
    simpleMappingData.mappingPipeline.setGatheringMetrics( getPipeline().isGatheringMetrics() );

    // Also set the name of this transform in the mapping pipeline for logging
    // purposes
    //
    simpleMappingData.mappingPipeline.setMappingTransformName( getTransformName() );

    initServletConfig();

    // We launch the pipeline in the processRow when the first row is
    // received.
    // This will allow the correct variables to be passed.
    // Otherwise the parent is the init() thread which will be gone once the
    // init is done.
    //
    try {
      simpleMappingData.mappingPipeline.prepareExecution();
    } catch ( HopException e ) {
      throw new HopException( BaseMessages.getString( PKG,
        "SimpleMapping.Exception.UnableToPrepareExecutionOfMapping" ), e );
    }

    // If there is no read/write logging transform set, we can insert the data from
    // the first mapping input/output transform...
    //
    MappingInput[] mappingInputs = simpleMappingData.mappingPipeline.findMappingInput();
    if ( mappingInputs.length == 0 ) {
      throw new HopException(
        "The simple mapping transform needs one Mapping Input transform to write to in the sub-pipeline" );
    }
    if ( mappingInputs.length > 1 ) {
      throw new HopException(
        "The simple mapping transform does not support multiple Mapping Input transforms to write to in the sub-pipeline" );
    }
    simpleMappingData.mappingInput = mappingInputs[ 0 ];
    simpleMappingData.mappingInput.setConnectorTransforms( new ITransform[ 0 ], new ArrayList<MappingValueRename>(), null );

    // LogTableField readField = data.mappingPipelineMeta.getPipelineLogTable().findField(PipelineLogTable.ID.LINES_READ);
    // if (readField.getSubject() == null) {
    // readField.setSubject(data.mappingInput.getTransformMeta());
    // }

    MappingOutput[] mappingOutputs = simpleMappingData.mappingPipeline.findMappingOutput();
    if ( mappingOutputs.length == 0 ) {
      throw new HopException(
        "The simple mapping transform needs one Mapping Output transform to read from in the sub-pipeline" );
    }
    if ( mappingOutputs.length > 1 ) {
      throw new HopException( "The simple mapping transform does not support "
        + "multiple Mapping Output transforms to read from in the sub-pipeline" );
    }
    simpleMappingData.mappingOutput = mappingOutputs[ 0 ];

    // LogTableField writeField = data.mappingPipelineMeta.getPipelineLogTable().findField(PipelineLogTable.ID.LINES_WRITTEN);
    // if (writeField.getSubject() == null && data.mappingOutputs != null && data.mappingOutputs.length >= 1) {
    // writeField.setSubject(data.mappingOutputs[0].getTransformMeta());
    // }

    // Finally, add the mapping pipeline to the active sub-pipelines
    // map in the parent pipeline
    //
    getPipeline().addActiveSubPipelineformation( getTransformName(), simpleMappingData.mappingPipeline );
  }

  void initServletConfig() {
    PipelineTransformUtil.initServletConfig( getPipeline(), getData().getMappingPipeline() );
  }

  public static void addInputRenames( List<MappingValueRename> renameList, List<MappingValueRename> addRenameList ) {
    for ( MappingValueRename rename : addRenameList ) {
      if ( renameList.indexOf( rename ) < 0 ) {
        renameList.add( rename );
      }
    }
  }

  public boolean init() {
    meta = (SimpleMappingMeta) smi;
    setData( (SimpleMappingData) sdi );
    SimpleMappingData simpleMappingData = getData();
    if ( super.init() ) {
      // First we need to load the mapping (pipeline)
      try {
        // Pass the MetaStore down to the metadata object...
        //
        simpleMappingData.mappingPipelineMeta =
          SimpleMappingMeta.loadMappingMeta( meta, meta.getMetaStore(), this, meta.getMappingParameters().isInheritingAllVariables() );
        if ( simpleMappingData.mappingPipelineMeta != null ) { // Do we have a mapping at all?

          // OK, now prepare the execution of the mapping.
          // This includes the allocation of RowSet buffers, the creation of the
          // sub- pipeline threads, etc.
          //
          prepareMappingExecution();

          // That's all for now...
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

  public void.dispose() {
    // Close the running pipeline
    if ( getData().wasStarted ) {
      if ( !getData().mappingPipeline.isFinished() ) {
        // Wait until the child pipeline has finished.
        getData().mappingPipeline.waitUntilFinished();
      }
      // Remove it from the list of active sub-pipelines...
      //
      getPipeline().removeActiveSubPipelineformation( getTransformName() );

      // See if there was an error in the sub-pipeline, in that case, flag error etc.
      if ( getData().mappingPipeline.getErrors() > 0 ) {
        logError( BaseMessages.getString( PKG, "SimpleMapping.Log.ErrorOccurredInSubPipeline" ) );
        setErrors( 1 );
      }
    }
    super.dispose();
  }

  public void stopRunning( ITransform transformMetaInterface, ITransformData iTransformData ) throws HopException {
    if ( getData().mappingPipeline != null ) {
      getData().mappingPipeline.stopAll();
    }
  }

  public void stopAll() {
    // Stop the mapping transform.
    if ( getData().mappingPipeline != null ) {
      getData().mappingPipeline.stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  public Pipeline getMappingPipeline() {
    return getData().mappingPipeline;
  }

  /**
   * For preview of the main data path, make sure we pass the row listener down to the Mapping Output transform...
   */
  public void addRowListener( RowListener rowListener ) {
    MappingOutput[] mappingOutputs = getData().mappingPipeline.findMappingOutput();
    if ( mappingOutputs == null || mappingOutputs.length == 0 ) {
      return; // Nothing to do here...
    }

    // Simple case: one output mapping transform : add the row listener over there
    //
    /*
     * if (mappingOutputs.length==1) { mappingOutputs[0].addRowListener(rowListener); } else { // Find the main data
     * path... //
     *
     *
     * }
     */

    // Add the row listener to all the outputs in the mapping...
    //
    for ( MappingOutput mappingOutput : mappingOutputs ) {
      mappingOutput.addRowListener( rowListener );
    }
  }

  public SimpleMappingData getData() {
    return data;
  }

  private void setData( SimpleMappingData data ) {
    this.data = data;
  }
}
