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

package org.apache.hop.pipeline.transforms.mapping;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.core.Const;
import org.apache.hop.core.Result;
import org.apache.hop.core.RowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogTableField;
import org.apache.hop.core.logging.PipelineLogTable;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.TransformWithMappingMeta;
import org.apache.hop.pipeline.PipelineMeta.PipelineType;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.RowListener;
import org.apache.hop.pipeline.transform.TransformDataInterface;
import org.apache.hop.pipeline.transform.TransformInterface;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.TransformMetaDataCombi;
import org.apache.hop.pipeline.transform.TransformMetaInterface;
import org.apache.hop.pipeline.transforms.PipelineTransformUtil;
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
public class Mapping extends BaseTransform implements TransformInterface {
  private static Class<?> PKG = MappingMeta.class; // for i18n purposes, needed by Translator!!

  private MappingMeta meta;
  private MappingData data;

  public Mapping( TransformMeta transformMeta, TransformDataInterface transformDataInterface, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) {
    super( transformMeta, transformDataInterface, copyNr, pipelineMeta, pipeline );
  }

  /**
   * Process a single row. In our case, we send one row of data to a piece of pipeline. In the pipeline, we
   * look up the MappingInput transform to send our rows to it. As a consequence, for the time being, there can only be one
   * MappingInput and one MappingOutput transform in the Mapping.
   */
  public boolean processRow( TransformMetaInterface smi, TransformDataInterface sdi ) throws HopException {
    try {
      meta = (MappingMeta) smi;
      setData( (MappingData) sdi );

      MappingInput[] mappingInputs = getData().getMappingPipeline().findMappingInput();
      MappingOutput[] mappingOutputs = getData().getMappingPipeline().findMappingOutput();

      getData().wasStarted = true;
      switch ( getData().mappingPipelineMeta.getPipelineType() ) {
        case Normal:

          // Before we start, let's see if there are loose ends to tie up...
          //
          List<RowSet> inputRowSets = getInputRowSets();
          if ( !inputRowSets.isEmpty() ) {
            for ( RowSet rowSet : inputRowSets ) {
              // Pass this rowset down to a mapping input transform in the
              // sub-pipeline...
              //
              if ( mappingInputs.length == 1 ) {
                // Simple case: only one input mapping. Move the RowSet over
                //
                mappingInputs[ 0 ].addRowSetToInputRowSets( rowSet );
              } else {
                // Difficult to see what's going on here.
                // TODO: figure out where this RowSet needs to go and where it
                // comes from.
                //
                throw new HopException(
                  "Unsupported situation detected where more than one Mapping Input transform needs to be handled.  "
                    + "To solve it, insert a dummy transform before the mapping transform." );
              }
            }
            clearInputRowSets();
          }


          // Do the same thing for output row sets
          //
          List<RowSet> outputRowSets = getOutputRowSets();
          if ( !outputRowSets.isEmpty() ) {
            for ( RowSet rowSet : outputRowSets ) {
              // Pass this rowset down to a mapping input transform in the
              // sub-pipeline...
              //
              if ( mappingOutputs.length == 1 ) {
                // Simple case: only one output mapping. Move the RowSet over
                //
                mappingOutputs[ 0 ].addRowSetToOutputRowSets( rowSet );
              } else {
                // Difficult to see what's going on here.
                // TODO: figure out where this RowSet needs to go and where it
                // comes from.
                //
                throw new HopException(
                  "Unsupported situation detected where more than one Mapping Output transform needs to be handled.  "
                    + "To solve it, insert a dummy transform after the mapping transform." );
              }
            }
            clearOutputRowSets();
          }

          // Start the mapping/sub- pipeline threads
          //
          getData().getMappingPipeline().startThreads();

          // The pipeline still runs in the background and might have some
          // more work to do.
          // Since everything is running in the MappingThreads we don't have to do
          // anything else here but wait...
          //
          if ( getPipelineMeta().getPipelineType() == PipelineType.Normal ) {
            getData().getMappingPipeline().waitUntilFinished();

            // Set some statistics from the mapping...
            // This will show up in HopGui, etc.
            //
            Result result = getData().getMappingPipeline().getResult();
            setErrors( result.getNrErrors() );
            setLinesRead( result.getNrLinesRead() );
            setLinesWritten( result.getNrLinesWritten() );
            setLinesInput( result.getNrLinesInput() );
            setLinesOutput( result.getNrLinesOutput() );
            setLinesUpdated( result.getNrLinesUpdated() );
            setLinesRejected( result.getNrLinesRejected() );
          }
          return false;

        case SingleThreaded:

          if ( mappingInputs.length > 1 || mappingOutputs.length > 1 ) {
            throw new HopException(
              "Multiple input or output transforms are not supported for a single threaded mapping." );
          }

          // Object[] row = getRow();
          // RowMetaInterface rowMeta = getInputRowMeta();

          // for (int count=0;count<(data.mappingPipelineMeta.getSizeRowset()/2) && row!=null;count++) {
          // // Pass each row over to the mapping input transform, fill the buffer...
          //
          // mappingInputs[0].getInputRowSets().get(0).putRow(rowMeta, row);
          //
          // row = getRow();
          // }

          if ( ( log != null ) && log.isDebug() ) {
            List<RowSet> mappingInputRowSets = mappingInputs[ 0 ].getInputRowSets();
            log.logDebug( "# of input buffers: " + mappingInputRowSets.size() );
            if ( mappingInputRowSets.size() > 0 ) {
              log.logDebug( "Input buffer 0 size: " + mappingInputRowSets.get( 0 ).size() );
            }
          }

          // Now execute one batch...Basic logging
          //
          boolean result = getData().singleThreadedPipelineExecutor.oneIteration();
          if ( !result ) {
            getData().singleThreadedPipelineExecutor.dispose();
            setOutputDone();
            return false;
          }
          return true;

        default:
          throw new HopException( "Pipeline type '"
            + getData().mappingPipelineMeta.getPipelineType().getDescription()
            + "' is an unsupported pipeline type for a mapping" );
      }
    } catch ( Throwable t ) {
      // Some unexpected situation occurred.
      // Better to stop the mapping pipeline.
      //
      if ( getData().getMappingPipeline() != null ) {
        getData().getMappingPipeline().stopAll();
      }

      // Forward the exception...
      //
      throw new HopException( t );
    }
  }


  public void prepareMappingExecution() throws HopException {
    initPipelineFromMeta();
    MappingData mappingData = getData();
    // We launch the pipeline in the processRow when the first row is
    // received.
    // This will allow the correct variables to be passed.
    // Otherwise the parent is the init() thread which will be gone once the
    // init is done.
    //
    try {
      mappingData.getMappingPipeline().prepareExecution();
    } catch ( HopException e ) {
      throw new HopException( BaseMessages.getString( PKG, "Mapping.Exception.UnableToPrepareExecutionOfMapping" ),
        e );
    }

    // Extra optional work to do for alternative execution engines...
    //
    switch ( mappingData.mappingPipelineMeta.getPipelineType() ) {
      case Normal:
        break;

      case SingleThreaded:
        mappingData.singleThreadedPipelineExecutor = new SingleThreadedPipelineExecutor( mappingData.getMappingPipeline() );
        if ( !mappingData.singleThreadedPipelineExecutor.init() ) {
          throw new HopException( BaseMessages.getString( PKG,
            "Mapping.Exception.UnableToInitSingleThreadedPipeline" ) );
        }
        break;
      default:
        break;
    }

    // If there is no read/write logging transform set, we can insert the data from
    // the first mapping input/output transform...
    //
    MappingInput[] mappingInputs = mappingData.getMappingPipeline().findMappingInput();
    LogTableField readField = mappingData.mappingPipelineMeta.getPipelineLogTable().findField( PipelineLogTable.ID.LINES_READ );
    if ( readField.getSubject() == null && mappingInputs != null && mappingInputs.length >= 1 ) {
      readField.setSubject( mappingInputs[ 0 ].getTransformMeta() );
    }
    MappingOutput[] mappingOutputs = mappingData.getMappingPipeline().findMappingOutput();
    LogTableField writeField = mappingData.mappingPipelineMeta.getPipelineLogTable().findField( PipelineLogTable.ID.LINES_WRITTEN );
    if ( writeField.getSubject() == null && mappingOutputs != null && mappingOutputs.length >= 1 ) {
      writeField.setSubject( mappingOutputs[ 0 ].getTransformMeta() );
    }

    // Before we add rowsets and all, we should note that the mapping transform did
    // not receive ANY input and output rowsets.
    // This is an exception to the general rule, built into
    // Pipeline.prepareExecution()
    //
    // A Mapping Input transform is supposed to read directly from the previous
    // transforms.
    // A Mapping Output transform is supposed to write directly to the next transforms.

    // OK, check the input mapping definitions and look up the transforms to read
    // from.
    //
    TransformInterface[] sourceTransforms;
    for ( MappingIODefinition inputDefinition : meta.getInputMappings() ) {
      // If we have a single transform to read from, we use this
      //
      if ( !Utils.isEmpty( inputDefinition.getInputTransformName() ) ) {
        TransformInterface sourceTransform = getPipeline().findRunThread( inputDefinition.getInputTransformName() );
        if ( sourceTransform == null ) {
          throw new HopException( BaseMessages.getString( PKG, "MappingDialog.Exception.TransformNameNotFound",
            inputDefinition.getInputTransformName() ) );
        }
        sourceTransforms = new TransformInterface[] { sourceTransform, };
      } else {
        // We have no defined source transform.
        // That means that we're reading from all input transforms that this mapping
        // transform has.
        //
        List<TransformMeta> prevTransforms = getPipelineMeta().findPreviousTransforms( getTransformMeta() );

        // TODO: Handle remote transforms from: getTransformMeta().getRemoteInputTransforms()
        //

        // Let's read data from all the previous transforms we find...
        // The origin is the previous transform
        // The target is the Mapping Input transform.
        //
        sourceTransforms = new TransformInterface[ prevTransforms.size() ];
        for ( int s = 0; s < sourceTransforms.length; s++ ) {
          sourceTransforms[ s ] = getPipeline().findRunThread( prevTransforms.get( s ).getName() );
        }
      }

      // What transform are we writing to?
      MappingInput mappingInputTarget = null;
      MappingInput[] mappingInputTransforms = mappingData.getMappingPipeline().findMappingInput();
      if ( Utils.isEmpty( inputDefinition.getOutputTransformName() ) ) {
        // No target was specifically specified.
        // That means we only expect one "mapping input" transform in the mapping...

        if ( mappingInputTransforms.length == 0 ) {
          throw new HopException( BaseMessages
            .getString( PKG, "MappingDialog.Exception.OneMappingInputTransformRequired" ) );
        }
        if ( mappingInputTransforms.length > 1 ) {
          throw new HopException( BaseMessages.getString( PKG,
            "MappingDialog.Exception.OnlyOneMappingInputTransformAllowed", "" + mappingInputTransforms.length ) );
        }

        mappingInputTarget = mappingInputTransforms[ 0 ];
      } else {
        // A target transform was specified. See if we can find it...
        for ( int s = 0; s < mappingInputTransforms.length && mappingInputTarget == null; s++ ) {
          if ( mappingInputTransforms[ s ].getTransformName().equals( inputDefinition.getOutputTransformName() ) ) {
            mappingInputTarget = mappingInputTransforms[ s ];
          }
        }
        // If we still didn't find it it's a drag.
        if ( mappingInputTarget == null ) {
          throw new HopException( BaseMessages.getString( PKG, "MappingDialog.Exception.TransformNameNotFound",
            inputDefinition.getOutputTransformName() ) );
        }
      }

      // Before we pass the field renames to the mapping input transform, let's add
      // functionality to rename it back on ALL
      // mapping output transforms.
      // To do this, we need a list of values that changed so we can revert that
      // in the metadata before the rows come back.
      //
      if ( inputDefinition.isRenamingOnOutput() ) {
        addInputRenames( getData().inputRenameList, inputDefinition.getValueRenames() );
      }

      mappingInputTarget.setConnectorTransforms( sourceTransforms, inputDefinition.getValueRenames(), getTransformName() );
    }

    // Now we have a List of connector threads.
    // If we start all these we'll be starting to pump data into the mapping
    // If we don't have any threads to start, nothings going in there...
    // However, before we send anything over, let's first explain to the mapping
    // output transforms where the data needs to go...
    //
    for ( MappingIODefinition outputDefinition : meta.getOutputMappings() ) {
      // OK, what is the source (input) transform in the mapping: it's the mapping
      // output transform...
      // What transform are we reading from here?
      //
      MappingOutput mappingOutputSource =
        (MappingOutput) mappingData.getMappingPipeline().findRunThread( outputDefinition.getInputTransformName() );
      if ( mappingOutputSource == null ) {
        // No source transform was specified: we're reading from a single Mapping
        // Output transform.
        // We should verify this if this is really the case...
        //
        MappingOutput[] mappingOutputTransforms = mappingData.getMappingPipeline().findMappingOutput();

        if ( mappingOutputTransforms.length == 0 ) {
          throw new HopException( BaseMessages.getString( PKG,
            "MappingDialog.Exception.OneMappingOutputTransformRequired" ) );
        }
        if ( mappingOutputTransforms.length > 1 ) {
          throw new HopException( BaseMessages.getString( PKG,
            "MappingDialog.Exception.OnlyOneMappingOutputTransformAllowed", "" + mappingOutputTransforms.length ) );
        }

        mappingOutputSource = mappingOutputTransforms[ 0 ];
      }

      // To what transforms in this pipeline are we writing to?
      //
      TransformInterface[] targetTransforms = pickupTargetTransformsFor( outputDefinition );

      // Now tell the mapping output transform where to look...
      // Also explain the mapping output transforms how to rename the values back...
      //
      mappingOutputSource
        .setConnectorTransforms( targetTransforms, getData().inputRenameList, outputDefinition.getValueRenames() );

      // Is this mapping copying or distributing?
      // Make sure the mapping output transform mimics this behavior:
      //
      mappingOutputSource.setDistributed( isDistributed() );
    }

    // Finally, add the mapping pipeline to the active sub-pipelines
    // map in the parent pipeline
    //
    getPipeline().addActiveSubPipeline( getTransformName(), getData().getMappingPipeline() );
  }

  @VisibleForTesting TransformInterface[] pickupTargetTransformsFor( MappingIODefinition outputDefinition )
    throws HopException {
    List<TransformInterface> result;
    if ( !Utils.isEmpty( outputDefinition.getOutputTransformName() ) ) {
      // If we have a target transform specification for the output of the mapping,
      // we need to send it over there...
      //
      result = getPipeline().findTransformInterfaces( outputDefinition.getOutputTransformName() );
      if ( Utils.isEmpty( result ) ) {
        throw new HopException( BaseMessages.getString( PKG, "MappingDialog.Exception.TransformNameNotFound",
          outputDefinition.getOutputTransformName() ) );
      }
    } else {
      // No target transform is specified.
      // See if we can find the next transforms in the pipeline..
      //
      List<TransformMeta> nextTransforms = getPipelineMeta().findNextTransforms( getTransformMeta() );

      // Let's send the data to all the next transforms we find...
      // The origin is the mapping output transform
      // The target is all the next transforms after this mapping transform.
      //
      result = new ArrayList<>();
      for ( TransformMeta nextTransform : nextTransforms ) {
        // need to take into the account different copies of the transform
        List<TransformInterface> copies = getPipeline().findTransformInterfaces( nextTransform.getName() );
        if ( copies != null ) {
          result.addAll( copies );
        }
      }
    }
    return result.toArray( new TransformInterface[ result.size() ] );
  }

  void initPipelineFromMeta() throws HopException {
    // Create the pipeline from meta-data...
    //
    getData().setMappingPipeline( new Pipeline( getData().mappingPipelineMeta, this ) );

    if ( getData().mappingPipelineMeta.getPipelineType() != PipelineType.Normal ) {
      getData().getMappingPipeline().getPipelineMeta().setUsingThreadPriorityManagment( false );
    }

    // Leave a path up so that we can set variables in sub-pipelines...
    //
    getData().getMappingPipeline().setParentPipeline( getPipeline() );

    // Pass down the safe mode flag to the mapping...
    //
    getData().getMappingPipeline().setSafeModeEnabled( getPipeline().isSafeModeEnabled() );

    // Pass down the metrics gathering flag:
    //
    getData().getMappingPipeline().setGatheringMetrics( getPipeline().isGatheringMetrics() );

    // Also set the name of this transform in the mapping pipeline for logging
    // purposes
    //
    getData().getMappingPipeline().setMappingTransformName( getTransformName() );

    initServletConfig();

    // Set the parameters values in the mapping.
    //

    MappingParameters mappingParameters = meta.getMappingParameters();
    if ( mappingParameters != null ) {
      TransformWithMappingMeta
        .activateParams( data.mappingPipeline, data.mappingPipeline, this, data.mappingPipelineMeta.listParameters(),
          mappingParameters.getVariable(), mappingParameters.getInputField(), meta.getMappingParameters().isInheritingAllVariables() );
    }

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

  public boolean init( TransformMetaInterface smi, TransformDataInterface sdi ) {
    meta = (MappingMeta) smi;
    setData( (MappingData) sdi );
    MappingData mappingData = getData();
    if ( !super.init( smi, sdi ) ) {
      return false;
    }
    // First we need to load the mapping (pipeline)
    try {
      // Pass the MetaStore down to the metadata object...
      //
      mappingData.mappingPipelineMeta = MappingMeta.loadMappingMeta( meta, meta.getMetaStore(), this, meta.getMappingParameters().isInheritingAllVariables() );
      if ( data.mappingPipelineMeta == null ) {
        // Do we have a mapping at all?
        logError( "No valid mapping was specified!" );
        return false;
      }

      // OK, now prepare the execution of the mapping.
      // This includes the allocation of RowSet buffers, the creation of the
      // sub- pipeline threads, etc.
      //
      prepareMappingExecution();

      lookupStatusTransformNumbers();
      // That's all for now...
      return true;
    } catch ( Exception e ) {
      logError( "Unable to load the mapping pipeline because of an error : " + e.toString() );
      logError( Const.getStackTracker( e ) );
      return false;
    }
  }

  public void dispose( TransformMetaInterface smi, TransformDataInterface sdi ) {
    // Close the running pipeline
    if ( getData().wasStarted ) {
      if ( !getData().mappingPipeline.isFinished() ) {
        // Wait until the child pipeline has finished.
        getData().getMappingPipeline().waitUntilFinished();
      }
      // Remove it from the list of active sub-pipelines...
      //
      getPipeline().removeActiveSubPipeline( getTransformName() );

      // See if there was an error in the sub-pipeline, in that case, flag error etc.
      if ( getData().getMappingPipeline().getErrors() > 0 ) {
        logError( BaseMessages.getString( PKG, "Mapping.Log.ErrorOccurredInSubPipeline" ) );
        setErrors( 1 );
      }
    }
    super.dispose( smi, sdi );
  }

  public void stopRunning( TransformMetaInterface transformMetaInterface, TransformDataInterface transformDataInterface )
    throws HopException {
    if ( getData().getMappingPipeline() != null ) {
      getData().getMappingPipeline().stopAll();
    }
  }

  public void stopAll() {
    // Stop the mapping transform.
    if ( getData().getMappingPipeline() != null ) {
      getData().getMappingPipeline().stopAll();
    }

    // Also stop this transform
    super.stopAll();
  }

  private void lookupStatusTransformNumbers() {
    MappingData mappingData = getData();
    if ( mappingData.getMappingPipeline() != null ) {
      List<TransformMetaDataCombi<TransformInterface, TransformMetaInterface, TransformDataInterface>> transforms = mappingData.getMappingPipeline().getTransforms();
      for ( int i = 0; i < transforms.size(); i++ ) {
        TransformMetaDataCombi sid = transforms.get( i );
        BaseTransform rt = (BaseTransform) sid.transform;
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameRead() ) ) {
          mappingData.linesReadTransformNr = i;
        }
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameInput() ) ) {
          mappingData.linesInputTransformNr = i;
        }
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameWritten() ) ) {
          mappingData.linesWrittenTransformNr = i;
        }
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameOutput() ) ) {
          mappingData.linesOutputTransformNr = i;
        }
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameUpdated() ) ) {
          mappingData.linesUpdatedTransformNr = i;
        }
        if ( rt.getTransformName().equals( getData().mappingPipelineMeta.getPipelineLogTable().getTransformNameRejected() ) ) {
          mappingData.linesRejectedTransformNr = i;
        }
      }
    }
  }

  @Override
  public long getLinesInput() {
    if ( getData() != null && getData().linesInputTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesInputTransformNr ).transform.getLinesInput();
    } else {
      return 0;
    }
  }

  @Override
  public long getLinesOutput() {
    if ( getData() != null && getData().linesOutputTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesOutputTransformNr ).transform.getLinesOutput();
    } else {
      return 0;
    }
  }

  @Override
  public long getLinesRead() {
    if ( getData() != null && getData().linesReadTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesReadTransformNr ).transform.getLinesRead();
    } else {
      return 0;
    }
  }

  @Override
  public long getLinesRejected() {
    if ( getData() != null && getData().linesRejectedTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesRejectedTransformNr ).transform.getLinesRejected();
    } else {
      return 0;
    }
  }

  @Override
  public long getLinesUpdated() {
    if ( getData() != null && getData().linesUpdatedTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesUpdatedTransformNr ).transform.getLinesUpdated();
    } else {
      return 0;
    }
  }

  @Override
  public long getLinesWritten() {
    if ( getData() != null && getData().linesWrittenTransformNr != -1 ) {
      return getData().getMappingPipeline().getTransforms().get( getData().linesWrittenTransformNr ).transform.getLinesWritten();
    } else {
      return 0;
    }
  }

  @Override
  public int rowsetInputSize() {
    int size = 0;
    for ( MappingInput input : getData().getMappingPipeline().findMappingInput() ) {
      for ( RowSet rowSet : input.getInputRowSets() ) {
        size += rowSet.size();
      }
    }
    return size;
  }

  @Override
  public int rowsetOutputSize() {
    int size = 0;
    for ( MappingOutput output : getData().getMappingPipeline().findMappingOutput() ) {
      for ( RowSet rowSet : output.getOutputRowSets() ) {
        size += rowSet.size();
      }
    }
    return size;
  }

  public Pipeline getMappingPipeline() {
    return getData().getMappingPipeline();
  }

  /**
   * For preview of the main data path, make sure we pass the row listener down to the Mapping Output transform...
   */
  public void addRowListener( RowListener rowListener ) {
    MappingOutput[] mappingOutputs = getData().getMappingPipeline().findMappingOutput();
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

  MappingData getData() {
    return data;
  }

  void setData( MappingData data ) {
    this.data = data;
  }
}
