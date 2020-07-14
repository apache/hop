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

package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.StringToHopRowFn;
import org.apache.hop.beam.core.shared.VariableValue;
import org.apache.hop.beam.core.transform.TransformBatchTransform;
import org.apache.hop.beam.core.transform.TransformTransform;
import org.apache.hop.beam.core.util.HopBeamUtil;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.util.BeamConst;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformIOMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BeamGenericTransformHandler extends BeamBaseStepHandler implements BeamStepHandler {

  private String metaStoreJson;

  public BeamGenericTransformHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, String metaStoreJson, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
    this.metaStoreJson = metaStoreJson;
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    // If we have no previous transform, it's an input transform.  We need to start from pipeline
    //
    boolean inputStep = input == null;
    boolean reduceParallelism = checkStepCopiesForReducedParallelism( transformMeta );
    reduceParallelism=reduceParallelism || needsSingleThreading( transformMeta );

    String stepMetaInterfaceXml = XmlHandler.openTag( TransformMeta.XML_TAG ) + transformMeta.getTransform().getXml() + XmlHandler.closeTag( TransformMeta.XML_TAG );


    // See if the transform has Info transforms
    //
    List<TransformMeta> infoStepMetas = pipelineMeta.findPreviousTransforms( transformMeta, true );
    List<String> infoSteps = new ArrayList<>();
    List<String> infoRowMetaJsons = new ArrayList<>();
    List<PCollectionView<List<HopRow>>> infoCollectionViews = new ArrayList<>();
    for ( TransformMeta infoStepMeta : infoStepMetas ) {
      if ( !previousSteps.contains( infoStepMeta ) ) {
        infoSteps.add( infoStepMeta.getName() );
        infoRowMetaJsons.add( JsonRowMeta.toJson( pipelineMeta.getTransformFields( infoStepMeta ) ) );
        PCollection<HopRow> infoCollection = stepCollectionMap.get( infoStepMeta.getName() );
        if ( infoCollection == null ) {
          throw new HopException( "Unable to find collection for transform '" + infoStepMeta.getName() + " providing info for '" + transformMeta.getName() + "'" );
        }
        infoCollectionViews.add( infoCollection.apply( View.asList() ) );
      }
    }

    // Get the list of variables from the PipelineMeta variable variables:
    //
    List<VariableValue> variableValues = getVariableValues( pipelineMeta );

    // Find out all the target transforms for this transform...
    //
    ITransformIOMeta ioMeta = transformMeta.getTransform().getTransformIOMeta();
    List<String> targetSteps = new ArrayList<String>();
    for ( IStream targetStream : ioMeta.getTargetStreams() ) {
      if ( targetStream.getTransformMeta() != null ) {
        targetSteps.add( targetStream.getTransformMeta().getName() );
      }
    }

    // For streaming pipelines we need to flush the rows in the buffer of a generic transform (Table Output, Neo4j Output, ...)
    // This is what the BeamJobConfig option "Streaming Hop Steps Flush Interval" is for...
    // Without a valid value we default to -1 to disable flushing.
    //
    int flushIntervalMs = Const.toInt(runConfiguration.getStreamingHopTransformsFlushInterval(), -1);
    int sizeRowsSet = Const.toInt(runConfiguration.getStreamingHopTransformsBufferSize(), 500);

    // TODO: take size rowset from the run configuration
    //
    int sizeRowSet = 5000;

    // Send all the information on their way to the right nodes
    //
    PTransform<PCollection<HopRow>, PCollectionTuple> transformTransform;
    if (needsBatching(transformMeta)) {
      transformTransform = new TransformBatchTransform( variableValues, metaStoreJson, transformPluginClasses, xpPluginClasses, sizeRowSet, flushIntervalMs,
        transformMeta.getName(), transformMeta.getTransformPluginId(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), inputStep,
        targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );
    } else {
      transformTransform = new TransformTransform( variableValues, metaStoreJson, transformPluginClasses, xpPluginClasses, sizeRowSet, flushIntervalMs,
        transformMeta.getName(), transformMeta.getTransformPluginId(), stepMetaInterfaceXml, JsonRowMeta.toJson( rowMeta ), inputStep,
        targetSteps, infoSteps, infoRowMetaJsons, infoCollectionViews );
    }

    if ( input == null ) {
      // Start from a dummy row and group over it.
      // Trick Beam into only running a single thread of the transform that comes next.
      //
      input = pipeline
        .apply( Create.of( Arrays.asList( "hop-single-value" ) ) ).setCoder( StringUtf8Coder.of() )
        .apply( WithKeys.of( (Void) null ) )
        .apply( GroupByKey.create() )
        .apply( Values.create() )
        .apply( Flatten.iterables() )
        .apply( ParDo.of( new StringToHopRowFn( transformMeta.getName(), JsonRowMeta.toJson( rowMeta ), transformPluginClasses, xpPluginClasses ) ) );

      // Store this new collection so we can hook up other transforms...
      //
      String tupleId = HopBeamUtil.createMainInputTupleId( transformMeta.getName() );
      stepCollectionMap.put( tupleId, input );
    } else if ( reduceParallelism ) {
      PCollection.IsBounded isBounded = input.isBounded();
      if (isBounded== PCollection.IsBounded.BOUNDED) {
        // group across all fields to get down to a single thread...
        //
        input = input.apply( WithKeys.of( (Void) null ) )
          .setCoder( KvCoder.of( VoidCoder.of(), input.getCoder() ) )
          .apply( GroupByKey.create() )
          .apply( Values.create() )
          .apply( Flatten.iterables() )
        ;
      } else {

        // Streaming: try partitioning over a single partition
        // NOTE: doesn't seem to work <sigh/>
        /*
        input = input
          .apply( Partition.of( 1, new SinglePartitionFn() ) )
          .apply( Flatten.pCollections() )
        ;
         */
        throw new HopException( "Unable to reduce parallel in an unbounded (streaming) pipeline in transform : "+transformMeta.getName() );
      }
    }

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollectionTuple tuple = input.apply( transformMeta.getName(), transformTransform );

    // The main collection
    //
    PCollection<HopRow> mainPCollection = tuple.get( new TupleTag<HopRow>( HopBeamUtil.createMainOutputTupleId( transformMeta.getName() ) ) );

    // Save this in the map
    //
    stepCollectionMap.put( transformMeta.getName(), mainPCollection );

    // Were there any targeted transforms in this transform?
    //
    for ( String targetStep : targetSteps ) {
      String tupleId = HopBeamUtil.createTargetTupleId( transformMeta.getName(), targetStep );
      PCollection<HopRow> targetPCollection = tuple.get( new TupleTag<HopRow>( tupleId ) );

      // Store this in the map as well
      //
      stepCollectionMap.put( tupleId, targetPCollection );
    }

    log.logBasic( "Handled transform (STEP) : " + transformMeta.getName() + ", gets data from " + previousSteps.size() + " previous transform(s), targets=" + targetSteps.size() + ", infos=" + infoSteps.size() );
  }

  public static boolean needsBatching( TransformMeta transformMeta ) {
    String value = transformMeta.getAttribute( BeamConst.STRING_HOP_BEAM, BeamConst.STRING_STEP_FLAG_BATCH );
    return value!=null && "true".equalsIgnoreCase( value );
  }

  public static boolean needsSingleThreading( TransformMeta transformMeta ) {
    String value = transformMeta.getAttribute( BeamConst.STRING_HOP_BEAM, BeamConst.STRING_STEP_FLAG_SINGLE_THREADED );
    return value!=null && "true".equalsIgnoreCase( value );
  }

  private boolean checkStepCopiesForReducedParallelism( TransformMeta transformMeta ) {
    if ( transformMeta.getCopiesString() == null ) {
      return false;
    }
    String copiesString = transformMeta.getCopiesString();

    String[] keyWords = new String[] { "BEAM_SINGLE", "SINGLE_BEAM", "BEAM_OUTPUT", "OUTPUT" };

    for ( String keyWord : keyWords ) {
      if ( copiesString.equalsIgnoreCase( keyWord ) ) {
        return true;
      }
    }
    return false;
  }


  private List<VariableValue> getVariableValues( IVariables variables ) {

    List<VariableValue> variableValues = new ArrayList<>();
    for ( String variable : variables.listVariables() ) {
      String value = variables.getVariable( variable );
      variableValues.add( new VariableValue( variable, value ) );
    }
    return variableValues;
  }
}
