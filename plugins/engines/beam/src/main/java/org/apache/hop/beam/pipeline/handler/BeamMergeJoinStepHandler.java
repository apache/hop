package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.AssemblerFn;
import org.apache.hop.beam.core.fn.KettleKeyValueFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BeamMergeJoinStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamMergeJoinStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, false, metaStore, pipelineMeta, stepPluginClasses, xpPluginClasses );
  }

  public boolean isInput() {
    return false;
  }

  public boolean isOutput() {
    return false;
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    MergeJoinMeta meta = (MergeJoinMeta) transformMeta.getTransform();

    String joinType = meta.getJoinType();
    String[] leftKeys = meta.getKeyFields1();
    String[] rightKeys = meta.getKeyFields2();

    TransformMeta leftInfoStep = meta.getTransformIOMeta().getInfoStreams().get( 0 ).getTransformMeta();
    if ( leftInfoStep == null ) {
      throw new HopException( "The left source transform isn't defined in the Merge Join transform called '" + transformMeta.getName() + "'" );
    }
    PCollection<HopRow> leftPCollection = stepCollectionMap.get( leftInfoStep.getName() );
    if ( leftPCollection == null ) {
      throw new HopException( "The left source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    IRowMeta leftRowMeta = pipelineMeta.getTransformFields( leftInfoStep );

    TransformMeta rightInfoStep = meta.getTransformIOMeta().getInfoStreams().get( 1 ).getTransformMeta();
    if ( rightInfoStep == null ) {
      throw new HopException( "The right source transform isn't defined in the Merge Join transform called '" + transformMeta.getName() + "'" );
    }
    PCollection<HopRow> rightPCollection = stepCollectionMap.get( rightInfoStep.getName() );
    if ( rightPCollection == null ) {
      throw new HopException( "The right source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    IRowMeta rightRowMeta = pipelineMeta.getTransformFields( rightInfoStep );

    // Create key-value pairs (KV) for the left collections
    //
    List<String> leftK = new ArrayList<>( Arrays.asList( leftKeys ) );
    IRowMeta leftKRowMeta = new RowMeta();
    List<String> leftV = new ArrayList<>();
    IRowMeta leftVRowMeta = new RowMeta();
    for ( String leftKey : leftKeys ) {
      leftKRowMeta.addValueMeta( leftRowMeta.searchValueMeta( leftKey ).clone() );
    }
    for ( IValueMeta valueMeta : leftRowMeta.getValueMetaList() ) {
      String valueName = valueMeta.getName();
      if ( Const.indexOfString( valueName, leftKeys ) < 0 ) {
        leftV.add( valueName );
        leftVRowMeta.addValueMeta( valueMeta.clone() );
      }
    }

    KettleKeyValueFn leftKVFn = new KettleKeyValueFn(
      JsonRowMeta.toJson( leftRowMeta ), stepPluginClasses, xpPluginClasses, leftK.toArray( new String[ 0 ] ), leftV.toArray( new String[ 0 ] ), transformMeta.getName() );
    PCollection<KV<HopRow, HopRow>> leftKVPCollection = leftPCollection.apply( ParDo.of( leftKVFn ) );

    // Create key-value pairs (KV) for the left collections
    //
    List<String> rightK = new ArrayList<>( Arrays.asList( rightKeys ) );
    IRowMeta rightKRowMeta = new RowMeta();
    List<String> rightV = new ArrayList<>();
    IRowMeta rightVRowMeta = new RowMeta();
    for ( String rightKey : rightKeys ) {
      rightKRowMeta.addValueMeta( rightRowMeta.searchValueMeta( rightKey ).clone() );
    }
    for ( IValueMeta valueMeta : rightRowMeta.getValueMetaList() ) {
      String valueName = valueMeta.getName();
      if ( Const.indexOfString( valueName, rightKeys ) < 0 ) {
        rightV.add( valueName );
        rightVRowMeta.addValueMeta( valueMeta.clone() );
      }
    }

    KettleKeyValueFn rightKVFn = new KettleKeyValueFn(
      JsonRowMeta.toJson( rightRowMeta ), stepPluginClasses, xpPluginClasses, rightK.toArray( new String[ 0 ] ), rightV.toArray( new String[ 0 ] ), transformMeta.getName() );
    PCollection<KV<HopRow, HopRow>> rightKVPCollection = rightPCollection.apply( ParDo.of( rightKVFn ) );

    PCollection<KV<HopRow, KV<HopRow, HopRow>>> kvpCollection;

    Object[] leftNull = RowDataUtil.allocateRowData( leftVRowMeta.size() );
    Object[] rightNull = RowDataUtil.allocateRowData( rightVRowMeta.size() );

    if ( MergeJoinMeta.join_types[ 0 ].equals( joinType ) ) {
      // Inner Join
      //
      kvpCollection = Join.innerJoin( leftKVPCollection, rightKVPCollection );
    } else if ( MergeJoinMeta.join_types[ 1 ].equals( joinType ) ) {
      // Left outer join
      //
      kvpCollection = Join.leftOuterJoin( leftKVPCollection, rightKVPCollection, new HopRow( rightNull ) );
    } else if ( MergeJoinMeta.join_types[ 2 ].equals( joinType ) ) {
      // Right outer join
      //
      kvpCollection = Join.rightOuterJoin( leftKVPCollection, rightKVPCollection, new HopRow( leftNull ) );
    } else if ( MergeJoinMeta.join_types[ 3 ].equals( joinType ) ) {
      // Full outer join
      //
      kvpCollection = Join.fullOuterJoin( leftKVPCollection, rightKVPCollection, new HopRow( leftNull ), new HopRow( rightNull ) );
    } else {
      throw new HopException( "Join type '" + joinType + "' is not recognized or supported" );
    }

    // This is the output of the transform, we'll try to mimic this
    //
    final IRowMeta outputRowMeta = leftVRowMeta.clone();
    outputRowMeta.addRowMeta( leftKRowMeta );
    outputRowMeta.addRowMeta( rightKRowMeta );
    outputRowMeta.addRowMeta( rightVRowMeta );

    // Now we need to collapse the results where we have a Key-Value pair of
    // The key (left or right depending but the same row metadata (leftKRowMeta == rightKRowMeta)
    //    The key is simply a HopRow
    // The value:
    //    The value is the resulting combination of the Value parts of the left and right side.
    //    These can be null depending on the join type
    // So we want to grab all this information and put it back together on a single row.
    //
    DoFn<KV<HopRow, KV<HopRow, HopRow>>, HopRow> assemblerFn = new AssemblerFn(
      JsonRowMeta.toJson( outputRowMeta ),
      JsonRowMeta.toJson( leftKRowMeta ),
      JsonRowMeta.toJson( leftVRowMeta ),
      JsonRowMeta.toJson( rightVRowMeta ),
      transformMeta.getName(),
      stepPluginClasses,
      xpPluginClasses
    );

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> stepPCollection = kvpCollection.apply( ParDo.of( assemblerFn ) );

    // Save this in the map
    //
    stepCollectionMap.put( transformMeta.getName(), stepPCollection );

    log.logBasic( "Handled Merge Join (STEP) : " + transformMeta.getName() );
  }
}
