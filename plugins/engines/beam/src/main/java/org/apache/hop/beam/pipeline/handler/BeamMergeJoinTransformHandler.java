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

package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.AssemblerFn;
import org.apache.hop.beam.core.fn.HopKeyValueFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.mergejoin.MergeJoinMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BeamMergeJoinTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamMergeJoinTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  public boolean isInput() {
    return false;
  }

  public boolean isOutput() {
    return false;
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    // Don't simply case but serialize/de-serialize the metadata to prevent classloader exceptions
    //
    MergeJoinMeta meta = new MergeJoinMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String joinType = meta.getJoinType();
    String[] leftKeys = meta.getKeyFields1();
    String[] rightKeys = meta.getKeyFields2();

    TransformMeta leftInfoTransform = meta.getTransformIOMeta().getInfoStreams().get( 0 ).getTransformMeta();
    if ( leftInfoTransform == null ) {
      throw new HopException( "The left source transform isn't defined in the Merge Join transform called '" + transformMeta.getName() + "'" );
    }
    PCollection<HopRow> leftPCollection = transformCollectionMap.get( leftInfoTransform.getName() );
    if ( leftPCollection == null ) {
      throw new HopException( "The left source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    IRowMeta leftRowMeta = pipelineMeta.getTransformFields( variables, leftInfoTransform );

    TransformMeta rightInfoTransform = meta.getTransformIOMeta().getInfoStreams().get( 1 ).getTransformMeta();
    if ( rightInfoTransform == null ) {
      throw new HopException( "The right source transform isn't defined in the Merge Join transform called '" + transformMeta.getName() + "'" );
    }
    PCollection<HopRow> rightPCollection = transformCollectionMap.get( rightInfoTransform.getName() );
    if ( rightPCollection == null ) {
      throw new HopException( "The right source collection in the pipeline couldn't be found (probably a programming error)" );
    }
    IRowMeta rightRowMeta = pipelineMeta.getTransformFields( variables, rightInfoTransform );

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

    HopKeyValueFn leftKVFn = new HopKeyValueFn(
      JsonRowMeta.toJson( leftRowMeta ), transformPluginClasses, xpPluginClasses, leftK.toArray( new String[ 0 ] ), leftV.toArray( new String[ 0 ] ), transformMeta.getName() );
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

    HopKeyValueFn rightKVFn = new HopKeyValueFn(
      JsonRowMeta.toJson( rightRowMeta ), transformPluginClasses, xpPluginClasses, rightK.toArray( new String[ 0 ] ), rightV.toArray( new String[ 0 ] ), transformMeta.getName() );
    PCollection<KV<HopRow, HopRow>> rightKVPCollection = rightPCollection.apply( ParDo.of( rightKVFn ) );

    PCollection<KV<HopRow, KV<HopRow, HopRow>>> kvpCollection;

    Object[] leftNull = RowDataUtil.allocateRowData( leftVRowMeta.size() );
    Object[] rightNull = RowDataUtil.allocateRowData( rightVRowMeta.size() );

    if ( MergeJoinMeta.joinTypes[ 0 ].equals( joinType ) ) {
      // Inner Join
      //
      kvpCollection = Join.innerJoin( leftKVPCollection, rightKVPCollection );
    } else if ( MergeJoinMeta.joinTypes[ 1 ].equals( joinType ) ) {
      // Left outer join
      //
      kvpCollection = Join.leftOuterJoin( leftKVPCollection, rightKVPCollection, new HopRow( rightNull ) );
    } else if ( MergeJoinMeta.joinTypes[ 2 ].equals( joinType ) ) {
      // Right outer join
      //
      kvpCollection = Join.rightOuterJoin( leftKVPCollection, rightKVPCollection, new HopRow( leftNull ) );
    } else if ( MergeJoinMeta.joinTypes[ 3 ].equals( joinType ) ) {
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
      transformPluginClasses,
      xpPluginClasses
    );

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> transformPCollection = kvpCollection.apply( ParDo.of( assemblerFn ) );

    // Save this in the map
    //
    transformCollectionMap.put( transformMeta.getName(), transformPCollection );

    log.logBasic( "Handled Merge Join (TRANSFORM) : " + transformMeta.getName() );
  }
}
