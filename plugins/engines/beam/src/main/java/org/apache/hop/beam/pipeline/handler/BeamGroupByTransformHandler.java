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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.GroupByTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.memgroupby.MemoryGroupByMeta;

import java.util.List;
import java.util.Map;

public class BeamGroupByTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamGroupByTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    MemoryGroupByMeta meta = new MemoryGroupByMeta();
    loadTransformMetadata(meta, transformMeta, metadataProvider, pipelineMeta);

    String[] aggregates = new String[ meta.getAggregateType().length ];
    for ( int i = 0; i < aggregates.length; i++ ) {
      aggregates[ i ] = MemoryGroupByMeta.getTypeDesc( meta.getAggregateType()[ i ] );
    }

    PTransform<PCollection<HopRow>, PCollection<HopRow>> transformTransform = new GroupByTransform(
      transformMeta.getName(),
      JsonRowMeta.toJson( rowMeta ),  // The io row
      transformPluginClasses,
      xpPluginClasses,
      meta.getGroupField(),
      meta.getSubjectField(),
      aggregates,
      meta.getAggregateField()
    );

    // Apply the transform transform to the previous io transform PCollection(s)
    //
    PCollection<HopRow> transformPCollection = input.apply( transformMeta.getName(), transformTransform );

    // Save this in the map
    //
    transformCollectionMap.put( transformMeta.getName(), transformPCollection );
    log.logBasic( "Handled Group By (TRANSFORM) : " + transformMeta.getName() + ", gets data from " + previousTransforms.size() + " previous transform(s)" );
  }
}
