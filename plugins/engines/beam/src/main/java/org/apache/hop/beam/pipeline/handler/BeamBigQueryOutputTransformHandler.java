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
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamBQOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.bq.BeamBQOutputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamBigQueryOutputTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamBigQueryOutputTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, true, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta beamOutputTransformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input  ) throws HopException {

    BeamBQOutputMeta beamOutputMeta = (BeamBQOutputMeta) beamOutputTransformMeta.getTransform();

    BeamBQOutputTransform beamOutputTransform = new BeamBQOutputTransform(
      beamOutputTransformMeta.getName(),
      variables.resolve( beamOutputMeta.getProjectId() ),
      variables.resolve( beamOutputMeta.getDatasetId() ),
      variables.resolve( beamOutputMeta.getTableId() ),
      beamOutputMeta.isCreatingIfNeeded(),
      beamOutputMeta.isTruncatingTable(),
      beamOutputMeta.isFailingIfNotEmpty(),
      JsonRowMeta.toJson(rowMeta),
      transformPluginClasses,
      xpPluginClasses
    );

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if ( previousTransforms.size() > 1 ) {
      throw new HopException( "Combining data from multiple transforms is not supported yet!" );
    }
    TransformMeta previousTransform = previousTransforms.get( 0 );

    // No need to store this, it's PDone.
    //
    input.apply( beamOutputTransform );
    log.logBasic( "Handled transform (BQ OUTPUT) : " + beamOutputTransformMeta.getName() + ", gets data from " + previousTransform.getName() );
  }
}
