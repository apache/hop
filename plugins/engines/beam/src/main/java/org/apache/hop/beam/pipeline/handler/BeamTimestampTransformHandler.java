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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.TimestampFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.window.BeamTimestampMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamTimestampTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamTimestampTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    BeamTimestampMeta beamTimestampMeta = (BeamTimestampMeta) transformMeta.getTransform();

    if ( !beamTimestampMeta.isReadingTimestamp() && StringUtils.isNotEmpty( beamTimestampMeta.getFieldName() ) ) {
      if ( rowMeta.searchValueMeta( beamTimestampMeta.getFieldName() ) == null ) {
        throw new HopException( "Please specify a valid field name '" + transformMeta.getName() + "'" );
      }
    }

    PCollection<HopRow> transformPCollection = input.apply( ParDo.of(
      new TimestampFn(
        transformMeta.getName(),
        JsonRowMeta.toJson( rowMeta ),
        variables.resolve( beamTimestampMeta.getFieldName() ),
        beamTimestampMeta.isReadingTimestamp(),
        transformPluginClasses,
        xpPluginClasses
      ) ) );


    // Save this in the map
    //
    transformCollectionMap.put( transformMeta.getName(), transformPCollection );
    log.logBasic( "Handled transform (TIMESTAMP) : " + transformMeta.getName() + ", gets data from " + previousTransforms.size() + " previous transform(s)" );
  }
}
