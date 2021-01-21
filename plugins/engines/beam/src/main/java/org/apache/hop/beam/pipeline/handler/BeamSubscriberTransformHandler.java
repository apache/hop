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
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamSubscribeTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.pubsub.BeamSubscribeMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamSubscriberTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamSubscriberTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, true, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    // A Beam subscriber transform
    //
    BeamSubscribeMeta inputMeta = (BeamSubscribeMeta) transformMeta.getTransform();

    IRowMeta outputRowMeta = pipelineMeta.getTransformFields( variables, transformMeta );
    String rowMetaJson = JsonRowMeta.toJson( outputRowMeta );

    // Verify some things:
    //
    if ( StringUtils.isEmpty( inputMeta.getTopic() ) ) {
      throw new HopException( "Please specify a topic to read from in Beam Pub/Sub Subscribe transform '" + transformMeta.getName() + "'" );
    }

    BeamSubscribeTransform subscribeTransform = new BeamSubscribeTransform(
      transformMeta.getName(),
      transformMeta.getName(),
      variables.resolve( inputMeta.getSubscription() ),
      variables.resolve( inputMeta.getTopic() ),
      inputMeta.getMessageType(),
      rowMetaJson,
      transformPluginClasses,
      xpPluginClasses
    );

    PCollection<HopRow> afterInput = pipeline.apply( subscribeTransform );
    transformCollectionMap.put( transformMeta.getName(), afterInput );

    log.logBasic( "Handled transform (SUBSCRIBE) : " + transformMeta.getName() );
  }
}
