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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.BeamDefaults;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.fn.WindowInfoFn;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.window.BeamWindowMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;

public class BeamWindowTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamWindowTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta inputRowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    BeamWindowMeta beamWindowMeta = (BeamWindowMeta) transformMeta.getTransform();

    if ( StringUtils.isEmpty( beamWindowMeta.getWindowType() ) ) {
      throw new HopException( "Please specify a window type in Beam Window transform '" + transformMeta.getName() + "'" );
    }

    String duration = variables.resolve( beamWindowMeta.getDuration() );
    long durationSeconds = Const.toLong( duration, -1L );

    PCollection<HopRow> transformPCollection;

    if ( BeamDefaults.WINDOW_TYPE_FIXED.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new HopException( "Please specify a valid positive window size (duration) for Beam window transform '" + transformMeta.getName() + "'" );
      }

      FixedWindows fixedWindows = FixedWindows
        .of( Duration.standardSeconds( durationSeconds ) );
      transformPCollection = input.apply( Window.into( fixedWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SLIDING.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new HopException( "Please specify a valid positive window size (duration) for Beam window transform '" + transformMeta.getName() + "'" );
      }

      String every = variables.resolve( beamWindowMeta.getEvery() );
      long everySeconds = Const.toLong( every, -1L );

      SlidingWindows slidingWindows = SlidingWindows
        .of( Duration.standardSeconds( durationSeconds ) )
        .every( Duration.standardSeconds( everySeconds ) );
      transformPCollection = input.apply( Window.into( slidingWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SESSION.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds < 600 ) {
        throw new HopException(
          "Please specify a window size (duration) of at least 600 (10 minutes) for Beam window transform '" + transformMeta.getName() + "'.  This is the minimum gap between session windows." );
      }

      Sessions sessionWindows = Sessions
        .withGapDuration( Duration.standardSeconds( durationSeconds ) );
      transformPCollection = input.apply( Window.into( sessionWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_GLOBAL.equals( beamWindowMeta.getWindowType() ) ) {

      transformPCollection = input.apply( Window.into( new GlobalWindows() ) );

    } else {
      throw new HopException( "Beam Window type '" + beamWindowMeta.getWindowType() + " is not supported in transform '" + transformMeta.getName() + "'" );
    }

    // Now get window information about the window if we asked about it...
    //
    if ( StringUtils.isNotEmpty( beamWindowMeta.getStartWindowField() ) ||
      StringUtils.isNotEmpty( beamWindowMeta.getEndWindowField() ) ||
      StringUtils.isNotEmpty( beamWindowMeta.getMaxWindowField() ) ) {

      WindowInfoFn windowInfoFn = new WindowInfoFn(
        transformMeta.getName(),
        variables.resolve( beamWindowMeta.getMaxWindowField() ),
        variables.resolve( beamWindowMeta.getStartWindowField() ),
        variables.resolve( beamWindowMeta.getMaxWindowField() ),
        JsonRowMeta.toJson( inputRowMeta ),
        transformPluginClasses,
        xpPluginClasses
      );

      transformPCollection = transformPCollection.apply( ParDo.of( windowInfoFn ) );
    }

    // Save this in the map
    //
    transformCollectionMap.put( transformMeta.getName(), transformPCollection );
    log.logBasic( "Handled transform (WINDOW) : " + transformMeta.getName() + ", gets data from " + previousTransforms.size() + " previous transform(s)" );
  }
}
