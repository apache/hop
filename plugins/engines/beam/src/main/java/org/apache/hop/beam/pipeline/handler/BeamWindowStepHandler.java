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
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.joda.time.Duration;

import java.util.List;
import java.util.Map;

public class BeamWindowStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamWindowStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, false, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta inputRowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    BeamWindowMeta beamWindowMeta = (BeamWindowMeta) transformMeta.getTransform();

    if ( StringUtils.isEmpty( beamWindowMeta.getWindowType() ) ) {
      throw new HopException( "Please specify a window type in Beam Window transform '" + transformMeta.getName() + "'" );
    }

    String duration = pipelineMeta.environmentSubstitute( beamWindowMeta.getDuration() );
    long durationSeconds = Const.toLong( duration, -1L );

    PCollection<HopRow> stepPCollection;

    if ( BeamDefaults.WINDOW_TYPE_FIXED.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new HopException( "Please specify a valid positive window size (duration) for Beam window transform '" + transformMeta.getName() + "'" );
      }

      FixedWindows fixedWindows = FixedWindows
        .of( Duration.standardSeconds( durationSeconds ) );
      stepPCollection = input.apply( Window.into( fixedWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SLIDING.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds <= 0 ) {
        throw new HopException( "Please specify a valid positive window size (duration) for Beam window transform '" + transformMeta.getName() + "'" );
      }

      String every = pipelineMeta.environmentSubstitute( beamWindowMeta.getEvery() );
      long everySeconds = Const.toLong( every, -1L );

      SlidingWindows slidingWindows = SlidingWindows
        .of( Duration.standardSeconds( durationSeconds ) )
        .every( Duration.standardSeconds( everySeconds ) );
      stepPCollection = input.apply( Window.into( slidingWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_SESSION.equals( beamWindowMeta.getWindowType() ) ) {

      if ( durationSeconds < 600 ) {
        throw new HopException(
          "Please specify a window size (duration) of at least 600 (10 minutes) for Beam window transform '" + transformMeta.getName() + "'.  This is the minimum gap between session windows." );
      }

      Sessions sessionWindows = Sessions
        .withGapDuration( Duration.standardSeconds( durationSeconds ) );
      stepPCollection = input.apply( Window.into( sessionWindows ) );

    } else if ( BeamDefaults.WINDOW_TYPE_GLOBAL.equals( beamWindowMeta.getWindowType() ) ) {

      stepPCollection = input.apply( Window.into( new GlobalWindows() ) );

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
        pipelineMeta.environmentSubstitute( beamWindowMeta.getMaxWindowField() ),
        pipelineMeta.environmentSubstitute( beamWindowMeta.getStartWindowField() ),
        pipelineMeta.environmentSubstitute( beamWindowMeta.getMaxWindowField() ),
        JsonRowMeta.toJson( inputRowMeta ),
        transformPluginClasses,
        xpPluginClasses
      );

      stepPCollection = stepPCollection.apply( ParDo.of( windowInfoFn ) );
    }

    // Save this in the map
    //
    stepCollectionMap.put( transformMeta.getName(), stepPCollection );
    log.logBasic( "Handled transform (WINDOW) : " + transformMeta.getName() + ", gets data from " + previousSteps.size() + " previous transform(s)" );
  }
}
