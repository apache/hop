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
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamSubscriberStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamSubscriberStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, true, false, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    // A Beam subscriber transform
    //
    BeamSubscribeMeta inputMeta = (BeamSubscribeMeta) transformMeta.getTransform();

    IRowMeta outputRowMeta = pipelineMeta.getTransformFields( transformMeta );
    String rowMetaJson = JsonRowMeta.toJson( outputRowMeta );

    // Verify some things:
    //
    if ( StringUtils.isEmpty( inputMeta.getTopic() ) ) {
      throw new HopException( "Please specify a topic to read from in Beam Pub/Sub Subscribe transform '" + transformMeta.getName() + "'" );
    }

    BeamSubscribeTransform subscribeTransform = new BeamSubscribeTransform(
      transformMeta.getName(),
      transformMeta.getName(),
      pipelineMeta.environmentSubstitute( inputMeta.getSubscription() ),
      pipelineMeta.environmentSubstitute( inputMeta.getTopic() ),
      inputMeta.getMessageType(),
      rowMetaJson,
      transformPluginClasses,
      xpPluginClasses
    );

    PCollection<HopRow> afterInput = pipeline.apply( subscribeTransform );
    stepCollectionMap.put( transformMeta.getName(), afterInput );

    log.logBasic( "Handled transform (SUBSCRIBE) : " + transformMeta.getName() );
  }
}
