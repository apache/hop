package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamPublishTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.pubsub.BeamPublishMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamPublisherStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamPublisherStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, true, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    BeamPublishMeta publishMeta = (BeamPublishMeta) transformMeta.getTransform();

    // some validation
    //
    if ( StringUtils.isEmpty( publishMeta.getTopic() ) ) {
      throw new HopException( "Please specify a topic to publish to in Beam Pub/Sub Publish transform '" + transformMeta.getName() + "'" );
    }

    BeamPublishTransform beamOutputTransform = new BeamPublishTransform(
      transformMeta.getName(),
      pipelineMeta.environmentSubstitute( publishMeta.getTopic() ),
      publishMeta.getMessageType(),
      publishMeta.getMessageField(),
      JsonRowMeta.toJson( rowMeta ),
      transformPluginClasses,
      xpPluginClasses
    );

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if ( previousSteps.size() > 1 ) {
      throw new HopException( "Combining data from multiple transforms is not supported yet!" );
    }
    TransformMeta previousStep = previousSteps.get( 0 );

    // No need to store this, it's PDone.
    //
    input.apply( beamOutputTransform );
    log.logBasic( "Handled transform (PUBLISH) : " + transformMeta.getName() + ", gets data from " + previousStep.getName() );
  }
}
