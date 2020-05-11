package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamKafkaOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.kafka.BeamProduceMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamKafkaOutputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamKafkaOutputStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, true, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta beamOutputStepMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    BeamProduceMeta beamProduceMeta = (BeamProduceMeta) beamOutputStepMeta.getTransform();

    BeamKafkaOutputTransform beamOutputTransform = new BeamKafkaOutputTransform(
      beamOutputStepMeta.getName(),
      pipelineMeta.environmentSubstitute( beamProduceMeta.getBootstrapServers() ),
      pipelineMeta.environmentSubstitute( beamProduceMeta.getTopic() ),
      pipelineMeta.environmentSubstitute( beamProduceMeta.getKeyField() ),
      pipelineMeta.environmentSubstitute( beamProduceMeta.getMessageField() ),
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
    log.logBasic( "Handled transform (KAFKA OUTPUT) : " + beamOutputStepMeta.getName() + ", gets data from " + previousStep.getName() );
  }
}
