package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamKafkaInputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.kafka.BeamConsumeMeta;
import org.apache.hop.beam.transforms.kafka.ConfigOption;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamKafkaInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamKafkaInputStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, true, false, metaStore, pipelineMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    // Input handling
    //
    BeamConsumeMeta beamConsumeMeta = (BeamConsumeMeta) transformMeta.getTransform();

    // Output rows (fields selection)
    //
    IRowMeta outputRowMeta = new RowMeta();
    beamConsumeMeta.getFields( outputRowMeta, transformMeta.getName(), null, null, pipelineMeta, null );

    String[] parameters = new String[beamConsumeMeta.getConfigOptions().size()];
    String[] values = new String[beamConsumeMeta.getConfigOptions().size()];
    String[] types = new String[beamConsumeMeta.getConfigOptions().size()];
    for (int i=0;i<parameters.length;i++) {
      ConfigOption option = beamConsumeMeta.getConfigOptions().get( i );
      parameters[i] = pipelineMeta.environmentSubstitute( option.getParameter() );
      values[i] = pipelineMeta.environmentSubstitute( option.getValue() );
      types[i] = option.getType()==null ? ConfigOption.Type.String.name() : option.getType().name();
    }

    BeamKafkaInputTransform beamInputTransform = new BeamKafkaInputTransform(
      transformMeta.getName(),
      transformMeta.getName(),
      pipelineMeta.environmentSubstitute( beamConsumeMeta.getBootstrapServers() ),
      pipelineMeta.environmentSubstitute( beamConsumeMeta.getTopics() ),
      pipelineMeta.environmentSubstitute( beamConsumeMeta.getGroupId() ),
      beamConsumeMeta.isUsingProcessingTime(),
      beamConsumeMeta.isUsingLogAppendTime(),
      beamConsumeMeta.isUsingCreateTime(),
      beamConsumeMeta.isRestrictedToCommitted(),
      beamConsumeMeta.isAllowingCommitOnConsumedOffset(),
      parameters,
      values,
      types,
      JsonRowMeta.toJson( outputRowMeta ),
      stepPluginClasses,
      xpPluginClasses
    );
    PCollection<HopRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( transformMeta.getName(), afterInput );
    log.logBasic( "Handled transform (KAFKA INPUT) : " + transformMeta.getName() );
  }
}
