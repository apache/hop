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
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamTimestampStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamTimestampStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, false, metaStore, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    BeamTimestampMeta beamTimestampMeta = (BeamTimestampMeta) transformMeta.getTransform();

    if ( !beamTimestampMeta.isReadingTimestamp() && StringUtils.isNotEmpty( beamTimestampMeta.getFieldName() ) ) {
      if ( rowMeta.searchValueMeta( beamTimestampMeta.getFieldName() ) == null ) {
        throw new HopException( "Please specify a valid field name '" + transformMeta.getName() + "'" );
      }
    }

    PCollection<HopRow> stepPCollection = input.apply( ParDo.of(
      new TimestampFn(
        transformMeta.getName(),
        JsonRowMeta.toJson( rowMeta ),
        pipelineMeta.environmentSubstitute( beamTimestampMeta.getFieldName() ),
        beamTimestampMeta.isReadingTimestamp(),
        transformPluginClasses,
        xpPluginClasses
      ) ) );


    // Save this in the map
    //
    stepCollectionMap.put( transformMeta.getName(), stepPCollection );
    log.logBasic( "Handled transform (TIMESTAMP) : " + transformMeta.getName() + ", gets data from " + previousSteps.size() + " previous transform(s)" );
  }
}
