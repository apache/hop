package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamBQOutputTransform;
import org.apache.hop.beam.core.transform.BeamOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.metastore.BeamJobConfig;
import org.apache.hop.beam.metastore.FieldDefinition;
import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.beam.transforms.bq.BeamBQOutputMeta;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.metastore.api.IMetaStore;

import java.util.List;
import java.util.Map;

public class BeamBigQueryOutputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamBigQueryOutputStepHandler( BeamJobConfig beamJobConfig, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( beamJobConfig, false, true, metaStore, pipelineMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta beamOutputStepMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input  ) throws HopException {

    BeamBQOutputMeta beamOutputMeta = (BeamBQOutputMeta) beamOutputStepMeta.getTransform();

    BeamBQOutputTransform beamOutputTransform = new BeamBQOutputTransform(
      beamOutputStepMeta.getName(),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getProjectId() ),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getDatasetId() ),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getTableId() ),
      beamOutputMeta.isCreatingIfNeeded(),
      beamOutputMeta.isTruncatingTable(),
      beamOutputMeta.isFailingIfNotEmpty(),
      JsonRowMeta.toJson(rowMeta),
      stepPluginClasses,
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
    log.logBasic( "Handled transform (BQ OUTPUT) : " + beamOutputStepMeta.getName() + ", gets data from " + previousStep.getName() );
  }
}
