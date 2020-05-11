package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamInputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamInputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamInputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamInputStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, true, false, metaStore, pipelineMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    // Input handling
    //
    BeamInputMeta beamInputMeta = (BeamInputMeta) transformMeta.getTransform();
    FileDefinition inputFileDefinition = beamInputMeta.loadFileDefinition( metaStore );
    IRowMeta fileRowMeta = inputFileDefinition.getRowMeta();

    // Apply the PBegin to HopRow transform:
    //
    if ( inputFileDefinition == null ) {
      throw new HopException( "We couldn't find or load the Beam Input transform file definition" );
    }
    String fileInputLocation = pipelineMeta.environmentSubstitute( beamInputMeta.getInputLocation() );

    BeamInputTransform beamInputTransform = new BeamInputTransform(
      transformMeta.getName(),
      transformMeta.getName(),
      fileInputLocation,
      pipelineMeta.environmentSubstitute( inputFileDefinition.getSeparator() ),
      JsonRowMeta.toJson( fileRowMeta ),
      stepPluginClasses,
      xpPluginClasses
    );
    PCollection<HopRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( transformMeta.getName(), afterInput );
    log.logBasic( "Handled transform (INPUT) : " + transformMeta.getName() );

  }
}
