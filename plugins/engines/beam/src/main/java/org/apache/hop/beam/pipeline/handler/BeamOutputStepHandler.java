package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metastore.FieldDefinition;
import org.apache.hop.beam.metastore.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamOutputStepHandler extends BeamBaseStepHandler implements BeamStepHandler {

  public BeamOutputStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IMetaStore metaStore, PipelineMeta pipelineMeta, List<String> stepPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, false, true, metaStore, pipelineMeta, stepPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta beamOutputStepMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputStepMeta.getTransform();
    FileDefinition outputFileDefinition;
    if ( StringUtils.isEmpty( beamOutputMeta.getFileDescriptionName() ) ) {
      // Create a default file definition using standard output and sane defaults...
      //
      outputFileDefinition = getDefaultFileDefition( beamOutputStepMeta );
    } else {
      outputFileDefinition = beamOutputMeta.loadFileDefinition( metaStore );
    }

    // Empty file definition? Add all fields in the output
    //
    addAllFieldsToEmptyFileDefinition( rowMeta, outputFileDefinition );

    // Apply the output transform from HopRow to PDone
    //
    if ( outputFileDefinition == null ) {
      throw new HopException( "We couldn't find or load the Beam Output transform file definition" );
    }
    if ( rowMeta == null || rowMeta.isEmpty() ) {
      throw new HopException( "No output fields found in the file definition or from previous transforms" );
    }

    BeamOutputTransform beamOutputTransform = new BeamOutputTransform(
      beamOutputStepMeta.getName(),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getOutputLocation() ),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getFilePrefix() ),
      pipelineMeta.environmentSubstitute( beamOutputMeta.getFileSuffix() ),
      pipelineMeta.environmentSubstitute( outputFileDefinition.getSeparator() ),
      pipelineMeta.environmentSubstitute( outputFileDefinition.getEnclosure() ),
      beamOutputMeta.isWindowed(),
      JsonRowMeta.toJson( rowMeta ),
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
    log.logBasic( "Handled transform (OUTPUT) : " + beamOutputStepMeta.getName() + ", gets data from " + previousStep.getName() );
  }

  private FileDefinition getDefaultFileDefition( TransformMeta beamOutputStepMeta ) throws HopTransformException {
    FileDefinition fileDefinition = new FileDefinition();

    fileDefinition.setName( "Default" );
    fileDefinition.setEnclosure( "\"" );
    fileDefinition.setSeparator( "," );

    return fileDefinition;
  }

  private void addAllFieldsToEmptyFileDefinition( IRowMeta rowMeta, FileDefinition fileDefinition ) throws HopTransformException {
    if ( fileDefinition.getFieldDefinitions().isEmpty() ) {
      for ( IValueMeta valueMeta : rowMeta.getValueMetaList() ) {
        fileDefinition.getFieldDefinitions().add( new FieldDefinition(
            valueMeta.getName(),
            valueMeta.getTypeDesc(),
            valueMeta.getLength(),
            valueMeta.getPrecision(),
            valueMeta.getConversionMask()
          )
        );
      }
    }
  }

}
