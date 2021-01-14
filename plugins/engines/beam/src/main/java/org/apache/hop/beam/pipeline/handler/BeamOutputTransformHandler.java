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
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamOutputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.metadata.FieldDefinition;
import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.beam.transforms.io.BeamOutputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamOutputTransformHandler extends BeamBaseTransformHandler implements IBeamTransformHandler {

  public BeamOutputTransformHandler( IVariables variables, IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( variables, runConfiguration, false, true, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleTransform( ILogChannel log, TransformMeta beamOutputTransformMeta, Map<String, PCollection<HopRow>> transformCollectionMap,
                                         Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousTransforms,
                                         PCollection<HopRow> input ) throws HopException {

    BeamOutputMeta beamOutputMeta = (BeamOutputMeta) beamOutputTransformMeta.getTransform();
    FileDefinition outputFileDefinition;
    if ( StringUtils.isEmpty( beamOutputMeta.getFileDefinitionName() ) ) {
      // Create a default file definition using standard output and sane defaults...
      //
      outputFileDefinition = getDefaultFileDefition( beamOutputTransformMeta );
    } else {
      outputFileDefinition = beamOutputMeta.loadFileDefinition( metadataProvider );
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
      beamOutputTransformMeta.getName(),
      variables.resolve( beamOutputMeta.getOutputLocation() ),
      variables.resolve( beamOutputMeta.getFilePrefix() ),
      variables.resolve( beamOutputMeta.getFileSuffix() ),
      variables.resolve( outputFileDefinition.getSeparator() ),
      variables.resolve( outputFileDefinition.getEnclosure() ),
      beamOutputMeta.isWindowed(),
      JsonRowMeta.toJson( rowMeta ),
      transformPluginClasses,
      xpPluginClasses
    );

    // Which transform do we apply this transform to?
    // Ignore info hops until we figure that out.
    //
    if ( previousTransforms.size() > 1 ) {
      throw new HopException( "Combining data from multiple transforms is not supported yet!" );
    }
    TransformMeta previousTransform = previousTransforms.get( 0 );

    // No need to store this, it's PDone.
    //
    input.apply( beamOutputTransform );
    log.logBasic( "Handled transform (OUTPUT) : " + beamOutputTransformMeta.getName() + ", gets data from " + previousTransform.getName() );
  }

  private FileDefinition getDefaultFileDefition( TransformMeta beamOutputTransformMeta ) throws HopTransformException {
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
