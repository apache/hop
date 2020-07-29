/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.beam.pipeline.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hop.beam.core.HopRow;
import org.apache.hop.beam.core.transform.BeamBQInputTransform;
import org.apache.hop.beam.core.util.JsonRowMeta;
import org.apache.hop.beam.engines.IBeamPipelineEngineRunConfiguration;
import org.apache.hop.beam.transforms.bq.BeamBQInputMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;

import java.util.List;
import java.util.Map;

public class BeamBigQueryInputStepHandler extends BeamBaseStepHandler implements IBeamStepHandler {

  public BeamBigQueryInputStepHandler( IBeamPipelineEngineRunConfiguration runConfiguration, IHopMetadataProvider metadataProvider, PipelineMeta pipelineMeta, List<String> transformPluginClasses, List<String> xpPluginClasses ) {
    super( runConfiguration, true, false, metadataProvider, pipelineMeta, transformPluginClasses, xpPluginClasses );
  }

  @Override public void handleStep( ILogChannel log, TransformMeta transformMeta, Map<String, PCollection<HopRow>> stepCollectionMap,
                                    Pipeline pipeline, IRowMeta rowMeta, List<TransformMeta> previousSteps,
                                    PCollection<HopRow> input ) throws HopException {

    // Input handling
    //
    BeamBQInputMeta beamInputMeta = (BeamBQInputMeta) transformMeta.getTransform();

    // Output rows (fields selection)
    //
    IRowMeta outputRowMeta = new RowMeta();
    beamInputMeta.getFields( outputRowMeta, transformMeta.getName(), null, null, pipelineMeta, null );

    BeamBQInputTransform beamInputTransform = new BeamBQInputTransform(
      transformMeta.getName(),
      transformMeta.getName(),
      pipelineMeta.environmentSubstitute( beamInputMeta.getProjectId() ),
      pipelineMeta.environmentSubstitute( beamInputMeta.getDatasetId() ),
      pipelineMeta.environmentSubstitute( beamInputMeta.getTableId() ),
      pipelineMeta.environmentSubstitute( beamInputMeta.getQuery() ),
      JsonRowMeta.toJson( outputRowMeta ),
      transformPluginClasses,
      xpPluginClasses
    );
    PCollection<HopRow> afterInput = pipeline.apply( beamInputTransform );
    stepCollectionMap.put( transformMeta.getName(), afterInput );
    log.logBasic( "Handled transform (BQ INPUT) : " + transformMeta.getName() );

  }
}
