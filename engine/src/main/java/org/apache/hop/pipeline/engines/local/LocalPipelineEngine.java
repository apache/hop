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

package org.apache.hop.pipeline.engines.local;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.parameters.INamedParams;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;

public class LocalPipelineEngine extends Pipeline implements IPipelineEngine<PipelineMeta> {

  public LocalPipelineEngine() {
    super();
  }

  public LocalPipelineEngine( PipelineMeta pipelineMeta ) {
    super( pipelineMeta );
  }

  public LocalPipelineEngine( PipelineMeta pipelineMeta, ILoggingObject parent ) {
    super( pipelineMeta, parent );
  }

  public <Parent extends IVariables & INamedParams> LocalPipelineEngine( Parent parent, String name, String filename, IMetaStore metaStore ) throws HopException {
    super( parent, name, filename, metaStore );
  }

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new LocalPipelineRunConfiguration();
  }

  @Override public void prepareExecution() throws HopException {

    if (!(pipelineRunConfiguration.getEngineRunConfiguration() instanceof LocalPipelineRunConfiguration)) {
      throw new HopException( "A local pipeline execution expects a local pipeline configuration, not class "+pipelineRunConfiguration.getEngineRunConfiguration().getClass().getName() );
    }
    LocalPipelineRunConfiguration config = (LocalPipelineRunConfiguration) pipelineRunConfiguration.getEngineRunConfiguration();

    int sizeRowsSet = Const.toInt( pipelineMeta.environmentSubstitute( config.getRowSetSize() ), Const.ROWS_IN_ROWSET );
    setRowSetSize( sizeRowsSet );
    setSafeModeEnabled( config.isSafeModeEnabled() );
    setSortingTransformsTopologically( config.isSortingTransformsTopologically() );
    setGatheringMetrics( config.isGatheringMetrics() );
    setFeedbackShown( config.isFeedbackShown() );
    setFeedbackSize( Const.toInt( environmentSubstitute( config.getFeedbackSize() ), Const.ROWS_UPDATE ) );

    super.prepareExecution();
  }
}
