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

    if (!(pipelineEngineRunConfiguration instanceof LocalPipelineRunConfiguration)) {
      throw new HopException( "A local pipeline execution expects a local pipeline configuration, not class "+pipelineEngineRunConfiguration.getClass().getName() );
    }
    LocalPipelineRunConfiguration config = (LocalPipelineRunConfiguration) pipelineEngineRunConfiguration;

    int sizeRowsSet = Const.toInt( pipelineMeta.environmentSubstitute( config.getRowSetSize() ), Const.ROWS_IN_ROWSET );

    setRowSetSize( Const.toInt( environmentSubstitute(config.getRowSetSize()), Const.ROWS_IN_ROWSET) );
    setSafeModeEnabled( config.isSafeModeEnabled() );
    setSortingTransformsTopologically( config.isSortingTransformsTopologically() );
    setGatheringMetrics( config.isGatheringMetrics() );
    setFeedbackShown( config.isFeedbackShown() );
    setFeedbackSize( Const.toInt( environmentSubstitute( config.getFeedbackSize() ), Const.ROWS_UPDATE ) );

    super.prepareExecution();
  }
}
