package org.apache.hop.trans.engines.local;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.config.IPipelineEngineRunConfiguration;
import org.apache.hop.trans.engine.IPipelineEngine;

public class LocalPipelineEngine extends Trans implements IPipelineEngine<TransMeta> {

  public LocalPipelineEngine() {
    super();
  }

  public LocalPipelineEngine( TransMeta transMeta ) {
    super( transMeta );
  }

  public LocalPipelineEngine( TransMeta transMeta, LoggingObjectInterface parent ) {
    super( transMeta, parent );
  }

  public <Parent extends VariableSpace & NamedParams> LocalPipelineEngine( Parent parent, String name, String filename, IMetaStore metaStore ) throws HopException {
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

    int sizeRowsSet = Const.toInt( transMeta.environmentSubstitute( config.getRowSetSize() ), Const.ROWS_IN_ROWSET );

    setRowSetSize( Const.toInt( environmentSubstitute(config.getRowSetSize()), Const.ROWS_IN_ROWSET) );
    setSafeModeEnabled( config.isSafeModeEnabled() );
    setSortingStepsTopologically( config.isSortingStepsTopologically() );
    setGatheringMetrics( config.isGatheringMetrics() );
    setFeedbackShown( config.isFeedbackShown() );
    setFeedbackSize( Const.toInt( environmentSubstitute( config.getFeedbackSize() ), Const.ROWS_UPDATE ) );

    super.prepareExecution();
  }
}
