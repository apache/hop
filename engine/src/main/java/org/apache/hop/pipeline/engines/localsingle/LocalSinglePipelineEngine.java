package org.apache.hop.pipeline.engines.localsingle;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.SingleThreadedPipelineExecutor;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.config.IPipelineEngineRunConfiguration;
import org.apache.hop.pipeline.engine.IPipelineEngine;

public class LocalSinglePipelineEngine extends Pipeline implements IPipelineEngine<PipelineMeta> {

  public LocalSinglePipelineEngine() {
    super();
  }

  public LocalSinglePipelineEngine( PipelineMeta pipelineMeta ) {
    super( pipelineMeta );
  }

  public LocalSinglePipelineEngine( PipelineMeta pipelineMeta, LoggingObjectInterface parent ) {
    super( pipelineMeta, parent );
  }

  public <Parent extends VariableSpace & NamedParams> LocalSinglePipelineEngine( Parent parent, String name, String filename, IMetaStore metaStore ) throws HopException {
    super( parent, name, filename, metaStore );
  }

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new LocalSinglePipelineRunConfiguration();
  }

  @Override public void prepareExecution() throws HopException {
    pipelineMeta.setPipelineType( PipelineMeta.PipelineType.SingleThreaded );
    super.prepareExecution();
  }

  @Override public void startThreads() throws HopException {
    super.startThreads();

    SingleThreadedPipelineExecutor executor = new SingleThreadedPipelineExecutor( this );

    if (!executor.init()) {
      throw new HopException( "Error initializing single threaded pipeline execution. See the log for more details." );
    }

    // Iterate until done.
    //
    while (executor.oneIteration() && !isStopped());

  }
}
