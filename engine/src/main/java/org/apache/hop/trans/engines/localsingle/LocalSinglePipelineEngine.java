package org.apache.hop.trans.engines.localsingle;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObjectInterface;
import org.apache.hop.core.parameters.NamedParams;
import org.apache.hop.core.variables.VariableSpace;
import org.apache.hop.metastore.api.IMetaStore;
import org.apache.hop.trans.SingleThreadedTransExecutor;
import org.apache.hop.trans.Trans;
import org.apache.hop.trans.TransMeta;
import org.apache.hop.trans.config.IPipelineEngineRunConfiguration;
import org.apache.hop.trans.engine.IPipelineEngine;
import org.apache.hop.trans.engines.local.LocalPipelineRunConfiguration;

public class LocalSinglePipelineEngine extends Trans implements IPipelineEngine<TransMeta> {

  public LocalSinglePipelineEngine() {
    super();
  }

  public LocalSinglePipelineEngine( TransMeta transMeta ) {
    super( transMeta );
  }

  public LocalSinglePipelineEngine( TransMeta transMeta, LoggingObjectInterface parent ) {
    super( transMeta, parent );
  }

  public <Parent extends VariableSpace & NamedParams> LocalSinglePipelineEngine( Parent parent, String name, String filename, IMetaStore metaStore ) throws HopException {
    super( parent, name, filename, metaStore );
  }

  @Override public IPipelineEngineRunConfiguration createDefaultPipelineEngineRunConfiguration() {
    return new LocalSinglePipelineRunConfiguration();
  }

  @Override public void prepareExecution() throws HopException {
    transMeta.setTransformationType( TransMeta.TransformationType.SingleThreaded );
    super.prepareExecution();
  }

  @Override public void startThreads() throws HopException {
    super.startThreads();

    SingleThreadedTransExecutor executor = new SingleThreadedTransExecutor( this );

    if (!executor.init()) {
      throw new HopException( "Error initializing single threaded pipeline execution. See the log for more details." );
    }

    // Iterate until done.
    //
    while (executor.oneIteration() && !isStopped());

  }
}
