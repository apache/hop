package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;

@ExtensionPoint(
  id = "PipelineStartCheckEnvironmentExtensionPoint",
  description = "At the start of a pipeline, verify it lives in the active environment",
  extensionPointId = "PipelinePrepareExecution"
)
/**
 * validate whether or not the pipeline about to be executed is part of the current environment
 */
public class PipelineStartCheckEnvironmentExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    String filename = pipeline.getFilename();

    try {
      EnvironmentUtil.validateFileInEnvironment( log, filename, pipeline );
    } catch ( Exception e ) {
      throw new HopException( "Validation error against pipeline '" + filename + "' in active environment", e );
    }
  }

}
