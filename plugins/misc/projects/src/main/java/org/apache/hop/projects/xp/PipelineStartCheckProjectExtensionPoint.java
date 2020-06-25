package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;

@ExtensionPoint(
  id = "PipelineStartCheckProjectExtensionPoint",
  description = "At the start of a pipeline, verify it lives in the active project",
  extensionPointId = "PipelinePrepareExecution"
)
/**
 * validate whether or not the pipeline about to be executed is part of the current project
 */
public class PipelineStartCheckProjectExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    String filename = pipeline.getFilename();

    try {
      ProjectsUtil.validateFileInProject( log, filename, pipeline );
    } catch ( Exception e ) {
      throw new HopException( "Validation error against pipeline '" + filename + "' in active project", e );
    }
  }

}
