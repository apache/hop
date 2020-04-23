package org.apache.hop.env.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.env.util.EnvironmentUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
  id = "WorkflowStartCheckEnvironmentExtensionPoint",
  description = "At the start of a job, verify it lives in the active environment",
  extensionPointId = "JobStart"
)
/**
 * set the debug level right before the step starts to run
 */
public class WorkflowStartCheckEnvironmentExtensionPoint implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object object ) throws HopException {

    if ( !( object instanceof IWorkflowEngine ) ) {
      return;
    }

    IWorkflowEngine workflow = (IWorkflowEngine) object;

    String filename = workflow.getFilename();

    try {
      EnvironmentUtil.validateFileInEnvironment( log, filename, workflow );
    } catch ( Exception e ) {
      throw new HopException( "Validation error against workflow '" + filename + "' in active environment", e );
    }
  }
}
