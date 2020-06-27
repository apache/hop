package org.apache.hop.projects.xp;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.projects.util.ProjectsUtil;
import org.apache.hop.workflow.engine.IWorkflowEngine;

@ExtensionPoint(
  id = "WorkflowStartCheckProjectExtensionPoint",
  description = "At the start of a workflow, verify it lives in the active project",
  extensionPointId = "WorkflowStart"
)
public class WorkflowStartCheckProjectExtensionPoint implements IExtensionPoint {

  @Override public void callExtensionPoint( ILogChannel log, Object object ) throws HopException {

    if ( !( object instanceof IWorkflowEngine ) ) {
      return;
    }

    IWorkflowEngine workflow = (IWorkflowEngine) object;

    String filename = workflow.getFilename();

    try {
      ProjectsUtil.validateFileInProject( log, filename, workflow );
    } catch ( Exception e ) {
      throw new HopException( "Validation error against workflow '" + filename + "' in active project", e );
    }
  }
}
