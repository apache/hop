package org.apache.hop.debug.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.Date;

@ExtensionPoint(
  id = "LogWorkflowExecutionTimeExtensionPoint",
  description = "Logs execution time of a workflow when it finishes",
  extensionPointId = "WorkflowStart"
)
/**
 * set the debug level right before the transform starts to run
 */
public class LogWorkflowExecutionTimeExtensionPoint implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IWorkflowEngine<WorkflowMeta> workflow ) throws HopException {


    // If the HOP_DEBUG_DURATION variable is set to N or FALSE, we don't log duration
    //
    String durationVariable = workflow.getVariable( Defaults.VARIABLE_HOP_DEBUG_DURATION, "Y" );
    if ( "N".equalsIgnoreCase( durationVariable ) || "FALSE".equalsIgnoreCase( durationVariable ) ) {
      // Nothing to do here
      return;
    }

    final long startTime = System.currentTimeMillis();

    workflow.addWorkflowFinishedListener( workflow1 -> {
      Date startDate = workflow1.getExecutionStartDate();
      Date endDate = workflow1.getExecutionEndDate();
      if ( startDate != null && endDate != null ) {
        long startTime1 = startDate.getTime();
        long endTime = endDate.getTime();
        double seconds = ( (double) endTime - (double) startTime1 ) / 1000;
        log.logBasic( "Workflow duration : " + seconds + " seconds [ " + Utils.getDurationHMS( seconds ) + " ]" );
      }
    } );
  }
}
