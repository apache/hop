package org.apache.hop.debug.pipeline;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.IExecutionFinishedListener;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;

import java.util.Date;

@ExtensionPoint(
  id = "LogPipelineExecutionTimeExtensionPoint",
  description = "Logs execution time of a transformation when it finishes",
  extensionPointId = "PipelinePrepareExecution"
)
/**
 * set the debug level right before the transform starts to run
 */
public class LogPipelineExecutionTimeExtensionPoint implements IExtensionPoint<IPipelineEngine<PipelineMeta>> {

  @Override public void callExtensionPoint( ILogChannel log, IPipelineEngine<PipelineMeta> pipeline ) throws HopException {

    // If the HOP_DEBUG_DURATION variable is set to N or FALSE, we don't log duration
    //
    String durationVariable = pipeline.getVariable( Defaults.VARIABLE_HOP_DEBUG_DURATION, "Y" );
    if ( "N".equalsIgnoreCase( durationVariable ) || "FALSE".equalsIgnoreCase( durationVariable ) ) {
      // Nothing to do here
      return;
    }

    pipeline.addExecutionFinishedListener( engine -> {
      Date startDate = pipeline.getExecutionStartDate();
      Date endDate = pipeline.getExecutionEndDate();
      if ( startDate != null && endDate != null ) {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        double seconds = ( (double) endTime - (double) startTime ) / 1000;
        log.logBasic( "Pipeline duration : " + seconds + " seconds [ " + Utils.getDurationHMS( seconds ) + " ]" );
      }
    } );

  }
}
