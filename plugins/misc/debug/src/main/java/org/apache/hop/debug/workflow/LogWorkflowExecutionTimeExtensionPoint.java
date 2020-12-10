/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.debug.workflow;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
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

  @Override public void callExtensionPoint( ILogChannel log, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow ) throws HopException {


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
