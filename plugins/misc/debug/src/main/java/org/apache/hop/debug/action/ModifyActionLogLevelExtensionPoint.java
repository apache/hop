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

package org.apache.hop.debug.action;

import org.apache.hop.core.Result;
import org.apache.hop.core.ResultFile;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPoint;
import org.apache.hop.core.extension.IExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.debug.util.DebugLevelUtil;
import org.apache.hop.debug.util.Defaults;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.engine.IPipelineEngine;
import org.apache.hop.workflow.IActionListener;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.ActionMeta;
import org.apache.hop.workflow.action.IAction;
import org.apache.hop.workflow.engine.IWorkflowEngine;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@ExtensionPoint(
  id = "ModifyActionLogLevelExtensionPoint",
  extensionPointId = "WorkflowStart",
  description = "Modify the logging level of an individual workflow entry if needed" )
public class ModifyActionLogLevelExtensionPoint implements IExtensionPoint<IWorkflowEngine<WorkflowMeta>> {

  public static final String STRING_REFERENCE_VARIABLE_SPACE = "REFERENCE_VARIABLE_SPACE";


  @Override public void callExtensionPoint( ILogChannel jobLog, IVariables variables, IWorkflowEngine<WorkflowMeta> workflow ) throws HopException {

    IWorkflowEngine<WorkflowMeta> rootWorkflow = workflow;
    IPipelineEngine<PipelineMeta> rootPipeline = null;
    while ( rootWorkflow != null || rootPipeline != null ) {

      if ( rootWorkflow != null ) {
        if ( rootWorkflow.getParentWorkflow() == null && rootWorkflow.getParentPipeline() == null ) {
          break;
        }
        rootPipeline = rootWorkflow.getParentPipeline();
        rootWorkflow = rootWorkflow.getParentWorkflow();
      } else {
        if ( rootPipeline.getParentWorkflow() == null && rootPipeline.getParentPipeline() == null ) {
          break;
        }
        rootWorkflow = rootPipeline.getParentWorkflow();
        rootPipeline = rootPipeline.getParentPipeline();
      }
    }
    Map<String, Object> rootDataMap;
    if ( rootWorkflow != null ) {
      rootDataMap = rootWorkflow.getExtensionDataMap();
    } else {
      rootDataMap = rootPipeline.getExtensionDataMap();
    }

    // Look for a reference variable variables in the root workflow.
    // If non exists, add it.  Only do this at the start of the root workflow, afterwards, never again.
    //
    final IVariables referenceSpace;
    synchronized ( rootDataMap ) {
      IVariables referenceVariables = (IVariables) rootDataMap.get( STRING_REFERENCE_VARIABLE_SPACE );
      if ( referenceVariables == null ) {
        referenceVariables = new Variables();
        referenceVariables.initializeFrom( workflow );
        rootDataMap.put( STRING_REFERENCE_VARIABLE_SPACE, referenceVariables );
      }
      referenceSpace = referenceVariables;
    }

    // Find the debug info in the workflow metadata
    //
    WorkflowMeta jobMeta = workflow.getWorkflowMeta();

    Map<String, String> entryLevelMap = jobMeta.getAttributesMap().get( Defaults.DEBUG_GROUP );
    if ( entryLevelMap == null ) {
      return;
    }

    jobLog.logDetailed( "Set debug level information on workflow : " + jobMeta.getName() );

    final Set<String> entries = new HashSet<>();
    for ( String key : entryLevelMap.keySet() ) {

      int index = key.indexOf( " : " );
      if ( index > 0 ) {
        String entryName = key.substring( 0, index );
        if ( !entries.contains( entryName ) ) {
          entries.add( entryName );
        }
      }
    }

    if ( entries.isEmpty() ) {
      return;
    }

    try {

      final LogLevel jobLogLevel = workflow.getLogLevel();
      final Set<String> variablesToIgnore = Defaults.VARIABLES_TO_IGNORE;

      jobLog.logDetailed( "Found debug level info for workflow actions : " + entries.toString() );

      workflow.addActionListener( new IActionListener<WorkflowMeta>() {

        @Override public void beforeExecution( IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionCopy, IAction action ) {

          ILogChannel log = action.getLogChannel();

          try {
            // Is this a workflow entry with debugging set on it?
            //
            if ( entries.contains( actionCopy.toString() ) ) {
              final ActionDebugLevel debugLevel = DebugLevelUtil.getActionDebugLevel( entryLevelMap, actionCopy.toString() );
              if ( debugLevel != null ) {
                // Set the debug level for this one...
                //
                log.setLogLevel( debugLevel.getLogLevel() );
                workflow.setLogLevel( debugLevel.getLogLevel() );
              }
            }
          } catch ( Exception e ) {
            log.logError( "Error setting logging level on action" );
          }
        }

        @Override public void afterExecution( IWorkflowEngine<WorkflowMeta> workflow, ActionMeta actionCopy, IAction action, Result result ) {

          ILogChannel log = action.getLogChannel();

          try {
            // Is this a workflow entry with debugging set on it?
            //
            if ( entries.contains( actionCopy.toString() ) ) {
              final ActionDebugLevel debugLevel = DebugLevelUtil.getActionDebugLevel( entryLevelMap, actionCopy.toString() );
              if ( debugLevel != null ) {
                // Set the debug level for this one...
                //
                log.setLogLevel( jobLogLevel );
                workflow.setLogLevel( jobLogLevel );

                // Set the debug level back to normal...
                //

                if ( debugLevel.isLoggingResult() ) {
                  log.logMinimal( "Action results: " );
                  log.logMinimal( "  - result=" + result.getResult() );
                  log.logMinimal( "  - stopped=" + result.isStopped() );
                  log.logMinimal( "  - linesRead=" + result.getNrLinesRead() );
                  log.logMinimal( "  - linesWritten=" + result.getNrLinesWritten() );
                  log.logMinimal( "  - linesInput=" + result.getNrLinesInput() );
                  log.logMinimal( "  - linesOutput=" + result.getNrLinesOutput() );
                  log.logMinimal( "  - linesRejected=" + result.getNrLinesRejected() );
                  log.logMinimal( "  - result row count=" + result.getRows().size() );
                  log.logMinimal( "  - result files count=" + result.getResultFilesList().size() );
                }
                if ( debugLevel.isLoggingResultRows() ) {
                  log.logMinimal( "Action result rows: " );
                  for ( RowMetaAndData rmad : result.getRows() ) {
                    log.logMinimal( " - " + rmad.toString() );
                  }
                }
                if ( debugLevel.isLoggingResultFiles() ) {
                  log.logMinimal( "Action result files: " );
                  for ( ResultFile resultFile : result.getResultFilesList() ) {
                    log.logMinimal( " - " + resultFile.getFile().toString() + " from " + resultFile.getOrigin() + " : " + resultFile.getComment() + " / " + resultFile.getTypeCode() );
                  }
                }
                if ( debugLevel.isLoggingVariables() ) {
                  if ( action instanceof IVariables ) {
                    log.logMinimal( "Action notable variables: " );

                    IVariables variables = (IVariables) action;
                    // See the variables set differently from the parent workflow
                    for ( String var : variables.getVariableNames() ) {
                      if ( !variablesToIgnore.contains( var ) ) {
                        String value = variables.getVariable( var );
                        String refValue = referenceSpace.getVariable( var );

                        if ( refValue == null || !refValue.equals( value ) ) {
                          // Something different!
                          //
                          log.logMinimal( " - " + var + "=" + value );
                        }
                      }
                    }
                  }
                }
              }
            }
          } catch ( Exception e ) {
            log.logError( "Error re-setting logging level on action" );
          }
        }
      } );
    } catch ( Exception e ) {
      jobLog.logError( "Unable to handle specific debug level for workflow", e );
    }
  }
}
