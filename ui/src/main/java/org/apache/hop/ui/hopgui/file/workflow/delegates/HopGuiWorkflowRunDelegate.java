/*! ******************************************************************************
 *
 * Hop : The Hop Orchestration Platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.apache.hop.ui.hopgui.file.workflow.delegates;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.hopgui.file.workflow.HopGuiWorkflowGraph;
import org.apache.hop.ui.workflow.dialog.WorkflowExecutionConfigurationDialog;
import org.apache.hop.workflow.WorkflowExecutionConfiguration;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HopGuiWorkflowRunDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGuiWorkflowGraph workflowGraph;
  private HopGui hopGui;

  private WorkflowExecutionConfiguration workflowExecutionConfiguration;

  /**
   * This contains a map between the name of a workflow and the WorkflowMeta object. If the workflow has no
   * name it will be mapped under a number [1], [2] etc.
   */
  private List<WorkflowMeta> jobMap;

  /**
   * @param hopGui
   */
  public HopGuiWorkflowRunDelegate( HopGui hopGui, HopGuiWorkflowGraph workflowGraph ) {
    this.hopGui = hopGui;
    this.workflowGraph = workflowGraph;

    workflowExecutionConfiguration = new WorkflowExecutionConfiguration();
    workflowExecutionConfiguration.setGatheringMetrics( true );

    jobMap = new ArrayList<>();
  }

  public void executeWorkflow( WorkflowMeta workflowMeta, boolean local, boolean remote, boolean safe, String startCopyName, int startCopyNr ) throws HopException {

    if ( workflowMeta == null ) {
      return;
    }

    WorkflowExecutionConfiguration executionConfiguration = getWorkflowExecutionConfiguration();

    // Remember the variables set previously
    //
    Map<String, String> variableMap = new HashMap<>();
    variableMap.putAll( executionConfiguration.getVariablesMap() ); // the default
    executionConfiguration.setVariablesMap( variableMap );
    executionConfiguration.getUsedVariables( workflowMeta );
    executionConfiguration.setStartCopyName( startCopyName );
    executionConfiguration.setStartCopyNr( startCopyNr );
    executionConfiguration.setLogLevel( DefaultLogLevel.getLogLevel() );

    WorkflowExecutionConfigurationDialog dialog = newWorkflowExecutionConfigurationDialog( executionConfiguration, workflowMeta );

    if ( !workflowMeta.isShowDialog() || dialog.open() ) {

      workflowGraph.workflowLogDelegate.addJobLog();

      // Set the variables that where specified...
      //
      for ( String varName : executionConfiguration.getVariablesMap().keySet() ) {
        String varValue = executionConfiguration.getVariablesMap().get( varName );
        workflowMeta.setVariable( varName, varValue );
      }

      // Set and activate the parameters...
      //
      for ( String paramName : executionConfiguration.getParametersMap().keySet() ) {
        String paramValue = executionConfiguration.getParametersMap().get( paramName );
        workflowMeta.setParameterValue( paramName, paramValue );
      }
      workflowMeta.activateParameters();

      // Set the log level
      //
      if ( executionConfiguration.getLogLevel() != null ) {
        workflowMeta.setLogLevel( executionConfiguration.getLogLevel() );
      }

      // Set the start transform name
      //
      if ( executionConfiguration.getStartCopyName() != null ) {
        workflowMeta.setStartCopyName( executionConfiguration.getStartCopyName() );
      }

      // Set the run options
      //
      workflowMeta.setClearingLog( executionConfiguration.isClearingLog() );

      ILogChannel log = workflowGraph.getWorkflowMeta().getLogChannel();
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiWorkflowMetaExecutionStart.id, workflowMeta );
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobExecutionConfiguration.id, executionConfiguration );

      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiPipelineBeforeStart.id, new Object[] { executionConfiguration, workflowMeta, workflowMeta } );
      } catch ( HopException e ) {
        log.logError( e.getMessage(), workflowMeta.getFilename() );
        return;
      }

      if ( workflowMeta.hasChanged() ) {
        workflowGraph.showSaveFileMessage();
      }

      workflowGraph.startJob( executionConfiguration );
    }
  }

  @VisibleForTesting
  WorkflowExecutionConfigurationDialog newWorkflowExecutionConfigurationDialog( WorkflowExecutionConfiguration executionConfiguration, WorkflowMeta workflowMeta ) {
    return new WorkflowExecutionConfigurationDialog( hopGui.getShell(), executionConfiguration, workflowMeta );
  }


  private static void showSaveJobBeforeRunningDialog( Shell shell ) {
    MessageBox m = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
    m.setText( BaseMessages.getString( PKG, "WorkflowLog.Dialog.SaveJobBeforeRunning.Title" ) );
    m.setMessage( BaseMessages.getString( PKG, "WorkflowLog.Dialog.SaveJobBeforeRunning.Message" ) );
    m.open();
  }

  private void monitorRemoteJob( final WorkflowMeta workflowMeta, final String serverObjectId, final SlaveServer remoteSlaveServer ) {
    // There is a workflow running in the background. When it finishes log the result on the console.
    // Launch in a separate thread to prevent GUI blocking...
    //
    Thread thread = new Thread( new Runnable() {
      public void run() {
        remoteSlaveServer.monitorRemoteJob( hopGui.getLog(), serverObjectId, workflowMeta.toString() );
      }
    } );

    thread.setName( "Monitor remote workflow '" + workflowMeta.getName() + "', carte object id=" + serverObjectId
      + ", slave server: " + remoteSlaveServer.getName() );
    thread.start();

  }


  /**
   * Gets workflowGraph
   *
   * @return value of workflowGraph
   */
  public HopGuiWorkflowGraph getWorkflowGraph() {
    return workflowGraph;
  }

  /**
   * @param workflowGraph The workflowGraph to set
   */
  public void setWorkflowGraph( HopGuiWorkflowGraph workflowGraph ) {
    this.workflowGraph = workflowGraph;
  }

  /**
   * Gets hopGui
   *
   * @return value of hopGui
   */
  public HopGui getHopGui() {
    return hopGui;
  }

  /**
   * @param hopGui The hopGui to set
   */
  public void setHopGui( HopGui hopGui ) {
    this.hopGui = hopGui;
  }

  /**
   * Gets workflowExecutionConfiguration
   *
   * @return value of workflowExecutionConfiguration
   */
  public WorkflowExecutionConfiguration getWorkflowExecutionConfiguration() {
    return workflowExecutionConfiguration;
  }

  /**
   * @param workflowExecutionConfiguration The workflowExecutionConfiguration to set
   */
  public void setWorkflowExecutionConfiguration( WorkflowExecutionConfiguration workflowExecutionConfiguration ) {
    this.workflowExecutionConfiguration = workflowExecutionConfiguration;
  }

  /**
   * Gets workflowMap
   *
   * @return value of workflowMap
   */
  public List<WorkflowMeta> getJobMap() {
    return jobMap;
  }

  /**
   * @param jobMap The workflowMap to set
   */
  public void setJobMap( List<WorkflowMeta> jobMap ) {
    this.jobMap = jobMap;
  }
}
