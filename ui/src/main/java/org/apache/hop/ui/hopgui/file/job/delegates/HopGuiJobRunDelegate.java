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

package org.apache.hop.ui.hopgui.file.job.delegates;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hop.cluster.SlaveServer;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.DefaultLogLevel;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;
import org.apache.hop.job.JobExecutionConfiguration;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.file.job.HopGuiJobGraph;
import org.apache.hop.ui.job.dialog.JobExecutionConfigurationDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HopGuiJobRunDelegate {
  private static Class<?> PKG = HopGui.class; // for i18n purposes, needed by Translator!!

  private HopGuiJobGraph jobGraph;
  private HopGui hopUi;

  private JobExecutionConfiguration jobExecutionConfiguration;

  /**
   * This contains a map between the name of a job and the JobMeta object. If the job has no
   * name it will be mapped under a number [1], [2] etc.
   */
  private List<JobMeta> jobMap;

  /**
   * @param hopUi
   */
  public HopGuiJobRunDelegate( HopGui hopUi, HopGuiJobGraph jobGraph ) {
    this.hopUi = hopUi;
    this.jobGraph = jobGraph;

    jobExecutionConfiguration = new JobExecutionConfiguration();
    jobExecutionConfiguration.setGatheringMetrics( true );

    jobMap = new ArrayList<>();
  }

  public void executeJob( JobMeta jobMeta, boolean local, boolean remote, boolean safe, String startCopyName, int startCopyNr ) throws HopException {

    if ( jobMeta == null ) {
      return;
    }

    JobExecutionConfiguration executionConfiguration = getJobExecutionConfiguration();

    // Remember the variables set previously
    //
    Map<String, String> variableMap = new HashMap<>();
    variableMap.putAll( executionConfiguration.getVariablesMap() ); // the default
    executionConfiguration.setVariablesMap( variableMap );
    executionConfiguration.getUsedVariables( jobMeta );
    executionConfiguration.setSafeModeEnabled( safe );
    executionConfiguration.setStartCopyName( startCopyName );
    executionConfiguration.setStartCopyNr( startCopyNr );
    executionConfiguration.setLogLevel( DefaultLogLevel.getLogLevel() );

    JobExecutionConfigurationDialog dialog = newJobExecutionConfigurationDialog( executionConfiguration, jobMeta );

    if ( !jobMeta.isShowDialog() || dialog.open() ) {

      jobGraph.jobLogDelegate.addJobLog();

      // Set the variables that where specified...
      //
      for ( String varName : executionConfiguration.getVariablesMap().keySet() ) {
        String varValue = executionConfiguration.getVariablesMap().get( varName );
        jobMeta.setVariable( varName, varValue );
      }

      // Set and activate the parameters...
      //
      for ( String paramName : executionConfiguration.getParametersMap().keySet() ) {
        String paramValue = executionConfiguration.getParametersMap().get( paramName );
        jobMeta.setParameterValue( paramName, paramValue );
      }
      jobMeta.activateParameters();

      // Set the log level
      //
      if ( executionConfiguration.getLogLevel() != null ) {
        jobMeta.setLogLevel( executionConfiguration.getLogLevel() );
      }

      // Set the start transform name
      //
      if ( executionConfiguration.getStartCopyName() != null ) {
        jobMeta.setStartCopyName( executionConfiguration.getStartCopyName() );
      }

      // Set the run options
      //
      jobMeta.setClearingLog( executionConfiguration.isClearingLog() );
      jobMeta.setSafeModeEnabled( executionConfiguration.isSafeModeEnabled() );
      jobMeta.setExpandingRemoteJob( executionConfiguration.isExpandingRemoteJob() );

      ILogChannel log = jobGraph.getJobMeta().getLogChannel();
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobMetaExecutionStart.id, jobMeta );
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiJobExecutionConfiguration.id, executionConfiguration );

      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.HopUiPipelineBeforeStart.id, new Object[] { executionConfiguration, jobMeta, jobMeta } );
      } catch ( HopException e ) {
        log.logError( e.getMessage(), jobMeta.getFilename() );
        return;
      }

      if ( !executionConfiguration.isExecutingLocally() && !executionConfiguration.isExecutingRemotely() ) {
        if ( jobMeta.hasChanged() ) {
          jobGraph.showSaveFileMessage();
        }
      }

      // Is this a local execution?
      //
      if ( executionConfiguration.isExecutingLocally() ) {
        jobGraph.startJob( executionConfiguration );
      } else if ( executionConfiguration.isExecutingRemotely() ) {
        // Executing remotely
        // Check if jobMeta has changed
        jobGraph.handleJobMetaChanges( jobMeta );

        // Activate the parameters, turn them into variables...
        // jobMeta.hasChanged()
        jobMeta.activateParameters();

        if ( executionConfiguration.getRemoteServer() != null ) {
          Job.sendToSlaveServer( jobMeta, executionConfiguration, hopUi.getMetaStore() );
          // TODO: bring back the slave server monitor?
          // TODO: remove difference between local and remote execution
          //     hopUi.delegates.slaves.addHopGuiSlave( executionConfiguration.getRemoteServer() );
        } else {
          MessageBox mb = new MessageBox( hopUi.getShell(), SWT.OK | SWT.ICON_ERROR );
          mb.setMessage( BaseMessages.getString( PKG, "HopGui.Dialog.NoRemoteServerSpecified.Message" ) );
          mb.setText( BaseMessages.getString( PKG, "HopGui.Dialog.NoRemoteServerSpecified.Title" ) );
          mb.open();
        }
      }
    }
  }

  @VisibleForTesting
  JobExecutionConfigurationDialog newJobExecutionConfigurationDialog( JobExecutionConfiguration executionConfiguration, JobMeta jobMeta ) {
    return new JobExecutionConfigurationDialog( hopUi.getShell(), executionConfiguration, jobMeta );
  }


  private static void showSaveJobBeforeRunningDialog( Shell shell ) {
    MessageBox m = new MessageBox( shell, SWT.OK | SWT.ICON_WARNING );
    m.setText( BaseMessages.getString( PKG, "JobLog.Dialog.SaveJobBeforeRunning.Title" ) );
    m.setMessage( BaseMessages.getString( PKG, "JobLog.Dialog.SaveJobBeforeRunning.Message" ) );
    m.open();
  }

  private void monitorRemoteJob( final JobMeta jobMeta, final String carteObjectId, final SlaveServer remoteSlaveServer ) {
    // There is a job running in the background. When it finishes log the result on the console.
    // Launch in a separate thread to prevent GUI blocking...
    //
    Thread thread = new Thread( new Runnable() {
      public void run() {
        remoteSlaveServer.monitorRemoteJob( hopUi.getLog(), carteObjectId, jobMeta.toString() );
      }
    } );

    thread.setName( "Monitor remote job '" + jobMeta.getName() + "', carte object id=" + carteObjectId
      + ", slave server: " + remoteSlaveServer.getName() );
    thread.start();

  }


  /**
   * Gets jobGraph
   *
   * @return value of jobGraph
   */
  public HopGuiJobGraph getJobGraph() {
    return jobGraph;
  }

  /**
   * @param jobGraph The jobGraph to set
   */
  public void setJobGraph( HopGuiJobGraph jobGraph ) {
    this.jobGraph = jobGraph;
  }

  /**
   * Gets hopUi
   *
   * @return value of hopUi
   */
  public HopGui getHopUi() {
    return hopUi;
  }

  /**
   * @param hopUi The hopUi to set
   */
  public void setHopUi( HopGui hopUi ) {
    this.hopUi = hopUi;
  }

  /**
   * Gets jobExecutionConfiguration
   *
   * @return value of jobExecutionConfiguration
   */
  public JobExecutionConfiguration getJobExecutionConfiguration() {
    return jobExecutionConfiguration;
  }

  /**
   * @param jobExecutionConfiguration The jobExecutionConfiguration to set
   */
  public void setJobExecutionConfiguration( JobExecutionConfiguration jobExecutionConfiguration ) {
    this.jobExecutionConfiguration = jobExecutionConfiguration;
  }

  /**
   * Gets jobMap
   *
   * @return value of jobMap
   */
  public List<JobMeta> getJobMap() {
    return jobMap;
  }

  /**
   * @param jobMap The jobMap to set
   */
  public void setJobMap( List<JobMeta> jobMap ) {
    this.jobMap = jobMap;
  }
}
