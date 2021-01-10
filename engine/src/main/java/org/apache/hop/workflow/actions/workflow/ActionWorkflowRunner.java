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

package org.apache.hop.workflow.actions.workflow;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.Workflow;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.engine.IWorkflowEngine;
import org.apache.hop.workflow.engines.local.LocalWorkflowEngine;

/**
 * @author Matt
 * @since 6-apr-2005
 */
public class ActionWorkflowRunner implements Runnable {
  private static final Class<?> PKG = Workflow.class; // For Translator

  private IWorkflowEngine<WorkflowMeta> workflow;
  private Result result;
  private ILogChannel log;
  private int entryNr;
  private boolean finished;

  /**
   *
   */
  public ActionWorkflowRunner( IWorkflowEngine<WorkflowMeta> workflow, Result result, int entryNr, ILogChannel log ) {
    this.workflow = workflow;
    this.result = result;
    this.log = log;
    this.entryNr = entryNr;
    finished = false;
  }

  public void run() {
    try {
      if ( workflow.isStopped() || ( workflow.getParentWorkflow() != null && workflow.getParentWorkflow().isStopped() ) ) {
        return;
      }

      // This JobEntryRunner is a replacement for the Workflow thread.
      // The workflow thread is never started because we simply want to wait for the result.
      //
      ExtensionPointHandler.callExtensionPoint( log, workflow, HopExtensionPoint.WorkflowStart.id, workflow );

      if (workflow instanceof LocalWorkflowEngine ) {
        // We don't want to re-initialize the variables because we defined them already
        //
        ((LocalWorkflowEngine)workflow).setInitializingVariablesOnStart( false );
      }
      result = workflow.startExecution();
    } catch ( HopException e ) {
      e.printStackTrace();
      log.logError( "An error occurred executing this action : ", e );
      result.setResult( false );
      result.setNrErrors( 1 );
    } finally {
      // Otherwise we will get a null pointer exception if 'workflow finished' listeners will be using it
      workflow.setResult( result );
      try {
        ExtensionPointHandler.callExtensionPoint( log, workflow, HopExtensionPoint.WorkflowFinish.id, workflow );
        workflow.fireWorkflowFinishListeners();

        //catch more general exception to prevent thread hanging
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        log.logError( BaseMessages.getString( PKG, "Job.Log.ErrorExecWorkflow", e.getMessage() ), e );
      }
      workflow.setFinished( true );
    }
    finished = true;
  }

  /**
   * @param result The result to set.
   */
  public void setResult( Result result ) {
    this.result = result;
  }

  /**
   * @return Returns the result.
   */
  public Result getResult() {
    return result;
  }

  /**
   * @return Returns the log.
   */
  public ILogChannel getLog() {
    return log;
  }

  /**
   * @param log The log to set.
   */
  public void setLog( ILogChannel log ) {
    this.log = log;
  }

  /**
   * Gets workflow
   *
   * @return value of workflow
   */
  public IWorkflowEngine<WorkflowMeta> getWorkflow() {
    return workflow;
  }

  /**
   * @param workflow The workflow to set
   */
  public void setWorkflow( IWorkflowEngine<WorkflowMeta> workflow ) {
    this.workflow = workflow;
  }

  /**
   * @return Returns the entryNr.
   */
  public int getEntryNr() {
    return entryNr;
  }

  /**
   * @param entryNr The entryNr to set.
   */
  public void setEntryNr( int entryNr ) {
    this.entryNr = entryNr;
  }

  /**
   * @return Returns the finished.
   */
  public boolean isFinished() {
    return finished;
  }

  public void waitUntilFinished() {
    while ( !isFinished() && !workflow.isStopped() ) {
      try {
        Thread.sleep( 0, 1 );
      } catch ( InterruptedException e ) {
        // Ignore errors
      }
    }
  }
}
