/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.job.entries.job;

import org.apache.hop.core.Result;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.logging.LogChannelInterface;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.Job;

/**
 * @author Matt
 * @since 6-apr-2005
 */
public class JobEntryJobRunner implements Runnable {
  private static Class<?> PKG = Job.class; // for i18n purposes, needed by Translator!!

  private Job job;
  private Result result;
  private LogChannelInterface log;
  private int entryNr;
  private boolean finished;

  /**
   *
   */
  public JobEntryJobRunner( Job job, Result result, int entryNr, LogChannelInterface log ) {
    this.job = job;
    this.result = result;
    this.log = log;
    this.entryNr = entryNr;
    finished = false;
  }

  public void run() {
    try {
      if ( job.isStopped() || ( job.getParentJob() != null && job.getParentJob().isStopped() ) ) {
        return;
      }

      // This JobEntryRunner is a replacement for the Job thread.
      // The job thread is never started because we simply want to wait for the result.
      //
      ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobStart.id, getJob() );

      job.fireJobStartListeners(); // Fire the start listeners
      result = job.execute( entryNr + 1, result );
    } catch ( HopException e ) {
      e.printStackTrace();
      log.logError( "An error occurred executing this job entry : ", e );
      result.setResult( false );
      result.setNrErrors( 1 );
    } finally {
      //[PDI-14981] otherwise will get null pointer exception if 'job finished' listeners will be using it
      job.setResult( result );
      try {
        ExtensionPointHandler.callExtensionPoint( log, HopExtensionPoint.JobFinish.id, getJob() );
        job.fireJobFinishListeners();

        //catch more general exception to prevent thread hanging
      } catch ( Exception e ) {
        result.setNrErrors( 1 );
        result.setResult( false );
        log.logError( BaseMessages.getString( PKG, "Job.Log.ErrorExecJob", e.getMessage() ), e );
      }
      job.setFinished( true );
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
  public LogChannelInterface getLog() {
    return log;
  }

  /**
   * @param log The log to set.
   */
  public void setLog( LogChannelInterface log ) {
    this.log = log;
  }

  /**
   * @return Returns the job.
   */
  public Job getJob() {
    return job;
  }

  /**
   * @param job The job to set.
   */
  public void setJob( Job job ) {
    this.job = job;
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
    while ( !isFinished() && !job.isStopped() ) {
      try {
        Thread.sleep( 0, 1 );
      } catch ( InterruptedException e ) {
        // Ignore errors
      }
    }
  }
}
