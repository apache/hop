/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
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

package org.apache.hop.ui.hopui.dialog;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.SQLStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.job.JobMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;

/**
 * Takes care of displaying a dialog that will handle the wait while getting the SQL for a job...
 *
 * @author Matt
 * @since 29-mrt-2006
 */
public class GetJobSQLProgressDialog {
  private static Class<?> PKG = GetJobSQLProgressDialog.class; // for i18n purposes, needed by Translator2!!

  private Shell shell;
  private JobMeta jobMeta;
  private List<SQLStatement> stats;

  /**
   * Creates a new dialog that will handle the wait while getting the SQL for a job...
   */
  public GetJobSQLProgressDialog( Shell shell, JobMeta jobMeta ) {
    this.shell = shell;
    this.jobMeta = jobMeta;
  }

  public List<SQLStatement> open() {
    IRunnableWithProgress op = new IRunnableWithProgress() {
      public void run( IProgressMonitor monitor ) throws InvocationTargetException, InterruptedException {
        // This is running in a new process: copy some HopVariables info
        // LocalVariables.getInstance().createHopVariables(Thread.currentThread(), kettleVariables.getLocalThread(),
        // true);
        // --> don't set variables if not running in different thread --> pmd.run(true,true, op);

        try {
          stats = jobMeta.getSQLStatements( new ProgressMonitorAdapter( monitor ) );
        } catch ( HopException e ) {
          throw new InvocationTargetException( e, BaseMessages.getString(
            PKG, "GetJobSQLProgressDialog.RuntimeError.UnableToGenerateSQL.Exception", e.getMessage() ) ); // Error
                                                                                                           // generating
                                                                                                           // SQL for
                                                                                                           // job:
                                                                                                           // \n{0}
        }
      }
    };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog( shell );
      pmd.run( false, false, op );
    } catch ( InvocationTargetException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetJobSQLProgressDialog.Dialog.UnableToGenerateSQL.Title" ),
        BaseMessages.getString( PKG, "GetJobSQLProgressDialog.Dialog.UnableToGenerateSQL.Message" ), e );
      stats = null;
    } catch ( InterruptedException e ) {
      new ErrorDialog( shell, BaseMessages.getString( PKG, "GetJobSQLProgressDialog.Dialog.UnableToGenerateSQL.Title" ),
        BaseMessages.getString( PKG, "GetJobSQLProgressDialog.Dialog.UnableToGenerateSQL.Message" ), e );
      stats = null;
    }

    return stats;
  }
}
