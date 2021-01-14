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

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.eclipse.core.runtime.IProgressMonitor;
import org.eclipse.jface.dialogs.ProgressMonitorDialog;
import org.eclipse.jface.operation.IRunnableWithProgress;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Takes care of displaying a dialog that will handle the wait while getting the SQL for a workflow...
 *
 * @author Matt
 * @since 29-mrt-2006
 */
public class GetWorkflowSqlProgressDialog {
  private static final Class<?> PKG = GetWorkflowSqlProgressDialog.class; // For Translator

  private Shell shell;
  private final IVariables variables;
  private WorkflowMeta workflowMeta;
  private List<SqlStatement> stats;

  /**
   * Creates a new dialog that will handle the wait while getting the SQL for a workflow...
   */
  public GetWorkflowSqlProgressDialog( Shell shell, IVariables variables, WorkflowMeta workflowMeta ) {
    this.shell = shell;
    this.variables = variables;
    this.workflowMeta = workflowMeta;
  }

  public List<SqlStatement> open() {
    IRunnableWithProgress op = monitor -> {
      // This is running in a new process: copy some HopVariables info
      // LocalVariables.getInstance().createHopVariables(Thread.currentThread(), kettleVariables.getLocalThread(),
      // true);
      // --> don't set variables if not running in different thread --> pmd.run(true,true, op);

      try {
        stats = workflowMeta.getSqlStatements( new ProgressMonitorAdapter( monitor ), variables );
      } catch ( HopException e ) {
        throw new InvocationTargetException( e, BaseMessages.getString(
          PKG, "GetJobSQLProgressDialog.RuntimeError.UnableToGenerateSQL.Exception", e.getMessage() ) ); // Error
        // generating
        // SQL for
        // workflow:
        // \n{0}
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
