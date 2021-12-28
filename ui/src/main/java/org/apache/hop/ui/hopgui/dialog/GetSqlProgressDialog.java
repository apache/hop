/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.ui.hopgui.dialog;

import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.eclipse.swt.widgets.Shell;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Takes care of displaying a dialog that will handle the wait while getting the SQL for a
 * pipeline...
 */
public class GetSqlProgressDialog {
  private static final Class<?> PKG = GetSqlProgressDialog.class; // For Translator

  private Shell shell;
  private final IVariables variables;
  private PipelineMeta pipelineMeta;
  private List<SqlStatement> stats;

  /** Creates a new dialog that will handle the wait while getting the SQL for a pipeline... */
  public GetSqlProgressDialog(Shell shell, IVariables variables, PipelineMeta pipelineMeta) {
    this.shell = shell;
    this.variables = variables;
    this.pipelineMeta = pipelineMeta;
  }

  public List<SqlStatement> open() {
    IRunnableWithProgress op =
        monitor -> {
          try {
            stats = pipelineMeta.getSqlStatements(variables, new ProgressMonitorAdapter(monitor));
          } catch (HopException e) {
            throw new InvocationTargetException(
                e,
                BaseMessages.getString(
                    PKG,
                    "GetSQLProgressDialog.RuntimeError.UnableToGenerateSQL.Exception",
                    e.getMessage()));
          }
        };

    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      pmd.run(false, op);
    } catch (InvocationTargetException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetSQLProgressDialog.Dialog.UnableToGenerateSQL.Title"),
          BaseMessages.getString(PKG, "GetSQLProgressDialog.Dialog.UnableToGenerateSQL.Message"),
          e);
      stats = null;
    } catch (InterruptedException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetSQLProgressDialog.Dialog.UnableToGenerateSQL.Title"),
          BaseMessages.getString(PKG, "GetSQLProgressDialog.Dialog.UnableToGenerateSQL.Message"),
          e);
      stats = null;
    }

    return stats;
  }
}
