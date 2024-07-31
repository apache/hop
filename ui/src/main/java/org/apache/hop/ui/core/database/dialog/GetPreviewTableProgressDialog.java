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

package org.apache.hop.ui.core.database.dialog;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Shell;

/**
 * Takes care of displaying a dialog that will handle the wait while we're getting rows for a
 * certain SQL query on a database.
 */
public class GetPreviewTableProgressDialog {
  private static final Class<?> PKG = GetPreviewTableProgressDialog.class;
  private final IVariables variables;

  private Shell shell;
  private DatabaseMeta dbMeta;
  private String tableName;
  private int limit;
  private List<Object[]> rows;
  private IRowMeta rowMeta;

  private Database db;

  /** Creates a new dialog that will handle the wait while we're doing the hard work. */
  public GetPreviewTableProgressDialog(
      Shell shell,
      IVariables variables,
      DatabaseMeta dbInfo,
      String schemaName,
      String tableName,
      int limit) {
    this.shell = shell;
    this.variables = variables;
    this.dbMeta = dbInfo;
    this.tableName = dbInfo.getQuotedSchemaTableCombination(variables, schemaName, tableName);
    this.limit = limit;
  }

  public List<Object[]> open() {
    if (!EnvironmentUtils.getInstance().isWeb()) {
      IRunnableWithProgress op =
          monitor -> {
            db = new Database(HopGui.getInstance().getLoggingObject(), variables, dbMeta);
            try {
              db.connect();

              if (limit > 0) {
                db.setQueryLimit(limit);
              }

              rows = db.getFirstRows(tableName, limit, new ProgressMonitorAdapter(monitor));
              rowMeta = db.getReturnRowMeta();
            } catch (HopException e) {
              throw new InvocationTargetException(
                  e, "Couldn't find any rows because of an error :" + e.toString());
            } finally {
              db.disconnect();
            }
          };
      try {
        final ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
        // Run something in the background to cancel active database queries, forecably if needed!
        Runnable run =
            () -> {
              IProgressMonitor monitor = pmd.getProgressMonitor();
              while (pmd.getShell() == null
                  || (!pmd.getShell().isDisposed() && !monitor.isCanceled())) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  // Ignore
                }
              }

              if (monitor.isCanceled()) { // Disconnect and see what happens!

                try {
                  db.cancelQuery();
                } catch (Exception e) {
                  // Ignore
                }
              }
            };
        // Start the cancel tracker in the background!
        new Thread(run).start();

        pmd.run(true, op);
      } catch (InvocationTargetException | InterruptedException e) {
        showErrorDialog(e);
        return null;
      }
    } else {
      try {
        Cursor cursor = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
        shell.setCursor(cursor);

        db = new Database(HopGui.getInstance().getLoggingObject(), variables, dbMeta);
        db.connect();

        if (limit > 0) {
          db.setQueryLimit(limit);
        }

        rows = db.getFirstRows(tableName, limit, null);
        rowMeta = db.getReturnRowMeta();

      } catch (HopDatabaseException e) {
        showErrorDialog(e);
        return null;
      } finally {
        db.disconnect();
      }
    }

    return rows;
  }

  /**
   * Showing an error dialog
   *
   * @param e
   */
  private void showErrorDialog(Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "GetPreviewTableProgressDialog.Error.Title"),
        BaseMessages.getString(PKG, "GetPreviewTableProgressDialog.Error.Message"),
        e);
  }

  /**
   * @return the rowMeta
   */
  public IRowMeta getRowMeta() {
    return rowMeta;
  }
}
