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
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.exception.HopDatabaseException;
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
 * Takes care of displaying a dialog that will handle the wait while we're finding out what tables,
 * views etc. we can reach in the database.
 */
public class GetDatabaseInfoProgressDialog {
  private static final Class<?> PKG = GetDatabaseInfoProgressDialog.class;

  private Shell shell;
  private final IVariables variables;
  private DatabaseMeta databaseMeta;

  /**
   * Creates a new dialog that will handle the wait while we're finding out what tables, views etc.
   * we can reach in the database.
   */
  public GetDatabaseInfoProgressDialog(
      Shell shell, IVariables variables, DatabaseMeta databaseMeta) {
    this.shell = shell;
    this.variables = variables;
    this.databaseMeta = databaseMeta;
  }

  public DatabaseMetaInformation open() {
    final DatabaseMetaInformation dmi = new DatabaseMetaInformation(variables, databaseMeta);

    if (!EnvironmentUtils.getInstance().isWeb()) {
      IRunnableWithProgress op =
          monitor -> {
            try {
              dmi.getData(
                  HopGui.getInstance().getLoggingObject(), new ProgressMonitorAdapter(monitor));
            } catch (Exception e) {
              throw new InvocationTargetException(
                  e,
                  BaseMessages.getString(
                      PKG, "GetDatabaseInfoProgressDialog.Error.GettingInfoTable", e.toString()));
            }
          };

      try {
        ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
        pmd.run(true, op);

        if (pmd.getProgressMonitor().isCanceled()) return null;
      } catch (InvocationTargetException | InterruptedException e) {
        showErrorDialog(e);
        return null;
      }
    } else {
      try {
        Cursor cursor = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
        shell.setCursor(cursor);
        dmi.getData(HopGui.getInstance().getLoggingObject(), null);
      } catch (HopDatabaseException e) {
        showErrorDialog(e);
        return null;
      }
    }

    return dmi;
  }

  /**
   * Showing an error dialog
   *
   * @param e
   */
  private void showErrorDialog(Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "GetDatabaseInfoProgressDialog.Error.Title"),
        BaseMessages.getString(PKG, "GetDatabaseInfoProgressDialog.Error.Message"),
        e);
  }
}
