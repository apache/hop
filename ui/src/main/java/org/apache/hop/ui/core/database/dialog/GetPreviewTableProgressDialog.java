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
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.IRunnableWithProgress;
import org.apache.hop.core.ProgressMonitorAdapter;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
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

  /** JDBC {@link java.sql.Statement#setQueryTimeout(int)} in seconds; 0 = driver default. */
  private final int queryTimeoutSeconds;

  private List<Object[]> rows = Collections.emptyList();
  @Getter private IRowMeta rowMeta;

  /** False when preview failed (error dialog was shown). */
  @Getter private boolean previewSucceeded = true;

  private Database db;

  /** Creates a new dialog that will handle the wait while we're doing the hard work. */
  public GetPreviewTableProgressDialog(
      Shell shell,
      IVariables variables,
      DatabaseMeta dbInfo,
      String schemaName,
      String tableName,
      int limit,
      int queryTimeoutSeconds) {
    this.shell = shell;
    this.variables = variables;
    this.dbMeta = dbInfo;
    this.tableName = dbInfo.getQuotedSchemaTableCombination(variables, schemaName, tableName);
    this.limit = limit;
    this.queryTimeoutSeconds = Math.max(0, queryTimeoutSeconds);
  }

  /**
   * @return preview rows; empty when none or on failure. On failure {@link #previewSucceeded} is
   *     false; callers typically use the Lombok-generated {@code isPreviewSucceeded()} accessor.
   */
  public List<Object[]> open() {
    previewSucceeded = true;
    rows = Collections.emptyList();
    if (!EnvironmentUtils.getInstance().isWeb()) {
      openDesktop();
    } else {
      openWeb();
    }
    return rows;
  }

  private void openDesktop() {
    IRunnableWithProgress op = this::runPreviewWithResources;
    try {
      ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);
      startCancelWatcherThread(pmd);
      pmd.run(true, op);
    } catch (InvocationTargetException e) {
      previewSucceeded = false;
      showErrorDialog(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      previewSucceeded = false;
      showErrorDialog(e);
    }
  }

  private void runPreviewWithResources(IProgressMonitor monitor) throws InvocationTargetException {
    try (Database database =
        new Database(HopGui.getInstance().getLoggingObject(), variables, dbMeta)) {
      db = database;
      database.connect();
      if (queryTimeoutSeconds > 0) {
        database.setStatementQueryTimeoutSeconds(queryTimeoutSeconds);
      }
      if (limit > 0) {
        database.setQueryLimit(limit);
      }
      rows = database.getFirstRows(tableName, limit, new ProgressMonitorAdapter(monitor));
      rowMeta = database.getReturnRowMeta();
    } catch (HopException e) {
      throw new InvocationTargetException(
          e, "Couldn't find any rows because of an error :" + e.toString());
    } finally {
      db = null;
    }
  }

  private void startCancelWatcherThread(ProgressMonitorDialog pmd) {
    Runnable run =
        () -> {
          IProgressMonitor monitor = pmd.getProgressMonitor();
          while (pmd.getShell() == null
              || (!pmd.getShell().isDisposed() && !monitor.isCanceled())) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
          }
          if (monitor.isCanceled()) {
            try {
              if (db != null) {
                db.cancelQuery();
              }
            } catch (Exception e) {
              // Ignore
            }
          }
        };
    Thread cancelWatcher = new Thread(run, "Hop-DB-Preview-CancelWatcher");
    cancelWatcher.setDaemon(true);
    cancelWatcher.start();
  }

  private void openWeb() {
    Cursor cursor = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
    try {
      shell.setCursor(cursor);
      try (Database database =
          new Database(HopGui.getInstance().getLoggingObject(), variables, dbMeta)) {
        db = database;
        database.connect();
        if (queryTimeoutSeconds > 0) {
          database.setStatementQueryTimeoutSeconds(queryTimeoutSeconds);
        }
        if (limit > 0) {
          database.setQueryLimit(limit);
        }
        rows = database.getFirstRows(tableName, limit, null);
        rowMeta = database.getReturnRowMeta();
      }
    } catch (HopException e) {
      previewSucceeded = false;
      showErrorDialog(e);
    } finally {
      if (!shell.isDisposed()) {
        shell.setCursor(null);
      }
      if (!cursor.isDisposed()) {
        cursor.dispose();
      }
      db = null;
    }
  }

  private void showErrorDialog(Exception e) {
    new ErrorDialog(
        shell,
        BaseMessages.getString(PKG, "GetPreviewTableProgressDialog.Error.Title"),
        BaseMessages.getString(PKG, "GetPreviewTableProgressDialog.Error.Message"),
        e);
  }
}
