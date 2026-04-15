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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.IProgressMonitor;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.SqlScriptStatement;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.logging.LoggingObjectType;
import org.apache.hop.core.logging.SimpleLoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.dialog.ProgressMonitorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.SQLStyledTextComp;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** Dialog that allows the user to launch SQL statements towards the database. */
public class SqlEditor {
  private static final Class<?> PKG = SqlEditor.class;

  public static final ILoggingObject loggingObject =
      new SimpleLoggingObject("SQL Editor", LoggingObjectType.HOP_GUI, null);

  private final PropsUi props;

  private TextComposite wScript;

  private Label wlPosition;

  private final String input;
  private final DatabaseMeta connection;
  private Shell shell;
  private final DbCache dbcache;

  private final ILogChannel log;
  private int style = SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;
  private final Shell parentShell;

  private final IVariables variables;

  /**
   * Holds the active database connection so the cancel watcher can call cancelQuery(). Use {@link
   * AtomicReference} for safe publication between the worker and cancel-watcher threads.
   */
  private final AtomicReference<Database> activeDb = new AtomicReference<>();

  public SqlEditor(
      Shell parent, int style, IVariables variables, DatabaseMeta ci, DbCache dbc, String sql) {
    props = PropsUi.getInstance();
    log = new LogChannel(ci);
    input = sql;
    connection = ci;
    dbcache = dbc;
    this.parentShell = parent;
    this.style = (style != SWT.None) ? style : this.style;
    this.variables = variables;
  }

  public void open() {
    shell = new Shell(parentShell, style);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageDatabase());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SQLEditor.Title"));

    int margin = PropsUi.getMargin();

    Button wExec = new Button(shell, SWT.PUSH);
    wExec.setText(BaseMessages.getString(PKG, "SQLEditor.Button.Execute"));
    wExec.addListener(
        SWT.Selection,
        e -> {
          try {
            exec();
          } catch (Exception ge) {
            // Ignore errors
          }
        });
    Button wClear = new Button(shell, SWT.PUSH);
    wClear.setText(BaseMessages.getString(PKG, "SQLEditor.Button.ClearCache"));
    wClear.setToolTipText(BaseMessages.getString(PKG, "SQLEditor.Button.ClearCache.Tooltip"));
    wClear.addListener(SWT.Selection, e -> clearCache());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wExec, wClear, wCancel}, margin, null);

    wlPosition = new Label(shell, SWT.NONE);
    wlPosition.setText(BaseMessages.getString(PKG, "SQLEditor.LineNr.Label", "0"));
    PropsUi.setLook(wlPosition);
    FormData fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.bottom = new FormAttachment(wExec, -margin);
    fdlPosition.right = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    // Script line
    Label wlScript = new Label(shell, SWT.NONE);
    wlScript.setText(BaseMessages.getString(PKG, "SQLEditor.Editor.Label"));
    PropsUi.setLook(wlScript);

    FormData fdlScript = new FormData();
    fdlScript.left = new FormAttachment(0, 0);
    fdlScript.top = new FormAttachment(0, 0);
    wlScript.setLayoutData(fdlScript);
    if (EnvironmentUtils.getInstance().isWeb()) {
      wScript =
          new StyledTextComp(
              variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    } else {
      wScript =
          new SQLStyledTextComp(
              variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
      wScript.addLineStyleListener(getSqlReservedWords());
    }
    wScript.setText("");
    PropsUi.setLook(wScript, Props.WIDGET_STYLE_FIXED);
    FormData fdScript = new FormData();
    fdScript.left = new FormAttachment(0, 0);
    fdScript.top = new FormAttachment(wlScript, margin);
    fdScript.right = new FormAttachment(100, -10);
    fdScript.bottom = new FormAttachment(wlPosition, -margin);
    wScript.setLayoutData(fdScript);
    wScript.addModifyListener(event -> setPosition());
    wScript.addKeyListener(
        new KeyAdapter() {
          @Override
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          @Override
          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wScript.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          @Override
          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wScript.addMouseListener(
        new MouseAdapter() {
          @Override
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          @Override
          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    getData();

    BaseDialog.defaultShellHandling(shell, c -> exec(), c -> cancel());
  }

  public void setPosition() {
    int lineNumber = wScript.getLineNumber();
    int columnNumber = wScript.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "SQLEditor.Position.Label", "" + lineNumber, "" + columnNumber));
  }

  private List<String> getSqlReservedWords() {
    if (connection == null) {
      return List.of();
    }
    return List.of(connection.getReservedWords());
  }

  private void clearCache() {
    MessageBox mb = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES | SWT.CANCEL);
    mb.setMessage(
        BaseMessages.getString(PKG, "SQLEditor.ClearWholeCache.Message", connection.getName()));
    mb.setText(BaseMessages.getString(PKG, "SQLEditor.ClearWholeCache.Title"));
    int answer = mb.open();

    switch (answer) {
      case SWT.NO:
        DbCache.getInstance().clear(connection.getName());

        mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        mb.setMessage(
            BaseMessages.getString(
                PKG, "SQLEditor.ConnectionCacheCleared.Message", connection.getName()));
        mb.setText(BaseMessages.getString(PKG, "SQLEditor.ConnectionCacheCleared.Title"));
        mb.open();

        break;
      case SWT.YES:
        DbCache.getInstance().clear(null);

        mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        mb.setMessage(BaseMessages.getString(PKG, "SQLEditor.WholeCacheCleared.Message"));
        mb.setText(BaseMessages.getString(PKG, "SQLEditor.WholeCacheCleared.Title"));
        mb.open();

        break;
      case SWT.CANCEL:
        break;
      default:
        break;
    }
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input != null) {
      wScript.setText(input);
    }
  }

  private void cancel() {
    dispose();
  }

  private void exec() {
    DatabaseMeta databaseMeta = connection;
    if (databaseMeta == null) {
      return;
    }

    String sqlScript =
        Utils.isEmpty(wScript.getSelectionText()) ? wScript.getText() : wScript.getSelectionText();
    StringBuilder message = new StringBuilder();
    ProgressMonitorDialog pmd = new ProgressMonitorDialog(shell);

    startSqlCancelWatcherVirtualThread(pmd);

    try {
      pmd.run(true, monitor -> runSqlScriptWithMonitor(monitor, databaseMeta, sqlScript, message));
    } catch (InterruptedException | InvocationTargetException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }

    EnterTextDialog dialog =
        new EnterTextDialog(
            shell,
            BaseMessages.getString(PKG, "SQLEditor.Result.Title"),
            BaseMessages.getString(PKG, "SQLEditor.Result.Message"),
            message.toString(),
            true);
    dialog.open();
  }

  private void startSqlCancelWatcherVirtualThread(ProgressMonitorDialog pmd) {
    Thread.ofVirtual()
        .name("Hop-SqlEditor-CancelWatcher")
        .start(
            () -> {
              IProgressMonitor monitor = pmd.getProgressMonitor();
              while (pmd.getShell() == null
                  || (!pmd.getShell().isDisposed() && !monitor.isCanceled())) {
                try {
                  Thread.sleep(100);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
              if (monitor.isCanceled()) {
                Database db = activeDb.get();
                if (db != null) {
                  try {
                    db.cancelQuery();
                  } catch (Exception ignored) {
                    // ignore
                  }
                }
              }
            });
  }

  private void runSqlScriptWithMonitor(
      IProgressMonitor monitor,
      DatabaseMeta databaseMeta,
      String sqlScript,
      StringBuilder message) {
    monitor.beginTask(BaseMessages.getString(PKG, "SQLEditor.Monitor.Execute"), 100);
    String raw =
        variables.getVariable(
            Const.HOP_QUERY_PREVIEW_TIMEOUT,
            EnvUtil.getSystemProperty(Const.HOP_QUERY_PREVIEW_TIMEOUT, "0"));
    int timeoutSeconds = Math.max(0, Const.toInt(variables.resolve(raw), 0));
    try (Database db = new Database(loggingObject, variables, databaseMeta)) {
      activeDb.set(db);
      if (timeoutSeconds > 0) {
        db.setStatementQueryTimeoutSeconds(timeoutSeconds);
      }
      db.connect();

      int[] nrStats = {0};
      runScriptStatementsLoop(db, databaseMeta, sqlScript, message, monitor, nrStats);
      message.append(
          BaseMessages.getString(PKG, "SQLEditor.Log.StatsExecuted", Integer.toString(nrStats[0])));
      message.append(Const.CR);
    } catch (HopDatabaseException dbe) {
      handleConnectFailure(dbe, message);
    } finally {
      activeDb.set(null);
      shell.getDisplay().asyncExec(this::refreshExecutionResults);
      monitor.done();
    }
  }

  private void runScriptStatementsLoop(
      Database db,
      DatabaseMeta databaseMeta,
      String sqlScript,
      StringBuilder message,
      IProgressMonitor monitor,
      int[] nrStats) {
    List<SqlScriptStatement> statements =
        databaseMeta.getIDatabase().getSqlScriptStatements(sqlScript + Const.CR);
    boolean aborted = false;
    for (SqlScriptStatement sql : statements) {
      if (aborted || monitor.isCanceled()) {
        break;
      }
      if (sql.isQuery()) {
        nrStats[0]++;
        executeSelectStatement(db, sql, nrStats[0], message);
      } else {
        nrStats[0]++;
        aborted = executeDdlStatement(db, sql, databaseMeta, message);
      }
    }
  }

  private void executeSelectStatement(
      Database db, SqlScriptStatement sql, int statNr, StringBuilder message) {
    log.logDetailed("launch SELECT statement: " + Const.CR + sql);
    try {
      List<Object[]> rows = db.getRows(sql.getStatement(), 1000);
      IRowMeta rowMeta = db.getReturnRowMeta();
      if (!rows.isEmpty()) {
        shell
            .getDisplay()
            .syncExec(
                () ->
                    new PreviewRowsDialog(
                            shell,
                            variables,
                            SWT.NONE,
                            BaseMessages.getString(
                                PKG, "SQLEditor.ResultRows.Title", Integer.toString(statNr)),
                            rowMeta,
                            rows)
                        .open());
      } else {
        shell
            .getDisplay()
            .syncExec(
                () -> {
                  MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
                  mb.setMessage(BaseMessages.getString(PKG, "SQLEditor.NoRows.Message", sql));
                  mb.setText(BaseMessages.getString(PKG, "SQLEditor.NoRows.Title"));
                  mb.open();
                });
      }
    } catch (HopDatabaseException dbe) {
      handleSelectExecutionFailure(dbe, sql, message);
    }
  }

  private void handleSelectExecutionFailure(
      HopDatabaseException dbe, SqlScriptStatement sql, StringBuilder message) {
    shell
        .getDisplay()
        .syncExec(
            () ->
                new ErrorDialog(
                    shell,
                    BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Title"),
                    BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Message", sql),
                    dbe));
  }

  private boolean executeDdlStatement(
      Database db, SqlScriptStatement sql, DatabaseMeta databaseMeta, StringBuilder message) {
    log.logDetailed("launch DDL statement: " + Const.CR + sql);
    int startLogLine = HopLogStore.getLastBufferLineNr();
    try {
      log.logDetailed("Executing SQL: " + Const.CR + sql);
      db.execStatement(sql.getStatement());
      message.append(BaseMessages.getString(PKG, "SQLEditor.Log.SQLExecuted", sql));
      message.append(Const.CR);
      if (dbcache != null) {
        dbcache.clear(databaseMeta.getName());
      }
      sql.setOk(true);
      return false;
    } catch (Exception dbe) {
      return handleDdlExecutionFailure(dbe, sql, message);
    } finally {
      int endLogLine = HopLogStore.getLastBufferLineNr();
      sql.setLoggingText(
          HopLogStore.getAppender()
              .getLogBufferFromTo(db.getLogChannelId(), true, startLogLine, endLogLine)
              .toString());
      sql.setComplete(true);
      shell.getDisplay().asyncExec(this::refreshExecutionResults);
    }
  }

  private boolean handleDdlExecutionFailure(
      Exception dbe, SqlScriptStatement sql, StringBuilder message) {
    sql.setOk(false);
    String error = BaseMessages.getString(PKG, "SQLEditor.Log.SQLExecError", sql, dbe.toString());
    message.append(error).append(Const.CR);
    boolean[] cancelLoop = {false};
    shell
        .getDisplay()
        .syncExec(
            () -> {
              ErrorDialog dialog =
                  new ErrorDialog(
                      shell,
                      BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Title"),
                      error,
                      dbe,
                      true);
              cancelLoop[0] = dialog.isCancelled();
            });
    return cancelLoop[0];
  }

  private void handleConnectFailure(HopDatabaseException dbe, StringBuilder message) {
    String error =
        BaseMessages.getString(
            PKG,
            "SQLEditor.Error.CouldNotConnect.Message",
            (connection == null ? "" : connection.getName()),
            dbe.getMessage());
    message.append(error).append(Const.CR);
    shell
        .getDisplay()
        .syncExec(
            () -> {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setMessage(error);
              mb.setText(BaseMessages.getString(PKG, "SQLEditor.Error.CouldNotConnect.Title"));
              mb.open();
            });
  }

  /** During or after an execution we will mark regions of the SQL editor dialog in green or red. */
  protected void refreshExecutionResults() {
    wScript.redraw();
  }
}
