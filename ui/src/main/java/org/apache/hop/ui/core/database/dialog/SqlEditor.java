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

package org.apache.hop.ui.core.database.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
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
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.KeyAdapter;
import org.eclipse.swt.events.KeyEvent;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;

import java.util.List;

/**
 * Dialog that allows the user to launch SQL statements towards the database.
 *
 * @author Matt
 * @since 13-10-2003
 */
public class SqlEditor {
  private static final Class<?> PKG = SqlEditor.class; // For Translator

  public static final ILoggingObject loggingObject =
      new SimpleLoggingObject("SQL Editor", LoggingObjectType.HOP_GUI, null);

  private PropsUi props;

  private Label wlScript;
  private StyledTextComp wScript;
  private FormData fdlScript, fdScript;

  private Label wlPosition;
  private FormData fdlPosition;

  private Button wExec, wClear, wCancel;
  private Listener lsExec, lsClear, lsCancel;

  private String input;
  private DatabaseMeta connection;
  private Shell shell;
  private DbCache dbcache;

  private ILogChannel log;
  private int style = SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN;
  private Shell parentShell;

  private IVariables variables;

  private List<SqlScriptStatement> statements;

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
    props.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageConnection());

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SQLEditor.Title"));

    int margin = props.getMargin();

    // Script line
    wlScript = new Label(shell, SWT.NONE);
    wlScript.setText(BaseMessages.getString(PKG, "SQLEditor.Editor.Label"));
    props.setLook(wlScript);

    fdlScript = new FormData();
    fdlScript.left = new FormAttachment(0, 0);
    fdlScript.top = new FormAttachment(0, 0);
    wlScript.setLayoutData(fdlScript);
    wScript =
        new StyledTextComp(
            this.variables, shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    wScript.setText("");
    props.setLook(wScript, Props.WIDGET_STYLE_FIXED);
    fdScript = new FormData();
    fdScript.left = new FormAttachment(0, 0);
    fdScript.top = new FormAttachment(wlScript, margin);
    fdScript.right = new FormAttachment(100, -10);
    fdScript.bottom = new FormAttachment(100, -70);
    wScript.setLayoutData(fdScript);

    wScript.addModifyListener(arg0 -> setPosition());

    wScript.addKeyListener(
        new KeyAdapter() {
          public void keyPressed(KeyEvent e) {
            setPosition();
          }

          public void keyReleased(KeyEvent e) {
            setPosition();
          }
        });
    wScript.addFocusListener(
        new FocusAdapter() {
          public void focusGained(FocusEvent e) {
            setPosition();
          }

          public void focusLost(FocusEvent e) {
            setPosition();
          }
        });
    wScript.addMouseListener(
        new MouseAdapter() {
          public void mouseDoubleClick(MouseEvent e) {
            setPosition();
          }

          public void mouseDown(MouseEvent e) {
            setPosition();
          }

          public void mouseUp(MouseEvent e) {
            setPosition();
          }
        });

    wlPosition = new Label(shell, SWT.NONE);
    wlPosition.setText(BaseMessages.getString(PKG, "SQLEditor.LineNr.Label", "0"));
    props.setLook(wlPosition);
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment(0, 0);
    fdlPosition.top = new FormAttachment(wScript, margin);
    fdlPosition.right = new FormAttachment(100, 0);
    wlPosition.setLayoutData(fdlPosition);

    wExec = new Button(shell, SWT.PUSH);
    wExec.setText(BaseMessages.getString(PKG, "SQLEditor.Button.Execute"));
    wClear = new Button(shell, SWT.PUSH);
    wClear.setText(BaseMessages.getString(PKG, "SQLEditor.Button.ClearCache"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Close"));

    wClear.setToolTipText(BaseMessages.getString(PKG, "SQLEditor.Button.ClearCache.Tooltip"));

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wExec, wClear, wCancel}, margin, null);

    // Add listeners
    lsCancel = e -> cancel();
    lsClear = e -> clearCache();
    lsExec =
        e -> {
          try {
            exec();
          } catch (Exception ge) {
            // Ignore errors
          }
        };

    wCancel.addListener(SWT.Selection, lsCancel);
    wClear.addListener(SWT.Selection, lsClear);
    wExec.addListener(SWT.Selection, lsExec);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    BaseTransformDialog.setSize(shell);

    getData();

    shell.open();
  }

  public void setPosition() {
    int lineNumber = wScript.getLineNumber();
    int columnNumber = wScript.getColumnNumber();
    wlPosition.setText(
        BaseMessages.getString(
            PKG, "SQLEditor.Position.Label", "" + lineNumber, "" + columnNumber));
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
      // if (connection!= null) wConnection.setText( connection );
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

    StringBuilder message = new StringBuilder();

    Database db = new Database(loggingObject, variables, databaseMeta);
    boolean first = true;

    try {
      db.connect(null);
      String sqlScript =
          Utils.isEmpty(wScript.getSelectionText())
              ? wScript.getText()
              : wScript.getSelectionText();

      // Multiple statements in the script need to be split into individual
      // executable statements
      statements = databaseMeta.getIDatabase().getSqlScriptStatements(sqlScript + Const.CR);

      int nrstats = 0;
      for (SqlScriptStatement sql : statements) {
        if (sql.isQuery()) {
          // A Query
          log.logDetailed("launch SELECT statement: " + Const.CR + sql);

          nrstats++;
          try {
            List<Object[]> rows = db.getRows(sql.getStatement(), 1000);
            IRowMeta rowMeta = db.getReturnRowMeta();
            if (rows.size() > 0) {
              PreviewRowsDialog prd =
                  new PreviewRowsDialog(
                      shell,
                      variables,
                      SWT.NONE,
                      BaseMessages.getString(
                          PKG, "SQLEditor.ResultRows.Title", Integer.toString(nrstats)),
                      rowMeta,
                      rows);
              prd.open();
            } else {
              MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
              mb.setMessage(BaseMessages.getString(PKG, "SQLEditor.NoRows.Message", sql));
              mb.setText(BaseMessages.getString(PKG, "SQLEditor.NoRows.Title"));
              mb.open();
            }
          } catch (HopDatabaseException dbe) {
            new ErrorDialog(
                shell,
                BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Title"),
                BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Message", sql),
                dbe);
          }
        } else {
          log.logDetailed("launch DDL statement: " + Const.CR + sql);

          // A DDL statement
          nrstats++;
          int startLogLine = HopLogStore.getLastBufferLineNr();
          try {

            log.logDetailed("Executing SQL: " + Const.CR + sql);
            db.execStatement(sql.getStatement());

            message.append(BaseMessages.getString(PKG, "SQLEditor.Log.SQLExecuted", sql));
            message.append(Const.CR);

            // Clear the database cache, in case we're using one...
            if (dbcache != null) {
              dbcache.clear(databaseMeta.getName());
            }

            // mark the statement in green in the dialog...
            //
            sql.setOk(true);
          } catch (Exception dbe) {
            sql.setOk(false);
            String error =
                BaseMessages.getString(PKG, "SQLEditor.Log.SQLExecError", sql, dbe.toString());
            message.append(error).append(Const.CR);
            ErrorDialog dialog =
                new ErrorDialog(
                    shell,
                    BaseMessages.getString(PKG, "SQLEditor.ErrorExecSQL.Title"),
                    error,
                    dbe,
                    true);
            if (dialog.isCancelled()) {
              break;
            }
          } finally {
            int endLogLine = HopLogStore.getLastBufferLineNr();
            sql.setLoggingText(
                HopLogStore.getAppender()
                    .getLogBufferFromTo(db.getLogChannelId(), true, startLogLine, endLogLine)
                    .toString());
            sql.setComplete(true);
            refreshExecutionResults();
          }
        }
      }
      message.append(
          BaseMessages.getString(PKG, "SQLEditor.Log.StatsExecuted", Integer.toString(nrstats)));

      message.append(Const.CR);
    } catch (HopDatabaseException dbe) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      String error =
          BaseMessages.getString(
              PKG,
              "SQLEditor.Error.CouldNotConnect.Message",
              (connection == null ? "" : connection.getName()),
              dbe.getMessage());
      message.append(error).append(Const.CR);
      mb.setMessage(error);
      mb.setText(BaseMessages.getString(PKG, "SQLEditor.Error.CouldNotConnect.Title"));
      mb.open();
    } finally {
      db.disconnect();
      refreshExecutionResults();
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

  /** During or after an execution we will mark regions of the SQL editor dialog in green or red. */
  protected void refreshExecutionResults() {
    wScript.redraw();
  }
}
