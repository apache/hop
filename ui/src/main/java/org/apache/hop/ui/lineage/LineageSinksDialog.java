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

package org.apache.hop.ui.lineage;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.lineage.hub.LineageHub;
import org.apache.hop.lineage.hub.LineageSinkStatus;
import org.apache.hop.lineage.spi.LineageSinkHealth;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * How the lineage sinks of this Hop process are doing: per sink what it sent, what it couldn't
 * deliver, what waits, and the last error. Refreshes itself while it is open.
 */
public class LineageSinksDialog {
  private static final Class<?> PKG = LineageSinksDialog.class;

  /** How often the table is refreshed while the dialog is open. */
  static final int REFRESH_MS = 2000;

  private final Shell parent;
  private final IVariables variables;
  private final LineageHub hub;

  private Shell shell;
  private Label wSummary;
  private TableView wSinks;

  public LineageSinksDialog(Shell parent, IVariables variables, LineageHub hub) {
    this.parent = parent;
    this.variables = variables;
    this.hub = hub;
  }

  public void open() {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "LineageSinksDialog.Title"));

    Button wRefresh = new Button(shell, SWT.PUSH);
    wRefresh.setText(BaseMessages.getString(PKG, "LineageSinksDialog.Button.Refresh"));
    wRefresh.addListener(SWT.Selection, e -> refresh());
    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> dispose());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wRefresh, wClose}, margin, null);

    wSummary = new Label(shell, SWT.LEFT | SWT.WRAP);
    PropsUi.setLook(wSummary);
    FormData fdSummary = new FormData();
    fdSummary.left = new FormAttachment(0, 0);
    fdSummary.top = new FormAttachment(0, 0);
    fdSummary.right = new FormAttachment(100, 0);
    wSummary.setLayoutData(fdSummary);

    ColumnInfo[] columns = {
      text("Sink"),
      text("Type"),
      text("ConfiguredWith"),
      text("State"),
      number("Sent"),
      number("Failed"),
      number("Rejected"),
      number("Dropped"),
      number("Waiting"),
      number("OnDisk"),
      text("LastSent"),
      text("LastErrorAt"),
      text("LastError"),
    };
    wSinks =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            0,
            true,
            null,
            props);
    wSinks.setSortable(false);
    FormData fdSinks = new FormData();
    fdSinks.left = new FormAttachment(0, 0);
    fdSinks.top = new FormAttachment(wSummary, margin);
    fdSinks.right = new FormAttachment(100, 0);
    fdSinks.bottom = new FormAttachment(wClose, -2 * margin);
    wSinks.setLayoutData(fdSinks);

    refresh();
    scheduleRefresh();

    BaseDialog.defaultShellHandling(shell, c -> dispose(), c -> dispose());
  }

  private static ColumnInfo text(String key) {
    return new ColumnInfo(
        BaseMessages.getString(PKG, "LineageSinksDialog.Column." + key),
        ColumnInfo.COLUMN_TYPE_TEXT,
        false,
        true);
  }

  private static ColumnInfo number(String key) {
    return new ColumnInfo(
        BaseMessages.getString(PKG, "LineageSinksDialog.Column." + key),
        ColumnInfo.COLUMN_TYPE_TEXT,
        true,
        true);
  }

  private void scheduleRefresh() {
    shell
        .getDisplay()
        .timerExec(
            REFRESH_MS,
            () -> {
              if (shell != null && !shell.isDisposed()) {
                refresh();
                scheduleRefresh();
              }
            });
  }

  /** Reads the sinks' status from the hub and shows it. */
  void refresh() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    wSummary.setText(
        BaseMessages.getString(
            PKG,
            hub.isEnabled()
                ? "LineageSinksDialog.Summary.Enabled"
                : "LineageSinksDialog.Summary.NotEnabled",
            String.valueOf(hub.getQueuedEventCount()),
            String.valueOf(hub.getDroppedEventCount())));
    List<LineageSinkStatus> sinks = hub.sinkStatus();
    wSinks.table.removeAll();
    for (LineageSinkStatus sink : sinks) {
      TableItem item = new TableItem(wSinks.table, SWT.NONE);
      String[] values = row(sink);
      for (int i = 0; i < values.length; i++) {
        item.setText(i + 1, Const.NVL(values[i], ""));
      }
    }
    if (sinks.isEmpty()) {
      TableItem item = new TableItem(wSinks.table, SWT.NONE);
      item.setText(1, BaseMessages.getString(PKG, "LineageSinksDialog.NoSinks"));
    }
    wSinks.setRowNums();
    wSinks.optWidth(true);
    shell.layout(true, true);
  }

  /** The cells of a sink's row, in column order. */
  static String[] row(LineageSinkStatus sink) {
    LineageSinkHealth health = sink.health();
    String state;
    if (sink.startError() != null) {
      state = BaseMessages.getString(PKG, "LineageSinksDialog.State.Failed");
    } else if (sink.started()) {
      state = BaseMessages.getString(PKG, "LineageSinksDialog.State.Running");
    } else {
      state = BaseMessages.getString(PKG, "LineageSinksDialog.State.NotStarted");
    }
    String lastError = sink.startError();
    long lastErrorMillis = 0;
    if (lastError == null && health != null) {
      lastError = health.lastError();
      lastErrorMillis = health.lastErrorMillis();
    }
    return new String[] {
      sink.name(),
      sink.type(),
      BaseMessages.getString(
          PKG,
          sink.fromRunConfiguration()
              ? "LineageSinksDialog.ConfiguredWith.RunConfiguration"
              : "LineageSinksDialog.ConfiguredWith.Variables"),
      state,
      health == null ? "" : String.valueOf(health.sent()),
      health == null ? "" : String.valueOf(health.failed()),
      health == null ? "" : String.valueOf(health.rejected()),
      health == null ? "" : String.valueOf(health.dropped()),
      health == null ? "" : String.valueOf(health.waiting()),
      health == null || health.spooled() < 0 ? "" : String.valueOf(health.spooled()),
      health == null ? "" : time(health.lastSentMillis()),
      time(lastErrorMillis),
      lastError,
    };
  }

  private static String time(long millis) {
    return millis <= 0 ? "" : new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date(millis));
  }

  private void dispose() {
    if (shell != null && !shell.isDisposed()) {
      PropsUi.getInstance().setScreen(new WindowProperty(shell));
      shell.dispose();
    }
  }
}
