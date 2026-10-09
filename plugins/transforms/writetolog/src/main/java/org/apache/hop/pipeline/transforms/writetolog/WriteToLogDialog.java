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

package org.apache.hop.pipeline.transforms.writetolog;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogLevel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextComposite;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class WriteToLogDialog extends BaseTransformDialog {
  private static final Class<?> PKG = WriteToLogDialog.class;

  private final WriteToLogMeta input;

  /**
   * Hand-built. The combo shows the translated {@link LogLevel} descriptions and maps the selection
   * back by position. An annotated combo stores {@code Enum.toString()}, which is neither the
   * translated label nor a value {@code Enum.valueOf} can read back.
   */
  private CCombo wLoglevel;

  private StyledTextComp wLogMessage;
  private TableView wFields;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] colinf;

  private GuiCompositeWidgets widgets;

  public WriteToLogDialog(
      Shell parent, IVariables variables, WriteToLogMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "WriteToLogDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wSpacer,
            wOk,
            WriteToLogMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run inside this call, before the field is assigned.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "WriteToLog.Tab.Options"),
                  "0100",
                  null,
                  this::addLogLevel);
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "WriteToLog.Tab.Message"),
                  "0200",
                  null,
                  this::addMessage);
            });

    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
            if (WriteToLogMeta.WIDGET_LIMIT_ROWS.equals(widgetId)) {
              enableFields();
            }
          }
        });

    populateLogLevel();
    populateLogMessage();
    populateFields();
    enableFields();
    searchPrevTransformFields();

    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /**
   * Log level combo on the Options tab. It shares that tab with the annotated fields, so the row
   * hangs below the last of them. Anchoring it to the top of the composite draws it on top of the
   * first annotated row.
   */
  private void addLogLevel(Composite parent) {
    Label wlLoglevel = new Label(parent, SWT.RIGHT);
    wlLoglevel.setText(BaseMessages.getString(PKG, "WriteToLogDialog.Loglevel.Label"));
    PropsUi.setLook(wlLoglevel);
    FormData fdlLoglevel = new FormData();
    fdlLoglevel.left = new FormAttachment(0, 0);
    fdlLoglevel.right = new FormAttachment(middle, -margin);
    // WIDGET_LIMIT_ROWS_NUMBER is the last annotated field on this tab (order 0400).
    Control lastOnTab = widgets.getWidgetsMap().get(WriteToLogMeta.WIDGET_LIMIT_ROWS_NUMBER);
    fdlLoglevel.top =
        lastOnTab == null ? new FormAttachment(0, 0) : new FormAttachment(lastOnTab, margin);
    wlLoglevel.setLayoutData(fdlLoglevel);

    wLoglevel = new CCombo(parent, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wLoglevel.setItems(LogLevel.getLogLevelDescriptions());
    PropsUi.setLook(wLoglevel);
    wLoglevel.setToolTipText(BaseMessages.getString(PKG, "WriteToLogDialog.Loglevel.Tooltip"));
    FormData fdLoglevel = new FormData();
    fdLoglevel.left = new FormAttachment(middle, 0);
    fdLoglevel.top = new FormAttachment(wlLoglevel, 0, SWT.CENTER);
    fdLoglevel.right = new FormAttachment(100, 0);
    wLoglevel.setLayoutData(fdLoglevel);
    wLoglevel.addListener(SWT.Selection, e -> input.setChanged());
  }

  /** Message tab: the log message template and the fields it can reference. */
  private void addMessage(Composite parent) {
    Label wlLogMessage = new Label(parent, SWT.NONE);
    wlLogMessage.setText(BaseMessages.getString(PKG, "WriteToLogDialog.LogMessage.Label"));
    PropsUi.setLook(wlLogMessage);
    FormData fdlLogMessage = new FormData();
    fdlLogMessage.left = new FormAttachment(0, 0);
    fdlLogMessage.right = new FormAttachment(100, 0);
    fdlLogMessage.top = new FormAttachment(0, 0);
    wlLogMessage.setLayoutData(fdlLogMessage);

    wLogMessage =
        new StyledTextComp(
            variables,
            parent,
            SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
            TextComposite.STYLE_TYPE_TEXT);
    PropsUi.setLook(wLogMessage, Props.WIDGET_STYLE_FIXED);
    wLogMessage.addListener(SWT.Modify, e -> input.setChanged());
    FormData fdLogMessage = new FormData();
    fdLogMessage.left = new FormAttachment(0, 0);
    fdLogMessage.top = new FormAttachment(wlLogMessage, margin);
    fdLogMessage.right = new FormAttachment(100, 0);
    // Preferred height. The field grid below keeps the rest of the tab.
    fdLogMessage.height = (int) (200 * props.getZoomFactor());
    wLogMessage.setLayoutData(fdLogMessage);

    Label wlFields = new Label(parent, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "WriteToLogDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(100, 0);
    fdlFields.top = new FormAttachment(wLogMessage, margin);
    wlFields.setLayoutData(fdlFields);

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "WriteToLogDialog.Fieldname.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false)
        };

    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            1,
            e -> input.setChanged(),
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  private void populateLogLevel() {
    LogLevel logLevel = input.getLogLevel();
    if (logLevel == null) {
      logLevel = LogLevel.BASIC;
    }
    wLoglevel.select(logLevel.getLevel());
  }

  private void persistLogLevel() {
    // Descriptions are translated, so the stored value is the enum at this position.
    int logLevelIndex = wLoglevel.getSelectionIndex();
    if (logLevelIndex < 0 || logLevelIndex >= LogLevel.values().length) {
      input.setLogLevel(LogLevel.BASIC);
    } else {
      input.setLogLevel(LogLevel.values()[logLevelIndex]);
    }
  }

  private void populateLogMessage() {
    wLogMessage.setText(Const.NVL(input.getLogMessage(), ""));
  }

  private void persistLogMessage() {
    input.setLogMessage(Const.NVL(wLogMessage.getText(), ""));
  }

  private void populateFields() {
    if (wFields == null || wFields.isDisposed()) {
      return;
    }
    wFields.clearAll();
    if (input.getLogFields() != null) {
      for (LogField field : input.getLogFields()) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        if (field != null) {
          item.setText(1, Const.NVL(field.getName(), ""));
        }
      }
    }
    if (wFields.table.getItemCount() == 0) {
      new TableItem(wFields.table, SWT.NONE);
    }
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void persistFields() {
    if (wFields == null || wFields.isDisposed()) {
      return;
    }
    List<LogField> fields = new ArrayList<>();
    for (TableItem item : wFields.getNonEmptyItems()) {
      LogField field = new LogField();
      field.setName(item.getText(1));
      fields.add(field);
    }
    input.setLogFields(fields);
  }

  private void setComboBoxes() {
    if (colinf == null) {
      return;
    }
    colinf[0].setComboValues(ConstUi.sortFieldNames(inputFields));
  }

  private void searchPrevTransformFields() {
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta == null) {
            return;
          }
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);
            for (int i = 0; i < row.size(); i++) {
              inputFields.add(row.getValueMeta(i).getName());
            }
            setComboBoxes();
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
          }
        });
  }

  private void enableFields() {
    setEnabled(
        WriteToLogMeta.WIDGET_LIMIT_ROWS_NUMBER, isChecked(WriteToLogMeta.WIDGET_LIMIT_ROWS));
  }

  private boolean isChecked(String widgetId) {
    if (widgets == null) {
      return false;
    }
    Control control = widgets.getWidgetsMap().get(widgetId);
    return control instanceof Button button && button.getSelection();
  }

  private void setEnabled(String widgetId, boolean enabled) {
    if (widgets == null) {
      return;
    }
    Control label = widgets.getLabelsMap().get(widgetId);
    if (label != null && !label.isDisposed()) {
      label.setEnabled(enabled);
    }
    Control widget = widgets.getWidgetsMap().get(widgetId);
    if (widget != null && !widget.isDisposed()) {
      widget.setEnabled(enabled);
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();

    widgets.getWidgetsContents(input, WriteToLogMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    persistLogLevel();
    persistLogMessage();
    persistFields();

    dispose();
  }
}
