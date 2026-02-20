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

package org.apache.hop.pipeline.transforms.delay;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class DelayDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DelayMeta.class;

  private final DelayMeta input;
  private ComboVar wTimeout;
  private CCombo wScaleTime;
  private Button wScaleTimeFromField;
  private Label wlScaleTimeField;
  private ComboVar wScaleTimeField;
  private String[] numericFieldNames = new String[0];
  private String[] stringFieldNames = new String[0];

  public DelayDialog(
      Shell parent, IVariables variables, DelayMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DelayDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Timeout label and combo
    Label wlTimeout = new Label(shell, SWT.RIGHT);
    wlTimeout.setText(BaseMessages.getString(PKG, "DelayDialog.Timeout.Label"));
    wlTimeout.setToolTipText(BaseMessages.getString(PKG, "DelayDialog.Timeout.Tooltip"));
    PropsUi.setLook(wlTimeout);
    FormData fdlTimeout = new FormData();
    fdlTimeout.left = new FormAttachment(0, 0);
    fdlTimeout.right = new FormAttachment(middle, -margin);
    fdlTimeout.top = new FormAttachment(wSpacer, margin);
    wlTimeout.setLayoutData(fdlTimeout);

    wTimeout = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTimeout.setToolTipText(BaseMessages.getString(PKG, "DelayDialog.Timeout.Tooltip"));
    PropsUi.setLook(wTimeout);
    wTimeout.addModifyListener(
        e -> {
          input.setChanged();
          wTimeout.setToolTipText(variables.resolve(wTimeout.getText()));
        });
    wTimeout.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            refreshTimeoutFieldItems();
          }
        });
    FormData fdTimeout = new FormData();
    fdTimeout.left = new FormAttachment(middle, 0);
    fdTimeout.top = new FormAttachment(wSpacer, margin);
    fdTimeout.right = new FormAttachment(100, 0);
    wTimeout.setLayoutData(fdTimeout);

    wScaleTime = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wScaleTime.add(BaseMessages.getString(PKG, "DelayDialog.MSScaleTime.Label"));
    wScaleTime.add(BaseMessages.getString(PKG, "DelayDialog.SScaleTime.Label"));
    wScaleTime.add(BaseMessages.getString(PKG, "DelayDialog.MnScaleTime.Label"));
    wScaleTime.add(BaseMessages.getString(PKG, "DelayDialog.HrScaleTime.Label"));
    wScaleTime.select(0); // +1: starts at -1
    PropsUi.setLook(wScaleTime);
    FormData fdScaleTime = new FormData();
    fdScaleTime.left = new FormAttachment(middle, 0);
    fdScaleTime.top = new FormAttachment(wTimeout, margin);
    fdScaleTime.right = new FormAttachment(100, 0);
    wScaleTime.setLayoutData(fdScaleTime);
    wScaleTime.addModifyListener(lsMod);

    wScaleTimeFromField = new Button(shell, SWT.CHECK);
    wScaleTimeFromField.setText(
        BaseMessages.getString(PKG, "DelayDialog.ScaleTimeFromField.Label"));
    PropsUi.setLook(wScaleTimeFromField);
    FormData fdScaleTimeFromField = new FormData();
    fdScaleTimeFromField.left = new FormAttachment(middle, 0);
    fdScaleTimeFromField.top = new FormAttachment(wScaleTime, margin);
    wScaleTimeFromField.setLayoutData(fdScaleTimeFromField);
    wScaleTimeFromField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent selectionEvent) {
            input.setChanged();
            enableScaleTimeControls();
          }
        });

    wlScaleTimeField = new Label(shell, SWT.RIGHT);
    wlScaleTimeField.setText(BaseMessages.getString(PKG, "DelayDialog.ScaleTimeField.Label"));
    wlScaleTimeField.setToolTipText(
        BaseMessages.getString(PKG, "DelayDialog.ScaleTimeField.Tooltip"));
    PropsUi.setLook(wlScaleTimeField);
    FormData fdlScaleTimeField = new FormData();
    fdlScaleTimeField.left = new FormAttachment(0, 0);
    fdlScaleTimeField.right = new FormAttachment(middle, -margin);
    fdlScaleTimeField.top = new FormAttachment(wScaleTimeFromField, margin);
    wlScaleTimeField.setLayoutData(fdlScaleTimeField);

    wScaleTimeField = new ComboVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wScaleTimeField.setToolTipText(
        BaseMessages.getString(PKG, "DelayDialog.ScaleTimeField.Tooltip"));
    PropsUi.setLook(wScaleTimeField);
    wScaleTimeField.addModifyListener(lsMod);
    wScaleTimeField.addFocusListener(
        new FocusAdapter() {
          @Override
          public void focusGained(FocusEvent e) {
            refreshScaleTimeFieldItems();
          }
        });
    FormData fdScaleTimeField = new FormData();
    fdScaleTimeField.left = new FormAttachment(middle, 0);
    fdScaleTimeField.top = new FormAttachment(wScaleTimeFromField, margin);
    fdScaleTimeField.right = new FormAttachment(100, 0);
    wScaleTimeField.setLayoutData(fdScaleTimeField);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    loadAvailableFields();
    wTimeout.setItems(numericFieldNames);
    wScaleTimeField.setItems(stringFieldNames);

    if (!Utils.isEmpty(input.getTimeoutField())) {
      wTimeout.setText(input.getTimeoutField());
    } else if (input.getTimeout() != null) {
      wTimeout.setText(input.getTimeout());
    }
    wTimeout.setToolTipText(variables.resolve(wTimeout.getText()));

    wScaleTime.select(input.getScaleTimeCode());
    wScaleTimeFromField.setSelection(input.isScaleTimeFromField());
    if (input.getScaleTimeField() != null) {
      wScaleTimeField.setText(input.getScaleTimeField());
    }

    enableScaleTimeControls();
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
    transformName = wTransformName.getText(); // return value
    String timeoutField = determineTimeoutField();
    if (timeoutField != null) {
      input.setTimeoutField(timeoutField);
      input.setTimeout(null);
    } else {
      input.setTimeoutField(null);
      input.setTimeout(wTimeout.getText());
    }
    input.setScaleTimeCode(wScaleTime.getSelectionIndex());
    input.setScaleTimeFromField(wScaleTimeFromField.getSelection());
    input.setScaleTimeField(
        wScaleTimeFromField.getSelection() && !Utils.isEmpty(wScaleTimeField.getText())
            ? wScaleTimeField.getText()
            : null);
    dispose();
  }

  private String determineTimeoutField() {
    loadAvailableFields();
    String candidate = wTimeout.getText();
    if (Utils.isEmpty(candidate)) {
      return null;
    }
    for (String field : numericFieldNames) {
      if (candidate.equals(field)) {
        return field;
      }
    }
    return null;
  }

  private void enableScaleTimeControls() {
    boolean useField = wScaleTimeFromField.getSelection();
    wScaleTime.setEnabled(!useField);
    wScaleTimeField.setEnabled(useField);
    wlScaleTimeField.setEnabled(useField);
  }

  private void loadAvailableFields() {
    try {
      IRowMeta prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (prevFields == null) {
        numericFieldNames = new String[0];
        stringFieldNames = new String[0];
        return;
      }

      List<String> numeric = new ArrayList<>();
      List<String> strings = new ArrayList<>();
      for (IValueMeta valueMeta : prevFields.getValueMetaList()) {
        switch (valueMeta.getType()) {
          case IValueMeta.TYPE_INTEGER, IValueMeta.TYPE_NUMBER, IValueMeta.TYPE_BIGNUMBER:
            numeric.add(valueMeta.getName());
            break;
          case IValueMeta.TYPE_STRING:
            strings.add(valueMeta.getName());
            break;
          default:
            break;
        }
      }
      numericFieldNames = numeric.toArray(new String[0]);
      stringFieldNames = strings.toArray(new String[0]);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DelayDialog.UnableToGetFields.Title"),
          BaseMessages.getString(PKG, "DelayDialog.UnableToGetFields.Message"),
          e);
      numericFieldNames = new String[0];
      stringFieldNames = new String[0];
    }
  }

  private void refreshTimeoutFieldItems() {
    loadAvailableFields();
    String previous = wTimeout.getText();
    wTimeout.setItems(numericFieldNames);
    if (!Utils.isEmpty(previous)) {
      wTimeout.setText(previous);
      wTimeout.getCComboWidget().setSelection(new Point(previous.length(), previous.length()));
    }
  }

  private void refreshScaleTimeFieldItems() {
    loadAvailableFields();
    String previous = wScaleTimeField.getText();
    wScaleTimeField.setItems(stringFieldNames);
    if (!Utils.isEmpty(previous)) {
      wScaleTimeField.setText(previous);
      wScaleTimeField
          .getCComboWidget()
          .setSelection(new Point(previous.length(), previous.length()));
    }
  }
}
