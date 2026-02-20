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

package org.apache.hop.pipeline.transforms.numberrange;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class NumberRangeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = NumberRangeMeta.class;

  private final NumberRangeMeta input;

  private CCombo inputFieldControl;
  private Text outputFieldControl;
  private Text fallBackValueControl;
  private TableView rulesControl;

  public NumberRangeDialog(
      Shell parent,
      IVariables variables,
      NumberRangeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "NumberRangeDialog.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Create controls
    inputFieldControl =
        createLineCombo(
            lsMod, BaseMessages.getString(PKG, "NumberRangeDialog.InputField"), wSpacer);
    outputFieldControl =
        createLine(
            lsMod, BaseMessages.getString(PKG, "NumberRangeDialog.OutputField"), inputFieldControl);

    inputFieldControl.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            loadComboOptions();
            shell.setCursor(null);
            busy.dispose();
          }
        });
    fallBackValueControl =
        createLine(
            lsMod,
            BaseMessages.getString(PKG, "NumberRangeDialog.DefaultValue"),
            outputFieldControl);

    createRulesTable(lsMod);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Creates the table of rules */
  private void createRulesTable(ModifyListener lsMod) {
    Label rulesLable = new Label(shell, SWT.NONE);
    rulesLable.setText(BaseMessages.getString(PKG, "NumberRangeDialog.Ranges"));
    PropsUi.setLook(rulesLable);
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment(0, 0);
    lableFormData.right = new FormAttachment(props.getMiddlePct(), -PropsUi.getMargin());
    lableFormData.top = new FormAttachment(fallBackValueControl, PropsUi.getMargin());
    rulesLable.setLayoutData(lableFormData);

    final int FieldsRows = input.getRules().size();

    ColumnInfo[] colinf = new ColumnInfo[3];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NumberRangeDialog.LowerBound"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NumberRangeDialog.UpperBound"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "NumberRangeDialog.Value"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    rulesControl =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(rulesLable, PropsUi.getMargin());
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * PropsUi.getMargin());
    rulesControl.setLayoutData(fdFields);
  }

  private Text createLine(ModifyListener lsMod, String lableText, Control prevControl) {
    // Value line
    Label lable = new Label(shell, SWT.RIGHT);
    lable.setText(lableText);
    PropsUi.setLook(lable);
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment(0, 0);
    lableFormData.right = new FormAttachment(props.getMiddlePct(), -PropsUi.getMargin());
    // In case it is the first control
    if (prevControl != null) {
      lableFormData.top = new FormAttachment(prevControl, PropsUi.getMargin());
    } else {
      lableFormData.top = new FormAttachment(0, PropsUi.getMargin());
    }
    lable.setLayoutData(lableFormData);

    Text control = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(control);
    control.addModifyListener(lsMod);
    FormData widgetFormData = new FormData();
    widgetFormData.left = new FormAttachment(props.getMiddlePct(), 0);
    // In case it is the first control
    if (prevControl != null) {
      widgetFormData.top = new FormAttachment(prevControl, PropsUi.getMargin());
    } else {
      widgetFormData.top = new FormAttachment(0, PropsUi.getMargin());
    }
    widgetFormData.right = new FormAttachment(100, 0);
    control.setLayoutData(widgetFormData);

    return control;
  }

  private CCombo createLineCombo(ModifyListener lsMod, String lableText, Control prevControl) {
    // Value line
    Label lable = new Label(shell, SWT.RIGHT);
    lable.setText(lableText);
    PropsUi.setLook(lable);
    FormData lableFormData = new FormData();
    lableFormData.left = new FormAttachment(0, 0);
    lableFormData.right = new FormAttachment(props.getMiddlePct(), -PropsUi.getMargin());
    // In case it is the first control
    if (prevControl != null) {
      lableFormData.top = new FormAttachment(prevControl, PropsUi.getMargin());
    } else {
      lableFormData.top = new FormAttachment(0, PropsUi.getMargin());
    }
    lable.setLayoutData(lableFormData);

    CCombo control = new CCombo(shell, SWT.BORDER);
    PropsUi.setLook(control);
    control.addModifyListener(lsMod);
    FormData widgetFormData = new FormData();
    widgetFormData.left = new FormAttachment(props.getMiddlePct(), 0);
    // In case it is the first control
    if (prevControl != null) {
      widgetFormData.top = new FormAttachment(prevControl, PropsUi.getMargin());
    } else {
      widgetFormData.top = new FormAttachment(0, PropsUi.getMargin());
    }
    widgetFormData.right = new FormAttachment(100, 0);
    control.setLayoutData(widgetFormData);

    return control;
  }

  // Read data from input (TextFileInputInfo)
  public void getData() {
    // Get fields
    String inputField = input.getInputField();
    if (inputField != null) {
      inputFieldControl.setText(inputField);
    }

    String outputField = input.getOutputField();
    if (outputField != null) {
      outputFieldControl.setText(outputField);
    }

    String fallBackValue = input.getFallBackValue();
    if (fallBackValue != null) {
      fallBackValueControl.setText(fallBackValue);
    }

    for (int i = 0; i < input.getRules().size(); i++) {
      NumberRangeRule rule = input.getRules().get(i);
      TableItem item = rulesControl.table.getItem(i);
      item.setText(1, Const.NVL(rule.getLowerBound(), ""));
      item.setText(2, Const.NVL(rule.getUpperBound(), ""));
      item.setText(3, Const.NVL(rule.getValue(), ""));
    }
    rulesControl.setRowNums();
    rulesControl.optWidth(true);
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

    input.setInputField(inputFieldControl.getText());
    input.setOutputField(outputFieldControl.getText());
    input.setFallBackValue(fallBackValueControl.getText());

    input.emptyRules();

    int count = rulesControl.nrNonEmpty();
    for (int i = 0; i < count; i++) {
      TableItem item = rulesControl.getNonEmpty(i);
      String lowerBoundStr = item.getText(1);
      String upperBoundStr = item.getText(2);
      String value = item.getText(3);

      input.addRule(lowerBoundStr, upperBoundStr, value);
    }

    dispose();
  }

  private void loadComboOptions() {
    try {
      String fieldvalue = null;
      if (inputFieldControl.getText() != null) {
        fieldvalue = inputFieldControl.getText();
      }
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        inputFieldControl.setItems(r.getFieldNames());
      }
      if (fieldvalue != null) {
        inputFieldControl.setText(fieldvalue);
      }

    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "NumberRangeDialog.Title"),
          BaseMessages.getString(PKG, "NumberRangeDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
