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

package org.apache.hop.pipeline.transforms.ifnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hop.core.Const;
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
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

public class IfNullDialog extends BaseTransformDialog {
  private static final Class<?> PKG = IfNullMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";
  public static final String CONST_SYSTEM_COMBO_NO = "System.Combo.No";

  private final IfNullMeta input;

  private int fieldsRows = 0;
  private ModifyListener oldlsMod;

  /** all fields from the previous transforms */
  private IRowMeta prevFields = null;

  /** List of ColumnInfo that should have the previous fields combo box */
  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  private Button wSelectFields;
  private Button wSelectValuesType;
  private Label wlFields;
  private Label wlValueTypes;
  private TableView wFields;
  private TableView wValueTypes;

  private Label wlReplaceByValue;

  private TextVar wReplaceByValue;

  private Label wlMask;
  private CCombo wMask;

  private Label wlSetEmptyStringAll;
  private Button wSetEmptyStringAll;

  public IfNullDialog(
      Shell parent, IVariables variables, IfNullMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "IfNullDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();
    oldlsMod = lsMod;
    fieldsRows = input.getFields().size();

    // ///////////////////////////////
    // START OF All Fields GROUP //
    // ///////////////////////////////

    Group wAllFields = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wAllFields);
    wAllFields.setText(BaseMessages.getString(PKG, "IfNullDialog.AllFields.Label"));

    FormLayout allFieldsgroupLayout = new FormLayout();
    allFieldsgroupLayout.marginWidth = 10;
    allFieldsgroupLayout.marginHeight = 10;
    wAllFields.setLayout(allFieldsgroupLayout);

    // Replace by Value
    wlReplaceByValue = new Label(wAllFields, SWT.RIGHT);
    wlReplaceByValue.setText(BaseMessages.getString(PKG, "IfNullDialog.ReplaceByValue.Label"));
    PropsUi.setLook(wlReplaceByValue);
    FormData fdlReplaceByValue = new FormData();
    fdlReplaceByValue.left = new FormAttachment(0, 0);
    fdlReplaceByValue.right = new FormAttachment(middle, -margin);
    fdlReplaceByValue.top = new FormAttachment(0, margin);
    wlReplaceByValue.setLayoutData(fdlReplaceByValue);

    wReplaceByValue = new TextVar(variables, wAllFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wReplaceByValue.setToolTipText(
        BaseMessages.getString(PKG, "IfNullDialog.ReplaceByValue.Tooltip"));
    PropsUi.setLook(wReplaceByValue);
    FormData fdReplaceByValue = new FormData();
    fdReplaceByValue.left = new FormAttachment(middle, 0);
    fdReplaceByValue.top = new FormAttachment(0, margin);
    fdReplaceByValue.right = new FormAttachment(100, 0);
    wReplaceByValue.setLayoutData(fdReplaceByValue);

    // SetEmptyStringAll line
    wlSetEmptyStringAll = new Label(wAllFields, SWT.RIGHT);
    wlSetEmptyStringAll.setText(
        BaseMessages.getString(PKG, "IfNullDialog.SetEmptyStringAll.Label"));
    PropsUi.setLook(wlSetEmptyStringAll);
    FormData fdlSetEmptyStringAll = new FormData();
    fdlSetEmptyStringAll.left = new FormAttachment(0, 0);
    fdlSetEmptyStringAll.top = new FormAttachment(wReplaceByValue, margin);
    fdlSetEmptyStringAll.right = new FormAttachment(middle, -margin);
    wlSetEmptyStringAll.setLayoutData(fdlSetEmptyStringAll);
    wSetEmptyStringAll = new Button(wAllFields, SWT.CHECK);
    wSetEmptyStringAll.setToolTipText(
        BaseMessages.getString(PKG, "IfNullDialog.SetEmptyStringAll.Tooltip"));
    PropsUi.setLook(wSetEmptyStringAll);
    FormData fdSetEmptyStringAll = new FormData();
    fdSetEmptyStringAll.left = new FormAttachment(middle, 0);
    fdSetEmptyStringAll.top = new FormAttachment(wlSetEmptyStringAll, 0, SWT.CENTER);
    fdSetEmptyStringAll.right = new FormAttachment(100, 0);
    wSetEmptyStringAll.setLayoutData(fdSetEmptyStringAll);
    wSetEmptyStringAll.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableSetEmptyStringAll();
          }
        });

    wlMask = new Label(wAllFields, SWT.RIGHT);
    wlMask.setText(BaseMessages.getString(PKG, "IfNullDialog.Mask.Label"));
    PropsUi.setLook(wlMask);
    FormData fdlMask = new FormData();
    fdlMask.left = new FormAttachment(0, 0);
    fdlMask.top = new FormAttachment(wSetEmptyStringAll, margin);
    fdlMask.right = new FormAttachment(middle, -margin);
    wlMask.setLayoutData(fdlMask);
    wMask = new CCombo(wAllFields, SWT.BORDER | SWT.READ_ONLY);
    wMask.setEditable(true);
    wMask.setItems(Const.getDateFormats());
    PropsUi.setLook(wMask);
    wMask.addModifyListener(lsMod);
    FormData fdMask = new FormData();
    fdMask.left = new FormAttachment(middle, 0);
    fdMask.top = new FormAttachment(wSetEmptyStringAll, margin);
    fdMask.right = new FormAttachment(100, 0);
    wMask.setLayoutData(fdMask);

    FormData fdAllFields = new FormData();
    fdAllFields.left = new FormAttachment(0, margin);
    fdAllFields.top = new FormAttachment(wSpacer, margin);
    fdAllFields.right = new FormAttachment(100, -margin);
    wAllFields.setLayoutData(fdAllFields);

    // ///////////////////////////////////////////////////////////
    // / END OF All Fields GROUP
    // ///////////////////////////////////////////////////////////

    // Select fields?
    Label wlSelectFields = new Label(shell, SWT.RIGHT);
    wlSelectFields.setText(BaseMessages.getString(PKG, "IfNullDialog.SelectFields.Label"));
    PropsUi.setLook(wlSelectFields);
    FormData fdlSelectFields = new FormData();
    fdlSelectFields.left = new FormAttachment(0, 0);
    fdlSelectFields.top = new FormAttachment(wAllFields, margin);
    fdlSelectFields.right = new FormAttachment(middle, -margin);
    wlSelectFields.setLayoutData(fdlSelectFields);
    wSelectFields = new Button(shell, SWT.CHECK);
    wSelectFields.setToolTipText(BaseMessages.getString(PKG, "IfNullDialog.SelectFields.Tooltip"));
    PropsUi.setLook(wSelectFields);
    FormData fdSelectFields = new FormData();
    fdSelectFields.left = new FormAttachment(middle, 0);
    fdSelectFields.top = new FormAttachment(wlSelectFields, 0, SWT.CENTER);
    fdSelectFields.right = new FormAttachment(100, 0);
    wSelectFields.setLayoutData(fdSelectFields);

    // Select type?
    Label wlSelectValuesType = new Label(shell, SWT.RIGHT);
    wlSelectValuesType.setText(BaseMessages.getString(PKG, "IfNullDialog.SelectValuesType.Label"));
    PropsUi.setLook(wlSelectValuesType);
    FormData fdlSelectValuesType = new FormData();
    fdlSelectValuesType.left = new FormAttachment(0, 0);
    fdlSelectValuesType.top = new FormAttachment(wSelectFields, margin);
    fdlSelectValuesType.right = new FormAttachment(middle, -margin);
    wlSelectValuesType.setLayoutData(fdlSelectValuesType);
    wSelectValuesType = new Button(shell, SWT.CHECK);
    wSelectValuesType.setToolTipText(
        BaseMessages.getString(PKG, "IfNullDialog.SelectValuesType.Tooltip"));
    PropsUi.setLook(wSelectValuesType);
    FormData fdSelectValuesType = new FormData();
    fdSelectValuesType.left = new FormAttachment(middle, 0);
    fdSelectValuesType.top = new FormAttachment(wlSelectValuesType, 0, SWT.CENTER);
    fdSelectValuesType.right = new FormAttachment(100, 0);
    wSelectValuesType.setLayoutData(fdSelectValuesType);

    wlValueTypes = new Label(shell, SWT.NONE);
    wlValueTypes.setText(BaseMessages.getString(PKG, "IfNullDialog.ValueTypes.Label"));
    PropsUi.setLook(wlValueTypes);
    FormData fdlValueTypes = new FormData();
    fdlValueTypes.left = new FormAttachment(0, 0);
    fdlValueTypes.top = new FormAttachment(wSelectValuesType, margin);
    wlValueTypes.setLayoutData(fdlValueTypes);

    int valueTypesRows = input.getValueTypes().size();

    ColumnInfo[] colval =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "IfNullDialog.ValueType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              IValueMeta.typeCodes),
          new ColumnInfo(
              BaseMessages.getString(PKG, "IfNullDialog.Value.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "IfNullDialog.Value.ConversionMask"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              Const.getDateFormats()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "IfNullDialog.Value.SetEmptyString"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
              })
        };
    colval[1].setUsingVariables(true);

    wValueTypes =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colval,
            valueTypesRows,
            oldlsMod,
            props);
    FormData fdValueTypes = new FormData();
    fdValueTypes.left = new FormAttachment(0, 0);
    fdValueTypes.top = new FormAttachment(wlValueTypes, margin);
    fdValueTypes.right = new FormAttachment(100, 0);
    fdValueTypes.bottom = new FormAttachment(wlValueTypes, (int) (190 * props.getZoomFactor()));
    wValueTypes.setLayoutData(fdValueTypes);

    getFirstData();

    addFields();

    wSelectValuesType.addListener(
        SWT.Selection,
        e -> {
          activeSelectValuesType();
          input.setChanged();
        });

    wSelectFields.addListener(
        SWT.Selection,
        e -> {
          activeSelectFields();
          input.setChanged();
        });

    getData();

    enableSetEmptyStringAll();

    activeSelectFields();

    activeSelectValuesType();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addFields() {
    int fieldsCols = 4;
    ColumnInfo[] colinf = new ColumnInfo[fieldsCols];

    // Table with fields
    wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "IfNullDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wValueTypes, margin);
    wlFields.setLayoutData(fdlFields);

    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "IfNullDialog.Fieldname.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "IfNullDialog.Value.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "IfNullDialog.Value.ConversionMask"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            Const.getDateFormats());
    colinf[1].setUsingVariables(true);
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "IfNullDialog.Value.SetEmptyString"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO)
            });

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            oldlsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -50);

    wFields.setLayoutData(fdFields);

    setComboValues();
    fieldColumns.add(colinf[0]);
  }

  private void activeSelectFields() {
    if (wSelectFields.getSelection()) {
      wSelectValuesType.setSelection(false);
      wlValueTypes.setEnabled(false);
      wValueTypes.setEnabled(false);
    }
    activeFields();
  }

  private void activeSelectValuesType() {
    if (wSelectValuesType.getSelection()) {
      wSelectFields.setSelection(false);
      wFields.setEnabled(false);
      wlFields.setEnabled(false);
    }
    activeFields();
  }

  private void activeFields() {
    wlFields.setEnabled(wSelectFields.getSelection());
    wFields.setEnabled(wSelectFields.getSelection());
    wGet.setEnabled(wSelectFields.getSelection());
    wlValueTypes.setEnabled(wSelectValuesType.getSelection());
    wValueTypes.setEnabled(wSelectValuesType.getSelection());
    wlReplaceByValue.setEnabled(!wSelectFields.getSelection() && !wSelectValuesType.getSelection());
    wReplaceByValue.setEnabled(!wSelectFields.getSelection() && !wSelectValuesType.getSelection());
    wlMask.setEnabled(!wSelectFields.getSelection() && !wSelectValuesType.getSelection());
    wMask.setEnabled(!wSelectFields.getSelection() && !wSelectValuesType.getSelection());
    wlSetEmptyStringAll.setEnabled(
        !wSelectFields.getSelection() && !wSelectValuesType.getSelection());
    wSetEmptyStringAll.setEnabled(
        !wSelectFields.getSelection() && !wSelectValuesType.getSelection());
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener insertListener = (tableItem, v) -> true;

        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, insertListener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  private void setComboValues() {
    Runnable fieldLoader =
        () -> {
          try {
            prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);

          } catch (HopException e) {
            String msg = BaseMessages.getString(PKG, "IfNullDialog.DoMapping.UnableToFindInput");
            logError(msg);
          }
          String[] prevTransformFieldNames = prevFields.getFieldNames();
          if (prevTransformFieldNames != null) {
            Arrays.sort(prevTransformFieldNames);

            for (ColumnInfo colInfo : fieldColumns) {
              if (colInfo != null) {
                colInfo.setComboValues(prevTransformFieldNames);
              }
            }
          }
        };
    new Thread(fieldLoader).start();
  }

  public void getFirstData() {
    wSelectFields.setSelection(input.isSelectFields());
    wSelectValuesType.setSelection(input.isSelectValuesType());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getReplaceAllByValue() != null) {
      wReplaceByValue.setText(input.getReplaceAllByValue());
    }
    if (input.getReplaceAllMask() != null) {
      wMask.setText(input.getReplaceAllMask());
    }
    wSetEmptyStringAll.setSelection(input.isSetEmptyStringAll());

    wSelectFields.setSelection(input.isSelectFields());
    wSelectValuesType.setSelection(input.isSelectValuesType());

    Table table = wValueTypes.table;
    if (!input.getValueTypes().isEmpty()) {
      table.removeAll();
    }
    for (int i = 0; i < input.getValueTypes().size(); i++) {
      ValueType valueType = input.getValueTypes().get(i);
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      if (valueType.getName() != null) {
        ti.setText(1, valueType.getName());
      }
      if (valueType.getValue() != null) {
        ti.setText(2, valueType.getValue());
      }
      if (valueType.getMask() != null) {
        ti.setText(3, valueType.getMask());
      }
      ti.setText(
          4,
          valueType.isSetEmptyString()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
    }

    wValueTypes.setRowNums();
    wValueTypes.removeEmptyRows();
    wValueTypes.optWidth(true);

    table = wFields.table;
    if (!input.getFields().isEmpty()) {
      table.removeAll();
    }
    for (int i = 0; i < input.getFields().size(); i++) {
      Field field = input.getFields().get(i);
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      if (field.getName() != null) {
        ti.setText(1, field.getName());
      }
      if (field.getValue() != null) {
        ti.setText(2, field.getValue());
      }
      if (field.getMask() != null) {
        ti.setText(3, field.getMask());
      }
      ti.setText(
          4,
          field.isSetEmptyString()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_NO));
    }

    wFields.setRowNums();
    wValueTypes.removeEmptyRows();
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void enableSetEmptyStringAll() {
    wMask.setText("");
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText(); // return value

    input.setSetEmptyStringAll(wSetEmptyStringAll.getSelection());

    if (wSetEmptyStringAll.getSelection()) {
      input.setReplaceAllByValue("");
      input.setReplaceAllMask("");

    } else {
      input.setReplaceAllByValue(wReplaceByValue.getText());
      input.setReplaceAllMask(wMask.getText());
    }

    input.setSelectFields(wSelectFields.getSelection());
    input.setSelectValuesType(wSelectValuesType.getSelection());

    int nrtypes = wValueTypes.nrNonEmpty();
    int nrFields = wFields.nrNonEmpty();

    input.getValueTypes().clear();
    for (int i = 0; i < nrtypes; i++) {
      TableItem ti = wValueTypes.getNonEmpty(i);
      ValueType valueType = new ValueType();
      valueType.setName(ti.getText(1));
      valueType.setSetEmptyString(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(ti.getText(4)));
      if (valueType.isSetEmptyString()) {
        valueType.setValue("");
        valueType.setMask("");
      } else {
        valueType.setValue(ti.getText(2));
        valueType.setMask(ti.getText(3));
      }
      input.getValueTypes().add(valueType);
    }

    input.getFields().clear();
    for (int i = 0; i < nrFields; i++) {
      TableItem ti = wFields.getNonEmpty(i);
      Field field = new Field();
      field.setName(ti.getText(1));
      field.setSetEmptyString(
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(ti.getText(4)));
      if (field.isSetEmptyString()) {
        field.setValue("");
        field.setMask("");
      } else {
        field.setValue(ti.getText(2));
        field.setMask(ti.getText(3));
      }
      input.getFields().add(field);
    }
    dispose();
  }
}
