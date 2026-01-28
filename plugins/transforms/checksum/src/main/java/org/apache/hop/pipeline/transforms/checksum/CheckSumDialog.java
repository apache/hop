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

package org.apache.hop.pipeline.transforms.checksum;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
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
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class CheckSumDialog extends BaseTransformDialog {
  private static final Class<?> PKG = CheckSumDialog.class;

  private final CheckSumMeta input;

  private CCombo wType;

  private TableView wFields;

  private Text wResult;

  private ColumnInfo[] colinf;

  private final List<String> inputFields = new ArrayList<>();

  private Label wlResultType;
  private CCombo wResultType;

  public CheckSumDialog(
      Shell parent, IVariables variables, CheckSumMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CheckSumDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).get(e -> get()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Type
    Label wlType = new Label(shell, SWT.RIGHT);
    wlType.setText(BaseMessages.getString(PKG, "CheckSumDialog.Type.Label"));
    PropsUi.setLook(wlType);
    FormData fdlType = new FormData();
    fdlType.left = new FormAttachment(0, 0);
    fdlType.right = new FormAttachment(middle, -margin);
    fdlType.top = new FormAttachment(wSpacer, margin);
    wlType.setLayoutData(fdlType);
    wType = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wType.setItems(CheckSumMeta.CheckSumType.getDescriptions());
    wType.select(0);
    PropsUi.setLook(wType);
    FormData fdType = new FormData();
    fdType.left = new FormAttachment(middle, 0);
    fdType.top = new FormAttachment(wSpacer, margin);
    fdType.right = new FormAttachment(100, 0);
    wType.setLayoutData(fdType);
    wType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activeResultType();
          }
        });

    // ResultType
    wlResultType = new Label(shell, SWT.RIGHT);
    wlResultType.setText(BaseMessages.getString(PKG, "CheckSumDialog.ResultType.Label"));
    PropsUi.setLook(wlResultType);
    FormData fdlResultType = new FormData();
    fdlResultType.left = new FormAttachment(0, 0);
    fdlResultType.right = new FormAttachment(middle, -margin);
    fdlResultType.top = new FormAttachment(wType, margin);
    wlResultType.setLayoutData(fdlResultType);
    wResultType = new CCombo(shell, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wResultType.setItems(CheckSumMeta.ResultType.getDescriptions());
    wResultType.select(0);
    PropsUi.setLook(wResultType);
    FormData fdResultType = new FormData();
    fdResultType.left = new FormAttachment(middle, 0);
    fdResultType.top = new FormAttachment(wType, margin);
    fdResultType.right = new FormAttachment(100, 0);
    wResultType.setLayoutData(fdResultType);
    wResultType.addSelectionListener(
        new SelectionAdapter() {

          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Result line...
    Label wlResult = new Label(shell, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "CheckSumDialog.Result.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wResultType, margin);
    wlResult.setLayoutData(fdlResult);
    wResult = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(middle, 0);
    fdResult.top = new FormAttachment(wResultType, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    // Table with fields
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "CheckSumDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wResult, margin);
    wlFields.setLayoutData(fdlFields);

    final int nrCols = 1;
    final int nrFields = input.getFields().size();

    colinf = new ColumnInfo[nrCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CheckSumDialog.Fieldname.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            nrFields,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    getData();
    activeResultType();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void activeResultType() {
    int currentType = wType.getSelectionIndex();
    // Only available for type MD5 and SHA
    boolean active =
        currentType == 2
            || currentType == 3
            || currentType == 4
            || currentType == 5
            || currentType == 6;
    wlResultType.setEnabled(active);
    wResultType.setEnabled(active);
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener insertListener =
            (tableItem, v) -> {
              tableItem.setText(2, BaseMessages.getString(PKG, "System.Combo.Yes"));
              return true;
            };
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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wType.setText(input.getCheckSumType().getDescription());
    wResult.setText(Const.NVL(input.getResultFieldName(), ""));
    wResultType.setText(input.getResultType().getDescription());

    Table table = wFields.table;
    if (!input.getFields().isEmpty()) {
      table.removeAll();
    }
    for (int i = 0; i < input.getFields().size(); i++) {
      Field field = input.getFields().get(i);
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      ti.setText(1, Const.NVL(field.getName(), ""));
    }

    wFields.setRowNums();
    wFields.optWidth(true);
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

    input.setCheckSumType(CheckSumMeta.CheckSumType.getTypeFromDescription(wType.getText()));
    input.setResultFieldName(wResult.getText());
    input.setResultType(CheckSumMeta.ResultType.getTypeFromDescription(wResultType.getText()));

    input.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      input.getFields().add(new Field(item.getText(1)));
    }
    dispose();
  }
}
