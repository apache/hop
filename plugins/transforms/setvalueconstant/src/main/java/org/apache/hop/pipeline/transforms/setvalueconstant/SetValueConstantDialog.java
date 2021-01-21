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

package org.apache.hop.pipeline.transforms.setvalueconstant;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SetValueConstantDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = SetValueConstantMeta.class; // For Translator

  private final SetValueConstantMeta input;

  /** all fields from the previous transforms */
  private final Map<String, Integer> inputFields;

  private TableView wFields;

  private ColumnInfo[] colinf;

  private Button wUseVars;

  public SetValueConstantDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(parent, variables, (BaseTransformMeta) in, tr, sname);
    input = (SetValueConstantMeta) in;
    inputFields = new HashMap<>();
  }

  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();
    ModifyListener oldlsMod = lsMod;
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SetValueConstantDialog.Shell.Title"));

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> get());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "SetValueConstantDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Use variable?
    Label wlUseVars = new Label(shell, SWT.RIGHT);
    wlUseVars.setText(BaseMessages.getString(PKG, "SetValueConstantDialog.useVars.Label"));
    props.setLook(wlUseVars);
    FormData fdlUseVars = new FormData();
    fdlUseVars.left = new FormAttachment(0, 0);
    fdlUseVars.right = new FormAttachment(middle, -margin);
    fdlUseVars.top = new FormAttachment(wTransformName, 2 * margin);
    wlUseVars.setLayoutData(fdlUseVars);
    wUseVars = new Button(shell, SWT.CHECK);
    wUseVars.setToolTipText(BaseMessages.getString(PKG, "SetValueConstantDialog.useVars.Tooltip"));
    props.setLook(wUseVars);
    FormData fdUseVars = new FormData();
    fdUseVars.left = new FormAttachment(middle, 0);
    fdUseVars.top = new FormAttachment(wlUseVars, 0, SWT.CENTER);
    fdUseVars.right = new FormAttachment(100, 0);
    wUseVars.setLayoutData(fdUseVars);

    // Table with fields
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SetValueConstantDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wUseVars, margin);
    wlFields.setLayoutData(fdlFields);

    int FieldsCols = 4;
    final int FieldsRows = input.getFields().size();
    colinf = new ColumnInfo[FieldsCols];
    colinf[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetValueConstantDialog.Fieldname.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {},
            false);
    colinf[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetValueConstantDialog.Value.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinf[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetValueConstantDialog.Value.ConversionMask"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            Const.getDateFormats());
    colinf[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SetValueConstantDialog.Value.SetEmptyString"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, "System.Combo.Yes"),
              BaseMessages.getString(PKG, "System.Combo.No")
            });

    wFields =
        new TableView(
          variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            oldlsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.put(row.getValueMeta(i).getName(), i);
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    // Add listeners
    lsDef =
        new SelectionAdapter() {
          public void widgetDefaultSelected(SelectionEvent e) {
            ok();
          }
        };

    wTransformName.addSelectionListener(lsDef);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged(changed);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);

    Const.sortStrings(fieldNames);
    colinf[0].setComboValues(fieldNames);
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformMeta);
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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wUseVars.setSelection(input.isUseVars());
    Table table = wFields.table;
    if (input.getFields().size() > 0) {
      table.removeAll();
    }
    for (int i = 0; i < input.getFields().size(); i++) {
      SetValueConstantMeta.Field field = input.getField(i);
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(0, "" + (i + 1));
      if (field.getFieldName() != null) {
        ti.setText(1, field.getFieldName());
      }
      if (field.getReplaceValue() != null) {
        ti.setText(2, field.getReplaceValue());
      }
      if (field.getReplaceMask() != null) {
        ti.setText(3, field.getReplaceMask());
      }
      ti.setText(
          4,
          field.isEmptyString()
              ? BaseMessages.getString(PKG, "System.Combo.Yes")
              : BaseMessages.getString(PKG, "System.Combo.No"));
    }

    wFields.setRowNums();
    wFields.removeEmptyRows();
    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
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

    input.setUseVars(wUseVars.getSelection());
    int count = wFields.nrNonEmpty();
    List<SetValueConstantMeta.Field> fields = new ArrayList<>();

    // CHECKSTYLE:Indentation:OFF
    for (int i = 0; i < count; i++) {
      TableItem ti = wFields.getNonEmpty(i);
      SetValueConstantMeta.Field field = new SetValueConstantMeta.Field();
      field.setFieldName(ti.getText(1));
      field.setEmptyString(
          BaseMessages.getString(PKG, "System.Combo.Yes").equalsIgnoreCase(ti.getText(4)));
      field.setReplaceValue(field.isEmptyString() ? "" : ti.getText(2));
      field.setReplaceMask(field.isEmptyString() ? "" : ti.getText(3));
      fields.add(field);
    }
    input.setFields(fields);

    dispose();
  }
}
