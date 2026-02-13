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

package org.apache.hop.pipeline.transforms.formula;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.formula.editor.FormulaEditor;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class FormulaDialog extends BaseTransformDialog {
  private static final Class<?> PKG = FormulaDialog.class;
  public static final String SYSTEM_COMBO_YES = "System.Combo.Yes";

  private TableView wFields;

  private FormulaMeta currentMeta;
  private FormulaMeta originalMeta;

  private final List<String> inputFields = new ArrayList<>();
  private ColumnInfo[] colinf;

  private String[] fieldNames;

  public FormulaDialog(
      Shell parent, IVariables variables, FormulaMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    // The order here is important... currentMeta is looked at for changes
    currentMeta = transformMeta;
    originalMeta = (FormulaMeta) transformMeta.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "FormulaDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    FormData fdFields;
    Shell parent = getParent();
    Display display = parent.getDisplay();

    ModifyListener lsMod = e -> currentMeta.setChanged();
    changed = currentMeta.hasChanged();

    Control lastControl = wSpacer;

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "FormulaDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = currentMeta.getFormulas() != null ? currentMeta.getFormulas().size() : 1;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.NewField.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.Formula.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.ValueType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.Replace.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO),
          new ColumnInfo(
              BaseMessages.getString(PKG, "FormulaDialog.NA.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, SYSTEM_COMBO_YES),
              BaseMessages.getString(PKG, "System.Combo.No")),
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
    wFields.setLayoutData(fdFields);

    new Thread(
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
                } catch (HopTransformException e) {
                  logError(BaseMessages.getString(PKG, "FormulaDialog.Log.UnableToFindInput"));
                }
              }
            })
        .start();

    colinf[1].setSelectionAdapter(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (fieldNames == null) {
              return;
            }

            TableView tv = (TableView) e.widget;
            TableItem item = tv.table.getItem(e.y);
            String formula = item.getText(e.x);

            try {
              if (!shell.isDisposed()) {
                FormulaEditor libFormulaEditor =
                    new FormulaEditor(
                        variables,
                        shell,
                        SWT.APPLICATION_MODAL | SWT.SHEET,
                        Const.NVL(formula, ""),
                        fieldNames);
                formula = libFormulaEditor.open();
                if (formula != null && !tv.isDisposed()) {
                  tv.setText(formula, e.x, e.y);
                }
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell, "Error", "There was an unexpected error in the formula editor", ex);
            }
          }
        });

    wFields.addModifyListener(
        arg0 ->
            // Now set the combo's
            shell.getDisplay().asyncExec(this::setComboBoxes));

    wTransformName.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ok();
          }
        });

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    currentMeta.setChanged(changed);

    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    shell
        .getDisplay()
        .syncExec(
            () -> {
              // Add the newly create fields.
              //
              String[] fieldNames = ConstUi.sortFieldNames(inputFields);
              colinf[5].setComboValues(fieldNames);
              FormulaDialog.this.fieldNames = fieldNames;
            });
  }

  /** Copy information from the meta-data currentMeta to the dialog fields. */
  public void getData() {

    if (currentMeta.getFormulas() != null) {
      for (int i = 0; i < currentMeta.getFormulas().size(); i++) {
        FormulaMetaFunction fn = currentMeta.getFormulas().get(i);
        TableItem item = wFields.table.getItem(i);
        item.setText(1, Const.NVL(fn.getFieldName(), ""));
        item.setText(2, Const.NVL(fn.getFormula(), ""));
        item.setText(3, Const.NVL(ValueMetaFactory.getValueMetaName(fn.getValueType()), ""));
        if (fn.getValueLength() >= 0) {
          item.setText(4, "" + fn.getValueLength());
        }
        if (fn.getValuePrecision() >= 0) {
          item.setText(5, "" + fn.getValuePrecision());
        }
        item.setText(6, Const.NVL(fn.getReplaceField(), ""));
        item.setText(
            7,
            Boolean.TRUE.equals(fn.isSetNa())
                ? BaseMessages.getString(PKG, SYSTEM_COMBO_YES)
                : BaseMessages.getString(PKG, "System.Combo.No"));
      }
    }

    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    currentMeta.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    currentMeta.getFormulas().clear();

    int nrNonEmptyFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      String fieldName = item.getText(1);
      String formulaString = item.getText(2);
      int valueType = ValueMetaFactory.getIdForValueMeta(item.getText(3));
      int valueLength = Const.toInt(item.getText(4), -1);
      int valuePrecision = Const.toInt(item.getText(5), -1);
      String replaceField = item.getText(6);
      boolean setNa =
          BaseMessages.getString(PKG, SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(7));

      currentMeta
          .getFormulas()
          .add(
              new FormulaMetaFunction(
                  fieldName,
                  formulaString,
                  valueType,
                  valueLength,
                  valuePrecision,
                  replaceField,
                  setNa));
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }
}
