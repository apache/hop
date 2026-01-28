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

package org.apache.hop.pipeline.transforms.janino;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.janino.editor.FormulaEditor;
import org.apache.hop.pipeline.transforms.util.JaninoCheckerUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class JaninoDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JaninoMeta.class;

  private TableView wFields;

  private final JaninoMeta currentMeta;
  private final JaninoMeta originalMeta;

  private final List<String> inputFields = new ArrayList<>();
  private ColumnInfo[] colinf;

  public JaninoDialog(
      Shell parent, IVariables variables, JaninoMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    // The order here is important... currentMeta is looked at for changes
    currentMeta = transformMeta;
    originalMeta = (JaninoMeta) currentMeta.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "JaninoDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = currentMeta.hasChanged();

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "JaninoDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wSpacer, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = currentMeta.getFormula() != null ? currentMeta.getFormula().length : 1;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.NewField.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.Janino.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.ValueType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "JaninoDialog.Replace.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {}),
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -margin);
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
                inputFields.add(row.getValueMeta(i).getName());
              }

              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "JaninoDialog.Log.UnableToFindInput"));
            }
          }
        };
    new Thread(runnable).start();

    colinf[1].setSelectionAdapter(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (inputFields == null) {
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
                        inputFields);
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
              String[] fieldNames = ConstUi.sortFieldNames(inputFields);
              colinf[5].setComboValues(fieldNames);
            });
  }

  /** Copy information from the meta-data currentMeta to the dialog fields. */
  public void getData() {
    if (currentMeta.getFormula() != null) {
      for (int i = 0; i < currentMeta.getFormula().length; i++) {
        JaninoMetaFunction fn = currentMeta.getFormula()[i];
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

    int nrNonEmptyFields = wFields.nrNonEmpty();

    // Check if code contains content that is not allowed
    JaninoCheckerUtil janinoCheckerUtil = new JaninoCheckerUtil();

    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      List<String> codeCheck = janinoCheckerUtil.checkCode(item.getText(2));
      if (!codeCheck.isEmpty()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setText("Invalid Code");
        mb.setMessage("Script contains code that is not allowed : " + codeCheck);
        mb.open();
        return;
      }
    }

    transformName = wTransformName.getText(); // return value

    currentMeta.allocate(wFields.nrNonEmpty());

    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      String fieldName = item.getText(1);
      String formula = item.getText(2);
      int valueType = ValueMetaFactory.getIdForValueMeta(item.getText(3));
      int valueLength = Const.toInt(item.getText(4), -1);
      int valuePrecision = Const.toInt(item.getText(5), -1);
      String replaceField = item.getText(6);

      currentMeta.getFormula()[i] =
          new JaninoMetaFunction(
              fieldName, formula, valueType, valueLength, valuePrecision, replaceField);
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }
}
