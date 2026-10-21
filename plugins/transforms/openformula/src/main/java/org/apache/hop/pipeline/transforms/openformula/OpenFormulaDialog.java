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

package org.apache.hop.pipeline.transforms.openformula;

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
import org.apache.hop.pipeline.transforms.openformula.editor.OpenFormulaEditor;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/**
 * Formula grid for the OpenFormula transform. The grid scrolls inside the box above the buttons.
 */
public class OpenFormulaDialog extends BaseTransformDialog {
  private static final Class<?> PKG = OpenFormulaMeta.class;

  private TableView wFields;
  private final OpenFormulaMeta currentMeta;
  private final OpenFormulaMeta originalMeta;
  private final List<String> inputFields = new ArrayList<>();
  private ColumnInfo[] colinf;
  private String[] fieldNames;

  public OpenFormulaDialog(
      Shell parent,
      IVariables variables,
      OpenFormulaMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    currentMeta = transformMeta;
    originalMeta = (OpenFormulaMeta) transformMeta.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "OpenFormulaDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = currentMeta.hasChanged();

    String group = BaseMessages.getString(PKG, "OpenFormulaDialog.Group.Formulas");
    GuiCompositeWidgets.addScrolledComposite(
        shell,
        variables,
        wSpacer,
        wOk,
        OpenFormulaMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
        currentMeta,
        widgets -> widgets.registerExtraGroup(group, "0100", null, this::addFormulas));

    loadInputFields();
    getData();
    currentMeta.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFormulas(Composite parent) {
    int fieldsRows = currentMeta.getFormulas() != null ? currentMeta.getFormulas().size() : 1;
    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.NewField.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.Formula.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.ValueType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "OpenFormulaDialog.Replace.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO),
        };

    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            Math.max(fieldsRows, 1),
            e -> currentMeta.setChanged(),
            props);

    Control above = null;
    for (Control child : parent.getChildren()) {
      if (child instanceof Link) {
        above = child;
      }
    }
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top =
        above == null ? new FormAttachment(0, 0) : new FormAttachment(above, PropsUi.getMargin());
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);

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
                OpenFormulaEditor editor =
                    new OpenFormulaEditor(
                        variables,
                        shell,
                        SWT.APPLICATION_MODAL | SWT.SHEET,
                        Const.NVL(formula, ""),
                        fieldNames);
                formula = editor.open();
                if (formula != null && !tv.isDisposed()) {
                  tv.setText(formula, e.x, e.y);
                  currentMeta.setChanged();
                }
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(PKG, "OpenFormulaDialog.EditorError.Title"),
                  BaseMessages.getString(PKG, "OpenFormulaDialog.EditorError.Message"),
                  ex);
            }
          }
        });
  }

  private void loadInputFields() {
    new Thread(
            () -> {
              TransformMeta source = pipelineMeta.findTransform(transformName);
              if (source == null) {
                return;
              }
              try {
                IRowMeta row = pipelineMeta.getPrevTransformFields(variables, source);
                for (int i = 0; i < row.size(); i++) {
                  inputFields.add(row.getValueMeta(i).getName());
                }
                setComboBoxes();
              } catch (HopTransformException e) {
                logError(BaseMessages.getString(PKG, "OpenFormulaDialog.Log.UnableToFindInput"));
              }
            })
        .start();
  }

  protected void setComboBoxes() {
    if (shell == null || shell.isDisposed() || colinf == null) {
      return;
    }
    shell
        .getDisplay()
        .syncExec(
            () -> {
              if (colinf == null || shell.isDisposed()) {
                return;
              }
              String[] names = ConstUi.sortFieldNames(inputFields);
              colinf[5].setComboValues(names);
              fieldNames = names;
            });
  }

  public void getData() {
    if (wFields == null || currentMeta.getFormulas() == null) {
      return;
    }
    for (int i = 0; i < currentMeta.getFormulas().size(); i++) {
      OpenFormulaMetaFunction fn = currentMeta.getFormulas().get(i);
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
    transformName = wTransformName.getText();
    currentMeta.getFormulas().clear();
    if (wFields != null) {
      int nrNonEmptyFields = wFields.nrNonEmpty();
      for (int i = 0; i < nrNonEmptyFields; i++) {
        TableItem item = wFields.getNonEmpty(i);
        currentMeta
            .getFormulas()
            .add(
                new OpenFormulaMetaFunction(
                    item.getText(1),
                    item.getText(2),
                    ValueMetaFactory.getIdForValueMeta(item.getText(3)),
                    Const.toInt(item.getText(4), -1),
                    Const.toInt(item.getText(5), -1),
                    item.getText(6)));
      }
    }
    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
    }
    dispose();
  }
}
