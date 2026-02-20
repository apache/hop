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

package org.apache.hop.pipeline.transforms.calculator;

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
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class CalculatorDialog extends BaseTransformDialog {
  private static final Class<?> PKG = CalculatorMeta.class;
  public static final String CONST_SYSTEM_COMBO_YES = "System.Combo.Yes";

  private Button wFailIfNoFile;

  private TableView wFields;

  private final CalculatorMeta currentMeta;
  private final CalculatorMeta originalMeta;

  private final List<String> inputFields = new ArrayList<>();
  private ColumnInfo[] colinf;

  public CalculatorDialog(
      Shell parent, IVariables variables, CalculatorMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    // The order here is important... currentMeta is looked at for changes
    currentMeta = transformMeta;
    originalMeta = transformMeta.clone();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CalculatorDialog.DialogTitle"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = currentMeta.hasChanged();

    // Fail if no File line
    wFailIfNoFile = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wFailIfNoFile);
    wFailIfNoFile.setToolTipText(
        BaseMessages.getString(PKG, "CalculatorDialog.FailIfNoFileTooltip"));
    wFailIfNoFile.setText(BaseMessages.getString(PKG, "CalculatorDialog.FailIfNoFile"));
    FormData fdFailIfNoFile = new FormData();
    fdFailIfNoFile.left = new FormAttachment(wlTransformName, margin);
    fdFailIfNoFile.top = new FormAttachment(wSpacer, margin);
    fdFailIfNoFile.right = new FormAttachment(100);
    wFailIfNoFile.setLayoutData(fdFailIfNoFile);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "CalculatorDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wFailIfNoFile, margin);
    wlFields.setLayoutData(fdlFields);

    final int nrFieldsRows = currentMeta.getFunctions().size();

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.NewFieldColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.CalculationColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.FieldAColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.FieldBColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.FieldCColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.ValueTypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.LengthColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.PrecisionColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.RemoveColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, "System.Combo.No"),
                BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              }),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.ConversionMask.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              6),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.DecimalSymbol.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.GroupingSymbol.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CalculatorDialog.CurrencySymbol.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[1].setSelectionAdapter(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            EnterSelectionDialog esd =
                new EnterSelectionDialog(
                    shell,
                    CalculationType.descriptions,
                    BaseMessages.getString(PKG, "CalculatorDialog.SelectCalculationType.Title"),
                    BaseMessages.getString(PKG, "CalculatorDialog.SelectCalculationType.Message"));
            String string = esd.open();
            if (string != null) {
              TableView tv = (TableView) e.widget;
              tv.setText(string, e.x, e.y);
              currentMeta.setChanged();
            }
          }
        });

    // Draw line separator
    Label hSeparator = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSeparator = new FormData();
    fdhSeparator.left = new FormAttachment(0, 0);
    fdhSeparator.right = new FormAttachment(100, 0);
    fdhSeparator.bottom = new FormAttachment(wOk, -margin);
    hSeparator.setLayoutData(fdhSeparator);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            nrFieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(hSeparator, -margin);
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
              logError(BaseMessages.getString(PKG, "CalculatorDialog.Log.UnableToFindInput"));
            }
          }
        };
    new Thread(runnable).start();

    wFields.addModifyListener(
        arg0 ->
            // Now set the combo's
            shell.getDisplay().asyncExec(this::setComboBoxes));

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final List<String> fields = new ArrayList<>();

    // Add the currentMeta fields...
    for (String s : inputFields) {
      fields.add(s);
    }

    shell
        .getDisplay()
        .syncExec(
            () -> {
              // Add the newly create fields.
              //
              int nrNonEmptyFields = wFields.nrNonEmpty();
              for (int i = 0; i < nrNonEmptyFields; i++) {
                TableItem item = wFields.getNonEmpty(i);
                fields.add(item.getText(1));
              }
            });

    String[] fieldNames = ConstUi.sortFieldNames(fields);
    colinf[2].setComboValues(fieldNames);
    colinf[3].setComboValues(fieldNames);
    colinf[4].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data currentMeta to the dialog fields. */
  public void getData() {
    for (int i = 0; i < currentMeta.getFunctions().size(); i++) {
      CalculatorMetaFunction fn = currentMeta.getFunctions().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(fn.getFieldName(), ""));
      item.setText(2, Const.NVL(fn.getCalcType().getDescription(), ""));
      item.setText(3, Const.NVL(fn.getFieldA(), ""));
      item.setText(4, Const.NVL(fn.getFieldB(), ""));
      item.setText(5, Const.NVL(fn.getFieldC(), ""));
      item.setText(6, Const.NVL(fn.getValueType(), ""));
      if (fn.getValueLength() >= 0) {
        item.setText(7, "" + fn.getValueLength());
      }
      if (fn.getValuePrecision() >= 0) {
        item.setText(8, "" + fn.getValuePrecision());
      }
      item.setText(
          9,
          fn.isRemovedFromResult()
              ? BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES)
              : BaseMessages.getString(PKG, "System.Combo.No"));
      item.setText(10, Const.NVL(fn.getConversionMask(), ""));
      item.setText(11, Const.NVL(fn.getDecimalSymbol(), ""));
      item.setText(12, Const.NVL(fn.getGroupingSymbol(), ""));
      item.setText(13, Const.NVL(fn.getCurrencySymbol(), ""));
    }

    wFailIfNoFile.setSelection(currentMeta.isFailIfNoFile());

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

    currentMeta.setFailIfNoFile(wFailIfNoFile.getSelection());
    currentMeta.getFunctions().clear();

    for (TableItem item : wFields.getNonEmptyItems()) {

      String fieldName = item.getText(1);
      CalculationType calcType = CalculationType.findByDescription(item.getText(2));

      String fieldA = item.getText(3);
      String fieldB = item.getText(4);
      String fieldC = item.getText(5);
      String valueType = item.getText(6);
      int valueLength = Const.toInt(item.getText(7), -1);
      int valuePrecision = Const.toInt(item.getText(8), -1);
      boolean removed =
          BaseMessages.getString(PKG, CONST_SYSTEM_COMBO_YES).equalsIgnoreCase(item.getText(9));
      String conversionMask = item.getText(10);
      String decimalSymbol = item.getText(11);
      String groupingSymbol = item.getText(12);
      String currencySymbol = item.getText(13);

      currentMeta
          .getFunctions()
          .add(
              new CalculatorMetaFunction(
                  fieldName,
                  calcType,
                  fieldA,
                  fieldB,
                  fieldC,
                  valueType,
                  valueLength,
                  valuePrecision,
                  conversionMask,
                  decimalSymbol,
                  groupingSymbol,
                  currencySymbol,
                  removed));
    }

    if (!originalMeta.equals(currentMeta)) {
      currentMeta.setChanged();
      changed = currentMeta.hasChanged();
    }

    dispose();
  }

  protected Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "calculator.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }
}
