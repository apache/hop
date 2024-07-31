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

package org.apache.hop.ui.core.dialog;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.variables.DescribedVariable;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** Allows the user to edit the system settings of the hop.config file. */
public class HopDescribedVariablesDialog extends Dialog {
  private static final Class<?> PKG = HopDescribedVariablesDialog.class;

  private TableView wFields;

  private Shell shell;
  private final PropsUi props;
  private String message;
  private List<DescribedVariable> describedVariables;
  private String selectedVariable;

  /**
   * Constructs a new dialog
   *
   * @param parent The parent shell to link to
   * @param selectedVariable
   */
  public HopDescribedVariablesDialog(
      Shell parent,
      String message,
      List<DescribedVariable> describedVariables,
      String selectedVariable) {
    super(parent, SWT.NONE);
    this.message = message;
    this.describedVariables = describedVariables;
    this.selectedVariable = selectedVariable;
    props = PropsUi.getInstance();
  }

  public List<DescribedVariable> open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    shell.setText(BaseMessages.getString(PKG, "HopDescribedVariablesDialog.Title"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/variable.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    PropsUi.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();

    // The buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wEncode = new Button(shell, SWT.PUSH);
    wEncode.setText(BaseMessages.getString(PKG, "HopDescribedVariablesDialog.Button.EncodeValue"));
    wEncode.addListener(SWT.Selection, e -> encodeSelectedValue());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wEncode, wCancel}, margin, wFields);

    // Message line at the top
    //
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(Const.NVL(message, ""));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, margin);
    wlFields.setLayoutData(fdlFields);

    int fieldsRows = 0;

    ColumnInfo[] columns = {
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Name.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Value.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Description.Label"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false,
          false),
    };
    columns[2].setDisabledListener(rowNr -> false);

    // Fields between the label and the buttons
    //
    wFields =
        new TableView(
            Variables.getADefaultVariableSpace(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            fieldsRows,
            null,
            props);

    wFields.setReadonly(false);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, 2 * margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return describedVariables;
  }

  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    try {
      for (DescribedVariable describedVariable : describedVariables) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        int col = 1;
        item.setText(col++, Const.NVL(describedVariable.getName(), ""));
        item.setText(col++, Const.NVL(describedVariable.getValue(), ""));
        item.setText(col++, Const.NVL(describedVariable.getDescription(), ""));
      }

      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth(true);

      // Select the selected variable...
      //
      if (selectedVariable != null) {
        for (TableItem item : wFields.table.getItems()) {
          if (item.getText(1).equals(selectedVariable)) {
            wFields.table.setSelection(item);
          }
        }
        wFields.table.showSelection();
      }

    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Title"),
          BaseMessages.getString(PKG, "HopPropertiesFileDialog.Exception.ErrorLoadingData.Message"),
          e);
    }
  }

  private void cancel() {
    describedVariables = null;
    dispose();
  }

  private void ok() {
    describedVariables.clear();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      String name = item.getText(1);
      String value = item.getText(2);
      String description = item.getText(3);
      describedVariables.add(new DescribedVariable(name, value, description));
    }

    dispose();
  }

  private void encodeSelectedValue() {
    try {
      ITwoWayPasswordEncoder encoder = Encr.getEncoder();
      for (int index : wFields.getSelectionIndices()) {
        TableItem item = wFields.table.getItem(index);
        String value = item.getText(2);
        String encoded = encoder.encode(value, true);
        item.setText(2, Const.NVL(encoded, ""));
      }
      // We can't undo after this operation
      //
      wFields.clearUndo();
      wFields.optimizeTableView();
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error encoding the value on the selected lines", e);
    }
  }
}
