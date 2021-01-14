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

package org.apache.hop.ui.core.dialog;

import org.apache.hop.core.Const;
import org.apache.hop.core.config.DescribedVariable;
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
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

import java.util.List;

/**
 * Allows the user to edit the system settings of the hop.config file.
 *
 * @author Matt
 */
public class HopDescribedVariablesDialog extends Dialog {
  private static final Class<?> PKG = HopDescribedVariablesDialog.class; // For Translator

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
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX);
    shell.setText(BaseMessages.getString(PKG, "HopDescribedVariablesDialog.Title"));
    shell.setImage(
        GuiResource.getInstance()
            .getImage("ui/images/variable.svg", ConstUi.SMALL_ICON_SIZE, ConstUi.SMALL_ICON_SIZE));
    props.setLook(shell);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);

    int margin = props.getMargin();

    // The buttons at the bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, wFields);

    // Message line at the top
    //
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(Const.NVL(message, ""));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(0, margin);
    wlFields.setLayoutData(fdlFields);

    int FieldsRows = 0;

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
            FieldsRows,
            null,
            props);

    wFields.setReadonly(false);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, 2 * margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener(
        new ShellAdapter() {
          public void shellClosed(ShellEvent e) {
            cancel();
          }
        });

    getData();

    BaseTransformDialog.setSize(shell);

    shell.open();
    while (!shell.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
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
}
