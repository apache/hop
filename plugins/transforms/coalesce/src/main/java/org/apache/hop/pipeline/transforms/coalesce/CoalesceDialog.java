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

package org.apache.hop.pipeline.transforms.coalesce;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.dialog.EnterOrderedListDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class CoalesceDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = CoalesceMeta.class; // For Translator

  private final CoalesceMeta input;

  private Button wEmptyStrings;

  private TableView wFields;

  private String[] fieldNames;

  public CoalesceDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (CoalesceMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    shell.setText(BaseMessages.getString(PKG, "CoalesceDialog.Shell.Title"));
    shell.setMinimumSize(500,300);
    setShellImage(shell, input);
    props.setLook(shell);

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    shell.setLayout(formLayout);

    int margin = props.getMargin();

    // The buttons at the bottom of the dialog
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // Transform name line
    //
    Label wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    wlTransformName.setLayoutData(new FormDataBuilder().left().top().result());
    props.setLook(wlTransformName);
    
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    wTransformName.addListener(SWT.Modify, e -> input.setChanged());
    wTransformName.addListener(SWT.DefaultSelection, e -> ok());
    wTransformName.setLayoutData(new FormDataBuilder().left().top(wlTransformName,margin).right(100, 0).result());
    props.setLook(wTransformName);
    
    // Treat empty strings as nulls
    //
    wEmptyStrings = new Button(shell, SWT.CHECK);
    wEmptyStrings.setText(BaseMessages.getString(PKG, "CoalesceDialog.Shell.EmptyStringsAsNulls"));
    wEmptyStrings.setLayoutData(new FormDataBuilder().left().top(wTransformName, margin*2).result());
    wEmptyStrings.addListener(SWT.Selection, e -> input.setChanged());
    props.setLook(wEmptyStrings);
    
    
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "CoalesceDialog.Fields.Label"));
    wlFields.setLayoutData(new FormDataBuilder().left().top(wEmptyStrings, margin*2).result());
    props.setLook(wlFields);
    
    SelectionAdapter pathSelection =
        new SelectionAdapter() {
          public void widgetSelected(SelectionEvent e) {

            EnterOrderedListDialog dialog = new EnterOrderedListDialog(shell, SWT.OPEN, fieldNames);

            String fields =
                wFields.getActiveTableItem().getText(wFields.getActiveTableColumn());

            String[] elements = fields.split("\\s*,\\s*");

            dialog.addToSelection(elements);

            String[] result = dialog.open();
            if (result != null) {
              wFields
                  .getActiveTableItem()
                  .setText(wFields.getActiveTableColumn(), String.join(", ", result));
            }
          }
        };

    ColumnInfo[] columns = new ColumnInfo[4];
    columns[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.Name.Label"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    columns[0].setToolTip(BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.Name.Tooltip"));
    columns[0].setUsingVariables(true);

    columns[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.ValueType.Label"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaBase.getTypes());
    columns[1].setToolTip(
        BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.ValueType.Tooltip"));

    columns[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.RemoveInputColumns.Label"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              BaseMessages.getString(PKG, "System.Combo.No"),
              BaseMessages.getString(PKG, "System.Combo.Yes")
            });
    columns[2].setToolTip(
        BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.RemoveInputColumns.Tooltip"));

    columns[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.InputFields.Label"),
            ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
            false);
    columns[3].setToolTip(
        BaseMessages.getString(PKG, "CoalesceDialog.ColumnInfo.InputFields.Tooltip"));
    columns[3].setUsingVariables(true);
    columns[3].setTextVarButtonSelectionListener(pathSelection);
    
    this.wFields =
        new TableView(
            this.getVariables(),
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.SINGLE,
            columns,
            input.getCoalesces().size(),
            e -> input.setChanged(),
            props);
    this.wFields.setLayoutData(
        new FormDataBuilder().left().right(100, 0).top(wlFields, Const.MARGIN).bottom(wOk, margin*2).result());

    this.wFields.getTable().addListener(SWT.Resize, new ColumnsResizer(3, 20, 10, 5, 52));

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    // Search the fields in the background
    final Runnable runnable =
        new Runnable() {
          @Override
          public void run() {
            TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
            if (transformMeta != null) {
              try {
                IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

                fieldNames = new String[row.size()];
                for (int i = 0; i < row.size(); i++) {
                  fieldNames[i] = row.getValueMeta(i).getName();
                }

                Const.sortStrings(fieldNames);
              } catch (HopException e) {
                logError(BaseMessages.getString(PKG, "CoalesceDialog.Log.UnableToFindInput"));
              }
            }
          }
        };
    new Thread(runnable).start();

    // Detect X or ALT-F4 or something that kills this window...
    shell.addListener(SWT.Close, e -> cancel());

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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    wEmptyStrings.setSelection(input.isTreatEmptyStringsAsNulls());

    List<Coalesce> coalesces = input.getCoalesces();
    for (int i = 0; i < coalesces.size(); i++) {

      Coalesce coalesce = coalesces.get(i);
      TableItem item = wFields.getTable().getItem(i);
      item.setText(1, StringUtils.stripToEmpty(coalesce.getName()));
      item.setText(2, ValueMetaBase.getTypeDesc(coalesce.getType()));
      item.setText(3, getStringFromBoolean(coalesce.isRemoveFields()));
      item.setText(4, String.join(", ", coalesce.getInputFields()));
    }

    wFields.setRowNums();
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

    transformName = wTransformName.getText();

    input.setTreatEmptyStringsAsNulls(wEmptyStrings.getSelection());

    int count = wFields.nrNonEmpty();

    List<Coalesce> coalesces = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      TableItem item = wFields.getNonEmpty(i);

      Coalesce coalesce = new Coalesce();
      coalesce.setName(item.getText(1));

      String typeValueText = item.getText(2);
      coalesce.setType(
          Utils.isEmpty(typeValueText)
              ? IValueMeta.TYPE_NONE
              : ValueMetaBase.getType(typeValueText));
      coalesce.setRemoveFields(getBooleanFromString(item.getText(3)));
      coalesce.setInputFields(item.getText(4));
      coalesces.add(coalesce);
    }

    input.setCoalesces(coalesces);

    dispose();
  }

  // TODO: Find a global function
  private static boolean getBooleanFromString(final String s) {

    if (Utils.isEmpty(s)) return false;

    return BaseMessages.getString(PKG, "System.Combo.Yes").equals(s); // $NON-NLS-1$
  }

  // TODO: Find a global function
  private static String getStringFromBoolean(final boolean b) {
    return b
        ? BaseMessages.getString(PKG, "System.Combo.Yes")
        : BaseMessages.getString(PKG, "System.Combo.No");
  }
}
