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

package org.apache.hop.pipeline.transforms.memgroupby;

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
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class MemoryGroupByDialog extends BaseTransformDialog {
  private static final Class<?> PKG = MemoryGroupByMeta.class;

  private TableView wGroup;

  private TableView wAgg;

  private Button wAlwaysAddResult;

  private final MemoryGroupByMeta input;

  private ColumnInfo[] ciKey;
  private ColumnInfo[] ciReturn;

  private final List<String> inputFields = new ArrayList<>();

  public MemoryGroupByDialog(
      Shell parent,
      IVariables variables,
      MemoryGroupByMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    SelectionListener lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Always pass a result rows as output
    //
    Label wlAlwaysAddResult = new Label(shell, SWT.RIGHT);
    wlAlwaysAddResult.setText(
        BaseMessages.getString(PKG, "MemoryGroupByDialog.AlwaysAddResult.Label"));
    wlAlwaysAddResult.setToolTipText(
        BaseMessages.getString(PKG, "MemoryGroupByDialog.AlwaysAddResult.ToolTip"));
    PropsUi.setLook(wlAlwaysAddResult);
    FormData fdlAlwaysAddResult = new FormData();
    fdlAlwaysAddResult.left = new FormAttachment(0, 0);
    fdlAlwaysAddResult.top = new FormAttachment(wTransformName, margin);
    fdlAlwaysAddResult.right = new FormAttachment(middle, -margin);
    wlAlwaysAddResult.setLayoutData(fdlAlwaysAddResult);
    wAlwaysAddResult = new Button(shell, SWT.CHECK);
    wAlwaysAddResult.setToolTipText(
        BaseMessages.getString(PKG, "MemoryGroupByDialog.AlwaysAddResult.ToolTip"));
    PropsUi.setLook(wAlwaysAddResult);
    FormData fdAlwaysAddResult = new FormData();
    fdAlwaysAddResult.left = new FormAttachment(middle, 0);
    fdAlwaysAddResult.top = new FormAttachment(wlAlwaysAddResult, 0, SWT.CENTER);
    fdAlwaysAddResult.right = new FormAttachment(100, 0);
    wAlwaysAddResult.setLayoutData(fdAlwaysAddResult);
    wAlwaysAddResult.addSelectionListener(lsSel);

    Label wlGroup = new Label(shell, SWT.NONE);
    wlGroup.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.Group.Label"));
    PropsUi.setLook(wlGroup);
    FormData fdlGroup = new FormData();
    fdlGroup.left = new FormAttachment(0, 0);
    fdlGroup.top = new FormAttachment(wlAlwaysAddResult, 2 * margin);
    wlGroup.setLayoutData(fdlGroup);

    int groupCols = 1;
    int groupRows = input.getGroups().size();

    ciKey = new ColumnInfo[groupCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.GroupField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    wGroup =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            groupRows,
            lsMod,
            props);

    Button wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.GetFields.Button"));
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(wlGroup, margin);
    fdGet.right = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    FormData fdGroup = new FormData();
    fdGroup.left = new FormAttachment(0, 0);
    fdGroup.top = new FormAttachment(wlGroup, margin);
    fdGroup.right = new FormAttachment(wGet, -margin);
    fdGroup.bottom = new FormAttachment(45, 0);
    wGroup.setLayoutData(fdGroup);

    // THE Aggregate fields
    Label wlAgg = new Label(shell, SWT.NONE);
    wlAgg.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.Aggregates.Label"));
    PropsUi.setLook(wlAgg);
    FormData fdlAgg = new FormData();
    fdlAgg.left = new FormAttachment(0, 0);
    fdlAgg.top = new FormAttachment(wGroup, margin);
    wlAgg.setLayoutData(fdlAgg);

    int aggCols = 4;
    int aggRows = input.getAggregates().size();

    ciReturn = new ColumnInfo[aggCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.Name"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.Subject"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            MemoryGroupByMeta.GroupType.getDescriptions());
    ciReturn[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.Value"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    ciReturn[3].setToolTip(
        BaseMessages.getString(PKG, "MemoryGroupByDialog.ColumnInfo.Value.Tooltip"));
    ciReturn[3].setUsingVariables(true);

    wAgg =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            aggRows,
            lsMod,
            props);

    Button wGetAgg = new Button(shell, SWT.PUSH);
    wGetAgg.setText(BaseMessages.getString(PKG, "MemoryGroupByDialog.GetLookupFields.Button"));
    FormData fdGetAgg = new FormData();
    fdGetAgg.top = new FormAttachment(wlAgg, margin);
    fdGetAgg.right = new FormAttachment(100, 0);
    wGetAgg.setLayoutData(fdGetAgg);

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

    // THE BUTTONS
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    FormData fdAgg = new FormData();
    fdAgg.left = new FormAttachment(0, 0);
    fdAgg.top = new FormAttachment(wlAgg, margin);
    fdAgg.right = new FormAttachment(wGetAgg, -margin);
    fdAgg.bottom = new FormAttachment(wOk, -margin);
    wAgg.setLayoutData(fdAgg);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wGet.addListener(SWT.Selection, e -> get());
    wGetAgg.addListener(SWT.Selection, e -> getAgg());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(backupChanged);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciKey[0].setComboValues(fieldNames);
    ciReturn[1].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "MemoryGroupByDialog.Log.GettingKeyInfo"));

    wAlwaysAddResult.setSelection(input.isAlwaysGivingBackOneRow());

    for (int i = 0; i < input.getGroups().size(); i++) {
      TableItem item = wGroup.table.getItem(i);
      item.setText(1, Const.NVL(input.getGroups().get(i).getField(), ""));
    }

    for (int i = 0; i < input.getAggregates().size(); i++) {
      GAggregate aggregate = input.getAggregates().get(i);
      TableItem item = wAgg.table.getItem(i);
      item.setText(1, Const.NVL(aggregate.getField(), ""));
      item.setText(2, Const.NVL(aggregate.getSubject(), ""));
      item.setText(3, aggregate.getType().getDescription());
      item.setText(4, Const.NVL(aggregate.getValueField(), ""));
    }

    wGroup.setRowNums();
    wGroup.optWidth(true);
    wAgg.setRowNums();
    wAgg.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setAlwaysGivingBackOneRow(wAlwaysAddResult.getSelection());

    input.getGroups().clear();
    for (TableItem item : wGroup.getNonEmptyItems()) {
      input.getGroups().add(new GGroup(item.getText(1)));
    }

    input.getAggregates().clear();
    for (TableItem item : wAgg.getNonEmptyItems()) {
      GAggregate aggregate = new GAggregate();
      aggregate.setField(item.getText(1));
      aggregate.setSubject(item.getText(2));
      aggregate.setType(MemoryGroupByMeta.GroupType.getTypeWithDescription(item.getText(3)));
      aggregate.setValueField(item.getText(4));
      input.getAggregates().add(aggregate);
    }

    transformName = wTransformName.getText();

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (!Utils.isEmpty(r)) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wGroup, 1, new int[] {1}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MemoryGroupByDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "MemoryGroupByDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void getAgg() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (!Utils.isEmpty(r)) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wAgg, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MemoryGroupByDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "MemoryGroupByDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }
}
