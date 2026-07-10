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
 *
 */

package org.apache.hop.pipeline.transforms.sortedschemamerge;

import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;

public class SortedSchemaMergeDialog extends BaseTransformDialog {

  private static final Class<?> PKG = SortedSchemaMergeMeta.class;

  private final SortedSchemaMergeMeta input;
  private String[] previousTransforms;
  private TableView wInputs;
  private TableView wSortKeys;

  public SortedSchemaMergeDialog(
      Shell parent,
      IVariables variables,
      SortedSchemaMergeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SortedSchemaMergeDialog.Title"));

    buildButtonBar()
        .ok(e -> ok())
        .get(e -> getSourceTransformNames())
        .cancel(e -> cancel())
        .build();

    changed = input.hasChanged();
    ModifyListener lsMod = e -> input.setChanged();
    previousTransforms = pipelineMeta.getPrevTransformNames(transformName);

    Label wlSortKeys = new Label(shell, SWT.NONE);
    wlSortKeys.setText(BaseMessages.getString(PKG, "SortedSchemaMergeDialog.SortKeys.Label"));
    PropsUi.setLook(wlSortKeys);
    FormData fdlSortKeys = new FormData();
    fdlSortKeys.left = new FormAttachment(0, 0);
    fdlSortKeys.top = new FormAttachment(wSpacer, margin);
    wlSortKeys.setLayoutData(fdlSortKeys);

    ColumnInfo[] sortKeyColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortedSchemaMergeDialog.SortKeys.Column.Field"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortedSchemaMergeDialog.SortKeys.Column.Ascending"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              true)
        };

    wSortKeys =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            sortKeyColumns,
            1,
            lsMod,
            props);
    FormData fdSortKeys = new FormData();
    fdSortKeys.left = new FormAttachment(0, 0);
    fdSortKeys.top = new FormAttachment(wlSortKeys, margin);
    fdSortKeys.right = new FormAttachment(100, 0);
    fdSortKeys.bottom = new FormAttachment(55, 0);
    wSortKeys.setLayoutData(fdSortKeys);

    Label wlNote = new Label(shell, SWT.WRAP);
    wlNote.setText(BaseMessages.getString(PKG, "SortedSchemaMergeDialog.SortedInputNote.Label"));
    PropsUi.setLook(wlNote);
    FormData fdlNote = new FormData();
    fdlNote.left = new FormAttachment(0, 0);
    fdlNote.right = new FormAttachment(100, 0);
    fdlNote.bottom = new FormAttachment(wOk, -margin);
    wlNote.setLayoutData(fdlNote);

    Label wlInputs = new Label(shell, SWT.NONE);
    wlInputs.setText(BaseMessages.getString(PKG, "SortedSchemaMergeDialog.Inputs.Label"));
    PropsUi.setLook(wlInputs);
    FormData fdlInputs = new FormData();
    fdlInputs.left = new FormAttachment(0, 0);
    fdlInputs.top = new FormAttachment(wSortKeys, margin);
    wlInputs.setLayoutData(fdlInputs);

    ColumnInfo[] inputColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SortedSchemaMergeDialog.Inputs.Column.Transform"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              previousTransforms,
              false)
        };

    wInputs =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            inputColumns,
            1,
            lsMod,
            props);
    FormData fdInputs = new FormData();
    fdInputs.left = new FormAttachment(0, 0);
    fdInputs.top = new FormAttachment(wlInputs, margin);
    fdInputs.right = new FormAttachment(100, 0);
    fdInputs.bottom = new FormAttachment(wlNote, -margin);
    wInputs.setLayoutData(fdInputs);

    populateDialog();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void populateDialog() {
    for (SortedSchemaMergeSortKey sortKey : input.getSortKeys()) {
      TableItem ti = new TableItem(wSortKeys.table, SWT.NONE);
      ti.setText(1, Const.NVL(sortKey.getFieldName(), ""));
      ti.setText(2, sortKey.isAscending() ? "Y" : "N");
    }
    wSortKeys.optimizeTableView();

    for (SortedSchemaMergeInput mergeInput : input.getInputs()) {
      TableItem ti = new TableItem(wInputs.table, SWT.NONE);
      ti.setText(1, Const.NVL(mergeInput.getTransformName(), ""));
    }
    wInputs.optimizeTableView();
  }

  private void getSourceTransformNames() {
    wInputs.removeAll();
    Table table = wInputs.table;
    for (String previousTransform : previousTransforms) {
      TableItem ti = new TableItem(table, SWT.NONE);
      ti.setText(1, previousTransform);
    }
    wInputs.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    transformName = wTransformName.getText();

    input.getSortKeys().clear();
    for (TableItem item : wSortKeys.getNonEmptyItems()) {
      SortedSchemaMergeSortKey sortKey = new SortedSchemaMergeSortKey();
      sortKey.setFieldName(item.getText(1));
      sortKey.setAscending(!"N".equalsIgnoreCase(item.getText(2)));
      input.getSortKeys().add(sortKey);
    }

    input.getInputs().clear();
    for (TableItem item : wInputs.getNonEmptyItems()) {
      input.getInputs().add(new SortedSchemaMergeInput(item.getText(1)));
    }

    dispose();
  }
}
