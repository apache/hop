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

package org.apache.hop.beam.transforms.bigtable;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class BeamBigtableOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamBigtableOutputDialog.class;
  private final BeamBigtableOutputMeta input;

  private TextVar wProjectId;
  private TextVar wInstanceId;
  private TextVar wTableId;
  private ComboVar wKeyField;
  private TableView wColumns;

  public BeamBigtableOutputDialog(
      Shell parent,
      IVariables variables,
      BeamBigtableOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).get(e -> getFields()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    Control lastControl = null;

    String[] sourceFields;
    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames();
    } catch (Exception e) {
      sourceFields = new String[] {};
      LogChannel.UI.logError("Error getting source fields", e);
    }

    Label wlProjectId = new Label(wContent, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.ProjectId"));
    PropsUi.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(0, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlInstanceId = new Label(wContent, SWT.RIGHT);
    wlInstanceId.setText(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.InstanceId"));
    PropsUi.setLook(wlInstanceId);
    FormData fdlInstanceId = new FormData();
    fdlInstanceId.left = new FormAttachment(0, 0);
    fdlInstanceId.top = new FormAttachment(lastControl, margin);
    fdlInstanceId.right = new FormAttachment(middle, -margin);
    wlInstanceId.setLayoutData(fdlInstanceId);
    wInstanceId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInstanceId);
    FormData fdInstanceId = new FormData();
    fdInstanceId.left = new FormAttachment(middle, 0);
    fdInstanceId.top = new FormAttachment(wlInstanceId, 0, SWT.CENTER);
    fdInstanceId.right = new FormAttachment(100, 0);
    wInstanceId.setLayoutData(fdInstanceId);
    lastControl = wInstanceId;

    Label wlTableId = new Label(wContent, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.TableId"));
    PropsUi.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    Label wlKeyField = new Label(wContent, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.KeyField"));
    PropsUi.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(lastControl, margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);
    wKeyField = new ComboVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wKeyField);
    wKeyField.setItems(sourceFields);
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(middle, 0);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    lastControl = wKeyField;

    Label wlColumns = new Label(wContent, SWT.LEFT);
    wlColumns.setText(BaseMessages.getString(PKG, "BeamBigtableOutputDialog.Columns"));
    PropsUi.setLook(wlColumns);
    FormData fdlColumns = new FormData();
    fdlColumns.left = new FormAttachment(0, 0);
    fdlColumns.top = new FormAttachment(lastControl, margin);
    fdlColumns.right = new FormAttachment(100, 0);
    wlColumns.setLayoutData(fdlColumns);
    lastControl = wlColumns;

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableOutputDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableOutputDialog.Column.Family"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableOutputDialog.Column.SourceField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              sourceFields,
              false)
        };

    wColumns =
        new TableView(
            variables, wContent, SWT.BORDER, columns, input.getColumns().size(), null, props);
    FormData fdColumns = new FormData();
    fdColumns.left = new FormAttachment(0, 0);
    fdColumns.top = new FormAttachment(lastControl, margin);
    fdColumns.right = new FormAttachment(100, 0);
    fdColumns.bottom = new FormAttachment(100, -margin);
    wColumns.setLayoutData(fdColumns);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();

    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void getFields() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          rowMeta, wColumns, 1, new int[] {1, 3}, new int[] {}, -1, -1, null);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields...", e);
    }
  }

  /** Populate the widgets. */
  public void getData() {
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wInstanceId.setText(Const.NVL(input.getInstanceId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));

    for (int i = 0; i < input.getColumns().size(); i++) {
      BigtableColumn column = input.getColumns().get(i);
      TableItem item = wColumns.table.getItem(i);
      item.setText(1, Const.NVL(column.getName(), ""));
      item.setText(2, Const.NVL(column.getFamily(), ""));
      item.setText(3, Const.NVL(column.getSourceField(), ""));
    }
    wColumns.optimizeTableView();
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamBigtableOutputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setProjectId(wProjectId.getText());
    in.setInstanceId(wInstanceId.getText());
    in.setTableId(wTableId.getText());
    in.setKeyField(wKeyField.getText());
    in.getColumns().clear();

    for (TableItem item : wColumns.getNonEmptyItems()) {
      String qualifier = item.getText(1);
      String family = item.getText(2);
      String sourceField = item.getText(3);
      in.getColumns().add(new BigtableColumn(qualifier, family, sourceField));
    }

    input.setChanged();
  }
}
