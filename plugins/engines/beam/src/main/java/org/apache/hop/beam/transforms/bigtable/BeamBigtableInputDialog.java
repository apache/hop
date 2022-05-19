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
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class BeamBigtableInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = BeamBigtableInputDialog.class; // For Translator
  private final BeamBigtableInputMeta input;

  private TextVar wProjectId;
  private TextVar wInstanceId;
  private TextVar wTableId;
  private TextVar wKeyField;
  private TableView wColumns;

  public BeamBigtableInputDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (BeamBigtableInputMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Buttons at the bottom!
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlProjectId = new Label(shell, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.ProjectId"));
    props.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(lastControl, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlInstanceId = new Label(shell, SWT.RIGHT);
    wlInstanceId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.InstanceId"));
    props.setLook(wlInstanceId);
    FormData fdlInstanceId = new FormData();
    fdlInstanceId.left = new FormAttachment(0, 0);
    fdlInstanceId.top = new FormAttachment(lastControl, margin);
    fdlInstanceId.right = new FormAttachment(middle, -margin);
    wlInstanceId.setLayoutData(fdlInstanceId);
    wInstanceId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wInstanceId);
    FormData fdInstanceId = new FormData();
    fdInstanceId.left = new FormAttachment(middle, 0);
    fdInstanceId.top = new FormAttachment(wlInstanceId, 0, SWT.CENTER);
    fdInstanceId.right = new FormAttachment(100, 0);
    wInstanceId.setLayoutData(fdInstanceId);
    lastControl = wInstanceId;

    Label wlTableId = new Label(shell, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.TableId"));
    props.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    Label wlKeyField = new Label(shell, SWT.RIGHT);
    wlKeyField.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.KeyField"));
    props.setLook(wlKeyField);
    FormData fdlKeyField = new FormData();
    fdlKeyField.left = new FormAttachment(0, 0);
    fdlKeyField.top = new FormAttachment(lastControl, margin);
    fdlKeyField.right = new FormAttachment(middle, -margin);
    wlKeyField.setLayoutData(fdlKeyField);
    wKeyField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wKeyField);
    FormData fdKeyField = new FormData();
    fdKeyField.left = new FormAttachment(middle, 0);
    fdKeyField.top = new FormAttachment(wlKeyField, 0, SWT.CENTER);
    fdKeyField.right = new FormAttachment(100, 0);
    wKeyField.setLayoutData(fdKeyField);
    lastControl = wKeyField;

    Label wlColumns = new Label(shell, SWT.LEFT);
    wlColumns.setText(BaseMessages.getString(PKG, "BeamBigtableInputDialog.Columns"));
    props.setLook(wlColumns);
    FormData fdlColumns = new FormData();
    fdlColumns.left = new FormAttachment(0, 0);
    fdlColumns.top = new FormAttachment(lastControl, margin);
    fdlColumns.right = new FormAttachment(100, 0);
    wlColumns.setLayoutData(fdlColumns);
    lastControl = wlColumns;

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableInputDialog.Column.Qualifier"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableInputDialog.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBigtableInputDialog.Column.TargetName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columns[0].setUsingVariables(false);
    columns[1].setUsingVariables(false);
    columns[2].setUsingVariables(false);

    wColumns =
        new TableView(
            variables, shell, SWT.BORDER, columns, input.getSourceColumns().size(), null, props);
    FormData fdColumns = new FormData();
    fdColumns.left = new FormAttachment(0, 0);
    fdColumns.top = new FormAttachment(lastControl, margin);
    fdColumns.right = new FormAttachment(100, 0);
    fdColumns.bottom = new FormAttachment(wOk, -2 * margin);
    wColumns.setLayoutData(fdColumns);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wInstanceId.setText(Const.NVL(input.getInstanceId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));

    for (int i = 0; i < input.getSourceColumns().size(); i++) {
      BigtableSourceColumn column = input.getSourceColumns().get(i);
      TableItem item = wColumns.table.getItem(i);
      item.setText(1, Const.NVL(column.getQualifier(), ""));
      item.setText(2, Const.NVL(column.getTargetType(), ""));
      item.setText(3, Const.NVL(column.getTargetFieldName(), ""));
    }
    wColumns.optimizeTableView();

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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamBigtableInputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setProjectId(wProjectId.getText());
    in.setInstanceId(wInstanceId.getText());
    in.setTableId(wTableId.getText());
    in.setKeyField(wKeyField.getText());

    in.getSourceColumns().clear();

    for (TableItem item : wColumns.getNonEmptyItems()) {
      String qualifier = item.getText(1);
      String type = item.getText(2);
      String name = item.getText(3);
      in.getSourceColumns().add(new BigtableSourceColumn(qualifier, type, name));
    }

    input.setChanged();
  }
}
