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

package org.apache.hop.beam.transforms.bq;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.beam.core.fn.BQSchemaAndRecordToHopFn;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class BeamBQInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamBQInputDialog.class;
  private final BeamBQInputMeta input;

  int middle;
  int margin;

  private TextVar wProjectId;
  private TextVar wDatasetId;
  private TextVar wTableId;
  private TextVar wQuery;
  private TableView wFields;

  public BeamBQInputDialog(
      Shell parent,
      IVariables variables,
      BeamBQInputMeta transformMeta,
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

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control lastControl = wTransformName;

    Label wlProjectId = new Label(shell, SWT.RIGHT);
    wlProjectId.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.ProjectId"));
    PropsUi.setLook(wlProjectId);
    FormData fdlProjectId = new FormData();
    fdlProjectId.left = new FormAttachment(0, 0);
    fdlProjectId.top = new FormAttachment(lastControl, margin);
    fdlProjectId.right = new FormAttachment(middle, -margin);
    wlProjectId.setLayoutData(fdlProjectId);
    wProjectId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wProjectId);
    FormData fdProjectId = new FormData();
    fdProjectId.left = new FormAttachment(middle, 0);
    fdProjectId.top = new FormAttachment(wlProjectId, 0, SWT.CENTER);
    fdProjectId.right = new FormAttachment(100, 0);
    wProjectId.setLayoutData(fdProjectId);
    lastControl = wProjectId;

    Label wlDatasetId = new Label(shell, SWT.RIGHT);
    wlDatasetId.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.DatasetId"));
    PropsUi.setLook(wlDatasetId);
    FormData fdlDatasetId = new FormData();
    fdlDatasetId.left = new FormAttachment(0, 0);
    fdlDatasetId.top = new FormAttachment(lastControl, margin);
    fdlDatasetId.right = new FormAttachment(middle, -margin);
    wlDatasetId.setLayoutData(fdlDatasetId);
    wDatasetId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDatasetId);
    FormData fdDatasetId = new FormData();
    fdDatasetId.left = new FormAttachment(middle, 0);
    fdDatasetId.top = new FormAttachment(wlDatasetId, 0, SWT.CENTER);
    fdDatasetId.right = new FormAttachment(100, 0);
    wDatasetId.setLayoutData(fdDatasetId);
    lastControl = wDatasetId;

    Label wlTableId = new Label(shell, SWT.RIGHT);
    wlTableId.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.TableId"));
    PropsUi.setLook(wlTableId);
    FormData fdlTableId = new FormData();
    fdlTableId.left = new FormAttachment(0, 0);
    fdlTableId.top = new FormAttachment(lastControl, margin);
    fdlTableId.right = new FormAttachment(middle, -margin);
    wlTableId.setLayoutData(fdlTableId);
    wTableId = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTableId);
    FormData fdTableId = new FormData();
    fdTableId.left = new FormAttachment(middle, 0);
    fdTableId.top = new FormAttachment(wlTableId, 0, SWT.CENTER);
    fdTableId.right = new FormAttachment(100, 0);
    wTableId.setLayoutData(fdTableId);
    lastControl = wTableId;

    Label wlQuery = new Label(shell, SWT.LEFT);
    wlQuery.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.Query"));
    PropsUi.setLook(wlQuery);
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment(0, 0);
    fdlQuery.top = new FormAttachment(lastControl, margin);
    fdlQuery.right = new FormAttachment(100, 0);
    wlQuery.setLayoutData(fdlQuery);
    wQuery = new TextVar(variables, shell, SWT.LEFT | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(wQuery, Props.WIDGET_STYLE_FIXED);
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment(0, 0);
    fdQuery.top = new FormAttachment(wlQuery, margin);
    fdQuery.right = new FormAttachment(100, 0);
    fdQuery.bottom = new FormAttachment(wlQuery, 250);
    wQuery.setLayoutData(fdQuery);
    lastControl = wQuery;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.Fields"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    fdlFields.right = new FormAttachment(middle, -margin);
    wlFields.setLayoutData(fdlFields);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wGet = new Button(shell, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.addListener(SWT.Selection, e -> getFields());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCancel}, margin, null);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBQInputDialog.Fields.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBQInputDialog.Fields.Column.NewName"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "BeamBQInputDialog.Fields.Column.HopType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
        };
    wFields =
        new TableView(
            Variables.getADefaultVariableSpace(),
            shell,
            SWT.NONE,
            columns,
            input.getFields().size(),
            null,
            props);
    PropsUi.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  public void getFields() {
    try {

      BeamBQInputMeta meta = new BeamBQInputMeta();
      getInfo(meta);

      BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();

      if (StringUtils.isNotEmpty(meta.getDatasetId())
          && StringUtils.isNotEmpty(meta.getTableId())) {

        Table table =
            bigQuery.getTable(
                variables.resolve(meta.getDatasetId()), variables.resolve(meta.getTableId()));

        if (table == null) {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setText(BaseMessages.getString(PKG, "BeamBQInputDialog.TableNotFound.Title"));
          mb.setMessage(
              variables.resolve(meta.getTableId())
                  + " "
                  + BaseMessages.getString(PKG, "BeamBQInputDialog.TableNotFound.Message"));
          switch (mb.open()) {
            case SWT.OK:
              break;
            default:
              break;
          }
          return;
        }
        TableDefinition definition = table.getDefinition();
        Schema schema = definition.getSchema();
        FieldList fieldList = schema.getFields();

        IRowMeta rowMeta = new RowMeta();
        for (int i = 0; i < fieldList.size(); i++) {
          Field field = fieldList.get(i);

          String name = field.getName();
          String type = field.getType().name();

          int hopType = BQSchemaAndRecordToHopFn.AvroType.valueOf(type).getHopType();
          rowMeta.addValueMeta(ValueMetaFactory.createValueMeta(name, hopType));
        }

        BaseTransformDialog.getFieldsFromPrevious(
            rowMeta, wFields, 1, new int[] {1}, new int[] {3}, -1, -1, true, null);
      }

    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting BQ fields", e);
    }
  }

  /** Populate the widgets. */
  public void getData() {
    wTransformName.setText(transformName);
    wProjectId.setText(Const.NVL(input.getProjectId(), ""));
    wDatasetId.setText(Const.NVL(input.getDatasetId(), ""));
    wTableId.setText(Const.NVL(input.getTableId(), ""));
    wQuery.setText(Const.NVL(input.getQuery(), ""));

    for (int i = 0; i < input.getFields().size(); i++) {
      BQField field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getNewName(), ""));
      item.setText(3, Const.NVL(field.getHopType(), ""));
    }
    wFields.removeEmptyRows();
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamBQInputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setProjectId(wProjectId.getText());
    in.setDatasetId(wDatasetId.getText());
    in.setTableId(wTableId.getText());
    in.setQuery(wQuery.getText());
    in.getFields().clear();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      String name = item.getText(1);
      String newName = item.getText(2);
      String hopType = item.getText(3);
      in.getFields().add(new BQField(name, newName, hopType));
    }

    input.setChanged();
  }
}
