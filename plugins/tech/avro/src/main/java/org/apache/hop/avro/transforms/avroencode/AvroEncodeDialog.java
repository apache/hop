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

package org.apache.hop.avro.transforms.avroencode;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

public class AvroEncodeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = AvroEncodeMeta.class; // For Translator

  private AvroEncodeMeta input;

  private TextVar wOutputField;
  private TextVar wSchemaName;
  private TextVar wNamespace;
  private TextVar wDocumentation;
  private TableView wFields;

  public AvroEncodeDialog(
      Shell parent,
      IVariables variables,
      Object baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);

    input = (AvroEncodeMeta) baseTransformMeta;
  }

  @Override
  public String open() {

    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    props.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // Some buttons at the bottom
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

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.TransformName.Label"));
    props.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
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

    Label wlOutputField = new Label(shell, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.OutputField.Label"));
    props.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(lastControl, margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutputField.setText(transformName);
    props.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    fdOutputField.right = new FormAttachment(100, 0);
    wOutputField.setLayoutData(fdOutputField);
    lastControl = wOutputField;

    Label wlSchemaName = new Label(shell, SWT.RIGHT);
    wlSchemaName.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.SchemaName.Label"));
    props.setLook(wlSchemaName);
    FormData fdlSchemaName = new FormData();
    fdlSchemaName.left = new FormAttachment(0, 0);
    fdlSchemaName.right = new FormAttachment(middle, -margin);
    fdlSchemaName.top = new FormAttachment(lastControl, margin);
    wlSchemaName.setLayoutData(fdlSchemaName);
    wSchemaName = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSchemaName.setText(transformName);
    props.setLook(wSchemaName);
    FormData fdSchemaName = new FormData();
    fdSchemaName.left = new FormAttachment(middle, 0);
    fdSchemaName.top = new FormAttachment(wlSchemaName, 0, SWT.CENTER);
    fdSchemaName.right = new FormAttachment(100, 0);
    wSchemaName.setLayoutData(fdSchemaName);
    lastControl = wSchemaName;

    Label wlNamespace = new Label(shell, SWT.RIGHT);
    wlNamespace.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.Namespace.Label"));
    props.setLook(wlNamespace);
    FormData fdlNamespace = new FormData();
    fdlNamespace.left = new FormAttachment(0, 0);
    fdlNamespace.right = new FormAttachment(middle, -margin);
    fdlNamespace.top = new FormAttachment(lastControl, margin);
    wlNamespace.setLayoutData(fdlNamespace);
    wNamespace = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNamespace.setText(transformName);
    props.setLook(wNamespace);
    FormData fdNamespace = new FormData();
    fdNamespace.left = new FormAttachment(middle, 0);
    fdNamespace.top = new FormAttachment(wlNamespace, 0, SWT.CENTER);
    fdNamespace.right = new FormAttachment(100, 0);
    wNamespace.setLayoutData(fdNamespace);
    lastControl = wNamespace;

    Label wlDocumentation = new Label(shell, SWT.RIGHT);
    wlDocumentation.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.Documentation.Label"));
    props.setLook(wlDocumentation);
    FormData fdlDocumentation = new FormData();
    fdlDocumentation.left = new FormAttachment(0, 0);
    fdlDocumentation.right = new FormAttachment(middle, -margin);
    fdlDocumentation.top = new FormAttachment(lastControl, margin);
    wlDocumentation.setLayoutData(fdlDocumentation);
    wDocumentation = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wDocumentation.setText(transformName);
    props.setLook(wDocumentation);
    FormData fdDocumentation = new FormData();
    fdDocumentation.left = new FormAttachment(middle, 0);
    fdDocumentation.top = new FormAttachment(wlDocumentation, 0, SWT.CENTER);
    fdDocumentation.right = new FormAttachment(100, 0);
    wDocumentation.setLayoutData(fdDocumentation);
    lastControl = wDocumentation;
    
    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "AvroEncodeDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] fieldsColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroEncodeDialog.Fields.Column.SourceField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroEncodeDialog.Fields.Column.TargetField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };

    wFields =
        new TableView(
            variables,
            shell,
            SWT.NONE,
            fieldsColumns,
            input.getSourceFields().size(),
            false,
            null,
            props);
    props.setLook(wFields);
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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    wOutputField.setText(Const.NVL(input.getOutputFieldName(), ""));
    wSchemaName.setText(Const.NVL(input.getSchemaName(), ""));
    wNamespace.setText(Const.NVL(input.getNamespace(), ""));
    wDocumentation.setText(Const.NVL(input.getDocumentation(), ""));

    int rowNr = 0;
    for (SourceField sourceField : input.getSourceFields()) {
      TableItem item = wFields.table.getItem(rowNr++);
      int col = 1;
      item.setText(col++, Const.NVL(sourceField.getSourceFieldName(), ""));
      item.setText(col++, Const.NVL(sourceField.getTargetFieldName(), ""));
    }

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setOutputFieldName(wOutputField.getText());
    input.setSchemaName(wSchemaName.getText());
    input.setNamespace(wNamespace.getText());
    input.setDocumentation(wDocumentation.getText());

    input.getSourceFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      int col = 1;
      String sourceField = item.getText(col++);
      String targetField = item.getText(col++);
      input.getSourceFields().add(new SourceField(sourceField, targetField));
    }

    transformName = wTransformName.getText(); // return value
    transformMeta.setChanged();

    dispose();
  }

  /** Add all the fields to the table view... */
  private void getFields() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          r, wFields, 1, new int[] {1}, new int[] {}, -1, -1, null);
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }
}
