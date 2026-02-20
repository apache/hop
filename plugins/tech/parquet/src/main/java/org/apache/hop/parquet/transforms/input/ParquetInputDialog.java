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

package org.apache.hop.parquet.transforms.input;

import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class ParquetInputDialog extends BaseTransformDialog {

  public static final Class<?> PKG = ParquetInputMeta.class;

  protected ParquetInputMeta input;

  private Combo wFilenameField;
  private TextVar wMetaFilename;
  private Button wNullWhenEmpty;
  private TableView wFields;

  private String returnValue;

  public ParquetInputDialog(
      Shell parent,
      IVariables variables,
      ParquetInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ParquetInput.Name"));

    buildButtonBar().ok(e -> ok()).get(e -> getFields()).cancel(e -> cancel()).build();

    Control lastControl = wSpacer;

    Label wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "ParquetInputDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wFilenameField;

    Label wlMetaFilename = new Label(shell, SWT.RIGHT);
    wlMetaFilename.setText(BaseMessages.getString(PKG, "ParquetInputDialog.MetaFilename.Label"));
    PropsUi.setLook(wlMetaFilename);
    FormData fdlMetaFilename = new FormData();
    fdlMetaFilename.left = new FormAttachment(0, 0);
    fdlMetaFilename.right = new FormAttachment(middle, -margin);
    fdlMetaFilename.top = new FormAttachment(lastControl, margin);
    wlMetaFilename.setLayoutData(fdlMetaFilename);
    wMetaFilename = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMetaFilename);
    FormData fdMetaFilename = new FormData();
    fdMetaFilename.left = new FormAttachment(middle, 0);
    fdMetaFilename.top = new FormAttachment(wlMetaFilename, 0, SWT.CENTER);
    fdMetaFilename.right = new FormAttachment(100, 0);
    wMetaFilename.setLayoutData(fdMetaFilename);
    lastControl = wMetaFilename;

    Label wlNullWhenEmpty = new Label(shell, SWT.RIGHT);
    wlNullWhenEmpty.setText(BaseMessages.getString(PKG, "ParquetInputDialog.NullWhenEmpty.Label"));
    PropsUi.setLook(wlNullWhenEmpty);
    FormData fdlNullWhenEmpty = new FormData();
    fdlNullWhenEmpty.left = new FormAttachment(0, 0);
    fdlNullWhenEmpty.right = new FormAttachment(middle, -margin);
    fdlNullWhenEmpty.top = new FormAttachment(lastControl, margin);
    wlNullWhenEmpty.setLayoutData(fdlNullWhenEmpty);
    wNullWhenEmpty = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wNullWhenEmpty);
    FormData fdNullWhenEmpty = new FormData();
    fdNullWhenEmpty.left = new FormAttachment(middle, 0);
    fdNullWhenEmpty.top = new FormAttachment(wlNullWhenEmpty, 0, SWT.CENTER);
    fdNullWhenEmpty.right = new FormAttachment(100, 0);
    wNullWhenEmpty.setLayoutData(fdNullWhenEmpty);
    lastControl = wNullWhenEmpty;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "ParquetInputDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, 2 * margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.SourceField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              new String[0]),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.TargetField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.TargetType.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.TargetFormat.Label"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.TargetLength.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              true,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetInputDialog.FieldsColumn.TargetPrecision.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              true,
              false),
        };
    wFields =
        new TableView(
            variables, shell, SWT.BORDER, columns, input.getFields().size(), false, null, props);
    PropsUi.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -50);
    wFields.setLayoutData(fdFields);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return returnValue;
  }

  private void getFields() {
    try {
      // Ask for a file to get metadata from...
      //
      String filename =
          BaseDialog.presentFileDialog(
              shell,
              new String[] {"*.parquet*", "*.*"},
              new String[] {"Parquet files", "All files"},
              true);
      if (filename != null) {
        IRowMeta rowMeta = ParquetInputMeta.extractRowMeta(variables, filename);

        BaseTransformDialog.getFieldsFromPrevious(
            rowMeta, wFields, 1, new int[] {1, 2}, new int[] {3}, -1, -1, null);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting parquet file fields", e);
    }
  }

  private void getData() {
    try {
      wFilenameField.setItems(
          pipelineMeta.getPrevTransformFields(variables, transformName).getFieldNames());
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
    wMetaFilename.setText(Const.NVL(input.getMetadataFilename(), ""));
    wNullWhenEmpty.setSelection(input.isSendingNullsRowWhenEmpty());
    for (int i = 0; i < input.getFields().size(); i++) {
      ParquetField field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      int index = 1;
      item.setText(index++, Const.NVL(field.getSourceField(), ""));
      item.setText(index++, Const.NVL(field.getTargetField(), ""));
      item.setText(index++, Const.NVL(field.getTargetType(), ""));
      item.setText(index++, Const.NVL(field.getTargetFormat(), ""));
      item.setText(index++, Const.NVL(field.getTargetLength(), ""));
      item.setText(index++, Const.NVL(field.getTargetPrecision(), ""));
    }
  }

  private void ok() {
    returnValue = wTransformName.getText();

    getInfo(input);
    input.setChanged();
    dispose();
  }

  private void getInfo(ParquetInputMeta meta) {
    meta.setFilenameField(wFilenameField.getText());
    meta.setMetadataFilename(wMetaFilename.getText());
    meta.setSendingNullsRowWhenEmpty(wNullWhenEmpty.getSelection());
    meta.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      int index = 1;
      meta.getFields()
          .add(
              new ParquetField(
                  item.getText(index++),
                  item.getText(index++),
                  item.getText(index++),
                  item.getText(index++),
                  item.getText(index++),
                  item.getText(index)));
    }
  }

  private void cancel() {
    returnValue = null;
    dispose();
  }
}
