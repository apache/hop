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

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.RowMetaAndData;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ParquetInputDialog extends BaseTransformDialog implements ITransformDialog {

  public static final Class<?> PKG = ParquetInputMeta.class;

  protected ParquetInputMeta input;

  private Combo wFilenameField;
  private TableView wFields;

  private String returnValue;

  public ParquetInputDialog(
      Shell parent,
      IVariables variables,
      Object in,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, transformName);
    input = (ParquetInputMeta) in;
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
    shell.setText(BaseMessages.getString(PKG, "ParquetInput.Name"));

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
    wlTransformName.setText(BaseMessages.getString(PKG, "ParquetInputDialog.TransformName.Label"));
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

    Label wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "ParquetInputDialog.FilenameField.Label"));
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wFilenameField;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "ParquetInputDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, margin);
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
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              Const.getDateFormats(),
              false),
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
    props.setLook(wFields);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    getData();

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
        FileObject fileObject = HopVfs.getFileObject(filename);

        long size = fileObject.getContent().getSize();
        InputStream inputStream = HopVfs.getInputStream(fileObject);

        // Reads the whole file into memory...
        //
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream((int) size);
        IOUtils.copy(inputStream, outputStream);
        ParquetStream inputFile = new ParquetStream(outputStream.toByteArray(), filename);
        // Empty list of fields to retrieve: we still grab the schema
        //
        ParquetReadSupport readSupport = new ParquetReadSupport(new ArrayList<>());
        ParquetReader<RowMetaAndData> reader =
            new ParquetReaderBuilder<>(readSupport, inputFile).build();

        // Read one empty row...
        //
        reader.read();

        // Now we have the schema...
        //
        MessageType schema = readSupport.getMessageType();
        IRowMeta rowMeta = new RowMeta();
        List<ColumnDescriptor> columns = schema.getColumns();
        for (ColumnDescriptor column : columns) {
          String sourceField = "";
          String[] path = column.getPath();
          if (path.length == 1) {
            sourceField = path[0];
          } else {
            for (int i = 0; i < path.length; i++) {
              if (i > 0) {
                sourceField += ".";
              }
              sourceField += path[i];
            }
          }
          PrimitiveType primitiveType = column.getPrimitiveType();
          int hopType = IValueMeta.TYPE_STRING;
          switch (primitiveType.getPrimitiveTypeName()) {
            case INT32:
            case INT64:
              hopType = IValueMeta.TYPE_INTEGER;
              break;
            case INT96:
              hopType = IValueMeta.TYPE_BINARY;
              break;
            case FLOAT:
            case DOUBLE:
              hopType = IValueMeta.TYPE_NUMBER;
              break;
            case BOOLEAN:
              hopType = IValueMeta.TYPE_BOOLEAN;
              break;
          }
          IValueMeta valueMeta = ValueMetaFactory.createValueMeta(sourceField, hopType, -1, -1);
          rowMeta.addValueMeta(valueMeta);
        }

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

    wTransformName.setText(Const.NVL(transformName, ""));
    wFilenameField.setText(Const.NVL(input.getFilenameField(), ""));
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

  @Override
  public void dispose() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
