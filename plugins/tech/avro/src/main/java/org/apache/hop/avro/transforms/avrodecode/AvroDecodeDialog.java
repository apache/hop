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

package org.apache.hop.avro.transforms.avrodecode;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.avro.transforms.avrodecode.AvroDecodeFieldFinder.FieldRow;
import org.apache.hop.avro.transforms.avroinput.AvroFileInputMeta;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaAvroRecord;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class AvroDecodeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AvroDecodeMeta.class;
  public static final String CONST_FILENAME = "filename";

  private AvroDecodeMeta input;

  private Combo wSourceField;
  private TableView wFields;
  private RowProducer rowProducer;

  public AvroDecodeDialog(
      Shell parent, IVariables variables, AvroDecodeMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AvroDecodeDialog.Shell.Title"));

    buildButtonBar()
        .ok(e -> ok())
        .custom(
            BaseMessages.getString(PKG, "AvroDecodeDialog.GetFieldsFromFile.Button"),
            e -> getFieldsFromFile())
        .custom(
            BaseMessages.getString(PKG, "AvroDecodeDialog.GetFieldsFromJson.Button"),
            e -> getFieldsFromJson())
        .cancel(e -> cancel())
        .build();

    Label wlSourceField = new Label(shell, SWT.RIGHT);
    wlSourceField.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.SourceField.Label"));
    PropsUi.setLook(wlSourceField);
    FormData fdlSourceField = new FormData();
    fdlSourceField.left = new FormAttachment(0, 0);
    fdlSourceField.right = new FormAttachment(middle, -margin);
    fdlSourceField.top = new FormAttachment(wSpacer, margin);
    wlSourceField.setLayoutData(fdlSourceField);
    wSourceField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSourceField.setText(transformName);
    PropsUi.setLook(wSourceField);
    FormData fdSourceField = new FormData();
    fdSourceField.left = new FormAttachment(middle, 0);
    fdSourceField.top = new FormAttachment(wlSourceField, 0, SWT.CENTER);
    fdSourceField.right = new FormAttachment(100, 0);
    wSourceField.setLayoutData(fdSourceField);
    Control lastControl = wSourceField;

    Label wlFields = new Label(shell, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] fieldsColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.SourceField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.SourceType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                "String", "Int", "Long", "Float", "Double", "Boolean", "Bytes", "Null", "Record",
                "Enum", "Array", "Map", "Union", "Fixed"
              },
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.TargetField"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.TargetType"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.TargetFormat"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              4),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.TargetLength"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Column.TargetPrecision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false)
        };
    fieldsColumns[2].setNamingSchemeType(NamingSchemeTypes.HOP_FIELD);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.NONE,
            fieldsColumns,
            input.getTargetFields().size(),
            false,
            null,
            props);
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

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {

    try {
      // Get the fields from the previous transforms:
      wSourceField.setItems(
          pipelineMeta.getPrevTransformFields(variables, transformMeta).getFieldNames());
    } catch (Exception e) {
      // Ignore exception
    }
    wSourceField.setText(Const.NVL(input.getSourceFieldName(), ""));

    int rowNr = 0;
    for (TargetField targetField : input.getTargetFields()) {
      TableItem item = wFields.table.getItem(rowNr++);
      int col = 1;
      item.setText(col++, Const.NVL(targetField.getSourceField(), ""));
      item.setText(col++, Const.NVL(targetField.getSourceAvroType(), ""));
      item.setText(col++, Const.NVL(targetField.getTargetFieldName(), ""));
      item.setText(col++, Const.NVL(targetField.getTargetType(), ""));
      item.setText(col++, Const.NVL(targetField.getTargetFormat(), ""));
      item.setText(col++, Const.NVL(targetField.getTargetLength(), ""));
      item.setText(col++, Const.NVL(targetField.getTargetPrecision(), ""));
    }
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    input.setSourceFieldName(wSourceField.getText());
    input.getTargetFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      int col = 1;
      String sourceField = item.getText(col++);
      String sourceType = item.getText(col++);
      String targetField = item.getText(col++);
      String targetType = item.getText(col++);
      String targetFormat = item.getText(col++);
      String targetLength = item.getText(col++);
      String targetPrecision = item.getText(col);
      input
          .getTargetFields()
          .add(
              new TargetField(
                  sourceField,
                  sourceType,
                  targetField,
                  targetType,
                  targetFormat,
                  targetLength,
                  targetPrecision));
    }

    transformName = wTransformName.getText(); // return value
    transformMeta.setChanged();

    dispose();
  }

  private void getFieldsFromFile() {
    try {
      Schema schema = schemaFromSelectedSourceField();
      if (schema == null) {
        schema = readSchemaFromAvroFile();
      }
      addFieldsFromSchema(schema);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "AvroDecodeDialog.GetFields.Error.Title"),
          BaseMessages.getString(PKG, "AvroDecodeDialog.GetFields.Error.Message"),
          e);
    }
  }

  private void getFieldsFromJson() {
    try {
      EnterTextDialog dialog =
          new EnterTextDialog(
              shell,
              BaseMessages.getString(PKG, "AvroDecodeDialog.GetFieldsFromJson.Title"),
              BaseMessages.getString(PKG, "AvroDecodeDialog.GetFieldsFromJson.Message"),
              "",
              true);
      String json = dialog.open();
      if (StringUtils.isBlank(json)) {
        return;
      }
      addFieldsFromSchema(AvroDecodeFieldFinder.schemaFromJson(json));
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "AvroDecodeDialog.GetFields.Error.Title"),
          BaseMessages.getString(PKG, "AvroDecodeDialog.GetFields.Error.Message"),
          e);
    }
  }

  /**
   * Schema already attached to the selected source field, when that field is an Avro record. Avro
   * Encode does this. Avro File Input and Kafka do not.
   */
  private Schema schemaFromSelectedSourceField() throws HopException {
    String fieldName = wSourceField.getText();
    if (StringUtils.isEmpty(fieldName)) {
      return null;
    }
    IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
    IValueMeta valueMeta = fields.searchValueMeta(fieldName);
    if (!(valueMeta instanceof ValueMetaAvroRecord avroValueMeta)) {
      return null;
    }
    Schema schema = avroValueMeta.getSchema();
    if (schema == null || schema.getType() != Schema.Type.RECORD || schema.getFields().isEmpty()) {
      return null;
    }
    return schema;
  }

  private Schema readSchemaFromAvroFile() throws HopException {
    String filename =
        BaseDialog.presentFileDialog(
            shell, new String[] {"*.avro", "*.*"}, new String[] {"Avro files", "All files"}, true);
    if (filename == null) {
      return null;
    }

    PipelineMeta previewPipeline = new PipelineMeta();
    previewPipeline.setName("Get Avro file details");

    InjectorMeta injector = new InjectorMeta();
    injector.getInjectorFields().add(new InjectorField(CONST_FILENAME, "String", "500", "-1"));
    TransformMeta injectorMeta = new TransformMeta("Filename", injector);
    injectorMeta.setLocation(50, 50);
    previewPipeline.addTransform(injectorMeta);

    AvroFileInputMeta fileInput = new AvroFileInputMeta();
    fileInput.setDataFilenameField(CONST_FILENAME);
    fileInput.setOutputFieldName("avro");
    fileInput.setRowsLimit("1");
    TransformMeta fileInputMeta = new TransformMeta("Avro", fileInput);
    fileInputMeta.setLocation(250, 50);
    previewPipeline.addTransform(fileInputMeta);
    previewPipeline.addPipelineHop(new PipelineHopMeta(injectorMeta, fileInputMeta));

    LocalPipelineEngine pipeline =
        new LocalPipelineEngine(previewPipeline, variables, loggingObject);
    pipeline.setMetadataProvider(metadataProvider);
    pipeline.prepareExecution();
    pipeline.setPreview(true);

    RowProducer rowProducer = pipeline.addRowProducer("Filename", 0);
    IEngineComponent avroComponent = pipeline.findComponent("Avro", 0);
    AtomicReference<Schema> schemaRef = new AtomicReference<>();
    avroComponent.addRowListener(
        new RowAdapter() {
          private boolean first = true;

          @Override
          public void rowWrittenEvent(IRowMeta rowMeta, Object[] row) throws HopTransformException {
            if (!first) {
              return;
            }
            first = false;
            int index = rowMeta.indexOfValue("avro");
            ValueMetaAvroRecord avroMeta = (ValueMetaAvroRecord) rowMeta.getValueMeta(index);
            try {
              GenericRecord genericRecord = avroMeta.getGenericRecord(row[index]);
              schemaRef.set(genericRecord.getSchema());
            } catch (Exception e) {
              throw new HopTransformException(e);
            }
          }
        });

    pipeline.startThreads();
    rowProducer.putRow(
        new RowMetaBuilder().addString(CONST_FILENAME).build(),
        new Object[] {variables.resolve(filename)});
    rowProducer.finished();
    pipeline.waitUntilFinished();
    return schemaRef.get();
  }

  private void addFieldsFromSchema(Schema schema) throws HopException {
    if (schema == null) {
      return;
    }
    List<FieldRow> rows = AvroDecodeFieldFinder.rowsForSchema(schema);
    if (rows.isEmpty()) {
      return;
    }
    for (FieldRow row : rows) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      item.setText(1, Const.NVL(row.sourceField(), ""));
      item.setText(2, Const.NVL(row.sourceAvroType(), ""));
      item.setText(3, Const.NVL(row.targetFieldName(), ""));
      item.setText(4, Const.NVL(row.targetType(), ""));
    }
    wFields.optimizeTableView();
  }
}
