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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hop.avro.transforms.avroinput.AvroFileInputMeta;
import org.apache.hop.avro.type.ValueMetaAvroRecord;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMetaBuilder;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineHopMeta;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.RowProducer;
import org.apache.hop.pipeline.engine.IEngineComponent;
import org.apache.hop.pipeline.engines.local.LocalPipelineEngine;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.RowAdapter;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.injector.InjectorField;
import org.apache.hop.pipeline.transforms.injector.InjectorMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class AvroDecodeDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = AvroDecodeMeta.class; // For Translator

  private AvroDecodeMeta input;

  private Combo wSourceField;
  private TableView wFields;
  private ColumnInfo[] fieldsColumns;
  private RowProducer rowProducer;

  public AvroDecodeDialog(
      Shell parent,
      IVariables variables,
      Object baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);

    input = (AvroDecodeMeta) baseTransformMeta;
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
    shell.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.Shell.Title"));

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
    wlTransformName.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.TransformName.Label"));
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

    Label wlSourceField = new Label(shell, SWT.RIGHT);
    wlSourceField.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.SourceField.Label"));
    props.setLook(wlSourceField);
    FormData fdlSourceField = new FormData();
    fdlSourceField.left = new FormAttachment(0, 0);
    fdlSourceField.right = new FormAttachment(middle, -margin);
    fdlSourceField.top = new FormAttachment(lastControl, margin);
    wlSourceField.setLayoutData(fdlSourceField);
    wSourceField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSourceField.setText(transformName);
    props.setLook(wSourceField);
    FormData fdSourceField = new FormData();
    fdSourceField.left = new FormAttachment(middle, 0);
    fdSourceField.top = new FormAttachment(wlSourceField, 0, SWT.CENTER);
    fdSourceField.right = new FormAttachment(100, 0);
    wSourceField.setLayoutData(fdSourceField);
    lastControl = wSourceField;

    Label wlFields = new Label(shell, SWT.RIGHT);
    wlFields.setText(BaseMessages.getString(PKG, "AvroDecodeDialog.Fields.Label"));
    props.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(lastControl, margin);
    wlFields.setLayoutData(fdlFields);

    fieldsColumns =
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
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              Const.getConversionFormats(),
              false),
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

  private void getFields() {
    try {
      String filename =
          BaseDialog.presentFileDialog(
              shell,
              new String[] {"*.avro", "*.*"},
              new String[] {"Avro files", "All files"},
              true);
      if (filename != null) {
        // Read the file
        // Grab the schema
        // Add all the fields to wFields
        //
        PipelineMeta pipelineMeta = new PipelineMeta();
        pipelineMeta.setName("Get Avro file details");

        // We'll inject the filename to minimize dependencies
        //
        InjectorMeta injector = new InjectorMeta();
        injector.getInjectorFields().add(new InjectorField("filename", "String", "500", "-1"));
        TransformMeta injectorMeta = new TransformMeta("Filename", injector);
        injectorMeta.setLocation(50, 50);
        pipelineMeta.addTransform(injectorMeta);

        // The Avro File Input transform
        //
        AvroFileInputMeta fileInput = new AvroFileInputMeta();
        fileInput.setDataFilenameField("filename");
        fileInput.setOutputFieldName("avro");
        fileInput.setRowsLimit("1");
        TransformMeta fileInputMeta = new TransformMeta("Avro", fileInput);
        fileInputMeta.setLocation(250, 50);
        pipelineMeta.addTransform(fileInputMeta);
        pipelineMeta.addPipelineHop(new PipelineHopMeta(injectorMeta, fileInputMeta));

        LocalPipelineEngine pipeline =
            new LocalPipelineEngine(pipelineMeta, variables, loggingObject);
        pipeline.setMetadataProvider(metadataProvider);
        pipeline.prepareExecution();
        pipeline.setPreview(true);

        RowProducer rowProducer = pipeline.addRowProducer("Filename", 0);

        IEngineComponent avroComponent = pipeline.findComponent("Avro", 0);
        Map<String, Schema.Field> fieldsMap = new HashMap<>();

        avroComponent.addRowListener(
            new RowAdapter() {
              private boolean first = true;

              @Override
              public void rowWrittenEvent(IRowMeta rowMeta, Object[] row)
                  throws HopTransformException {
                if (first) {
                  first = false;

                  int index = rowMeta.indexOfValue("avro");
                  ValueMetaAvroRecord avroMeta = (ValueMetaAvroRecord) rowMeta.getValueMeta(index);
                  Object avroValue = row[index];

                  try {
                    GenericRecord genericRecord = avroMeta.getGenericRecord(avroValue);
                    Schema schema = genericRecord.getSchema();
                    List<Schema.Field> fields = schema.getFields();
                    for (Schema.Field field : fields) {
                      fieldsMap.put(field.name(), field);
                    }
                  } catch (Exception e) {
                    throw new HopTransformException(e);
                  }
                }
              }
            });

        pipeline.startThreads();
        rowProducer.putRow(
            new RowMetaBuilder().addString("filename").build(),
            new Object[] {variables.resolve(filename)});
        rowProducer.finished();

        pipeline.waitUntilFinished();

        List<String> names = new ArrayList<>(fieldsMap.keySet());
        Collections.sort(names, Comparator.comparing(String::toLowerCase));
        for (String name : names) {
          Schema.Field field = fieldsMap.get(name);
          String typeDesc = StringUtil.initCap(field.schema().getType().name().toLowerCase());
          int hopType = AvroDecode.getStandardHopType(field);
          String hopTypeDesc = ValueMetaFactory.getValueMetaName(hopType);

          TableItem item = new TableItem(wFields.table, SWT.NONE);
          item.setText(1, Const.NVL(field.name(), ""));
          item.setText(2, typeDesc);
          item.setText(3, Const.NVL(field.name(), ""));
          item.setText(4, hopTypeDesc);
        }
        wFields.optimizeTableView();
      }
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields", e);
    }
  }
}
