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

package org.apache.hop.pipeline.transforms.getvariable;

import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transforms.getvariable.GetVariableMeta.FieldDefinition;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class GetVariableDialog extends BaseTransformDialog {
  private static final Class<?> PKG = GetVariableMeta.class;

  private TableView wFields;

  private final GetVariableMeta input;

  public GetVariableDialog(
      Shell parent,
      IVariables variables,
      GetVariableMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "GetVariableDialog.DialogTitle"));

    buildButtonBar()
        .ok(e -> ok())
        .preview(e -> preview())
        .get(e -> grabVariables())
        .cancel(e -> cancel())
        .build();

    // See if the transform receives input.
    //
    boolean isReceivingInput = !pipelineMeta.findPreviousTransforms(transformMeta).isEmpty();

    wPreview.setEnabled(isReceivingInput);

    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "GetVariableDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wSpacer, margin);
    wlFields.setLayoutData(fdlFields);

    final int fieldsRows = input.getFieldDefinitions().size();

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetVariableDialog.NameColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetVariableDialog.VariableColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Format"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Currency"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Decimal"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "System.Column.Group"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "GetVariableDialog.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              IValueMeta.TrimType.getDescriptions()),
        };

    colinf[1].setToolTip(BaseMessages.getString(PKG, "GetVariableDialog.VariableColumn.Tooltip"));
    colinf[1].setUsingVariables(true);

    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            fieldsRows,
            null,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, -50);
    wFields.setLayoutData(fdFields);

    getData();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    for (int i = 0; i < input.getFieldDefinitions().size(); i++) {
      TableItem item = wFields.table.getItem(i);
      FieldDefinition currentField = input.getFieldDefinitions().get(i);
      int index = 1;
      item.setText(index++, Const.NVL(currentField.getFieldName(), ""));
      item.setText(index++, Const.NVL(currentField.getVariableString(), ""));
      item.setText(index++, Const.NVL(currentField.getFieldType(), ""));
      item.setText(index++, Const.NVL(currentField.getFieldFormat(), ""));
      item.setText(
          index++, currentField.getFieldLength() < 0 ? "" : ("" + currentField.getFieldLength()));
      item.setText(
          index++,
          currentField.getFieldPrecision() < 0 ? "" : ("" + currentField.getFieldPrecision()));
      item.setText(index++, Const.NVL(currentField.getCurrency(), ""));
      item.setText(index++, Const.NVL(currentField.getDecimal(), ""));
      item.setText(index++, Const.NVL(currentField.getGroup(), ""));
      if (currentField.getTrimType() != null) {
        item.setText(index, currentField.getTrimType().getDescription());
      }
    }

    wFields.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void getInfo(GetVariableMeta input) throws HopException {
    transformName = wTransformName.getText(); // return value

    input.getFieldDefinitions().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      FieldDefinition field = new FieldDefinition();
      input.getFieldDefinitions().add(field);
      int index = 1;
      field.setFieldName(item.getText(index++));
      field.setVariableString(item.getText(index++));
      field.setFieldType(item.getText(index++));
      field.setFieldFormat(item.getText(index++));
      field.setFieldLength(Const.toInt(item.getText(index++), -1));
      field.setFieldPrecision(Const.toInt(item.getText(index++), -1));
      field.setCurrency(item.getText(index++));
      field.setDecimal(item.getText(index++));
      field.setGroup(item.getText(index++));
      field.setTrimType(IValueMeta.TrimType.lookupDescription(item.getText(index)));
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    try {
      getInfo(input);
    } catch (HopException e) {
      new ErrorDialog(shell, "Error", "Error saving transform information", e);
    }
    input.setChanged();
    dispose();
  }

  // Preview the data
  private void preview() {
    try {
      // Create the Access input transform
      GetVariableMeta oneMeta = new GetVariableMeta();
      getInfo(oneMeta);

      PipelineMeta previewMeta =
          PipelinePreviewFactory.generatePreviewPipeline(
              pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

      // We always just want to preview a single output row
      //
      PipelinePreviewProgressDialog progressDialog =
          new PipelinePreviewProgressDialog(
              shell,
              variables,
              previewMeta,
              new String[] {wTransformName.getText()},
              new int[] {1});
      progressDialog.open();

      if (!progressDialog.isCancelled()) {
        Pipeline pipeline = progressDialog.getPipeline();
        String loggingText = progressDialog.getLoggingText();

        if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
          EnterTextDialog etd =
              new EnterTextDialog(
                  shell,
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                  BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                  loggingText,
                  true);
          etd.setReadOnly();
          etd.open();
        }

        PreviewRowsDialog prd =
            new PreviewRowsDialog(
                shell,
                variables,
                SWT.NONE,
                wTransformName.getText(),
                progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                progressDialog.getPreviewRows(wTransformName.getText()),
                loggingText);
        prd.open();
      }

    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "GetVariableDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "GetVariableDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }

  private void grabVariables() {
    if (pipelineMeta == null) {
      return;
    }
    String[] key = variables.getVariableNames();
    wFields.removeAll();

    for (String s : key) {
      TableItem tableItem = new TableItem(wFields.table, 0);
      tableItem.setText(1, s);
      tableItem.setText(2, "${" + s + "}");
      tableItem.setText(3, "String");
    }
    wFields.optimizeTableView();
  }
}
