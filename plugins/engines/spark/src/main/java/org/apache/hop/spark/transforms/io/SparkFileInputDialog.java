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
 */

package org.apache.hop.spark.transforms.io;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
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
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class SparkFileInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SparkFileInputMeta.class;

  private final SparkFileInputMeta input;

  private TextVar wFilePath;
  private CCombo wFormat;
  private Button wHeader;
  private TextVar wSeparator;
  private TextVar wQuote;
  private Button wInferSchema;
  private Text wExtraOptions;
  private TableView wFields;

  public SparkFileInputDialog(
      Shell parent,
      IVariables variables,
      SparkFileInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    this.input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN | SWT.MAX);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Transform name
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control last = wTransformName;

    wFilePath = labeledTextVar(lsMod, middle, margin, last, "SparkFileInputDialog.FilePath");
    last = wFilePath;

    // Format
    Label wlFormat = new Label(shell, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.Format"));
    PropsUi.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.top = new FormAttachment(last, margin);
    fdlFormat.right = new FormAttachment(middle, -margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wFormat.setItems(
        new String[] {
          SparkFileInputMeta.FORMAT_CSV,
          SparkFileInputMeta.FORMAT_PARQUET,
          SparkFileInputMeta.FORMAT_JSON,
          SparkFileInputMeta.FORMAT_ORC,
          SparkFileInputMeta.FORMAT_TEXT
        });
    PropsUi.setLook(wFormat);
    wFormat.addModifyListener(lsMod);
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.top = new FormAttachment(wlFormat, 0, SWT.CENTER);
    fdFormat.right = new FormAttachment(100, 0);
    wFormat.setLayoutData(fdFormat);
    last = wFormat;

    wHeader = new Button(shell, SWT.CHECK);
    wHeader.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.Header"));
    PropsUi.setLook(wHeader);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(middle, 0);
    fdHeader.top = new FormAttachment(last, margin);
    wHeader.setLayoutData(fdHeader);
    last = wHeader;

    wSeparator = labeledTextVar(lsMod, middle, margin, last, "SparkFileInputDialog.Separator");
    last = wSeparator;
    wQuote = labeledTextVar(lsMod, middle, margin, last, "SparkFileInputDialog.Quote");
    last = wQuote;

    wInferSchema = new Button(shell, SWT.CHECK);
    wInferSchema.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.InferSchema"));
    PropsUi.setLook(wInferSchema);
    FormData fdInfer = new FormData();
    fdInfer.left = new FormAttachment(middle, 0);
    fdInfer.top = new FormAttachment(last, margin);
    wInferSchema.setLayoutData(fdInfer);
    last = wInferSchema;

    Label wlExtra = new Label(shell, SWT.RIGHT);
    wlExtra.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.ExtraOptions"));
    PropsUi.setLook(wlExtra);
    FormData fdlExtra = new FormData();
    fdlExtra.left = new FormAttachment(0, 0);
    fdlExtra.top = new FormAttachment(last, margin);
    fdlExtra.right = new FormAttachment(middle, -margin);
    wlExtra.setLayoutData(fdlExtra);
    wExtraOptions = new Text(shell, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.V_SCROLL);
    PropsUi.setLook(wExtraOptions);
    wExtraOptions.addModifyListener(lsMod);
    FormData fdExtra = new FormData();
    fdExtra.left = new FormAttachment(middle, 0);
    fdExtra.top = new FormAttachment(wlExtra, 0, SWT.TOP);
    fdExtra.right = new FormAttachment(100, 0);
    fdExtra.height = 50;
    wExtraOptions.setLayoutData(fdExtra);
    last = wExtraOptions;

    // Fields table
    Label wlFields = new Label(shell, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "SparkFileInputDialog.Fields"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(last, margin);
    wlFields.setLayoutData(fdlFields);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkFileInputDialog.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkFileInputDialog.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkFileInputDialog.Column.Length"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "SparkFileInputDialog.Column.Precision"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    wFields =
        new TableView(
            variables,
            shell,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            input.getFields() == null ? 1 : input.getFields().size(),
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wOk, -2 * margin);
    wFields.setLayoutData(fdFields);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private TextVar labeledTextVar(
      ModifyListener lsMod, int middle, int margin, Control last, String labelKey) {
    Label wl = new Label(shell, SWT.RIGHT);
    wl.setText(BaseMessages.getString(PKG, labelKey));
    PropsUi.setLook(wl);
    FormData fdl = new FormData();
    fdl.left = new FormAttachment(0, 0);
    fdl.top = new FormAttachment(last, margin);
    fdl.right = new FormAttachment(middle, -margin);
    wl.setLayoutData(fdl);
    TextVar text = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(text);
    text.addModifyListener(lsMod);
    String tooltip = BaseMessages.getString(PKG, labelKey + ".Tooltip");
    if (!Utils.isEmpty(tooltip) && !tooltip.startsWith("!")) {
      text.setToolTipText(tooltip);
      wl.setToolTipText(tooltip);
    }
    FormData fd = new FormData();
    fd.left = new FormAttachment(middle, 0);
    fd.top = new FormAttachment(wl, 0, SWT.CENTER);
    fd.right = new FormAttachment(100, 0);
    text.setLayoutData(fd);
    return text;
  }

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wFilePath.setText(Const.NVL(input.getFilePath(), ""));
    wFormat.setText(Const.NVL(input.getFileFormat(), SparkFileInputMeta.FORMAT_CSV));
    wHeader.setSelection(input.isHeader());
    wSeparator.setText(Const.NVL(input.getSeparator(), ","));
    wQuote.setText(Const.NVL(input.getQuote(), "\""));
    wInferSchema.setSelection(input.isInferSchema());
    wExtraOptions.setText(Const.NVL(input.getExtraOptions(), ""));
    if (input.getFields() != null) {
      for (int i = 0; i < input.getFields().size(); i++) {
        SparkField f = input.getFields().get(i);
        TableItem item = wFields.table.getItem(i);
        item.setText(1, Const.NVL(f.getName(), ""));
        item.setText(2, Const.NVL(f.getHopType(), "String"));
        item.setText(3, f.getLength() >= 0 ? Integer.toString(f.getLength()) : "");
        item.setText(4, f.getPrecision() >= 0 ? Integer.toString(f.getPrecision()) : "");
      }
    }
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
    transformName = wTransformName.getText();
    input.setFilePath(wFilePath.getText());
    input.setFileFormat(wFormat.getText());
    input.setHeader(wHeader.getSelection());
    input.setSeparator(wSeparator.getText());
    input.setQuote(wQuote.getText());
    input.setInferSchema(wInferSchema.getSelection());
    input.setExtraOptions(wExtraOptions.getText());
    List<SparkField> fields = new ArrayList<>();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      SparkField f = new SparkField();
      f.setName(item.getText(1));
      f.setHopType(item.getText(2));
      f.setLength(Const.toInt(item.getText(3), -1));
      f.setPrecision(Const.toInt(item.getText(4), -1));
      fields.add(f);
    }
    input.setFields(fields);
    input.setChanged();
    dispose();
  }
}
