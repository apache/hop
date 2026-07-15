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

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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
import org.eclipse.swt.widgets.Text;

public class SparkFileOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SparkFileOutputMeta.class;

  private final SparkFileOutputMeta input;

  private TextVar wFilePath;
  private CCombo wFormat;
  private CCombo wSaveMode;
  private Button wHeader;
  private TextVar wSeparator;
  private TextVar wQuote;
  private TextVar wPartitionBy;
  private TextVar wCoalesce;
  private Text wExtraOptions;

  public SparkFileOutputDialog(
      Shell parent,
      IVariables variables,
      SparkFileOutputMeta transformMeta,
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
    shell.setText(BaseMessages.getString(PKG, "SparkFileOutputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

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

    last = labeledTextVar(lsMod, middle, margin, last, "SparkFileOutputDialog.FilePath");
    wFilePath = (TextVar) last;

    Label wlFormat = new Label(shell, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "SparkFileOutputDialog.Format"));
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

    Label wlMode = new Label(shell, SWT.RIGHT);
    wlMode.setText(BaseMessages.getString(PKG, "SparkFileOutputDialog.SaveMode"));
    PropsUi.setLook(wlMode);
    FormData fdlMode = new FormData();
    fdlMode.left = new FormAttachment(0, 0);
    fdlMode.top = new FormAttachment(last, margin);
    fdlMode.right = new FormAttachment(middle, -margin);
    wlMode.setLayoutData(fdlMode);
    wSaveMode = new CCombo(shell, SWT.BORDER | SWT.READ_ONLY);
    wSaveMode.setItems(
        new String[] {
          SparkFileOutputMeta.MODE_OVERWRITE,
          SparkFileOutputMeta.MODE_APPEND,
          SparkFileOutputMeta.MODE_IGNORE,
          SparkFileOutputMeta.MODE_ERROR
        });
    PropsUi.setLook(wSaveMode);
    wSaveMode.addModifyListener(lsMod);
    FormData fdMode = new FormData();
    fdMode.left = new FormAttachment(middle, 0);
    fdMode.top = new FormAttachment(wlMode, 0, SWT.CENTER);
    fdMode.right = new FormAttachment(100, 0);
    wSaveMode.setLayoutData(fdMode);
    last = wSaveMode;

    wHeader = new Button(shell, SWT.CHECK);
    wHeader.setText(BaseMessages.getString(PKG, "SparkFileOutputDialog.Header"));
    PropsUi.setLook(wHeader);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(middle, 0);
    fdHeader.top = new FormAttachment(last, margin);
    wHeader.setLayoutData(fdHeader);
    last = wHeader;

    last = labeledTextVar(lsMod, middle, margin, last, "SparkFileOutputDialog.Separator");
    wSeparator = (TextVar) last;
    last = labeledTextVar(lsMod, middle, margin, last, "SparkFileOutputDialog.Quote");
    wQuote = (TextVar) last;
    last = labeledTextVar(lsMod, middle, margin, last, "SparkFileOutputDialog.PartitionBy");
    wPartitionBy = (TextVar) last;
    last = labeledTextVar(lsMod, middle, margin, last, "SparkFileOutputDialog.Coalesce");
    wCoalesce = (TextVar) last;

    Label wlExtra = new Label(shell, SWT.RIGHT);
    wlExtra.setText(BaseMessages.getString(PKG, "SparkFileOutputDialog.ExtraOptions"));
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
    fdExtra.height = 60;
    wExtraOptions.setLayoutData(fdExtra);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wExtraOptions);

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
    wSaveMode.setText(Const.NVL(input.getSaveMode(), SparkFileOutputMeta.MODE_OVERWRITE));
    wHeader.setSelection(input.isHeader());
    wSeparator.setText(Const.NVL(input.getSeparator(), ","));
    wQuote.setText(Const.NVL(input.getQuote(), "\""));
    wPartitionBy.setText(Const.NVL(input.getPartitionByColumns(), ""));
    wCoalesce.setText(Const.NVL(input.getCoalescePartitions(), ""));
    wExtraOptions.setText(Const.NVL(input.getExtraOptions(), ""));
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
    input.setSaveMode(wSaveMode.getText());
    input.setHeader(wHeader.getSelection());
    input.setSeparator(wSeparator.getText());
    input.setQuote(wQuote.getText());
    input.setPartitionByColumns(wPartitionBy.getText());
    input.setCoalescePartitions(wCoalesce.getText());
    input.setExtraOptions(wExtraOptions.getText());
    input.setChanged();
    dispose();
  }
}
