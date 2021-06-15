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

package org.apache.hop.avro.transforms.avroinput;

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
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class AvroFileInputDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = AvroFileInputMeta.class; // For Translator

  private AvroFileInputMeta input;

  private Combo wFilenameField;
  private Text wOutputField;
  private TextVar wRowsLimit;

  public AvroFileInputDialog(
      Shell parent,
      IVariables variables,
      Object baseTransformMeta,
      PipelineMeta pipelineMeta,
      String transformName) {
    super(parent, variables, (BaseTransformMeta) baseTransformMeta, pipelineMeta, transformName);

    input = (AvroFileInputMeta) baseTransformMeta;
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
    shell.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = props.getMargin();

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.TransformName.Label"));
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
    wlFilenameField.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.FilenameField.Label"));
    props.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(lastControl, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilenameField.setText(transformName);
    props.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);
    lastControl = wFilenameField;

    Label wlOutputField = new Label(shell, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.OutputField.Label"));
    props.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(lastControl, margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutputField.setText(transformName);
    props.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    fdOutputField.right = new FormAttachment(100, 0);
    wOutputField.setLayoutData(fdOutputField);
    lastControl = wOutputField;

    Label wlRowsLimit = new Label(shell, SWT.RIGHT);
    wlRowsLimit.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.RowsLimit.Label"));
    props.setLook(wlRowsLimit);
    FormData fdlRowsLimit = new FormData();
    fdlRowsLimit.left = new FormAttachment(0, 0);
    fdlRowsLimit.right = new FormAttachment(middle, -margin);
    fdlRowsLimit.top = new FormAttachment(lastControl, margin);
    wlRowsLimit.setLayoutData(fdlRowsLimit);
    wRowsLimit = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRowsLimit.setText(transformName);
    props.setLook(wRowsLimit);
    FormData fdRowsLimit = new FormData();
    fdRowsLimit.left = new FormAttachment(middle, 0);
    fdRowsLimit.top = new FormAttachment(wlRowsLimit, 0, SWT.CENTER);
    fdRowsLimit.right = new FormAttachment(100, 0);
    wRowsLimit.setLayoutData(fdRowsLimit);
    lastControl = wRowsLimit;

    // Some buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, lastControl);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      wFilenameField.setItems(fields.getFieldNames());
    } catch (Exception e) {
      new ErrorDialog(shell, "Error", "Error getting input fields", e);
    }

    wFilenameField.setText(Const.NVL(input.getDataFilenameField(), ""));
    wOutputField.setText(Const.NVL(input.getOutputFieldName(), ""));
    wRowsLimit.setText(Const.NVL(input.getRowsLimit(), ""));

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

    input.setDataFilenameField(wFilenameField.getText());
    input.setOutputFieldName(wOutputField.getText());
    input.setRowsLimit(wRowsLimit.getText());

    transformName = wTransformName.getText(); // return value
    transformMeta.setChanged();

    dispose();
  }
}
