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
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class AvroFileInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AvroFileInputMeta.class;

  private AvroFileInputMeta input;

  private Combo wFilenameField;
  private Text wOutputField;
  private TextVar wRowsLimit;

  public AvroFileInputDialog(
      Shell parent,
      IVariables variables,
      AvroFileInputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);

    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "AvroFileInputDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    Label wlFilenameField = new Label(shell, SWT.RIGHT);
    wlFilenameField.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.FilenameField.Label"));
    PropsUi.setLook(wlFilenameField);
    FormData fdlFilenameField = new FormData();
    fdlFilenameField.left = new FormAttachment(0, 0);
    fdlFilenameField.right = new FormAttachment(middle, -margin);
    fdlFilenameField.top = new FormAttachment(wSpacer, margin);
    wlFilenameField.setLayoutData(fdlFilenameField);
    wFilenameField = new Combo(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFilenameField.setText(transformName);
    PropsUi.setLook(wFilenameField);
    FormData fdFilenameField = new FormData();
    fdFilenameField.left = new FormAttachment(middle, 0);
    fdFilenameField.top = new FormAttachment(wlFilenameField, 0, SWT.CENTER);
    fdFilenameField.right = new FormAttachment(100, 0);
    wFilenameField.setLayoutData(fdFilenameField);
    Control lastControl = wFilenameField;

    Label wlOutputField = new Label(shell, SWT.RIGHT);
    wlOutputField.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.OutputField.Label"));
    PropsUi.setLook(wlOutputField);
    FormData fdlOutputField = new FormData();
    fdlOutputField.left = new FormAttachment(0, 0);
    fdlOutputField.right = new FormAttachment(middle, -margin);
    fdlOutputField.top = new FormAttachment(lastControl, margin);
    wlOutputField.setLayoutData(fdlOutputField);
    wOutputField = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOutputField.setText(transformName);
    PropsUi.setLook(wOutputField);
    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(middle, 0);
    fdOutputField.top = new FormAttachment(wlOutputField, 0, SWT.CENTER);
    fdOutputField.right = new FormAttachment(100, 0);
    wOutputField.setLayoutData(fdOutputField);
    lastControl = wOutputField;

    Label wlRowsLimit = new Label(shell, SWT.RIGHT);
    wlRowsLimit.setText(BaseMessages.getString(PKG, "AvroFileInputDialog.RowsLimit.Label"));
    PropsUi.setLook(wlRowsLimit);
    FormData fdlRowsLimit = new FormData();
    fdlRowsLimit.left = new FormAttachment(0, 0);
    fdlRowsLimit.right = new FormAttachment(middle, -margin);
    fdlRowsLimit.top = new FormAttachment(lastControl, margin);
    wlRowsLimit.setLayoutData(fdlRowsLimit);
    wRowsLimit = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRowsLimit.setText(transformName);
    PropsUi.setLook(wRowsLimit);
    FormData fdRowsLimit = new FormData();
    fdRowsLimit.left = new FormAttachment(middle, 0);
    fdRowsLimit.top = new FormAttachment(wlRowsLimit, 0, SWT.CENTER);
    fdRowsLimit.right = new FormAttachment(100, 0);
    wRowsLimit.setLayoutData(fdRowsLimit);
    lastControl = wRowsLimit;

    getData();
    focusTransformName();
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
