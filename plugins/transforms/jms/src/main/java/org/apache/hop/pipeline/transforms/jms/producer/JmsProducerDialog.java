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

package org.apache.hop.pipeline.transforms.jms.producer;

import org.apache.hop.core.Const;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.jms.consumer.JmsDestinationType;
import org.apache.hop.pipeline.transforms.jms.shared.JmsDialogHelper;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class JmsProducerDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JmsProducerMeta.class;

  private final JmsProducerMeta input;

  private CCombo wConnection;
  private CCombo wDestinationType;
  private TextVar wDestination;
  private Button wTransacted;
  private CCombo wBodyField;
  private CCombo wKeyField;

  public JmsProducerDialog(
      Shell parent,
      IVariables variables,
      JmsProducerMeta transformMeta,
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

    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "JmsProducerDialog.Shell.Title"));

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
    wTransformName.addModifyListener(e -> input.setChanged());
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);
    Control last = wTransformName;

    wConnection =
        JmsDialogHelper.labeledCombo(
            shell, PKG, variables, middle, margin, last, "JmsProducerDialog.Connection", true);
    wConnection.setItems(JmsDialogHelper.listConnectionNames(metadataProvider));
    last = wConnection;

    wDestinationType =
        JmsDialogHelper.labeledCombo(
            shell, PKG, variables, middle, margin, last, "JmsProducerDialog.DestinationType", true);
    wDestinationType.setItems(
        new String[] {JmsDestinationType.QUEUE.name(), JmsDestinationType.TOPIC.name()});
    last = wDestinationType;

    wDestination =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsProducerDialog.Destination");
    last = wDestination;

    wTransacted =
        JmsDialogHelper.labeledCheckbox(
            shell, PKG, middle, margin, last, "JmsProducerDialog.Transacted");
    last = wTransacted;

    wBodyField =
        JmsDialogHelper.labeledCombo(
            shell, PKG, variables, middle, margin, last, "JmsProducerDialog.BodyField", false);
    last = wBodyField;

    wKeyField =
        JmsDialogHelper.labeledCombo(
            shell, PKG, variables, middle, margin, last, "JmsProducerDialog.KeyField", false);

    String[] incomingFields = getIncomingFieldNames();
    wBodyField.setItems(incomingFields);
    wKeyField.setItems(incomingFields);

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wKeyField);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private String[] getIncomingFieldNames() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      return rowMeta.getFieldNames();
    } catch (Exception e) {
      // The dialog must still open when the upstream row layout cannot be resolved.
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "JmsProducerDialog.FieldsError.Title"),
          BaseMessages.getString(PKG, "JmsProducerDialog.FieldsError.Message"),
          e);
      return new String[0];
    }
  }

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wDestinationType.setText(
        Const.NVL(input.getDestinationType(), JmsDestinationType.QUEUE.name()));
    wDestination.setText(Const.NVL(input.getDestination(), ""));
    wTransacted.setSelection(input.isTransacted());
    wBodyField.setText(Const.NVL(input.getBodyField(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));

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
    input.setConnectionName(wConnection.getText());
    input.setDestinationType(wDestinationType.getText());
    input.setDestination(wDestination.getText());
    input.setTransacted(wTransacted.getSelection());
    input.setBodyField(wBodyField.getText());
    input.setKeyField(wKeyField.getText());
    dispose();
  }
}
