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

package org.apache.hop.pipeline.transforms.jms.consumer;

import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transforms.jms.shared.JmsDialogHelper;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
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

public class JmsConsumerDialog extends BaseTransformDialog {
  private static final Class<?> PKG = JmsConsumerMeta.class;

  private final JmsConsumerMeta input;

  private CCombo wConnection;
  private CCombo wDestinationType;
  private TextVar wDestination;
  private TextVar wMessageSelector;
  private TextVar wDurableSubscription;
  private Button wTransacted;
  private TextVar wMaxMessages;
  private TextVar wReceiveTimeout;
  private TextVar wBodyField;
  private TextVar wKeyField;
  private TextVar wDestinationField;
  private TextVar wMessageIdField;
  private TextVar wTimestampField;

  public JmsConsumerDialog(
      Shell parent,
      IVariables variables,
      JmsConsumerMeta transformMeta,
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
    shell.setText(BaseMessages.getString(PKG, "JmsConsumerDialog.Shell.Title"));

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
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.Connection", true);
    wConnection.setItems(JmsDialogHelper.listConnectionNames(metadataProvider));
    last = wConnection;

    wDestinationType =
        JmsDialogHelper.labeledCombo(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.DestinationType", true);
    wDestinationType.setItems(
        new String[] {JmsDestinationType.QUEUE.name(), JmsDestinationType.TOPIC.name()});
    last = wDestinationType;

    wDestination =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.Destination");
    last = wDestination;

    wMessageSelector =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.MessageSelector");
    last = wMessageSelector;

    wDurableSubscription =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.DurableSubscription");
    last = wDurableSubscription;

    wTransacted =
        JmsDialogHelper.labeledCheckbox(
            shell, PKG, middle, margin, last, "JmsConsumerDialog.Transacted");
    last = wTransacted;

    wMaxMessages =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.MaxMessages");
    last = wMaxMessages;

    wReceiveTimeout =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.ReceiveTimeout");
    last = wReceiveTimeout;

    wBodyField =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.BodyField");
    last = wBodyField;
    wKeyField =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.KeyField");
    last = wKeyField;
    wDestinationField =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.DestinationField");
    last = wDestinationField;
    wMessageIdField =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.MessageIdField");
    last = wMessageIdField;
    wTimestampField =
        JmsDialogHelper.labeledText(
            shell, PKG, variables, middle, margin, last, "JmsConsumerDialog.TimestampField");

    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wCancel}, margin, wTimestampField);

    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();
    input.setChanged(changed);
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void getData() {
    wTransformName.setText(Const.NVL(transformName, ""));
    wConnection.setText(Const.NVL(input.getConnectionName(), ""));
    wDestinationType.setText(
        Const.NVL(input.getDestinationType(), JmsDestinationType.QUEUE.name()));
    wDestination.setText(Const.NVL(input.getDestination(), ""));
    wMessageSelector.setText(Const.NVL(input.getMessageSelector(), ""));
    wDurableSubscription.setText(Const.NVL(input.getDurableSubscription(), ""));
    wTransacted.setSelection(input.isTransacted());
    wMaxMessages.setText(Const.NVL(input.getMaxMessages(), "0"));
    wReceiveTimeout.setText(Const.NVL(input.getReceiveTimeout(), "5000"));
    wBodyField.setText(Const.NVL(input.getBodyField(), ""));
    wKeyField.setText(Const.NVL(input.getKeyField(), ""));
    wDestinationField.setText(Const.NVL(input.getDestinationField(), ""));
    wMessageIdField.setText(Const.NVL(input.getMessageIdField(), ""));
    wTimestampField.setText(Const.NVL(input.getTimestampField(), ""));

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
    input.setMessageSelector(wMessageSelector.getText());
    input.setDurableSubscription(wDurableSubscription.getText());
    input.setTransacted(wTransacted.getSelection());
    input.setMaxMessages(wMaxMessages.getText());
    input.setReceiveTimeout(wReceiveTimeout.getText());
    input.setBodyField(wBodyField.getText());
    input.setKeyField(wKeyField.getText());
    input.setDestinationField(wDestinationField.getText());
    input.setMessageIdField(wMessageIdField.getText());
    input.setTimestampField(wTimestampField.getText());
    dispose();
  }
}
