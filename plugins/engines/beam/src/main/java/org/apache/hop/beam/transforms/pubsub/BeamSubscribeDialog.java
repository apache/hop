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

package org.apache.hop.beam.transforms.pubsub;

import org.apache.hop.beam.core.BeamDefaults;
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
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamSubscribeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamSubscribe.class;
  private final BeamSubscribeMeta input;

  private TextVar wSubscription;
  private TextVar wTopic;
  private Combo wMessageType;
  private TextVar wMessageField;

  public BeamSubscribeDialog(
      Shell parent,
      IVariables variables,
      BeamSubscribeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamSubscribeDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    changed = input.hasChanged();

    Control lastControl = null;

    Label wlSubscription = new Label(wContent, SWT.RIGHT);
    wlSubscription.setText(BaseMessages.getString(PKG, "BeamSubscribeDialog.Subscription"));
    PropsUi.setLook(wlSubscription);
    FormData fdlSubscription = new FormData();
    fdlSubscription.left = new FormAttachment(0, 0);
    fdlSubscription.top = new FormAttachment(0, margin);
    fdlSubscription.right = new FormAttachment(middle, -margin);
    wlSubscription.setLayoutData(fdlSubscription);
    wSubscription = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSubscription);
    FormData fdSubscription = new FormData();
    fdSubscription.left = new FormAttachment(middle, 0);
    fdSubscription.top = new FormAttachment(wlSubscription, 0, SWT.CENTER);
    fdSubscription.right = new FormAttachment(100, 0);
    wSubscription.setLayoutData(fdSubscription);
    lastControl = wSubscription;

    Label wlTopic = new Label(wContent, SWT.RIGHT);
    wlTopic.setText(BaseMessages.getString(PKG, "BeamSubscribeDialog.Topic"));
    PropsUi.setLook(wlTopic);
    FormData fdlTopic = new FormData();
    fdlTopic.left = new FormAttachment(0, 0);
    fdlTopic.top = new FormAttachment(lastControl, margin);
    fdlTopic.right = new FormAttachment(middle, -margin);
    wlTopic.setLayoutData(fdlTopic);
    wTopic = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTopic);
    FormData fdTopic = new FormData();
    fdTopic.left = new FormAttachment(middle, 0);
    fdTopic.top = new FormAttachment(wlTopic, 0, SWT.CENTER);
    fdTopic.right = new FormAttachment(100, 0);
    wTopic.setLayoutData(fdTopic);
    lastControl = wTopic;

    Label wlMessageType = new Label(wContent, SWT.RIGHT);
    wlMessageType.setText(BaseMessages.getString(PKG, "BeamSubscribeDialog.MessageType"));
    PropsUi.setLook(wlMessageType);
    FormData fdlMessageType = new FormData();
    fdlMessageType.left = new FormAttachment(0, 0);
    fdlMessageType.top = new FormAttachment(lastControl, margin);
    fdlMessageType.right = new FormAttachment(middle, -margin);
    wlMessageType.setLayoutData(fdlMessageType);
    wMessageType = new Combo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageType);
    wMessageType.setItems(BeamDefaults.PUBSUB_MESSAGE_TYPES);
    FormData fdMessageType = new FormData();
    fdMessageType.left = new FormAttachment(middle, 0);
    fdMessageType.top = new FormAttachment(wlMessageType, 0, SWT.CENTER);
    fdMessageType.right = new FormAttachment(100, 0);
    wMessageType.setLayoutData(fdMessageType);
    lastControl = wMessageType;

    Label wlMessageField = new Label(wContent, SWT.RIGHT);
    wlMessageField.setText(BaseMessages.getString(PKG, "BeamSubscribeDialog.MessageField"));
    PropsUi.setLook(wlMessageField);
    FormData fdlMessageField = new FormData();
    fdlMessageField.left = new FormAttachment(0, 0);
    fdlMessageField.top = new FormAttachment(lastControl, margin);
    fdlMessageField.right = new FormAttachment(middle, -margin);
    wlMessageField.setLayoutData(fdlMessageField);
    wMessageField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMessageField);
    FormData fdMessageField = new FormData();
    fdMessageField.left = new FormAttachment(middle, 0);
    fdMessageField.top = new FormAttachment(wlMessageField, 0, SWT.CENTER);
    fdMessageField.right = new FormAttachment(100, 0);
    wMessageField.setLayoutData(fdMessageField);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wSubscription.setText(Const.NVL(input.getSubscription(), ""));
    wTopic.setText(Const.NVL(input.getTopic(), ""));
    wMessageType.setText(Const.NVL(input.getMessageType(), ""));
    wMessageField.setText(Const.NVL(input.getMessageField(), ""));
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

    getInfo(input);

    dispose();
  }

  private void getInfo(BeamSubscribeMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setSubscription(wSubscription.getText());
    in.setTopic(wTopic.getText());
    in.setMessageType(wMessageType.getText());
    in.setMessageField(wMessageField.getText());

    input.setChanged();
  }
}
