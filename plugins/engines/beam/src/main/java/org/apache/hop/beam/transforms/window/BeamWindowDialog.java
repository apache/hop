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

package org.apache.hop.beam.transforms.window;

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
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamWindowDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamWindowDialog.class;
  private final BeamWindowMeta input;

  private Combo wWindowType;
  private TextVar wDuration;
  private TextVar wEvery;
  private TextVar wStartTimeField;
  private TextVar wEndTimeField;
  private TextVar wMaxTimeField;
  private TextVar wAllowedLateness;
  private Button wDiscardFiredPanes;
  private Combo wTriggerType;

  public BeamWindowDialog(
      Shell parent, IVariables variables, BeamWindowMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamWindowDialog.DialogTitle"));
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

    Label wlWindowType = new Label(wContent, SWT.RIGHT);
    wlWindowType.setText(BaseMessages.getString(PKG, "BeamWindowDialog.WindowType"));
    PropsUi.setLook(wlWindowType);
    FormData fdlWindowType = new FormData();
    fdlWindowType.left = new FormAttachment(0, 0);
    fdlWindowType.top = new FormAttachment(0, margin);
    fdlWindowType.right = new FormAttachment(middle, -margin);
    wlWindowType.setLayoutData(fdlWindowType);
    wWindowType = new Combo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wWindowType);
    wWindowType.setItems(BeamDefaults.WINDOW_TYPES);
    FormData fdWindowType = new FormData();
    fdWindowType.left = new FormAttachment(middle, 0);
    fdWindowType.top = new FormAttachment(wlWindowType, 0, SWT.CENTER);
    fdWindowType.right = new FormAttachment(100, 0);
    wWindowType.setLayoutData(fdWindowType);
    lastControl = wWindowType;

    Label wlDuration = new Label(wContent, SWT.RIGHT);
    wlDuration.setText(BaseMessages.getString(PKG, "BeamWindowDialog.Duration"));
    PropsUi.setLook(wlDuration);
    FormData fdlDuration = new FormData();
    fdlDuration.left = new FormAttachment(0, 0);
    fdlDuration.top = new FormAttachment(lastControl, margin);
    fdlDuration.right = new FormAttachment(middle, -margin);
    wlDuration.setLayoutData(fdlDuration);
    wDuration = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDuration);
    FormData fdDuration = new FormData();
    fdDuration.left = new FormAttachment(middle, 0);
    fdDuration.top = new FormAttachment(wlDuration, 0, SWT.CENTER);
    fdDuration.right = new FormAttachment(100, 0);
    wDuration.setLayoutData(fdDuration);
    lastControl = wDuration;

    Label wlEvery = new Label(wContent, SWT.RIGHT);
    wlEvery.setText(BaseMessages.getString(PKG, "BeamWindowDialog.Every"));
    PropsUi.setLook(wlEvery);
    FormData fdlEvery = new FormData();
    fdlEvery.left = new FormAttachment(0, 0);
    fdlEvery.top = new FormAttachment(lastControl, margin);
    fdlEvery.right = new FormAttachment(middle, -margin);
    wlEvery.setLayoutData(fdlEvery);
    wEvery = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEvery);
    FormData fdEvery = new FormData();
    fdEvery.left = new FormAttachment(middle, 0);
    fdEvery.top = new FormAttachment(wlEvery, 0, SWT.CENTER);
    fdEvery.right = new FormAttachment(100, 0);
    wEvery.setLayoutData(fdEvery);
    lastControl = wEvery;

    Label wlStartTimeField = new Label(wContent, SWT.RIGHT);
    wlStartTimeField.setText(BaseMessages.getString(PKG, "BeamWindowDialog.StartTimeField"));
    PropsUi.setLook(wlStartTimeField);
    FormData fdlStartTimeField = new FormData();
    fdlStartTimeField.left = new FormAttachment(0, 0);
    fdlStartTimeField.top = new FormAttachment(lastControl, margin);
    fdlStartTimeField.right = new FormAttachment(middle, -margin);
    wlStartTimeField.setLayoutData(fdlStartTimeField);
    wStartTimeField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wStartTimeField);
    FormData fdStartTimeField = new FormData();
    fdStartTimeField.left = new FormAttachment(middle, 0);
    fdStartTimeField.top = new FormAttachment(wlStartTimeField, 0, SWT.CENTER);
    fdStartTimeField.right = new FormAttachment(100, 0);
    wStartTimeField.setLayoutData(fdStartTimeField);
    lastControl = wStartTimeField;

    Label wlEndTimeField = new Label(wContent, SWT.RIGHT);
    wlEndTimeField.setText(BaseMessages.getString(PKG, "BeamWindowDialog.EndTimeField"));
    PropsUi.setLook(wlEndTimeField);
    FormData fdlEndTimeField = new FormData();
    fdlEndTimeField.left = new FormAttachment(0, 0);
    fdlEndTimeField.top = new FormAttachment(lastControl, margin);
    fdlEndTimeField.right = new FormAttachment(middle, -margin);
    wlEndTimeField.setLayoutData(fdlEndTimeField);
    wEndTimeField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEndTimeField);
    FormData fdEndTimeField = new FormData();
    fdEndTimeField.left = new FormAttachment(middle, 0);
    fdEndTimeField.top = new FormAttachment(wlEndTimeField, 0, SWT.CENTER);
    fdEndTimeField.right = new FormAttachment(100, 0);
    wEndTimeField.setLayoutData(fdEndTimeField);
    lastControl = wEndTimeField;

    Label wlMaxTimeField = new Label(wContent, SWT.RIGHT);
    wlMaxTimeField.setText(BaseMessages.getString(PKG, "BeamWindowDialog.MaxTimeField"));
    PropsUi.setLook(wlMaxTimeField);
    FormData fdlMaxTimeField = new FormData();
    fdlMaxTimeField.left = new FormAttachment(0, 0);
    fdlMaxTimeField.top = new FormAttachment(lastControl, margin);
    fdlMaxTimeField.right = new FormAttachment(middle, -margin);
    wlMaxTimeField.setLayoutData(fdlMaxTimeField);
    wMaxTimeField = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxTimeField);
    FormData fdMaxTimeField = new FormData();
    fdMaxTimeField.left = new FormAttachment(middle, 0);
    fdMaxTimeField.top = new FormAttachment(wlMaxTimeField, 0, SWT.CENTER);
    fdMaxTimeField.right = new FormAttachment(100, 0);
    wMaxTimeField.setLayoutData(fdMaxTimeField);
    lastControl = wMaxTimeField;

    Label wlAllowedLateness = new Label(wContent, SWT.RIGHT);
    wlAllowedLateness.setText(BaseMessages.getString(PKG, "BeamWindowDialog.AllowedLateness"));
    PropsUi.setLook(wlAllowedLateness);
    FormData fdlAllowedLateness = new FormData();
    fdlAllowedLateness.left = new FormAttachment(0, 0);
    fdlAllowedLateness.top = new FormAttachment(lastControl, margin);
    fdlAllowedLateness.right = new FormAttachment(middle, -margin);
    wlAllowedLateness.setLayoutData(fdlAllowedLateness);
    wAllowedLateness = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAllowedLateness);
    FormData fdAllowedLateness = new FormData();
    fdAllowedLateness.left = new FormAttachment(middle, 0);
    fdAllowedLateness.top = new FormAttachment(wlAllowedLateness, 0, SWT.CENTER);
    fdAllowedLateness.right = new FormAttachment(100, 0);
    wAllowedLateness.setLayoutData(fdAllowedLateness);
    lastControl = wAllowedLateness;

    Label wlDiscardFiredPanes = new Label(wContent, SWT.RIGHT);
    wlDiscardFiredPanes.setText(BaseMessages.getString(PKG, "BeamWindowDialog.DiscardFiredPanes"));
    PropsUi.setLook(wlDiscardFiredPanes);
    FormData fdlDiscardFiredPanes = new FormData();
    fdlDiscardFiredPanes.left = new FormAttachment(0, 0);
    fdlDiscardFiredPanes.top = new FormAttachment(lastControl, margin);
    fdlDiscardFiredPanes.right = new FormAttachment(middle, -margin);
    wlDiscardFiredPanes.setLayoutData(fdlDiscardFiredPanes);
    wDiscardFiredPanes = new Button(wContent, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wDiscardFiredPanes);
    FormData fdDiscardFiredPanes = new FormData();
    fdDiscardFiredPanes.left = new FormAttachment(middle, 0);
    fdDiscardFiredPanes.top = new FormAttachment(wlDiscardFiredPanes, 0, SWT.CENTER);
    fdDiscardFiredPanes.right = new FormAttachment(100, 0);
    wDiscardFiredPanes.setLayoutData(fdDiscardFiredPanes);
    lastControl = wlDiscardFiredPanes;

    Label wlTriggerType = new Label(wContent, SWT.RIGHT);
    wlTriggerType.setText(BaseMessages.getString(PKG, "BeamWindowDialog.TriggerType"));
    PropsUi.setLook(wlTriggerType);
    FormData fdlTriggerType = new FormData();
    fdlTriggerType.left = new FormAttachment(0, 0);
    fdlTriggerType.top = new FormAttachment(lastControl, margin);
    fdlTriggerType.right = new FormAttachment(middle, -margin);
    wlTriggerType.setLayoutData(fdlTriggerType);
    wTriggerType = new Combo(wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTriggerType);
    wTriggerType.setItems(WindowTriggerType.getDescriptions());
    FormData fdTriggerType = new FormData();
    fdTriggerType.left = new FormAttachment(middle, 0);
    fdTriggerType.top = new FormAttachment(wlTriggerType, 0, SWT.CENTER);
    fdTriggerType.right = new FormAttachment(100, 0);
    wTriggerType.setLayoutData(fdTriggerType);
    lastControl = wlTriggerType;

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
    wWindowType.setText(Const.NVL(input.getWindowType(), ""));
    wDuration.setText(Const.NVL(input.getDuration(), ""));
    wEvery.setText(Const.NVL(input.getEvery(), ""));
    wStartTimeField.setText(Const.NVL(input.getStartWindowField(), ""));
    wEndTimeField.setText(Const.NVL(input.getEndWindowField(), ""));
    wMaxTimeField.setText(Const.NVL(input.getMaxWindowField(), ""));
    wAllowedLateness.setText(Const.NVL(input.getAllowedLateness(), ""));
    wDiscardFiredPanes.setSelection(input.isDiscardingFiredPanes());
    if (input.getTriggeringType() != null) {
      wTriggerType.setText(input.getTriggeringType().getDescription());
    }
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

  private void getInfo(BeamWindowMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setWindowType(wWindowType.getText());
    in.setDuration(wDuration.getText());
    in.setEvery(wEvery.getText());
    in.setStartWindowField(wStartTimeField.getText());
    in.setEndWindowField(wEndTimeField.getText());
    in.setMaxWindowField(wMaxTimeField.getText());
    in.setAllowedLateness(wAllowedLateness.getText());
    in.setDiscardingFiredPanes(wDiscardFiredPanes.getSelection());
    in.setTriggeringType(WindowTriggerType.findDescription(wTriggerType.getText()));

    input.setChanged();
  }
}
