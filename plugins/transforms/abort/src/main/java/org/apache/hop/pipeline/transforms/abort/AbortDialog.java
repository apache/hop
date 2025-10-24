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

package org.apache.hop.pipeline.transforms.abort;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.SwtSvgImageUtil;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class AbortDialog extends BaseTransformDialog {
  private static final Class<?> PKG = AbortDialog.class;

  private TextVar wRowThreshold;

  private TextVar wMessage;

  private Button wAlwaysLogRows;

  private final AbortMeta input;
  private ModifyListener lsMod;
  private SelectionAdapter lsSelMod;
  private Button wAbortButton;
  private Button wAbortWithErrorButton;
  private Button wSafeStopButton;
  private Group wOptionsGroup;
  private Label hSpacer;

  private final int middle = props.getMiddlePct();
  private final int margin = PropsUi.getMargin();

  public AbortDialog(
      Shell parent, IVariables variables, AbortMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    shell.setMinimumSize(400, 520);
    setShellImage(shell, input);

    lsMod = e -> input.setChanged();
    lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 15;
    formLayout.marginHeight = 15;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "AbortDialog.Shell.Title"));

    // Some buttons at the bottom
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    positionBottomButtons(shell, new Button[] {wOk, wCancel}, PropsUi.getMargin(), null);

    Label wicon = new Label(shell, SWT.RIGHT);
    wicon.setImage(getImage());
    FormData fdlicon = new FormData();
    fdlicon.top = new FormAttachment(0, 0);
    fdlicon.right = new FormAttachment(100, 0);
    wicon.setLayoutData(fdlicon);
    PropsUi.setLook(wicon);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "AbortDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(wicon, 0, SWT.CENTER);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);

    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.right = new FormAttachment(wicon, -margin);
    wTransformName.setLayoutData(fdTransformName);

    Label spacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdSpacer = new FormData();
    fdSpacer.left = new FormAttachment(0, 0);
    fdSpacer.top = new FormAttachment(wicon, 0);
    fdSpacer.right = new FormAttachment(100, 0);
    spacer.setLayoutData(fdSpacer);

    hSpacer = new Label(shell, SWT.HORIZONTAL | SWT.SEPARATOR);
    FormData fdhSpacer = new FormData();
    fdhSpacer.height = 2;
    fdhSpacer.left = new FormAttachment(0, 0);
    fdhSpacer.bottom = new FormAttachment(wCancel, -15);
    fdhSpacer.right = new FormAttachment(100, 0);
    hSpacer.setLayoutData(fdhSpacer);

    buildOptions(spacer);
    buildLogging(wOptionsGroup);

    getData();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void buildOptions(Control widgetAbove) {
    wOptionsGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    PropsUi.setLook(wOptionsGroup);
    wOptionsGroup.setText(BaseMessages.getString(PKG, "AbortDialog.Options.Group.Label"));
    FormLayout flOptionsGroup = new FormLayout();
    flOptionsGroup.marginHeight = 15;
    flOptionsGroup.marginWidth = 15;
    wOptionsGroup.setLayout(flOptionsGroup);

    FormData fdOptionsGroup = new FormData();
    fdOptionsGroup.left = new FormAttachment(0, 0);
    fdOptionsGroup.top = new FormAttachment(widgetAbove, 15);
    fdOptionsGroup.right = new FormAttachment(100, 0);
    wOptionsGroup.setLayoutData(fdOptionsGroup);

    wAbortButton = new Button(wOptionsGroup, SWT.RADIO);
    wAbortButton.addSelectionListener(lsSelMod);
    wAbortButton.setText(BaseMessages.getString(PKG, "AbortDialog.Options.Abort.Label"));
    FormData fdAbort = new FormData();
    fdAbort.left = new FormAttachment(middle, margin);
    fdAbort.top = new FormAttachment(0, 0);
    wAbortButton.setLayoutData(fdAbort);
    PropsUi.setLook(wAbortButton);

    wAbortWithErrorButton = new Button(wOptionsGroup, SWT.RADIO);
    wAbortWithErrorButton.addSelectionListener(lsSelMod);
    wAbortWithErrorButton.setText(
        BaseMessages.getString(PKG, "AbortDialog.Options.AbortWithError.Label"));
    FormData fdAbortWithError = new FormData();
    fdAbortWithError.left = new FormAttachment(middle, margin);
    fdAbortWithError.top = new FormAttachment(wAbortButton, 10);
    wAbortWithErrorButton.setLayoutData(fdAbortWithError);
    PropsUi.setLook(wAbortWithErrorButton);

    wSafeStopButton = new Button(wOptionsGroup, SWT.RADIO);
    wSafeStopButton.addSelectionListener(lsSelMod);
    wSafeStopButton.setText(BaseMessages.getString(PKG, "AbortDialog.Options.SafeStop.Label"));
    FormData fdSafeStop = new FormData();
    fdSafeStop.left = new FormAttachment(middle, margin);
    fdSafeStop.top = new FormAttachment(wAbortWithErrorButton, 10);
    wSafeStopButton.setLayoutData(fdSafeStop);
    PropsUi.setLook(wSafeStopButton);

    Label wlRowThreshold = new Label(wOptionsGroup, SWT.RIGHT);
    wlRowThreshold.setText(BaseMessages.getString(PKG, "AbortDialog.Options.RowThreshold.Label"));
    PropsUi.setLook(wlRowThreshold);
    FormData fdlRowThreshold = new FormData();
    fdlRowThreshold.left = new FormAttachment(0, 0);
    fdlRowThreshold.top = new FormAttachment(wSafeStopButton, 10);
    fdlRowThreshold.right = new FormAttachment(middle, -margin);
    wlRowThreshold.setLayoutData(fdlRowThreshold);

    wRowThreshold = new TextVar(variables, wOptionsGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wRowThreshold.setText("");
    PropsUi.setLook(wRowThreshold);
    wRowThreshold.addModifyListener(lsMod);
    wRowThreshold.setToolTipText(
        BaseMessages.getString(PKG, "AbortDialog.Options.RowThreshold.Tooltip"));
    FormData fdRowThreshold = new FormData();
    fdRowThreshold.left = new FormAttachment(wlRowThreshold, margin);
    fdRowThreshold.top = new FormAttachment(wlRowThreshold, 0, SWT.CENTER);
    fdRowThreshold.right = new FormAttachment(100, 0);
    wRowThreshold.setLayoutData(fdRowThreshold);
  }

  private void buildLogging(Composite widgetAbove) {
    Group wLoggingGroup = new Group(shell, SWT.SHADOW_ETCHED_IN);
    PropsUi.setLook(wLoggingGroup);
    wLoggingGroup.setText(BaseMessages.getString(PKG, "AbortDialog.Logging.Group"));
    FormLayout flLoggingGroup = new FormLayout();
    flLoggingGroup.marginHeight = 15;
    flLoggingGroup.marginWidth = 15;
    wLoggingGroup.setLayout(flLoggingGroup);

    wLoggingGroup.setLayoutData(
        FormDataBuilder.builder()
            .left()
            .top(widgetAbove, 15)
            .right(100, 0)
            .bottom(hSpacer, -15)
            .build());

    Label wlMessage = new Label(wLoggingGroup, SWT.RIGHT);
    wlMessage.setText(BaseMessages.getString(PKG, "AbortDialog.Logging.AbortMessage.Label"));
    PropsUi.setLook(wlMessage);
    FormData fdlMessage = new FormData();
    fdlMessage.left = new FormAttachment(0, 0);
    fdlMessage.top = new FormAttachment(0, 0);
    fdlMessage.right = new FormAttachment(middle, -margin);
    wlMessage.setLayoutData(fdlMessage);

    wMessage = new TextVar(variables, wLoggingGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wMessage.setText("");
    PropsUi.setLook(wMessage);
    wMessage.addModifyListener(lsMod);
    wMessage.setToolTipText(
        BaseMessages.getString(PKG, "AbortDialog.Logging.AbortMessage.Tooltip"));
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(wlMessage, margin);
    fdMessage.top = new FormAttachment(wlMessage, 0, SWT.CENTER);
    fdMessage.right = new FormAttachment(100, 0);
    wMessage.setLayoutData(fdMessage);

    wAlwaysLogRows = new Button(wLoggingGroup, SWT.CHECK);
    wAlwaysLogRows.setText(BaseMessages.getString(PKG, "AbortDialog.Logging.AlwaysLogRows.Label"));
    PropsUi.setLook(wAlwaysLogRows);
    wAlwaysLogRows.setToolTipText(
        BaseMessages.getString(PKG, "AbortDialog.Logging.AlwaysLogRows.Tooltip"));
    FormData fdAlwaysLogRows = new FormData();
    fdAlwaysLogRows.left = new FormAttachment(middle, margin);
    fdAlwaysLogRows.top = new FormAttachment(wMessage, 10);
    wAlwaysLogRows.setLayoutData(fdAlwaysLogRows);
    wAlwaysLogRows.addSelectionListener(lsSelMod);
  }

  private Image getImage() {
    return SwtSvgImageUtil.getImage(
        shell.getDisplay(),
        getClass().getClassLoader(),
        "abort.svg",
        ConstUi.LARGE_ICON_SIZE,
        ConstUi.LARGE_ICON_SIZE);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getRowThreshold() != null) {
      wRowThreshold.setText(input.getRowThreshold());
    }
    if (input.getMessage() != null) {
      wMessage.setText(input.getMessage());
    }
    wAlwaysLogRows.setSelection(input.isAlwaysLogRows());

    wAbortButton.setSelection(input.isAbort());
    wAbortWithErrorButton.setSelection(input.isAbortWithError());
    wSafeStopButton.setSelection(input.isSafeStop());

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void getInfo() {
    input.setRowThreshold(wRowThreshold.getText());
    input.setMessage(wMessage.getText());
    input.setAlwaysLogRows(wAlwaysLogRows.getSelection());

    AbortMeta.AbortOption abortOption = AbortMeta.AbortOption.ABORT;
    if (wAbortWithErrorButton.getSelection()) {
      abortOption = AbortMeta.AbortOption.ABORT_WITH_ERROR;
    } else if (wSafeStopButton.getSelection()) {
      abortOption = AbortMeta.AbortOption.SAFE_STOP;
    }
    input.setAbortOption(abortOption);
  }

  /** Cancel the dialog. */
  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    getInfo();
    // return value
    transformName = wTransformName.getText();
    dispose();
  }
}
