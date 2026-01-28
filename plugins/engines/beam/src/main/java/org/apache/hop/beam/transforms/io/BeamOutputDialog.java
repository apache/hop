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

package org.apache.hop.beam.transforms.io;

import org.apache.hop.beam.metadata.FileDefinition;
import org.apache.hop.core.Const;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class BeamOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = BeamOutput.class;
  private final BeamOutputMeta input;

  private boolean getpreviousFields = false;

  private TextVar wOutputLocation;
  private MetaSelectionLine<FileDefinition> wFileDefinition;
  private TextVar wFilePrefix;
  private TextVar wFileSuffix;
  private Button wWindowed;

  public BeamOutputDialog(
      Shell parent, IVariables variables, BeamOutputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "BeamOutputDialog.DialogTitle"));
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

    Control lastControl = null;

    Label wlOutputLocation = new Label(wContent, SWT.RIGHT);
    wlOutputLocation.setText(BaseMessages.getString(PKG, "BeamOutputDialog.OutputLocation"));
    PropsUi.setLook(wlOutputLocation);
    FormData fdlOutputLocation = new FormData();
    fdlOutputLocation.left = new FormAttachment(0, 0);
    fdlOutputLocation.top = new FormAttachment(0, margin);
    fdlOutputLocation.right = new FormAttachment(middle, -margin);
    wlOutputLocation.setLayoutData(fdlOutputLocation);
    wOutputLocation = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputLocation);
    FormData fdOutputLocation = new FormData();
    fdOutputLocation.left = new FormAttachment(middle, 0);
    fdOutputLocation.top = new FormAttachment(wlOutputLocation, 0, SWT.CENTER);
    fdOutputLocation.right = new FormAttachment(100, 0);
    wOutputLocation.setLayoutData(fdOutputLocation);
    lastControl = wOutputLocation;

    Label wlFilePrefix = new Label(wContent, SWT.RIGHT);
    wlFilePrefix.setText(BaseMessages.getString(PKG, "BeamOutputDialog.FilePrefix"));
    PropsUi.setLook(wlFilePrefix);
    FormData fdlFilePrefix = new FormData();
    fdlFilePrefix.left = new FormAttachment(0, 0);
    fdlFilePrefix.top = new FormAttachment(lastControl, margin);
    fdlFilePrefix.right = new FormAttachment(middle, -margin);
    wlFilePrefix.setLayoutData(fdlFilePrefix);
    wFilePrefix = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilePrefix);
    FormData fdFilePrefix = new FormData();
    fdFilePrefix.left = new FormAttachment(middle, 0);
    fdFilePrefix.top = new FormAttachment(wlFilePrefix, 0, SWT.CENTER);
    fdFilePrefix.right = new FormAttachment(100, 0);
    wFilePrefix.setLayoutData(fdFilePrefix);
    lastControl = wFilePrefix;

    Label wlFileSuffix = new Label(wContent, SWT.RIGHT);
    wlFileSuffix.setText(BaseMessages.getString(PKG, "BeamOutputDialog.FileSuffix"));
    PropsUi.setLook(wlFileSuffix);
    FormData fdlFileSuffix = new FormData();
    fdlFileSuffix.left = new FormAttachment(0, 0);
    fdlFileSuffix.top = new FormAttachment(lastControl, margin);
    fdlFileSuffix.right = new FormAttachment(middle, -margin);
    wlFileSuffix.setLayoutData(fdlFileSuffix);
    wFileSuffix = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileSuffix);
    FormData fdFileSuffix = new FormData();
    fdFileSuffix.left = new FormAttachment(middle, 0);
    fdFileSuffix.top = new FormAttachment(wlFileSuffix, 0, SWT.CENTER);
    fdFileSuffix.right = new FormAttachment(100, 0);
    wFileSuffix.setLayoutData(fdFileSuffix);
    lastControl = wFileSuffix;

    Label wlWindowed = new Label(wContent, SWT.RIGHT);
    wlWindowed.setText(BaseMessages.getString(PKG, "BeamOutputDialog.Windowed"));
    PropsUi.setLook(wlWindowed);
    FormData fdlWindowed = new FormData();
    fdlWindowed.left = new FormAttachment(0, 0);
    fdlWindowed.top = new FormAttachment(lastControl, margin);
    fdlWindowed.right = new FormAttachment(middle, -margin);
    wlWindowed.setLayoutData(fdlWindowed);
    wWindowed = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wWindowed);
    FormData fdWindowed = new FormData();
    fdWindowed.left = new FormAttachment(middle, 0);
    fdWindowed.top = new FormAttachment(wlWindowed, 0, SWT.CENTER);
    fdWindowed.right = new FormAttachment(100, 0);
    wWindowed.setLayoutData(fdWindowed);
    lastControl = wWindowed;

    wFileDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            FileDefinition.class,
            wContent,
            SWT.NONE,
            BaseMessages.getString(PKG, "BeamOutputDialog.FileDefinition"),
            BaseMessages.getString(PKG, "BeamOutputDialog.FileDefinition"));
    PropsUi.setLook(wFileDefinition);
    FormData fdFileDefinition = new FormData();
    fdFileDefinition.left = new FormAttachment(0, 0);
    fdFileDefinition.top = new FormAttachment(lastControl, margin);
    fdFileDefinition.right = new FormAttachment(100, 0);
    wFileDefinition.setLayoutData(fdFileDefinition);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    try {
      wFileDefinition.fillItems();
    } catch (Exception e) {
      log.logError("Error getting file definition items", e);
    }

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Populate the widgets. */
  public void getData() {
    wFileDefinition.setText(Const.NVL(input.getFileDefinitionName(), ""));
    wOutputLocation.setText(Const.NVL(input.getOutputLocation(), ""));
    wFilePrefix.setText(Const.NVL(input.getFilePrefix(), ""));
    wFileSuffix.setText(Const.NVL(input.getFileSuffix(), ""));
    wWindowed.setSelection(input.isWindowed());
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

  private void getInfo(BeamOutputMeta in) {
    transformName = wTransformName.getText(); // return value

    in.setFileDefinitionName(wFileDefinition.getText());
    in.setOutputLocation(wOutputLocation.getText());
    in.setFilePrefix(wFilePrefix.getText());
    in.setFileSuffix(wFileSuffix.getText());
    in.setWindowed(wWindowed.getSelection());

    input.setChanged();
  }
}
