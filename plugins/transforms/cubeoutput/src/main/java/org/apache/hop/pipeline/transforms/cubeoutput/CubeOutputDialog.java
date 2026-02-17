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

package org.apache.hop.pipeline.transforms.cubeoutput;

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
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class CubeOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = CubeOutputMeta.class;

  private TextVar wFilename;
  private Button wCreatingParentFolders;
  private Button wAddToResult;
  private Button wDoNotOpenNewFileInit;

  private final CubeOutputMeta input;

  public CubeOutputDialog(
      Shell parent, IVariables variables, CubeOutputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CubeOutputDialog.Shell.Text"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

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

    // Filename line
    Label wlFilename = new Label(wContent, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "CubeOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(0, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);
    Button wbFilename = new Button(wContent, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "CubeOutputDialog.Browse.Button"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(0, margin);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wContent, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(0, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Creating parent folders
    //
    Label wlCreatingParentFolders = new Label(wContent, SWT.RIGHT);
    wlCreatingParentFolders.setText(
        BaseMessages.getString(PKG, "CubeOutputDialog.CreatingParentFolders.Label"));
    PropsUi.setLook(wlCreatingParentFolders);
    FormData fdlCreatingParentFolders = new FormData();
    fdlCreatingParentFolders.left = new FormAttachment(0, 0);
    fdlCreatingParentFolders.top = new FormAttachment(wFilename, margin);
    fdlCreatingParentFolders.right = new FormAttachment(middle, -margin);
    wlCreatingParentFolders.setLayoutData(fdlCreatingParentFolders);
    wCreatingParentFolders = new Button(wContent, SWT.CHECK);
    wCreatingParentFolders.setToolTipText(
        BaseMessages.getString(PKG, "CubeOutputDialog.CreatingParentFolders.Tooltip"));
    PropsUi.setLook(wCreatingParentFolders);
    FormData fdCreatingParentFolders = new FormData();
    fdCreatingParentFolders.left = new FormAttachment(middle, 0);
    fdCreatingParentFolders.top = new FormAttachment(wlCreatingParentFolders, 0, SWT.CENTER);
    fdCreatingParentFolders.right = new FormAttachment(100, 0);
    wCreatingParentFolders.setLayoutData(fdCreatingParentFolders);
    wCreatingParentFolders.addListener(SWT.Selection, e -> input.setChanged());

    // Open new File at Init
    //
    Label wlDoNotOpenNewFileInit = new Label(wContent, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText(
        BaseMessages.getString(PKG, "CubeOutputDialog.DoNotOpenNewFileInit.Label"));
    PropsUi.setLook(wlDoNotOpenNewFileInit);
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
    fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreatingParentFolders, margin);
    fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button(wContent, SWT.CHECK);
    wDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "CubeOutputDialog.DoNotOpenNewFileInit.Tooltip"));
    PropsUi.setLook(wDoNotOpenNewFileInit);
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
    fdDoNotOpenNewFileInit.top = new FormAttachment(wlDoNotOpenNewFileInit, 0, SWT.CENTER);
    fdDoNotOpenNewFileInit.right = new FormAttachment(100, 0);
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addListener(SWT.Selection, e -> input.setChanged());

    // Add File to the result files name
    //
    Label wlAddToResult = new Label(wContent, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "CubeOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wContent, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "CubeOutputDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdAddToResult.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdAddToResult);
    wAddToResult.addListener(SWT.Selection, e -> input.setChanged());

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    // Add listeners
    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFilename,
                variables,
                new String[] {"*.cube", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "CubeOutputDialog.FilterNames.Options.CubeFiles"),
                  BaseMessages.getString(PKG, "CubeOutputDialog.FilterNames.Options.AllFiles")
                },
                true));

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getFilename() != null) {
      wFilename.setText(input.getFilename());
    }
    wCreatingParentFolders.setSelection(input.isFilenameCreatingParentFolders());
    wDoNotOpenNewFileInit.setSelection(input.isDoNotOpenNewFileInit());
    wAddToResult.setSelection(input.isAddToResultFilenames());
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

    transformName = wTransformName.getText(); // return value
    input.setFilenameCreatingParentFolders(wCreatingParentFolders.getSelection());
    input.setAddToResultFilenames(wAddToResult.getSelection());
    input.setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());
    input.setFilename(wFilename.getText());

    dispose();
  }
}
