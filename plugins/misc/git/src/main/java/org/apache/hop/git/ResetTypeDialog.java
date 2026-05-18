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

package org.apache.hop.git;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.jgit.api.ResetCommand.ResetType;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

public class ResetTypeDialog extends Dialog {
  private static final Class<?> PKG = GitGuiPlugin.class;

  private AtomicReference<ResetType> resetType = new AtomicReference<>(ResetType.MIXED);

  private Shell shell;

  public ResetTypeDialog(Shell parent) {
    super(parent, SWT.NONE);
  }

  public ResetType open(RevCommit commit) {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setImage(GuiResource.getInstance().getImageHopUi());
    shell.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Header"));
    PropsUi.setLook(shell);

    // TODO: save state

    // Some buttons
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString("System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());

    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString("System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());

    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, PropsUi.getMargin(), null);

    Label label = new Label(shell, SWT.NONE);
    label.setText(
        BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Message", commit.getShortMessage()));
    label.setLayoutData(FormDataBuilder.builder().fullWidth().top().result());
    PropsUi.setLook(label);

    // Reset type choices
    Composite composite = new Composite(shell, SWT.NONE);
    composite.setLayout(new RowLayout(SWT.VERTICAL));
    composite.setLayoutData(
        FormDataBuilder.builder()
            .fullWidth()
            .top(label, PropsUi.getMargin())
            .bottom(wOk, -PropsUi.getMargin())
            .result());
    PropsUi.setLook(composite);

    //    Group group = new Group(shell, SWT.NONE);
    //    group.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.ResetCommit.Type.Label"));
    //    group.setLayout(new RowLayout(SWT.VERTICAL));
    //    group.setLayoutData(
    //        FormDataBuilder.builder().fullWidth().top().bottom(wOk,
    // -PropsUi.getMargin()).result());
    //    PropsUi.setLook(group);

    Button wbSoft = new Button(composite, SWT.RADIO);
    wbSoft.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Soft.Label"));
    wbSoft.setToolTipText(
        BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Soft.Tooltip"));
    wbSoft.setSelection(resetType.get() == ResetType.SOFT);
    wbSoft.addListener(SWT.Selection, e -> resetType.set(ResetType.SOFT));
    PropsUi.setLook(wbSoft);

    Button wbMixed = new Button(composite, SWT.RADIO);
    wbMixed.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Mixed.Label"));
    wbMixed.setToolTipText(
        BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Mixed.Tooltip"));
    wbMixed.setSelection(resetType.get() == ResetType.MIXED);
    wbMixed.addListener(SWT.Selection, e -> resetType.set(ResetType.MIXED));
    PropsUi.setLook(wbMixed);

    Button wbHard = new Button(composite, SWT.RADIO);
    wbHard.setText(BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Hard.Label"));
    wbHard.setToolTipText(
        BaseMessages.getString(PKG, "GitGuiPlugin.Dialog.Reset.Type.Hard.Tooltip"));
    wbHard.setSelection(resetType.get() == ResetType.HARD);
    wbHard.addListener(SWT.Selection, e -> resetType.set(ResetType.HARD));
    PropsUi.setLook(wbHard);

    shell.setDefaultButton(wOk);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return resetType.get();
  }

  public void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  private void cancel() {
    resetType.set(null);
    dispose();
  }

  private void ok() {
    dispose();
  }
}
