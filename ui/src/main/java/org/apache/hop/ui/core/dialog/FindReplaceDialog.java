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

package org.apache.hop.ui.core.dialog;

import org.apache.commons.lang3.StringUtils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.FindReplaceOperations;
import org.apache.hop.ui.core.widget.IFindReplaceTarget;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * Find / find-and-replace dialog for multi-line editors ({@link
 * org.apache.hop.ui.core.widget.TextComposite} and the explorer content editor).
 */
public class FindReplaceDialog extends Dialog {
  private static final Class<?> PKG = FindReplaceDialog.class;

  private static String lastFind = "";
  private static String lastReplace = "";
  private static boolean lastCaseSensitive = false;

  private final IFindReplaceTarget target;
  private final boolean replaceMode;
  private final PropsUi props;

  private Shell shell;
  private Text wFind;
  private Text wReplace;
  private Button wCaseSensitive;
  private Label wlStatus;

  private FindReplaceDialog(Shell parent, IFindReplaceTarget target, boolean replaceMode) {
    super(parent, SWT.NONE);
    this.target = target;
    this.replaceMode = replaceMode;
    this.props = PropsUi.getInstance();
  }

  /**
   * Open a find or find-and-replace dialog for the given editor.
   *
   * @param parent parent shell
   * @param target target editor
   * @param replaceMode when true, show replace controls
   */
  public static void open(Shell parent, IFindReplaceTarget target, boolean replaceMode) {
    if (parent == null || target == null || target.isDisposed()) {
      return;
    }
    new FindReplaceDialog(parent, target, replaceMode).openDialog();
  }

  private void openDialog() {
    Shell parent = getParent();
    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL | SWT.SHEET);
    PropsUi.setLook(shell);
    shell.setImage(
        replaceMode
            ? GuiResource.getInstance().getImageFindReplace()
            : GuiResource.getInstance().getImageSearch());
    shell.setText(
        BaseMessages.getString(
            PKG,
            replaceMode
                ? "FindReplaceDialog.Shell.ReplaceTitle"
                : "FindReplaceDialog.Shell.FindTitle"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);

    int margin = PropsUi.getMargin();
    int middle = props.getMiddlePct();

    // Find
    Label wlFind = new Label(shell, SWT.RIGHT);
    PropsUi.setLook(wlFind);
    wlFind.setText(BaseMessages.getString(PKG, "FindReplaceDialog.Find.Label"));
    FormData fdlFind = new FormData();
    fdlFind.left = new FormAttachment(0, 0);
    fdlFind.top = new FormAttachment(0, margin);
    fdlFind.right = new FormAttachment(middle, -margin);
    wlFind.setLayoutData(fdlFind);

    wFind = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFind);
    FormData fdFind = new FormData();
    fdFind.left = new FormAttachment(middle, 0);
    fdFind.top = new FormAttachment(wlFind, 0, SWT.CENTER);
    fdFind.right = new FormAttachment(100, 0);
    wFind.setLayoutData(fdFind);

    // Replace (optional)
    Label wlReplace = null;
    if (replaceMode) {
      wlReplace = new Label(shell, SWT.RIGHT);
      PropsUi.setLook(wlReplace);
      wlReplace.setText(BaseMessages.getString(PKG, "FindReplaceDialog.Replace.Label"));
      FormData fdlReplace = new FormData();
      fdlReplace.left = new FormAttachment(0, 0);
      fdlReplace.top = new FormAttachment(wFind, margin);
      fdlReplace.right = new FormAttachment(middle, -margin);
      wlReplace.setLayoutData(fdlReplace);

      wReplace = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
      PropsUi.setLook(wReplace);
      FormData fdReplace = new FormData();
      fdReplace.left = new FormAttachment(middle, 0);
      fdReplace.top = new FormAttachment(wlReplace, 0, SWT.CENTER);
      fdReplace.right = new FormAttachment(100, 0);
      wReplace.setLayoutData(fdReplace);
    }

    // Case sensitive
    wCaseSensitive = new Button(shell, SWT.CHECK);
    PropsUi.setLook(wCaseSensitive);
    wCaseSensitive.setText(BaseMessages.getString(PKG, "FindReplaceDialog.CaseSensitive.Label"));
    FormData fdCase = new FormData();
    fdCase.left = new FormAttachment(middle, 0);
    fdCase.top = new FormAttachment(replaceMode ? wReplace : wFind, margin);
    fdCase.right = new FormAttachment(100, 0);
    wCaseSensitive.setLayoutData(fdCase);

    // Status line
    wlStatus = new Label(shell, SWT.LEFT);
    PropsUi.setLook(wlStatus);
    FormData fdStatus = new FormData();
    fdStatus.left = new FormAttachment(0, 0);
    fdStatus.right = new FormAttachment(100, 0);
    fdStatus.top = new FormAttachment(wCaseSensitive, margin);
    wlStatus.setLayoutData(fdStatus);

    // Buttons
    Button wFindNext = new Button(shell, SWT.PUSH);
    wFindNext.setText(BaseMessages.getString(PKG, "FindReplaceDialog.FindNext.Button"));
    wFindNext.addListener(SWT.Selection, e -> findNext(true));

    Button wFindPrev = new Button(shell, SWT.PUSH);
    wFindPrev.setText(BaseMessages.getString(PKG, "FindReplaceDialog.FindPrevious.Button"));
    wFindPrev.addListener(SWT.Selection, e -> findNext(false));

    Button wReplaceOne = null;
    Button wReplaceAll = null;
    if (replaceMode) {
      wReplaceOne = new Button(shell, SWT.PUSH);
      wReplaceOne.setText(BaseMessages.getString(PKG, "FindReplaceDialog.Replace.Button"));
      wReplaceOne.addListener(SWT.Selection, e -> replaceOne());
      wReplaceOne.setEnabled(target.isEditable());

      wReplaceAll = new Button(shell, SWT.PUSH);
      wReplaceAll.setText(BaseMessages.getString(PKG, "FindReplaceDialog.ReplaceAll.Button"));
      wReplaceAll.addListener(SWT.Selection, e -> replaceAll());
      wReplaceAll.setEnabled(target.isEditable());
    }

    Button wClose = new Button(shell, SWT.PUSH);
    wClose.setText(BaseMessages.getString(PKG, "System.Button.Close"));
    wClose.addListener(SWT.Selection, e -> close());

    Button[] buttons;
    if (replaceMode) {
      buttons = new Button[] {wFindNext, wFindPrev, wReplaceOne, wReplaceAll, wClose};
    } else {
      buttons = new Button[] {wFindNext, wFindPrev, wClose};
    }
    BaseTransformDialog.positionBottomButtons(shell, buttons, margin, wlStatus);

    // Defaults
    String selection = target.getSelectionText();
    if (StringUtils.isNotEmpty(selection) && !selection.contains("\n")) {
      wFind.setText(selection);
    } else if (StringUtils.isNotEmpty(lastFind)) {
      wFind.setText(lastFind);
    }
    if (replaceMode && wReplace != null) {
      wReplace.setText(lastReplace != null ? lastReplace : "");
    }
    wCaseSensitive.setSelection(lastCaseSensitive);

    shell.setDefaultButton(wFindNext);
    wFind.selectAll();
    wFind.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> findNext(true), c -> close(), false);
  }

  private void findNext(boolean forward) {
    if (target.isDisposed()) {
      close();
      return;
    }
    String query = wFind.getText();
    if (StringUtils.isEmpty(query)) {
      setStatus(BaseMessages.getString(PKG, "FindReplaceDialog.Status.EmptyFind"));
      return;
    }
    rememberOptions();
    if (FindReplaceOperations.find(target, query, wCaseSensitive.getSelection(), forward)) {
      setStatus("");
    } else {
      setStatus(BaseMessages.getString(PKG, "FindReplaceDialog.Status.NotFound"));
    }
  }

  private void replaceOne() {
    if (target.isDisposed() || !target.isEditable()) {
      return;
    }
    String query = wFind.getText();
    if (StringUtils.isEmpty(query)) {
      setStatus(BaseMessages.getString(PKG, "FindReplaceDialog.Status.EmptyFind"));
      return;
    }
    rememberOptions();
    String replacement = wReplace != null ? wReplace.getText() : "";
    if (FindReplaceOperations.replaceOne(
        target, query, replacement, wCaseSensitive.getSelection())) {
      setStatus("");
    } else {
      setStatus(BaseMessages.getString(PKG, "FindReplaceDialog.Status.NotFound"));
    }
  }

  private void replaceAll() {
    if (target.isDisposed() || !target.isEditable()) {
      return;
    }
    String query = wFind.getText();
    if (StringUtils.isEmpty(query)) {
      setStatus(BaseMessages.getString(PKG, "FindReplaceDialog.Status.EmptyFind"));
      return;
    }
    rememberOptions();
    String replacement = wReplace != null ? wReplace.getText() : "";
    int count =
        FindReplaceOperations.replaceAll(target, query, replacement, wCaseSensitive.getSelection());
    setStatus(
        BaseMessages.getString(
            PKG, "FindReplaceDialog.Status.ReplaceAllCount", Integer.toString(count)));
  }

  private void rememberOptions() {
    lastFind = wFind.getText();
    if (wReplace != null) {
      lastReplace = wReplace.getText();
    }
    lastCaseSensitive = wCaseSensitive.getSelection();
  }

  private void setStatus(String message) {
    if (wlStatus != null && !wlStatus.isDisposed()) {
      wlStatus.setText(message != null ? message : "");
    }
  }

  private void close() {
    props.setScreen(new WindowProperty(shell));
    shell.dispose();
  }
}
