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

package org.apache.hop.workflow.actions.folderscompare;

import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the Folders compare action settings. */
public class ActionFoldersCompareDialog extends ActionDialog {
  private static final Class<?> PKG = ActionFoldersCompare.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionFoldersCompare.Filetype.All")};

  private Text wName;

  private TextVar wFilename1;

  private TextVar wFilename2;

  private ActionFoldersCompare action;

  private boolean changed;

  private Button wIncludeSubfolders;

  private Label wlCompareFileContent;
  private Button wCompareFileContent;

  private CCombo wCompareOnly;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlCompareFileSize;
  private Button wCompareFileSize;

  public ActionFoldersCompareDialog(
      Shell parent, ActionFoldersCompare action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionFoldersCompare.Name.Default"));
    }
  }

  @Override
  public IAction open() {

    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.MIN | SWT.MAX | SWT.RESIZE);
    PropsUi.setLook(shell);
    WorkflowDialog.setShellImage(shell, action);

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Name line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Name.Label"));
    PropsUi.setLook(wlName);
    FormData fdlName = new FormData();
    fdlName.left = new FormAttachment(0, 0);
    fdlName.right = new FormAttachment(middle, -margin);
    fdlName.top = new FormAttachment(0, margin);
    wlName.setLayoutData(fdlName);
    wName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wName);
    wName.addModifyListener(lsMod);
    FormData fdName = new FormData();
    fdName.left = new FormAttachment(middle, 0);
    fdName.top = new FormAttachment(0, margin);
    fdName.right = new FormAttachment(100, 0);
    wName.setLayoutData(fdName);

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(shell, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Settings.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout(groupLayout);

    Label wlIncludeSubfolders = new Label(wSettings, SWT.RIGHT);
    wlIncludeSubfolders.setText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.IncludeSubfolders.Label"));
    PropsUi.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment(0, 0);
    fdlIncludeSubfolders.top = new FormAttachment(0, margin);
    fdlIncludeSubfolders.right = new FormAttachment(middle, -margin);
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeSubfolders);
    wIncludeSubfolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.IncludeSubfolders.Tooltip"));
    FormData fdIncludeSubfolders = new FormData();
    fdIncludeSubfolders.left = new FormAttachment(middle, 0);
    fdIncludeSubfolders.top = new FormAttachment(wlIncludeSubfolders, 0, SWT.CENTER);
    fdIncludeSubfolders.right = new FormAttachment(100, 0);
    wIncludeSubfolders.setLayoutData(fdIncludeSubfolders);
    wIncludeSubfolders.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Compare Only?
    Label wlCompareOnly = new Label(wSettings, SWT.RIGHT);
    wlCompareOnly.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.CompareOnly.Label"));
    PropsUi.setLook(wlCompareOnly);
    FormData fdlCompareOnly = new FormData();
    fdlCompareOnly.left = new FormAttachment(0, 0);
    fdlCompareOnly.right = new FormAttachment(middle, -margin);
    fdlCompareOnly.top = new FormAttachment(wlIncludeSubfolders, 2 * margin);
    wlCompareOnly.setLayoutData(fdlCompareOnly);
    wCompareOnly = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wCompareOnly.add(BaseMessages.getString(PKG, "ActionFoldersCompare.All_CompareOnly.Label"));
    wCompareOnly.add(BaseMessages.getString(PKG, "ActionFoldersCompare.Files_CompareOnly.Label"));
    wCompareOnly.add(BaseMessages.getString(PKG, "ActionFoldersCompare.Folders_CompareOnly.Label"));
    wCompareOnly.add(BaseMessages.getString(PKG, "ActionFoldersCompare.Specify_CompareOnly.Label"));
    wCompareOnly.select(0); // +1: starts at -1
    PropsUi.setLook(wCompareOnly);
    FormData fdCompareOnly = new FormData();
    fdCompareOnly.left = new FormAttachment(middle, 0);
    fdCompareOnly.top = new FormAttachment(wlCompareOnly, 0, SWT.CENTER);
    fdCompareOnly.right = new FormAttachment(100, -margin);
    wCompareOnly.setLayoutData(fdCompareOnly);
    wCompareOnly.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            specifyCompareOnlyActivate();
          }
        });

    // Wildcard
    wlWildcard = new Label(wSettings, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wCompareOnly, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
        new TextVar(
            variables,
            wSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionFoldersCompare.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wCompareOnly, margin);
    fdWildcard.right = new FormAttachment(100, -margin);
    wWildcard.setLayoutData(fdWildcard);

    wlCompareFileSize = new Label(wSettings, SWT.RIGHT);
    wlCompareFileSize.setText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.CompareFileSize.Label"));
    PropsUi.setLook(wlCompareFileSize);
    FormData fdlCompareFileSize = new FormData();
    fdlCompareFileSize.left = new FormAttachment(0, 0);
    fdlCompareFileSize.top = new FormAttachment(wWildcard, margin);
    fdlCompareFileSize.right = new FormAttachment(middle, -margin);
    wlCompareFileSize.setLayoutData(fdlCompareFileSize);
    wCompareFileSize = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wCompareFileSize);
    wCompareFileSize.setToolTipText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.CompareFileSize.Tooltip"));
    FormData fdCompareFileSize = new FormData();
    fdCompareFileSize.left = new FormAttachment(middle, 0);
    fdCompareFileSize.top = new FormAttachment(wlCompareFileSize, 0, SWT.CENTER);
    fdCompareFileSize.right = new FormAttachment(100, 0);
    wCompareFileSize.setLayoutData(fdCompareFileSize);
    wCompareFileSize.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    wlCompareFileContent = new Label(wSettings, SWT.RIGHT);
    wlCompareFileContent.setText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.CompareFileContent.Label"));
    PropsUi.setLook(wlCompareFileContent);
    FormData fdlCompareFileContent = new FormData();
    fdlCompareFileContent.left = new FormAttachment(0, 0);
    fdlCompareFileContent.top = new FormAttachment(wlCompareFileSize, 2 * margin);
    fdlCompareFileContent.right = new FormAttachment(middle, -margin);
    wlCompareFileContent.setLayoutData(fdlCompareFileContent);
    wCompareFileContent = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wCompareFileContent);
    wCompareFileContent.setToolTipText(
        BaseMessages.getString(PKG, "ActionFoldersCompare.CompareFileContent.Tooltip"));
    FormData fdCompareFileContent = new FormData();
    fdCompareFileContent.left = new FormAttachment(middle, 0);
    fdCompareFileContent.top = new FormAttachment(wlCompareFileContent, 0, SWT.CENTER);
    fdCompareFileContent.right = new FormAttachment(100, 0);
    wCompareFileContent.setLayoutData(fdCompareFileContent);
    wCompareFileContent.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wName, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // Filename 1 line
    Label wlFilename1 = new Label(shell, SWT.RIGHT);
    wlFilename1.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Filename1.Label"));
    PropsUi.setLook(wlFilename1);
    FormData fdlFilename1 = new FormData();
    fdlFilename1.left = new FormAttachment(0, 0);
    fdlFilename1.top = new FormAttachment(wSettings, 2 * margin);
    fdlFilename1.right = new FormAttachment(middle, -margin);
    wlFilename1.setLayoutData(fdlFilename1);

    // Browse folders button ...
    Button wbDirectory1 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDirectory1);
    wbDirectory1.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.FolderBrowse.Label"));
    FormData fdbDirectory1 = new FormData();
    fdbDirectory1.right = new FormAttachment(100, -margin);
    fdbDirectory1.top = new FormAttachment(wSettings, 2 * margin);
    wbDirectory1.setLayoutData(fdbDirectory1);

    wbDirectory1.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            BaseDialog.presentDirectoryDialog(shell, wFilename1, variables);
          }
        });

    // Browse files ..
    Button wbFilename1 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename1);
    wbFilename1.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.FileBrowse.Label"));
    FormData fdbFilename1 = new FormData();
    fdbFilename1.right = new FormAttachment(wbDirectory1, -margin);
    fdbFilename1.top = new FormAttachment(wSettings, 2 * margin);
    wbFilename1.setLayoutData(fdbFilename1);

    wFilename1 = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename1);
    wFilename1.addModifyListener(lsMod);
    FormData fdFilename1 = new FormData();
    fdFilename1.left = new FormAttachment(middle, 0);
    fdFilename1.top = new FormAttachment(wSettings, 2 * margin);
    fdFilename1.right = new FormAttachment(wbFilename1, -margin);
    wFilename1.setLayoutData(fdFilename1);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename1.addModifyListener(
        e -> wFilename1.setToolTipText(variables.resolve(wFilename1.getText())));

    wbFilename1.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename1, variables, new String[] {"*"}, FILETYPES, false));

    // Filename 2 line
    Label wlFilename2 = new Label(shell, SWT.RIGHT);
    wlFilename2.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.Filename2.Label"));
    PropsUi.setLook(wlFilename2);
    FormData fdlFilename2 = new FormData();
    fdlFilename2.left = new FormAttachment(0, 0);
    fdlFilename2.top = new FormAttachment(wFilename1, margin);
    fdlFilename2.right = new FormAttachment(middle, -margin);
    wlFilename2.setLayoutData(fdlFilename2);

    // Browse folders button ...
    Button wbDirectory2 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDirectory2);
    wbDirectory2.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.FolderBrowse.Label"));
    FormData fdbDirectory2 = new FormData();
    fdbDirectory2.right = new FormAttachment(100, -margin);
    fdbDirectory2.top = new FormAttachment(wFilename1, margin);
    wbDirectory2.setLayoutData(fdbDirectory2);

    wbDirectory2.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wFilename2, variables));

    // Browse files...
    Button wbFilename2 = new Button(shell, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename2);
    wbFilename2.setText(BaseMessages.getString(PKG, "ActionFoldersCompare.FileBrowse.Label"));
    FormData fdbFilename2 = new FormData();
    fdbFilename2.right = new FormAttachment(wbDirectory2, -margin);
    fdbFilename2.top = new FormAttachment(wFilename1, margin);
    wbFilename2.setLayoutData(fdbFilename2);

    wFilename2 = new TextVar(variables, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename2);
    wFilename2.addModifyListener(lsMod);
    FormData fdFilename2 = new FormData();
    fdFilename2.left = new FormAttachment(middle, 0);
    fdFilename2.top = new FormAttachment(wFilename1, margin);
    fdFilename2.right = new FormAttachment(wbFilename2, -margin);
    wFilename2.setLayoutData(fdFilename2);

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename2.addModifyListener(
        e -> wFilename2.setToolTipText(variables.resolve(wFilename2.getText())));

    wbFilename2.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wFilename2, variables, new String[] {"*"}, FILETYPES, false));

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wOk, wCancel}, margin, wFilename2);

    getData();
    specifyCompareOnlyActivate();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void specifyCompareOnlyActivate() {
    wWildcard.setEnabled(wCompareOnly.getSelectionIndex() == 3);
    wlWildcard.setEnabled(wCompareOnly.getSelectionIndex() == 3);

    wCompareFileContent.setEnabled(wCompareOnly.getSelectionIndex() != 2);
    wlCompareFileContent.setEnabled(wCompareOnly.getSelectionIndex() != 2);

    wCompareFileSize.setEnabled(wCompareOnly.getSelectionIndex() != 2);
    wlCompareFileSize.setEnabled(wCompareOnly.getSelectionIndex() != 2);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (action.getName() != null) {
      wName.setText(action.getName());
    }
    if (action.getCompareOnly() != null) {
      if (action.getCompareOnly().equals("only_files")) {
        wCompareOnly.select(1);
      } else if (action.getCompareOnly().equals("only_folders")) {
        wCompareOnly.select(2);
      } else if (action.getCompareOnly().equals("specify")) {
        wCompareOnly.select(3);
      } else {
        wCompareOnly.select(0);
      }
    } else {
      wCompareOnly.select(0);
    }

    if (action.getWildcard() != null) {
      wWildcard.setText(action.getWildcard());
    }
    if (action.getFilename1() != null) {
      wFilename1.setText(action.getFilename1());
    }
    if (action.getFilename2() != null) {
      wFilename2.setText(action.getFilename2());
    }

    wIncludeSubfolders.setSelection(action.isIncludeSubfolders());
    wCompareFileContent.setSelection(action.isCompareFileContent());
    wCompareFileSize.setSelection(action.isCompareFileSize());

    wName.selectAll();
    wName.setFocus();
  }

  private void cancel() {
    action.setChanged(changed);
    action = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wName.getText())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setText(BaseMessages.getString(PKG, "System.TransformActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setIncludeSubfolders(wIncludeSubfolders.getSelection());
    action.setCompareFileContent(wCompareFileContent.getSelection());
    action.setCompareFileSize(wCompareFileSize.getSelection());

    if (wCompareOnly.getSelectionIndex() == 1) {
      action.setCompareOnly("only_files");
    } else if (wCompareOnly.getSelectionIndex() == 2) {
      action.setCompareOnly("only_folders");
    } else if (wCompareOnly.getSelectionIndex() == 3) {
      action.setCompareOnly("specify");
    } else {
      action.setCompareOnly("all");
    }

    action.setName(wName.getText());
    action.setWildcard(wWildcard.getText());
    action.setFilename1(wFilename1.getText());
    action.setFilename2(wFilename2.getText());
    dispose();
  }
}
