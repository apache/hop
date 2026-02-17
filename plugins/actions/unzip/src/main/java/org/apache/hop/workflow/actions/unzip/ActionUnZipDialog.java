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

package org.apache.hop.workflow.actions.unzip;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.workflow.WorkflowMeta;
import org.apache.hop.workflow.action.IAction;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;

/** This dialog allows you to edit the Unzip action settings. */
public class ActionUnZipDialog extends ActionDialog {
  private static final Class<?> PKG = ActionUnZip.class;

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionUnZip.Filetype.Zip"),
        BaseMessages.getString(PKG, "ActionUnZip.Filetype.Jar"),
        BaseMessages.getString(PKG, "ActionUnZip.Filetype.All")
      };
  public static final String CONST_ACTION_UN_ZIP_BROWSE_FOLDERS_LABEL =
      "ActionUnZip.BrowseFolders.Label";

  private Label wlZipFilename;
  private Button wbZipFilename;
  private Button wbSourceDirectory;
  private TextVar wZipFilename;

  private ActionUnZip action;

  private TextVar wTargetDirectory;

  private Label wlMovetoDirectory;
  private TextVar wMovetoDirectory;

  private Label wlCreateMoveToDirectory;
  private Button wCreateMoveToDirectory;

  private TextVar wWildcard;

  private TextVar wWildcardExclude;

  private CCombo wAfterUnZip;

  private Button wAddFileToResult;

  private Button wbMovetoDirectory;

  private Button wSetModificationDateToOriginal;

  private Label wlWildcardSource;
  private TextVar wWildcardSource;

  private Button wArgsPrevious;

  private Button wRootZip;

  private CCombo wIfFileExists;

  private CCombo wSuccessCondition;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddOriginalTimestamp;
  private Button wAddOriginalTimestamp;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Button wCreateFolder;

  private boolean changed;

  public ActionUnZipDialog(
      Shell parent, ActionUnZip action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionUnZip.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionUnZip.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionUnZip.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // file source grouping?
    // ////////////////////////
    // START OF file source GROUP///
    // /
    Group wSource = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSource);
    wSource.setText(BaseMessages.getString(PKG, "ActionUnZip.Source.Group.Label"));

    FormLayout groupSourceLayout = new FormLayout();
    groupSourceLayout.marginWidth = 10;
    groupSourceLayout.marginHeight = 10;

    wSource.setLayout(groupSourceLayout);

    // Args from previous
    // Get args from previous
    Label wlArgsPrevious = new Label(wSource, SWT.RIGHT);
    wlArgsPrevious.setText(BaseMessages.getString(PKG, "ActionUnZip.ArgsPrevious.Label"));
    PropsUi.setLook(wlArgsPrevious);
    FormData fdlArgsPrevious = new FormData();
    fdlArgsPrevious.left = new FormAttachment(0, 0);
    fdlArgsPrevious.top = new FormAttachment(0, margin);
    fdlArgsPrevious.right = new FormAttachment(middle, -margin);
    wlArgsPrevious.setLayoutData(fdlArgsPrevious);
    wArgsPrevious = new Button(wSource, SWT.CHECK);
    PropsUi.setLook(wArgsPrevious);
    wArgsPrevious.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.ArgsPrevious.Tooltip"));
    FormData fdArgsPrevious = new FormData();
    fdArgsPrevious.left = new FormAttachment(middle, 0);
    fdArgsPrevious.top = new FormAttachment(wlArgsPrevious, 0, SWT.CENTER);
    fdArgsPrevious.right = new FormAttachment(100, 0);
    wArgsPrevious.setLayoutData(fdArgsPrevious);
    wArgsPrevious.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setArgdPrevious();
            action.setChanged();
          }
        });

    // ZipFilename line
    wlZipFilename = new Label(wSource, SWT.RIGHT);
    wlZipFilename.setText(BaseMessages.getString(PKG, "ActionUnZip.ZipFilename.Label"));
    PropsUi.setLook(wlZipFilename);
    FormData fdlZipFilename = new FormData();
    fdlZipFilename.left = new FormAttachment(0, 0);
    fdlZipFilename.top = new FormAttachment(wlArgsPrevious, margin);
    fdlZipFilename.right = new FormAttachment(middle, -margin);
    wlZipFilename.setLayoutData(fdlZipFilename);

    // Browse Source folders button ...
    wbSourceDirectory = new Button(wSource, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceDirectory);
    wbSourceDirectory.setText(
        BaseMessages.getString(PKG, CONST_ACTION_UN_ZIP_BROWSE_FOLDERS_LABEL));
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment(100, 0);
    fdbSourceDirectory.top = new FormAttachment(wlArgsPrevious, margin);
    wbSourceDirectory.setLayoutData(fdbSourceDirectory);

    wbSourceDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wZipFilename, variables));

    // Browse files...
    wbZipFilename = new Button(wSource, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbZipFilename);
    wbZipFilename.setText(BaseMessages.getString(PKG, "ActionUnZip.BrowseFiles.Label"));
    FormData fdbZipFilename = new FormData();
    fdbZipFilename.right = new FormAttachment(wbSourceDirectory, -margin);
    fdbZipFilename.top = new FormAttachment(wlArgsPrevious, margin);
    wbZipFilename.setLayoutData(fdbZipFilename);

    wZipFilename = new TextVar(variables, wSource, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wZipFilename);
    wZipFilename.addModifyListener(lsMod);
    FormData fdZipFilename = new FormData();
    fdZipFilename.left = new FormAttachment(middle, 0);
    fdZipFilename.top = new FormAttachment(wlArgsPrevious, margin);

    fdZipFilename.right = new FormAttachment(wbZipFilename, -margin);
    wZipFilename.setLayoutData(fdZipFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wZipFilename.addModifyListener(
        e -> wZipFilename.setToolTipText(variables.resolve(wZipFilename.getText())));

    wbZipFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wZipFilename,
                variables,
                new String[] {"*.zip;*.ZIP", "*.jar;*.JAR", "*"},
                FILETYPES,
                false));

    // WildcardSource line
    wlWildcardSource = new Label(wSource, SWT.RIGHT);
    wlWildcardSource.setText(BaseMessages.getString(PKG, "ActionUnZip.WildcardSource.Label"));
    PropsUi.setLook(wlWildcardSource);
    FormData fdlWildcardSource = new FormData();
    fdlWildcardSource.left = new FormAttachment(0, 0);
    fdlWildcardSource.top = new FormAttachment(wZipFilename, margin);
    fdlWildcardSource.right = new FormAttachment(middle, -margin);
    wlWildcardSource.setLayoutData(fdlWildcardSource);
    wWildcardSource =
        new TextVar(
            variables,
            wSource,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.WildcardSource.Tooltip"));
    PropsUi.setLook(wWildcardSource);
    wWildcardSource.addModifyListener(lsMod);
    FormData fdWildcardSource = new FormData();
    fdWildcardSource.left = new FormAttachment(middle, 0);
    fdWildcardSource.top = new FormAttachment(wZipFilename, margin);
    fdWildcardSource.right = new FormAttachment(100, 0);
    wWildcardSource.setLayoutData(fdWildcardSource);

    FormData fdSource = new FormData();
    fdSource.left = new FormAttachment(0, margin);
    fdSource.top = new FormAttachment(wName, margin);
    fdSource.right = new FormAttachment(100, -margin);
    wSource.setLayoutData(fdSource);
    // ///////////////////////////////////////////////////////////
    // / END OF FILE SOURCE
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF UNZIPPED FILES GROUP///
    // /
    Group wUnzippedFiles = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wUnzippedFiles);
    wUnzippedFiles.setText(BaseMessages.getString(PKG, "ActionUnZip.UnzippedFiles.Group.Label"));

    FormLayout groupLayoutUnzipped = new FormLayout();
    groupLayoutUnzipped.marginWidth = 10;
    groupLayoutUnzipped.marginHeight = 10;

    wUnzippedFiles.setLayout(groupLayoutUnzipped);

    // Use zipfile name as root directory
    // Use zipfile name as root directory
    Label wlRootZip = new Label(wUnzippedFiles, SWT.RIGHT);
    wlRootZip.setText(BaseMessages.getString(PKG, "ActionUnZip.RootZip.Label"));
    PropsUi.setLook(wlRootZip);
    FormData fdlRootZip = new FormData();
    fdlRootZip.left = new FormAttachment(0, 0);
    fdlRootZip.top = new FormAttachment(wSource, margin);
    fdlRootZip.right = new FormAttachment(middle, -margin);
    wlRootZip.setLayoutData(fdlRootZip);
    wRootZip = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wRootZip);
    wRootZip.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.RootZip.Tooltip"));
    FormData fdRootZip = new FormData();
    fdRootZip.left = new FormAttachment(middle, 0);
    fdRootZip.top = new FormAttachment(wlRootZip, 0, SWT.CENTER);
    fdRootZip.right = new FormAttachment(100, 0);
    wRootZip.setLayoutData(fdRootZip);
    wRootZip.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // TargetDirectory line
    Label wlTargetDirectory = new Label(wUnzippedFiles, SWT.RIGHT);
    wlTargetDirectory.setText(BaseMessages.getString(PKG, "ActionUnZip.TargetDir.Label"));
    PropsUi.setLook(wlTargetDirectory);
    FormData fdlTargetDirectory = new FormData();
    fdlTargetDirectory.left = new FormAttachment(0, 0);
    fdlTargetDirectory.top = new FormAttachment(wlRootZip, margin);
    fdlTargetDirectory.right = new FormAttachment(middle, -margin);
    wlTargetDirectory.setLayoutData(fdlTargetDirectory);

    // Browse folders button ...
    Button wbTargetDirectory = new Button(wUnzippedFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTargetDirectory);
    wbTargetDirectory.setText(
        BaseMessages.getString(PKG, CONST_ACTION_UN_ZIP_BROWSE_FOLDERS_LABEL));
    FormData fdbTargetDirectory = new FormData();
    fdbTargetDirectory.right = new FormAttachment(100, 0);
    fdbTargetDirectory.top = new FormAttachment(wlRootZip, margin);
    wbTargetDirectory.setLayoutData(fdbTargetDirectory);
    wbTargetDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wTargetDirectory, variables));

    wTargetDirectory =
        new TextVar(
            variables,
            wUnzippedFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.TargetDir.Tooltip"));
    PropsUi.setLook(wTargetDirectory);
    wTargetDirectory.addModifyListener(lsMod);
    FormData fdTargetDirectory = new FormData();
    fdTargetDirectory.left = new FormAttachment(middle, 0);
    fdTargetDirectory.top = new FormAttachment(wlRootZip, margin);
    fdTargetDirectory.right = new FormAttachment(wbTargetDirectory, -margin);
    wTargetDirectory.setLayoutData(fdTargetDirectory);

    // Create Folder
    Label wlCreateFolder = new Label(wUnzippedFiles, SWT.RIGHT);
    wlCreateFolder.setText(BaseMessages.getString(PKG, "ActionUnZip.CreateFolder.Label"));
    PropsUi.setLook(wlCreateFolder);
    FormData fdlCreateFolder = new FormData();
    fdlCreateFolder.left = new FormAttachment(0, 0);
    fdlCreateFolder.top = new FormAttachment(wTargetDirectory, margin);
    fdlCreateFolder.right = new FormAttachment(middle, -margin);
    wlCreateFolder.setLayoutData(fdlCreateFolder);
    wCreateFolder = new Button(wUnzippedFiles, SWT.CHECK);
    wCreateFolder.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.CreateFolder.Tooltip"));
    PropsUi.setLook(wCreateFolder);
    FormData fdCreateFolder = new FormData();
    fdCreateFolder.left = new FormAttachment(middle, 0);
    fdCreateFolder.top = new FormAttachment(wlCreateFolder, 0, SWT.CENTER);
    fdCreateFolder.right = new FormAttachment(100, 0);
    wCreateFolder.setLayoutData(fdCreateFolder);
    wCreateFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Wildcard line
    Label wlWildcard = new Label(wUnzippedFiles, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionUnZip.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wlCreateFolder, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
        new TextVar(
            variables,
            wUnzippedFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wlCreateFolder, margin);
    fdWildcard.right = new FormAttachment(100, 0);
    wWildcard.setLayoutData(fdWildcard);

    // Wildcard to exclude
    Label wlWildcardExclude = new Label(wUnzippedFiles, SWT.RIGHT);
    wlWildcardExclude.setText(BaseMessages.getString(PKG, "ActionUnZip.WildcardExclude.Label"));
    PropsUi.setLook(wlWildcardExclude);
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment(0, 0);
    fdlWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdlWildcardExclude.right = new FormAttachment(middle, -margin);
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude =
        new TextVar(
            variables,
            wUnzippedFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.WildcardExclude.Tooltip"));
    PropsUi.setLook(wWildcardExclude);
    wWildcardExclude.addModifyListener(lsMod);
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment(middle, 0);
    fdWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdWildcardExclude.right = new FormAttachment(100, 0);
    wWildcardExclude.setLayoutData(fdWildcardExclude);

    // Create multi-part file?
    wlAddDate = new Label(wUnzippedFiles, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "ActionUnZip.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wWildcardExclude, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    wAddDate.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.AddDate.Tooltip"));
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setDateTime();
          }
        });
    // Create multi-part file?
    wlAddTime = new Label(wUnzippedFiles, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "ActionUnZip.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wlAddDate, margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    wAddTime.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.AddTime.Tooltip"));
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setDateTime();
          }
        });

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wUnzippedFiles, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "ActionUnZip.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wlAddTime, margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(BaseMessages.getString(PKG, "ActionUnZip.SpecifyFormat.Tooltip"));
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment(middle, 0);
    fdSpecifyFormat.top = new FormAttachment(wlSpecifyFormat, 0, SWT.CENTER);
    fdSpecifyFormat.right = new FormAttachment(100, 0);
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setDateTimeFormat();
          }
        });

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label(wUnzippedFiles, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "ActionUnZip.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wUnzippedFiles, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    PropsUi.setLook(wDateTimeFormat);
    wDateTimeFormat.addModifyListener(lsMod);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, 0);
    fdDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, margin);
    fdDateTimeFormat.right = new FormAttachment(100, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    wlAddOriginalTimestamp = new Label(wUnzippedFiles, SWT.RIGHT);
    wlAddOriginalTimestamp.setText(
        BaseMessages.getString(PKG, "ActionUnZip.AddOriginalTimestamp.Label"));
    PropsUi.setLook(wlAddOriginalTimestamp);
    FormData fdlAddOriginalTimestamp = new FormData();
    fdlAddOriginalTimestamp.left = new FormAttachment(0, 0);
    fdlAddOriginalTimestamp.top = new FormAttachment(wDateTimeFormat, margin);
    fdlAddOriginalTimestamp.right = new FormAttachment(middle, -margin);
    wlAddOriginalTimestamp.setLayoutData(fdlAddOriginalTimestamp);
    wAddOriginalTimestamp = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wAddOriginalTimestamp);
    wAddOriginalTimestamp.setToolTipText(
        BaseMessages.getString(PKG, "ActionUnZip.AddOriginalTimestamp.Tooltip"));
    FormData fdAddOriginalTimestamp = new FormData();
    fdAddOriginalTimestamp.left = new FormAttachment(middle, 0);
    fdAddOriginalTimestamp.top = new FormAttachment(wlAddOriginalTimestamp, 0, SWT.CENTER);
    fdAddOriginalTimestamp.right = new FormAttachment(100, 0);
    wAddOriginalTimestamp.setLayoutData(fdAddOriginalTimestamp);
    wAddOriginalTimestamp.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Create multi-part file?
    Label wlSetModificationDateToOriginal = new Label(wUnzippedFiles, SWT.RIGHT);
    wlSetModificationDateToOriginal.setText(
        BaseMessages.getString(PKG, "ActionUnZip.SetModificationDateToOriginal.Label"));
    PropsUi.setLook(wlSetModificationDateToOriginal);
    FormData fdlSetModificationDateToOriginal = new FormData();
    fdlSetModificationDateToOriginal.left = new FormAttachment(0, 0);
    fdlSetModificationDateToOriginal.top = new FormAttachment(wlAddOriginalTimestamp, margin);
    fdlSetModificationDateToOriginal.right = new FormAttachment(middle, -margin);
    wlSetModificationDateToOriginal.setLayoutData(fdlSetModificationDateToOriginal);
    wSetModificationDateToOriginal = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wSetModificationDateToOriginal);
    wSetModificationDateToOriginal.setToolTipText(
        BaseMessages.getString(PKG, "ActionUnZip.SetModificationDateToOriginal.Tooltip"));
    FormData fdSetModificationDateToOriginal = new FormData();
    fdSetModificationDateToOriginal.left = new FormAttachment(middle, 0);
    fdSetModificationDateToOriginal.top =
        new FormAttachment(wlSetModificationDateToOriginal, 0, SWT.CENTER);
    fdSetModificationDateToOriginal.right = new FormAttachment(100, 0);
    wSetModificationDateToOriginal.setLayoutData(fdSetModificationDateToOriginal);
    wSetModificationDateToOriginal.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });
    // If File Exists
    Label wlIfFileExists = new Label(wUnzippedFiles, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionUnZip.IfFileExists.Label"));
    PropsUi.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.right = new FormAttachment(middle, -margin);
    fdlIfFileExists.top = new FormAttachment(wlSetModificationDateToOriginal, margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wUnzippedFiles, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.setItems(ActionUnZip.typeIfFileExistsDesc);
    wIfFileExists.select(0); // +1: starts at -1
    PropsUi.setLook(wIfFileExists);

    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wlIfFileExists, 0, SWT.CENTER);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    wIfFileExists.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            // Do nothing
          }
        });

    // After Zipping
    Label wlAfterUnZip = new Label(wUnzippedFiles, SWT.RIGHT);
    wlAfterUnZip.setText(BaseMessages.getString(PKG, "ActionUnZip.AfterUnZip.Label"));
    PropsUi.setLook(wlAfterUnZip);
    FormData fdlAfterUnZip = new FormData();
    fdlAfterUnZip.left = new FormAttachment(0, 0);
    fdlAfterUnZip.right = new FormAttachment(middle, -margin);
    fdlAfterUnZip.top = new FormAttachment(wIfFileExists, margin);
    wlAfterUnZip.setLayoutData(fdlAfterUnZip);
    wAfterUnZip = new CCombo(wUnzippedFiles, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wAfterUnZip.add(BaseMessages.getString(PKG, "ActionUnZip.Do_Nothing_AfterUnZip.Label"));
    wAfterUnZip.add(BaseMessages.getString(PKG, "ActionUnZip.Delete_Files_AfterUnZip.Label"));
    wAfterUnZip.add(BaseMessages.getString(PKG, "ActionUnZip.Move_Files_AfterUnZip.Label"));
    wAfterUnZip.select(0); // +1: starts at -1

    PropsUi.setLook(wAfterUnZip);
    FormData fdAfterUnZip = new FormData();
    fdAfterUnZip.left = new FormAttachment(middle, 0);
    fdAfterUnZip.top = new FormAttachment(wIfFileExists, margin);
    fdAfterUnZip.right = new FormAttachment(100, 0);
    wAfterUnZip.setLayoutData(fdAfterUnZip);

    wAfterUnZip.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            afterUnZipActivate();
          }
        });

    // moveTo Directory
    wlMovetoDirectory = new Label(wUnzippedFiles, SWT.RIGHT);
    wlMovetoDirectory.setText(BaseMessages.getString(PKG, "ActionUnZip.MovetoDirectory.Label"));
    PropsUi.setLook(wlMovetoDirectory);
    FormData fdlMovetoDirectory = new FormData();
    fdlMovetoDirectory.left = new FormAttachment(0, 0);
    fdlMovetoDirectory.top = new FormAttachment(wAfterUnZip, margin);
    fdlMovetoDirectory.right = new FormAttachment(middle, -margin);
    wlMovetoDirectory.setLayoutData(fdlMovetoDirectory);
    wMovetoDirectory =
        new TextVar(
            variables,
            wUnzippedFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.MovetoDirectory.Tooltip"));
    PropsUi.setLook(wMovetoDirectory);

    // Browse folders button ...
    wbMovetoDirectory = new Button(wUnzippedFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbMovetoDirectory);
    wbMovetoDirectory.setText(
        BaseMessages.getString(PKG, CONST_ACTION_UN_ZIP_BROWSE_FOLDERS_LABEL));
    FormData fdbMovetoDirectory = new FormData();
    fdbMovetoDirectory.right = new FormAttachment(100, 0);
    fdbMovetoDirectory.top = new FormAttachment(wAfterUnZip, margin);
    wbMovetoDirectory.setLayoutData(fdbMovetoDirectory);
    wbMovetoDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wMovetoDirectory, variables));

    wMovetoDirectory.addModifyListener(lsMod);
    FormData fdMovetoDirectory = new FormData();
    fdMovetoDirectory.left = new FormAttachment(middle, 0);
    fdMovetoDirectory.top = new FormAttachment(wAfterUnZip, margin);
    fdMovetoDirectory.right = new FormAttachment(wbMovetoDirectory, -margin);
    wMovetoDirectory.setLayoutData(fdMovetoDirectory);

    // create move to folder
    wlCreateMoveToDirectory = new Label(wUnzippedFiles, SWT.RIGHT);
    wlCreateMoveToDirectory.setText(
        BaseMessages.getString(PKG, "ActionUnZip.createMoveToFolder.Label"));
    PropsUi.setLook(wlCreateMoveToDirectory);
    FormData fdlcreateMoveToDirectory = new FormData();
    fdlcreateMoveToDirectory.left = new FormAttachment(0, 0);
    fdlcreateMoveToDirectory.top = new FormAttachment(wMovetoDirectory, margin);
    fdlcreateMoveToDirectory.right = new FormAttachment(middle, -margin);
    wlCreateMoveToDirectory.setLayoutData(fdlcreateMoveToDirectory);
    wCreateMoveToDirectory = new Button(wUnzippedFiles, SWT.CHECK);
    PropsUi.setLook(wCreateMoveToDirectory);
    wCreateMoveToDirectory.setToolTipText(
        BaseMessages.getString(PKG, "ActionUnZip.createMoveToFolder.Tooltip"));
    FormData fdCreateMoveToDirectory = new FormData();
    fdCreateMoveToDirectory.left = new FormAttachment(middle, 0);
    fdCreateMoveToDirectory.top = new FormAttachment(wlCreateMoveToDirectory, 0, SWT.CENTER);
    fdCreateMoveToDirectory.right = new FormAttachment(100, 0);
    wCreateMoveToDirectory.setLayoutData(fdCreateMoveToDirectory);
    wCreateMoveToDirectory.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdUnzippedFiles = new FormData();
    fdUnzippedFiles.left = new FormAttachment(0, margin);
    fdUnzippedFiles.top = new FormAttachment(wSource, margin);
    fdUnzippedFiles.right = new FormAttachment(100, -margin);
    wUnzippedFiles.setLayoutData(fdUnzippedFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF UNZIPPED FILES
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(500, -margin);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);
    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF ADVANCED TAB ///
    // ////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionUnZip.Tab.Advanced.Label"));

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout(advancedLayout);

    // file result grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionUnZip.FileResult.Group.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFileResult.setLayout(groupLayout);

    // Add file to result
    // Add File to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(BaseMessages.getString(PKG, "ActionUnZip.AddFileToResult.Label"));
    PropsUi.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.top = new FormAttachment(wSource, margin);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    PropsUi.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionUnZip.AddFileToResult.Tooltip"));
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment(middle, 0);
    fdAddFileToResult.top = new FormAttachment(wlAddFileToResult, 0, SWT.CENTER);
    fdAddFileToResult.right = new FormAttachment(100, 0);
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wUnzippedFiles, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FILE RESULT
    // ///////////////////////////////////////////////////////////

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionUnZip.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionUnZip.SuccessCondition.Label") + " ");
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, 0);
    fdlSuccessCondition.right = new FormAttachment(middle, 0);
    fdlSuccessCondition.top = new FormAttachment(wFileResult, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionUnZip.SuccessWhenAllWorksFine.Label"));
    wSuccessCondition.add(BaseMessages.getString(PKG, "ActionUnZip.SuccessWhenAtLeat.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionUnZip.SuccessWhenNrErrorsLessThan.Label"));
    wSuccessCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, 0);
    fdSuccessCondition.top = new FormAttachment(wFileResult, margin);
    fdSuccessCondition.right = new FormAttachment(100, 0);
    wSuccessCondition.setLayoutData(fdSuccessCondition);
    wSuccessCondition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeSuccessCondition();
          }
        });

    // Success when number of errors less than
    wlNrErrorsLessThan = new Label(wSuccessOn, SWT.RIGHT);
    wlNrErrorsLessThan.setText(
        BaseMessages.getString(PKG, "ActionUnZip.NrBadFormedLessThan.Label") + " ");
    PropsUi.setLook(wlNrErrorsLessThan);
    FormData fdlNrErrorsLessThan = new FormData();
    fdlNrErrorsLessThan.left = new FormAttachment(0, 0);
    fdlNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdlNrErrorsLessThan.right = new FormAttachment(middle, -margin);
    wlNrErrorsLessThan.setLayoutData(fdlNrErrorsLessThan);

    wNrErrorsLessThan =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionUnZip.NrBadFormedLessThan.Tooltip"));
    PropsUi.setLook(wNrErrorsLessThan);
    wNrErrorsLessThan.addModifyListener(lsMod);
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment(middle, 0);
    fdNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdNrErrorsLessThan.right = new FormAttachment(100, -margin);
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(wFileResult, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(500, -margin);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    PropsUi.setLook(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Advanced TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    focusActionName();
    setArgdPrevious();
    afterUnZipActivate();
    setDateTimeFormat();
    activeSuccessCondition();
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void setDateTimeFormat() {
    if (wSpecifyFormat.getSelection()) {
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
    }

    wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection());
    wAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wlAddDate.setEnabled(!wSpecifyFormat.getSelection());
    wAddTime.setEnabled(!wSpecifyFormat.getSelection());
    wlAddTime.setEnabled(!wSpecifyFormat.getSelection());
    setDateTime();
  }

  private void setDateTime() {
    boolean enable =
        wAddDate.getSelection() || wAddTime.getSelection() || wSpecifyFormat.getSelection();
    wlAddOriginalTimestamp.setEnabled(enable);
    wAddOriginalTimestamp.setEnabled(enable);
  }

  public void afterUnZipActivate() {

    action.setChanged();
    if (wAfterUnZip.getSelectionIndex() == 2) {
      wMovetoDirectory.setEnabled(true);
      wlMovetoDirectory.setEnabled(true);
      wbMovetoDirectory.setEnabled(true);
      wCreateMoveToDirectory.setEnabled(true);
      wlCreateMoveToDirectory.setEnabled(true);
    } else {
      wMovetoDirectory.setEnabled(false);
      wlMovetoDirectory.setEnabled(false);
      wbMovetoDirectory.setEnabled(false);
      wCreateMoveToDirectory.setEnabled(false);
      wlCreateMoveToDirectory.setEnabled(false);
    }
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
    wNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
  }

  private void setArgdPrevious() {
    wlZipFilename.setEnabled(!wArgsPrevious.getSelection());
    wZipFilename.setEnabled(!wArgsPrevious.getSelection());
    wbZipFilename.setEnabled(!wArgsPrevious.getSelection());
    wbSourceDirectory.setEnabled(!wArgsPrevious.getSelection());
    wWildcardSource.setEnabled(!wArgsPrevious.getSelection());
    wlWildcardSource.setEnabled(!wArgsPrevious.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wZipFilename.setText(Const.nullToEmpty(action.getZipFilename()));
    wWildcardSource.setText(Const.nullToEmpty(action.getWildcardSource()));

    wWildcard.setText(Const.nullToEmpty(action.getWildcard()));
    wWildcardExclude.setText(Const.nullToEmpty(action.getWildcardExclude()));
    wTargetDirectory.setText(Const.nullToEmpty(action.getSourceDirectory()));
    wMovetoDirectory.setText(Const.nullToEmpty(action.getMoveToDirectory()));

    if (action.afterUnzip >= 0) {
      wAfterUnZip.select(action.afterUnzip);
    } else {
      wAfterUnZip.select(0); // NOTHING
    }

    wAddFileToResult.setSelection(action.isAddFileToResult());
    wArgsPrevious.setSelection(action.isFromPrevious());
    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());

    wDateTimeFormat.setText(Const.nullToEmpty(action.getDateTimeFormat()));
    wSpecifyFormat.setSelection(action.isSpecifyFormat());

    wRootZip.setSelection(action.isRootZip());
    wCreateFolder.setSelection(action.isCreateFolder());

    wNrErrorsLessThan.setText(Const.NVL(action.getNrLimit(), "10"));

    if (action.getSuccessCondition() != null) {
      if (action.getSuccessCondition().equals(action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED)) {
        wSuccessCondition.select(1);
      } else if (action.getSuccessCondition().equals(action.SUCCESS_IF_ERRORS_LESS)) {
        wSuccessCondition.select(2);
      } else {
        wSuccessCondition.select(0);
      }
    } else {
      wSuccessCondition.select(0);
    }

    wAddOriginalTimestamp.setSelection(action.isAddOriginalTimestamp());
    wSetModificationDateToOriginal.setSelection(action.isSetOriginalModificationDate());
    wIfFileExists.select(action.getIfFileExist().getOrignalCode());
    wCreateMoveToDirectory.setSelection(action.isCreateMoveToDirectory());
  }

  @Override
  protected void onActionNameModified() {
    action.setChanged();
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
    action.setName(wName.getText());
    action.setZipFilename(wZipFilename.getText());
    action.setWildcardSource(wWildcardSource.getText());

    action.setWildcard(wWildcard.getText());
    action.setWildcardExclude(wWildcardExclude.getText());
    action.setSourceDirectory(wTargetDirectory.getText());

    action.setMoveToDirectory(wMovetoDirectory.getText());

    action.afterUnzip = wAfterUnZip.getSelectionIndex();

    action.setAddFileToResult(wAddFileToResult.getSelection());

    action.setFromPrevious(wArgsPrevious.getSelection());
    action.setAddDate(wAddDate.getSelection());
    action.setAddTime(wAddTime.getSelection());
    action.setSpecifyFormat(wSpecifyFormat.getSelection());
    action.setDateTimeFormat(wDateTimeFormat.getText());

    action.setRootZip(wRootZip.getSelection());
    action.setCreateFolder(wCreateFolder.getSelection());
    action.setNrLimit(wNrErrorsLessThan.getText());

    if (wSuccessCondition.getSelectionIndex() == 1) {
      action.setSuccessCondition(action.SUCCESS_IF_AT_LEAST_X_FILES_UN_ZIPPED);
    } else if (wSuccessCondition.getSelectionIndex() == 2) {
      action.setSuccessCondition(action.SUCCESS_IF_ERRORS_LESS);
    } else {
      action.setSuccessCondition(action.SUCCESS_IF_NO_ERRORS);
    }

    FileExistsEnum fileExistsEnum =
        FileExistsEnum.getFileExistsEnum(wIfFileExists.getSelectionIndex());
    action.setIfFileExist(fileExistsEnum);
    action.setCreateMoveToDirectory(wCreateMoveToDirectory.getSelection());
    action.setAddOriginalTimestamp(wAddOriginalTimestamp.getSelection());
    action.setSetOriginalModificationDate(wSetModificationDateToOriginal.getSelection());
    dispose();
  }
}
