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

package org.apache.hop.workflow.actions.zipfile;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ComboVar;
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

/** This dialog allows you to edit the Zip File action settings. */
public class ActionZipFileDialog extends ActionDialog {
  private static final Class<?> PKG = ActionZipFile.class;

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionZipFile.Filetype.Zip"),
        BaseMessages.getString(PKG, "ActionZipFile.Filetype.All")
      };

  private Button wCreateParentFolder;

  private Label wlZipFilename;

  private Button wbZipFilename;

  private TextVar wZipFilename;

  private ActionZipFile action;

  private Label wlSourceDirectory;

  private TextVar wSourceDirectory;

  private Label wlMovetoDirectory;

  private TextVar wMovetoDirectory;

  private Label wlWildcard;

  private TextVar wWildcard;

  private Label wlWildcardExclude;

  private TextVar wWildcardExclude;

  private Button wIncludeSubfolders;

  private CCombo wCompressionRate;

  private CCombo wIfFileExists;

  private CCombo wAfterZip;

  private Button wAddFileToResult;

  private Button wbSourceDirectory;
  private Button wbSourceFile;

  private Button wbMovetoDirectory;

  private Button wGetFromPrevious;

  private Label wlAddDate;

  private Button wAddDate;

  private Label wlAddTime;

  private Button wAddTime;

  private Button wbShowFiles;

  private Button wSpecifyFormat;

  private Label wlDateTimeFormat;

  private CCombo wDateTimeFormat;

  private Label wlCreateMoveToDirectory;

  private Button wCreateMoveToDirectory;

  private ComboVar wStoredSourcePathDepth;

  private boolean changed;

  public ActionZipFileDialog(
      Shell parent, ActionZipFile action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionZipFile.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionZipFile.Title"), action);
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
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionZipFile.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // SourceFile grouping?
    // ////////////////////////
    // START OF SourceFile GROUP///
    // /
    Group wSourceFiles = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSourceFiles);
    wSourceFiles.setText(BaseMessages.getString(PKG, "ActionZipFile.SourceFiles.Group.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wSourceFiles.setLayout(groupLayout);

    // Get Result from previous?
    // Result from previous?
    Label wlGetFromPrevious = new Label(wSourceFiles, SWT.RIGHT);
    wlGetFromPrevious.setText(BaseMessages.getString(PKG, "ActionZipFile.getFromPrevious.Label"));
    PropsUi.setLook(wlGetFromPrevious);
    FormData fdlGetFromPrevious = new FormData();
    fdlGetFromPrevious.left = new FormAttachment(0, 0);
    fdlGetFromPrevious.top = new FormAttachment(0, margin);
    fdlGetFromPrevious.right = new FormAttachment(middle, -margin);
    wlGetFromPrevious.setLayoutData(fdlGetFromPrevious);
    wGetFromPrevious = new Button(wSourceFiles, SWT.CHECK);
    PropsUi.setLook(wGetFromPrevious);
    wGetFromPrevious.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.getFromPrevious.Tooltip"));
    FormData fdGetFromPrevious = new FormData();
    fdGetFromPrevious.left = new FormAttachment(middle, 0);
    fdGetFromPrevious.top = new FormAttachment(wlGetFromPrevious, 0, SWT.CENTER);
    fdGetFromPrevious.right = new FormAttachment(100, 0);
    wGetFromPrevious.setLayoutData(fdGetFromPrevious);
    wGetFromPrevious.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            setGetFromPrevious();
          }
        });

    // TargetDirectory line
    wlSourceDirectory = new Label(wSourceFiles, SWT.RIGHT);
    wlSourceDirectory.setText(BaseMessages.getString(PKG, "ActionZipFile.SourceDir.Label"));
    PropsUi.setLook(wlSourceDirectory);
    FormData fdlSourceDirectory = new FormData();
    fdlSourceDirectory.left = new FormAttachment(0, 0);
    fdlSourceDirectory.top = new FormAttachment(wlGetFromPrevious, margin);
    fdlSourceDirectory.right = new FormAttachment(middle, -margin);
    wlSourceDirectory.setLayoutData(fdlSourceDirectory);

    // Browse folders button ...
    wbSourceDirectory = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceDirectory);
    wbSourceDirectory.setText(BaseMessages.getString(PKG, "ActionZipFile.BrowseFolders.Label"));
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment(100, 0);
    fdbSourceDirectory.top = new FormAttachment(wlGetFromPrevious, margin);
    wbSourceDirectory.setLayoutData(fdbSourceDirectory);
    wbSourceDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wSourceDirectory, variables));

    // Browse Destination file browse button ...
    wbSourceFile = new Button(wSourceFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceFile);
    wbSourceFile.setText(BaseMessages.getString(PKG, "ActionZipFile.BrowseFiles.Label"));
    FormData fdbSourceFile = new FormData();
    fdbSourceFile.right = new FormAttachment(wbSourceDirectory, -margin);
    fdbSourceFile.top = new FormAttachment(wlGetFromPrevious, margin);
    wbSourceFile.setLayoutData(fdbSourceFile);
    wbSourceFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wSourceDirectory, variables, new String[] {"*"}, FILETYPES, false));

    wSourceDirectory =
        new TextVar(
            variables,
            wSourceFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionZipFile.SourceDir.Tooltip"));
    PropsUi.setLook(wSourceDirectory);
    wSourceDirectory.addModifyListener(lsMod);
    FormData fdSourceDirectory = new FormData();
    fdSourceDirectory.left = new FormAttachment(middle, 0);
    fdSourceDirectory.top = new FormAttachment(wlGetFromPrevious, margin);
    fdSourceDirectory.right = new FormAttachment(wbSourceFile, -margin);
    wSourceDirectory.setLayoutData(fdSourceDirectory);

    // Wildcard line
    wlWildcard = new Label(wSourceFiles, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionZipFile.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wSourceDirectory, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);
    wWildcard =
        new TextVar(
            variables,
            wSourceFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionZipFile.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wSourceDirectory, margin);
    fdWildcard.right = new FormAttachment(100, 0);
    wWildcard.setLayoutData(fdWildcard);

    // Wildcard to exclude
    wlWildcardExclude = new Label(wSourceFiles, SWT.RIGHT);
    wlWildcardExclude.setText(BaseMessages.getString(PKG, "ActionZipFile.WildcardExclude.Label"));
    PropsUi.setLook(wlWildcardExclude);
    FormData fdlWildcardExclude = new FormData();
    fdlWildcardExclude.left = new FormAttachment(0, 0);
    fdlWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdlWildcardExclude.right = new FormAttachment(middle, -margin);
    wlWildcardExclude.setLayoutData(fdlWildcardExclude);
    wWildcardExclude =
        new TextVar(
            variables,
            wSourceFiles,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionZipFile.WildcardExclude.Tooltip"));
    PropsUi.setLook(wWildcardExclude);
    wWildcardExclude.addModifyListener(lsMod);
    FormData fdWildcardExclude = new FormData();
    fdWildcardExclude.left = new FormAttachment(middle, 0);
    fdWildcardExclude.top = new FormAttachment(wWildcard, margin);
    fdWildcardExclude.right = new FormAttachment(100, 0);
    wWildcardExclude.setLayoutData(fdWildcardExclude);

    // Include sub-folders?
    //
    Label wlIncludeSubfolders = new Label(wSourceFiles, SWT.RIGHT);
    wlIncludeSubfolders.setText(
        BaseMessages.getString(PKG, "ActionZipFile.IncludeSubfolders.Label"));
    PropsUi.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment(0, 0);
    fdlIncludeSubfolders.top = new FormAttachment(wWildcardExclude, margin);
    fdlIncludeSubfolders.right = new FormAttachment(middle, -margin);
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSourceFiles, SWT.CHECK);
    PropsUi.setLook(wIncludeSubfolders);
    wIncludeSubfolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.IncludeSubfolders.Tooltip"));
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

    FormData fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment(0, margin);
    fdSourceFiles.top = new FormAttachment(0, margin);
    fdSourceFiles.right = new FormAttachment(100, -margin);
    wSourceFiles.setLayoutData(fdSourceFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF SourceFile GROUP
    // ///////////////////////////////////////////////////////////

    // ZipFile grouping?
    // ////////////////////////
    // START OF ZipFile GROUP///
    // /
    Group wZipFile = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wZipFile);
    wZipFile.setText(BaseMessages.getString(PKG, "ActionZipFile.ZipFile.Group.Label"));

    FormLayout groupLayoutzipfile = new FormLayout();
    groupLayoutzipfile.marginWidth = 10;
    groupLayoutzipfile.marginHeight = 10;

    wZipFile.setLayout(groupLayoutzipfile);

    // ZipFilename line
    wlZipFilename = new Label(wZipFile, SWT.RIGHT);
    wlZipFilename.setText(BaseMessages.getString(PKG, "ActionZipFile.ZipFilename.Label"));
    PropsUi.setLook(wlZipFilename);
    FormData fdlZipFilename = new FormData();
    fdlZipFilename.left = new FormAttachment(0, 0);
    fdlZipFilename.top = new FormAttachment(wSourceFiles, margin);
    fdlZipFilename.right = new FormAttachment(middle, -margin);
    wlZipFilename.setLayoutData(fdlZipFilename);
    wbZipFilename = new Button(wZipFile, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbZipFilename);
    wbZipFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbZipFilename = new FormData();
    fdbZipFilename.right = new FormAttachment(100, 0);
    fdbZipFilename.top = new FormAttachment(wSourceFiles, 0);
    wbZipFilename.setLayoutData(fdbZipFilename);
    wZipFilename = new TextVar(variables, wZipFile, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wZipFilename);
    wZipFilename.addModifyListener(lsMod);
    FormData fdZipFilename = new FormData();
    fdZipFilename.left = new FormAttachment(middle, 0);
    fdZipFilename.top = new FormAttachment(wSourceFiles, margin);
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
                new String[] {"*.zip;*.ZIP", "*"},
                FILETYPES,
                true));

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(wZipFile, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "ActionZipFile.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(wZipFilename, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wZipFile, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.CreateParentFolder.Tooltip"));
    PropsUi.setLook(wCreateParentFolder);
    FormData fdCreateParentFolder = new FormData();
    fdCreateParentFolder.left = new FormAttachment(middle, 0);
    fdCreateParentFolder.top = new FormAttachment(wlCreateParentFolder, 0, SWT.CENTER);
    fdCreateParentFolder.right = new FormAttachment(100, 0);
    wCreateParentFolder.setLayoutData(fdCreateParentFolder);
    wCreateParentFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Create multi-part file?
    wlAddDate = new Label(wZipFile, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "ActionZipFile.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wlCreateParentFolder, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wZipFile, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    wAddDate.setToolTipText(BaseMessages.getString(PKG, "ActionZipFile.AddDate.Tooltip"));
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
          }
        });

    // Create multi-part file?
    wlAddTime = new Label(wZipFile, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "ActionZipFile.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wlAddDate, margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wZipFile, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    wAddTime.setToolTipText(BaseMessages.getString(PKG, "ActionZipFile.AddTime.Tooltip"));
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
          }
        });

    // Specify date time format?
    Label wlSpecifyFormat = new Label(wZipFile, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "ActionZipFile.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wlAddTime, margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wZipFile, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.SpecifyFormat.Tooltip"));
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
    wlDateTimeFormat = new Label(wZipFile, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "ActionZipFile.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wlSpecifyFormat, margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wZipFile, SWT.BORDER | SWT.READ_ONLY);
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

    wbShowFiles = new Button(wZipFile, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "ActionZipFile.ShowFile.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.top = new FormAttachment(wDateTimeFormat, margin);
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (!Utils.isEmpty(wZipFilename.getText())) {
              ActionZipFile actionZipFile = new ActionZipFile();
              String[] filename = new String[1];
              filename[0] =
                  ActionZipFileDialog.this.action.getFullFilename(
                      wZipFilename.getText(),
                      wAddDate.getSelection(),
                      wAddTime.getSelection(),
                      wSpecifyFormat.getSelection(),
                      wDateTimeFormat.getText());
              if (filename != null && filename.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        filename,
                        BaseMessages.getString(PKG, "ActionZipFile.SelectOutputFiles.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "ActionZipFile.SelectOutputFiles.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "ActionZipFile.NoFilesFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            }
          }
        });

    FormData fdZipFile = new FormData();
    fdZipFile.left = new FormAttachment(0, margin);
    fdZipFile.top = new FormAttachment(wSourceFiles, margin);
    fdZipFile.right = new FormAttachment(100, -margin);
    wZipFile.setLayoutData(fdZipFile);

    // ///////////////////////////////////////////////////////////
    // END OF ZipFile GROUP
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
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionZipFile.Tab.Advanced.Label"));

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout(advancedLayout);

    // ////////////////////////////
    // START OF Settings GROUP
    //
    Group wSettings = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionZipFile.Advanced.Group.Label"));
    FormLayout groupLayoutSettings = new FormLayout();
    groupLayoutSettings.marginWidth = 10;
    groupLayoutSettings.marginHeight = 10;
    wSettings.setLayout(groupLayoutSettings);

    // Compression Rate
    Label wlCompressionRate = new Label(wSettings, SWT.RIGHT);
    wlCompressionRate.setText(BaseMessages.getString(PKG, "ActionZipFile.CompressionRate.Label"));
    PropsUi.setLook(wlCompressionRate);
    FormData fdlCompressionRate = new FormData();
    fdlCompressionRate.left = new FormAttachment(0, -margin);
    fdlCompressionRate.right = new FormAttachment(middle, -margin);
    fdlCompressionRate.top = new FormAttachment(wZipFile, margin);
    wlCompressionRate.setLayoutData(fdlCompressionRate);
    wCompressionRate = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wCompressionRate.add(
        BaseMessages.getString(PKG, "ActionZipFile.NO_COMP_CompressionRate.Label"));
    wCompressionRate.add(
        BaseMessages.getString(PKG, "ActionZipFile.DEF_COMP_CompressionRate.Label"));
    wCompressionRate.add(
        BaseMessages.getString(PKG, "ActionZipFile.BEST_COMP_CompressionRate.Label"));
    wCompressionRate.add(
        BaseMessages.getString(PKG, "ActionZipFile.BEST_SPEED_CompressionRate.Label"));
    wCompressionRate.select(1); // +1: starts at -1

    PropsUi.setLook(wCompressionRate);
    FormData fdCompressionRate = new FormData();
    fdCompressionRate.left = new FormAttachment(middle, 0);
    fdCompressionRate.top = new FormAttachment(wZipFile, margin);
    fdCompressionRate.right = new FormAttachment(100, 0);
    wCompressionRate.setLayoutData(fdCompressionRate);

    // If File Exists
    Label wlIfFileExists = new Label(wSettings, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionZipFile.IfZipFileExists.Label"));
    PropsUi.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, -margin);
    fdlIfFileExists.right = new FormAttachment(middle, -margin);
    fdlIfFileExists.top = new FormAttachment(wCompressionRate, margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.add(
        BaseMessages.getString(PKG, "ActionZipFile.Create_NewFile_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionZipFile.Append_File_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionZipFile.Do_Nothing_IfFileExists.Label"));

    wIfFileExists.add(BaseMessages.getString(PKG, "ActionZipFile.Fail_IfFileExists.Label"));
    wIfFileExists.select(3); // +1: starts at -1

    PropsUi.setLook(wIfFileExists);
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wCompressionRate, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    // After Zipping
    Label wlAfterZip = new Label(wSettings, SWT.RIGHT);
    wlAfterZip.setText(BaseMessages.getString(PKG, "ActionZipFile.AfterZip.Label"));
    PropsUi.setLook(wlAfterZip);
    FormData fdlAfterZip = new FormData();
    fdlAfterZip.left = new FormAttachment(0, -margin);
    fdlAfterZip.right = new FormAttachment(middle, -margin);
    fdlAfterZip.top = new FormAttachment(wIfFileExists, margin);
    wlAfterZip.setLayoutData(fdlAfterZip);
    wAfterZip = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wAfterZip.add(BaseMessages.getString(PKG, "ActionZipFile.Do_Nothing_AfterZip.Label"));
    wAfterZip.add(BaseMessages.getString(PKG, "ActionZipFile.Delete_Files_AfterZip.Label"));
    wAfterZip.add(BaseMessages.getString(PKG, "ActionZipFile.Move_Files_AfterZip.Label"));

    wAfterZip.select(0); // +1: starts at -1

    PropsUi.setLook(wAfterZip);
    FormData fdAfterZip = new FormData();
    fdAfterZip.left = new FormAttachment(middle, 0);
    fdAfterZip.top = new FormAttachment(wIfFileExists, margin);
    fdAfterZip.right = new FormAttachment(100, 0);
    wAfterZip.setLayoutData(fdAfterZip);

    wAfterZip.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            afterZipActivate();
          }
        });

    // moveTo Directory
    wlMovetoDirectory = new Label(wSettings, SWT.RIGHT);
    wlMovetoDirectory.setText(BaseMessages.getString(PKG, "ActionZipFile.MovetoDirectory.Label"));
    PropsUi.setLook(wlMovetoDirectory);
    FormData fdlMovetoDirectory = new FormData();
    fdlMovetoDirectory.left = new FormAttachment(0, 0);
    fdlMovetoDirectory.top = new FormAttachment(wAfterZip, margin);
    fdlMovetoDirectory.right = new FormAttachment(middle, -margin);
    wlMovetoDirectory.setLayoutData(fdlMovetoDirectory);

    // Browse folders button ...
    wbMovetoDirectory = new Button(wSettings, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbMovetoDirectory);
    wbMovetoDirectory.setText(BaseMessages.getString(PKG, "ActionZipFile.BrowseFolders.Label"));
    FormData fdbMovetoDirectory = new FormData();
    fdbMovetoDirectory.right = new FormAttachment(100, 0);
    fdbMovetoDirectory.top = new FormAttachment(wAfterZip, margin);
    wbMovetoDirectory.setLayoutData(fdbMovetoDirectory);
    wbMovetoDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wMovetoDirectory, variables));

    wMovetoDirectory =
        new TextVar(
            variables,
            wSettings,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionZipFile.MovetoDirectory.Tooltip"));
    PropsUi.setLook(wMovetoDirectory);
    wMovetoDirectory.addModifyListener(lsMod);
    FormData fdMovetoDirectory = new FormData();
    fdMovetoDirectory.left = new FormAttachment(middle, 0);
    fdMovetoDirectory.top = new FormAttachment(wAfterZip, margin);
    fdMovetoDirectory.right = new FormAttachment(wbMovetoDirectory, -margin);
    wMovetoDirectory.setLayoutData(fdMovetoDirectory);

    // create moveto folder
    wlCreateMoveToDirectory = new Label(wSettings, SWT.RIGHT);
    wlCreateMoveToDirectory.setText(
        BaseMessages.getString(PKG, "ActionZipFile.createMoveToDirectory.Label"));
    PropsUi.setLook(wlCreateMoveToDirectory);
    FormData fdlCreateMoveToDirectory = new FormData();
    fdlCreateMoveToDirectory.left = new FormAttachment(0, 0);
    fdlCreateMoveToDirectory.top = new FormAttachment(wMovetoDirectory, margin);
    fdlCreateMoveToDirectory.right = new FormAttachment(middle, -margin);
    wlCreateMoveToDirectory.setLayoutData(fdlCreateMoveToDirectory);
    wCreateMoveToDirectory = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wCreateMoveToDirectory);
    wCreateMoveToDirectory.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.createMoveToDirectory.Tooltip"));
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

    Label wlStoredSourcePathDepth = new Label(wSettings, SWT.RIGHT);
    wlStoredSourcePathDepth.setText(
        BaseMessages.getString(PKG, "ActionZipFile.StoredSourcePathDepth.Label"));
    PropsUi.setLook(wlStoredSourcePathDepth);
    FormData fdlStoredSourcePathDepth = new FormData();
    fdlStoredSourcePathDepth.left = new FormAttachment(0, 0);
    fdlStoredSourcePathDepth.top = new FormAttachment(wlCreateMoveToDirectory, margin);
    fdlStoredSourcePathDepth.right = new FormAttachment(middle, -margin);
    wlStoredSourcePathDepth.setLayoutData(fdlStoredSourcePathDepth);
    wStoredSourcePathDepth = new ComboVar(variables, wSettings, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wStoredSourcePathDepth);
    wStoredSourcePathDepth.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.StoredSourcePathDepth.Tooltip"));
    FormData fdStoredSourcePathDepth = new FormData();
    fdStoredSourcePathDepth.left = new FormAttachment(middle, 0);
    fdStoredSourcePathDepth.top = new FormAttachment(wlCreateMoveToDirectory, margin);
    fdStoredSourcePathDepth.right = new FormAttachment(100, 0);
    wStoredSourcePathDepth.setLayoutData(fdStoredSourcePathDepth);
    wStoredSourcePathDepth.setItems(
        new String[] {
          "0 : /project-hop/work/transfer/input/project/file.txt",
          "1 : file.txt",
          "2 : project/file.txt",
          "3 : input/project/file.txt",
          "4 : transfer/input/project/file.txt",
          "5 : work/transfer/input/project/file.txt",
          "6 : project-hop/work/transfer/input/project/file.txt",
          "7 : project-hop/work/transfer/input/project/file.txt",
          "8 : project-hop/work/transfer/input/project/file.txt",
        });

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(wZipFile, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);
    // ///////////////////////////////////////////////////////////
    // / END OF Settings GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionZipFile.FileResult.Group.Label"));

    FormLayout groupLayoutresult = new FormLayout();
    groupLayoutresult.marginWidth = 10;
    groupLayoutresult.marginHeight = 10;

    wFileResult.setLayout(groupLayoutresult);

    // Add file to result
    // Add File to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(BaseMessages.getString(PKG, "ActionZipFile.AddFileToResult.Label"));
    PropsUi.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.top = new FormAttachment(wSettings, margin);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    PropsUi.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionZipFile.AddFileToResult.Tooltip"));
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
    fdFileResult.top = new FormAttachment(wSettings, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FILE RESULT GROUP
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
    setGetFromPrevious();
    afterZipActivate();
    setDateTimeFormat();

    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  public void setGetFromPrevious() {
    wlSourceDirectory.setEnabled(!wGetFromPrevious.getSelection());
    wSourceDirectory.setEnabled(!wGetFromPrevious.getSelection());
    wWildcard.setEnabled(!wGetFromPrevious.getSelection());
    wlWildcard.setEnabled(!wGetFromPrevious.getSelection());
    wWildcardExclude.setEnabled(!wGetFromPrevious.getSelection());
    wlWildcardExclude.setEnabled(!wGetFromPrevious.getSelection());
    wbSourceDirectory.setEnabled(!wGetFromPrevious.getSelection());
    wlZipFilename.setEnabled(!wGetFromPrevious.getSelection());
    wZipFilename.setEnabled(!wGetFromPrevious.getSelection());
    wbZipFilename.setEnabled(!wGetFromPrevious.getSelection());
    wbSourceFile.setEnabled(!wGetFromPrevious.getSelection());
    wlAddDate.setEnabled(!wGetFromPrevious.getSelection());
    wAddDate.setEnabled(!wGetFromPrevious.getSelection());
    wlAddTime.setEnabled(!wGetFromPrevious.getSelection());
    wAddTime.setEnabled(!wGetFromPrevious.getSelection());
    wbShowFiles.setEnabled(!wGetFromPrevious.getSelection());
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
  }

  public void afterZipActivate() {

    action.setChanged();
    if (wAfterZip.getSelectionIndex() == 2) {
      wMovetoDirectory.setEnabled(true);
      wlMovetoDirectory.setEnabled(true);
      wbMovetoDirectory.setEnabled(true);
      wlCreateMoveToDirectory.setEnabled(true);
      wCreateMoveToDirectory.setEnabled(true);
    } else {
      wMovetoDirectory.setEnabled(false);
      wlMovetoDirectory.setEnabled(false);
      wbMovetoDirectory.setEnabled(false);
      wlCreateMoveToDirectory.setEnabled(false);
      wCreateMoveToDirectory.setEnabled(true);
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wZipFilename.setText(Const.nullToEmpty(action.getZipFilename()));

    if (action.getCompressionRate() >= 0) {
      wCompressionRate.select(action.getCompressionRate());
    } else {
      wCompressionRate.select(1); // DEFAULT
    }

    if (action.getIfZipFileExists() >= 0) {
      wIfFileExists.select(action.getIfZipFileExists());
    } else {
      wIfFileExists.select(2); // NOTHING
    }

    wWildcard.setText(Const.NVL(action.getWildCard(), ""));
    wWildcardExclude.setText(Const.NVL(action.getExcludeWildCard(), ""));
    wSourceDirectory.setText(Const.NVL(action.getSourceDirectory(), ""));
    wMovetoDirectory.setText(Const.NVL(action.getMoveToDirectory(), ""));
    if (action.getAfterZip() >= 0) {
      wAfterZip.select(action.getAfterZip());
    } else {
      wAfterZip.select(0); // NOTHING
    }

    wAddFileToResult.setSelection(action.isAddFileToResult());
    wGetFromPrevious.setSelection(action.isFromPrevious());
    wCreateParentFolder.setSelection(action.isCreateParentFolder());
    wAddDate.setSelection(action.isAddDate());
    wAddTime.setSelection(action.isAddTime());

    wDateTimeFormat.setText(Const.NVL(action.getDateTimeFormat(), ""));
    wSpecifyFormat.setSelection(action.isSpecifyFormat());
    wCreateMoveToDirectory.setSelection(action.isCreateMoveToDirectory());
    wIncludeSubfolders.setSelection(action.isIncludingSubFolders());

    wStoredSourcePathDepth.setText(Const.NVL(action.getStoredSourcePathDepth(), ""));
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

    action.setCompressionRate(wCompressionRate.getSelectionIndex());
    action.setIfZipFileExists(wIfFileExists.getSelectionIndex());

    action.setWildCard(wWildcard.getText());
    action.setExcludeWildCard(wWildcardExclude.getText());
    action.setSourceDirectory(wSourceDirectory.getText());

    action.setMoveToDirectory(wMovetoDirectory.getText());

    action.setAfterZip(wAfterZip.getSelectionIndex());

    action.setAddFileToResult(wAddFileToResult.getSelection());
    action.setFromPrevious(wGetFromPrevious.getSelection());
    action.setCreateParentFolder(wCreateParentFolder.getSelection());
    action.setAddDate(wAddDate.getSelection());
    action.setAddTime(wAddTime.getSelection());
    action.setSpecifyFormat(wSpecifyFormat.getSelection());
    action.setDateTimeFormat(wDateTimeFormat.getText());
    action.setCreateMoveToDirectory(wCreateMoveToDirectory.getSelection());
    action.setIncludingSubFolders(wIncludeSubfolders.getSelection());

    action.setStoredSourcePathDepth(wStoredSourcePathDepth.getText());

    dispose();
  }
}
