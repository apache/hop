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

package org.apache.hop.workflow.actions.evalfilesmetrics;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
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
import org.eclipse.swt.widgets.TableItem;

/** This dialog allows you to edit the eval files metrics action settings. */
public class ActionEvalFilesMetricsDialog extends ActionDialog {
  private static final Class<?> PKG = ActionEvalFilesMetrics.class;

  private static final String[] FILETYPES =
      new String[] {BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Filetype.All")};

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder;
  private Button wbSourceDirectory;

  private TextVar wSourceFileFolder;

  private ActionEvalFilesMetrics action;

  private boolean changed;

  private Label wlFields;

  private TableView wFields;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Label wlResultFilenamesWildcard;
  private TextVar wResultFilenamesWildcard;

  private Label wlResultFieldFile;
  private TextVar wResultFieldFile;

  private Label wlResultFieldWildcard;
  private TextVar wResultFieldWildcard;

  private Label wlResultFieldIncludeSubFolders;
  private TextVar wResultFieldIncludeSubFolders;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  private CCombo wSuccessNumberCondition;

  private Label wlScale;
  private CCombo wScale;

  private CCombo wSourceFiles;

  private CCombo wEvaluationType;

  private Label wlCompareValue;
  private TextVar wCompareValue;

  private Label wlMinValue;
  private TextVar wMinValue;

  private Label wlMaxValue;
  private TextVar wMaxValue;

  public ActionEvalFilesMetricsDialog(
      Shell parent,
      ActionEvalFilesMetrics action,
      WorkflowMeta workflowMeta,
      IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Title"), action);
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> action.setChanged();
    changed = action.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, lsMod);
    addAdvancedTab(wTabFolder, lsMod);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    refresh();
    refreshSize();
    refreshSourceFiles();
    wTabFolder.setSelection(0);
    focusActionName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void addAdvancedTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Tab.Advanced.Label"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);
    wAdvancedComp.setLayout(contentLayout);

    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Scale
    wlScale = new Label(wSuccessOn, SWT.RIGHT);
    wlScale.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.Scale.Label"));
    PropsUi.setLook(wlScale);
    FormData fdlScale = new FormData();
    fdlScale.left = new FormAttachment(0, 0);
    fdlScale.right = new FormAttachment(middle, -margin);
    fdlScale.top = new FormAttachment(0, margin);
    wlScale.setLayoutData(fdlScale);

    wScale = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wScale.setItems(ActionEvalFilesMetrics.Scale.getDescriptions());
    wScale.select(0); // +1: starts at -1

    PropsUi.setLook(wScale);
    FormData fdScale = new FormData();
    fdScale.left = new FormAttachment(middle, 0);
    fdScale.top = new FormAttachment(0, margin);
    fdScale.right = new FormAttachment(100, 0);
    wScale.setLayoutData(fdScale);
    wScale.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });

    // Success number Condition
    Label wlSuccessNumberCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessNumberCondition.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessNumberCondition);
    FormData fdlSuccessNumberCondition = new FormData();
    fdlSuccessNumberCondition.left = new FormAttachment(0, 0);
    fdlSuccessNumberCondition.right = new FormAttachment(middle, -margin);
    fdlSuccessNumberCondition.top = new FormAttachment(wScale, margin);
    wlSuccessNumberCondition.setLayoutData(fdlSuccessNumberCondition);

    wSuccessNumberCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessNumberCondition.setItems(ActionEvalFilesMetrics.SuccesConditionType.getDescriptions());
    wSuccessNumberCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessNumberCondition);
    FormData fdSuccessNumberCondition = new FormData();
    fdSuccessNumberCondition.left = new FormAttachment(middle, 0);
    fdSuccessNumberCondition.top = new FormAttachment(wScale, margin);
    fdSuccessNumberCondition.right = new FormAttachment(100, 0);
    wSuccessNumberCondition.setLayoutData(fdSuccessNumberCondition);
    wSuccessNumberCondition.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            refresh();
            action.setChanged();
          }
        });

    // Compare with value
    wlCompareValue = new Label(wSuccessOn, SWT.RIGHT);
    wlCompareValue.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.CompareValue.Label"));
    PropsUi.setLook(wlCompareValue);
    FormData fdlCompareValue = new FormData();
    fdlCompareValue.left = new FormAttachment(0, 0);
    fdlCompareValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdlCompareValue.right = new FormAttachment(middle, -margin);
    wlCompareValue.setLayoutData(fdlCompareValue);

    wCompareValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.CompareValue.Tooltip"));
    PropsUi.setLook(wCompareValue);
    wCompareValue.addModifyListener(lsMod);
    FormData fdCompareValue = new FormData();
    fdCompareValue.left = new FormAttachment(middle, 0);
    fdCompareValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdCompareValue.right = new FormAttachment(100, -margin);
    wCompareValue.setLayoutData(fdCompareValue);

    // Min value
    wlMinValue = new Label(wSuccessOn, SWT.RIGHT);
    wlMinValue.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.MinValue.Label"));
    PropsUi.setLook(wlMinValue);
    FormData fdlMinValue = new FormData();
    fdlMinValue.left = new FormAttachment(0, 0);
    fdlMinValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdlMinValue.right = new FormAttachment(middle, -margin);
    wlMinValue.setLayoutData(fdlMinValue);

    wMinValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.MinValue.Tooltip"));
    PropsUi.setLook(wMinValue);
    wMinValue.addModifyListener(lsMod);
    FormData fdMinValue = new FormData();
    fdMinValue.left = new FormAttachment(middle, 0);
    fdMinValue.top = new FormAttachment(wSuccessNumberCondition, margin);
    fdMinValue.right = new FormAttachment(100, -margin);
    wMinValue.setLayoutData(fdMinValue);

    // Maximum value
    wlMaxValue = new Label(wSuccessOn, SWT.RIGHT);
    wlMaxValue.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.MaxValue.Label"));
    PropsUi.setLook(wlMaxValue);
    FormData fdlMaxValue = new FormData();
    fdlMaxValue.left = new FormAttachment(0, 0);
    fdlMaxValue.top = new FormAttachment(wMinValue, margin);
    fdlMaxValue.right = new FormAttachment(middle, -margin);
    wlMaxValue.setLayoutData(fdlMaxValue);

    wMaxValue =
        new TextVar(
            variables,
            wSuccessOn,
            SWT.SINGLE | SWT.LEFT | SWT.BORDER,
            BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.MaxValue.Tooltip"));
    PropsUi.setLook(wMaxValue);
    wMaxValue.addModifyListener(lsMod);
    FormData fdMaxValue = new FormData();
    fdMaxValue.left = new FormAttachment(middle, 0);
    fdMaxValue.top = new FormAttachment(wMinValue, margin);
    fdMaxValue.right = new FormAttachment(100, -margin);
    wMaxValue.setLayoutData(fdMaxValue);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(0, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////
  }

  private void addGeneralTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Settings.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout(groupLayout);

    // SourceFiles
    Label wlSourceFiles = new Label(wSettings, SWT.RIGHT);
    wlSourceFiles.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.SourceFiles.Label"));
    PropsUi.setLook(wlSourceFiles);
    FormData fdlSourceFiles = new FormData();
    fdlSourceFiles.left = new FormAttachment(0, 0);
    fdlSourceFiles.right = new FormAttachment(middle, -margin);
    fdlSourceFiles.top = new FormAttachment(0, margin);
    wlSourceFiles.setLayoutData(fdlSourceFiles);

    wSourceFiles = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSourceFiles.setItems(ActionEvalFilesMetrics.SourceFilesType.getDescriptions());
    wSourceFiles.select(0); // +1: starts at -1

    PropsUi.setLook(wSourceFiles);
    FormData fdSourceFiles = new FormData();
    fdSourceFiles.left = new FormAttachment(middle, 0);
    fdSourceFiles.top = new FormAttachment(0, margin);
    fdSourceFiles.right = new FormAttachment(100, 0);
    wSourceFiles.setLayoutData(fdSourceFiles);
    wSourceFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
            refreshSourceFiles();
          }
        });

    // ResultFilenamesWildcard
    wlResultFilenamesWildcard = new Label(wSettings, SWT.RIGHT);
    wlResultFilenamesWildcard.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.ResultFilenamesWildcard.Label"));
    PropsUi.setLook(wlResultFilenamesWildcard);
    FormData fdlResultFilenamesWildcard = new FormData();
    fdlResultFilenamesWildcard.left = new FormAttachment(0, 0);
    fdlResultFilenamesWildcard.top = new FormAttachment(wSourceFiles, margin);
    fdlResultFilenamesWildcard.right = new FormAttachment(middle, -margin);
    wlResultFilenamesWildcard.setLayoutData(fdlResultFilenamesWildcard);

    wResultFilenamesWildcard =
        new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResultFilenamesWildcard.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.ResultFilenamesWildcard.Tooltip"));
    PropsUi.setLook(wResultFilenamesWildcard);
    wResultFilenamesWildcard.addModifyListener(lsMod);
    FormData fdResultFilenamesWildcard = new FormData();
    fdResultFilenamesWildcard.left = new FormAttachment(middle, 0);
    fdResultFilenamesWildcard.top = new FormAttachment(wSourceFiles, margin);
    fdResultFilenamesWildcard.right = new FormAttachment(100, -margin);
    wResultFilenamesWildcard.setLayoutData(fdResultFilenamesWildcard);

    // ResultFieldFile
    wlResultFieldFile = new Label(wSettings, SWT.RIGHT);
    wlResultFieldFile.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.ResultFieldFile.Label"));
    PropsUi.setLook(wlResultFieldFile);
    FormData fdlResultFieldFile = new FormData();
    fdlResultFieldFile.left = new FormAttachment(0, 0);
    fdlResultFieldFile.top = new FormAttachment(wResultFilenamesWildcard, margin);
    fdlResultFieldFile.right = new FormAttachment(middle, -margin);
    wlResultFieldFile.setLayoutData(fdlResultFieldFile);

    wResultFieldFile = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResultFieldFile.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.ResultFieldFile.Tooltip"));
    PropsUi.setLook(wResultFieldFile);
    wResultFieldFile.addModifyListener(lsMod);
    FormData fdResultFieldFile = new FormData();
    fdResultFieldFile.left = new FormAttachment(middle, 0);
    fdResultFieldFile.top = new FormAttachment(wResultFilenamesWildcard, margin);
    fdResultFieldFile.right = new FormAttachment(100, -margin);
    wResultFieldFile.setLayoutData(fdResultFieldFile);

    // ResultFieldWildcard
    wlResultFieldWildcard = new Label(wSettings, SWT.RIGHT);
    wlResultFieldWildcard.setText(
        BaseMessages.getString(PKG, "ActionEvalWildcardsMetrics.ResultFieldWildcard.Label"));
    PropsUi.setLook(wlResultFieldWildcard);
    FormData fdlResultFieldWildcard = new FormData();
    fdlResultFieldWildcard.left = new FormAttachment(0, 0);
    fdlResultFieldWildcard.top = new FormAttachment(wResultFieldFile, margin);
    fdlResultFieldWildcard.right = new FormAttachment(middle, -margin);
    wlResultFieldWildcard.setLayoutData(fdlResultFieldWildcard);

    wResultFieldWildcard = new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResultFieldWildcard.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalWildcardsMetrics.ResultFieldWildcard.Tooltip"));
    PropsUi.setLook(wResultFieldWildcard);
    wResultFieldWildcard.addModifyListener(lsMod);
    FormData fdResultFieldWildcard = new FormData();
    fdResultFieldWildcard.left = new FormAttachment(middle, 0);
    fdResultFieldWildcard.top = new FormAttachment(wResultFieldFile, margin);
    fdResultFieldWildcard.right = new FormAttachment(100, -margin);
    wResultFieldWildcard.setLayoutData(fdResultFieldWildcard);

    // ResultFieldIncludeSubFolders
    wlResultFieldIncludeSubFolders = new Label(wSettings, SWT.RIGHT);
    wlResultFieldIncludeSubFolders.setText(
        BaseMessages.getString(
            PKG, "ActionEvalIncludeSubFolderssMetrics.ResultFieldIncludeSubFolders.Label"));
    PropsUi.setLook(wlResultFieldIncludeSubFolders);
    FormData fdlResultFieldIncludeSubFolders = new FormData();
    fdlResultFieldIncludeSubFolders.left = new FormAttachment(0, 0);
    fdlResultFieldIncludeSubFolders.top = new FormAttachment(wResultFieldWildcard, margin);
    fdlResultFieldIncludeSubFolders.right = new FormAttachment(middle, -margin);
    wlResultFieldIncludeSubFolders.setLayoutData(fdlResultFieldIncludeSubFolders);

    wResultFieldIncludeSubFolders =
        new TextVar(variables, wSettings, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResultFieldIncludeSubFolders.setToolTipText(
        BaseMessages.getString(
            PKG, "ActionEvalIncludeSubFolderssMetrics.ResultFieldIncludeSubFolders.Tooltip"));
    PropsUi.setLook(wResultFieldIncludeSubFolders);
    wResultFieldIncludeSubFolders.addModifyListener(lsMod);
    FormData fdResultFieldIncludeSubFolders = new FormData();
    fdResultFieldIncludeSubFolders.left = new FormAttachment(middle, 0);
    fdResultFieldIncludeSubFolders.top = new FormAttachment(wResultFieldWildcard, margin);
    fdResultFieldIncludeSubFolders.right = new FormAttachment(100, -margin);
    wResultFieldIncludeSubFolders.setLayoutData(fdResultFieldIncludeSubFolders);

    // EvaluationType
    Label wlEvaluationType = new Label(wSettings, SWT.RIGHT);
    wlEvaluationType.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetricsDialog.EvaluationType.Label"));
    PropsUi.setLook(wlEvaluationType);
    FormData fdlEvaluationType = new FormData();
    fdlEvaluationType.left = new FormAttachment(0, 0);
    fdlEvaluationType.right = new FormAttachment(middle, -margin);
    fdlEvaluationType.top = new FormAttachment(wResultFieldIncludeSubFolders, margin);
    wlEvaluationType.setLayoutData(fdlEvaluationType);

    wEvaluationType = new CCombo(wSettings, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wEvaluationType.setItems(ActionEvalFilesMetrics.EvaluationType.getDescriptions());
    wEvaluationType.select(0); // +1: starts at -1

    PropsUi.setLook(wEvaluationType);
    FormData fdEvaluationType = new FormData();
    fdEvaluationType.left = new FormAttachment(middle, 0);
    fdEvaluationType.top = new FormAttachment(wResultFieldIncludeSubFolders, margin);
    fdEvaluationType.right = new FormAttachment(100, 0);
    wEvaluationType.setLayoutData(fdEvaluationType);
    wEvaluationType.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            refreshSize();
            action.setChanged();
          }
        });

    FormData fdSettings = new FormData();
    fdSettings.left = new FormAttachment(0, margin);
    fdSettings.top = new FormAttachment(0, margin);
    fdSettings.right = new FormAttachment(100, -margin);
    wSettings.setLayoutData(fdSettings);

    // ///////////////////////////////////////////////////////////
    // / END OF SETTINGS GROUP
    // ///////////////////////////////////////////////////////////

    // SourceFileFolder line
    wlSourceFileFolder = new Label(wGeneralComp, SWT.RIGHT);
    wlSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SourceFileFolder.Label"));
    PropsUi.setLook(wlSourceFileFolder);
    FormData fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment(0, 0);
    fdlSourceFileFolder.top = new FormAttachment(wSettings, margin);
    fdlSourceFileFolder.right = new FormAttachment(middle, -margin);
    wlSourceFileFolder.setLayoutData(fdlSourceFileFolder);

    // Browse Source folders button ...
    wbSourceDirectory = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceDirectory);
    wbSourceDirectory.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.BrowseFolders.Label"));
    FormData fdbSourceDirectory = new FormData();
    fdbSourceDirectory.right = new FormAttachment(100, 0);
    fdbSourceDirectory.top = new FormAttachment(wSettings, margin);
    wbSourceDirectory.setLayoutData(fdbSourceDirectory);

    wbSourceDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wSourceFileFolder, variables));

    // Browse Source files button ...
    wbSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceFileFolder);
    wbSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.BrowseFiles.Label"));
    FormData fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment(wbSourceDirectory, -margin);
    fdbSourceFileFolder.top = new FormAttachment(wSettings, margin);
    wbSourceFileFolder.setLayoutData(fdbSourceFileFolder);

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaSourceFileFolder);
    wbaSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.FilenameAdd.Button"));
    FormData fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment(wbSourceFileFolder, -margin);
    fdbaSourceFileFolder.top = new FormAttachment(wSettings, margin);
    wbaSourceFileFolder.setLayoutData(fdbaSourceFileFolder);

    wSourceFileFolder = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.SourceFileFolder.Tooltip"));

    PropsUi.setLook(wSourceFileFolder);
    wSourceFileFolder.addModifyListener(lsMod);
    FormData fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment(middle, 0);
    fdSourceFileFolder.top = new FormAttachment(wSettings, margin);
    fdSourceFileFolder.right = new FormAttachment(wbaSourceFileFolder, -margin);
    wSourceFileFolder.setLayoutData(fdSourceFileFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener(
        e -> wSourceFileFolder.setToolTipText(variables.resolve(wSourceFileFolder.getText())));

    wbSourceFileFolder.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wSourceFileFolder, variables, new String[] {"*"}, FILETYPES, false));

    // Wildcard
    wlWildcard = new Label(wGeneralComp, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wSourceFileFolder, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);

    wWildcard = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWildcard.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wSourceFileFolder, margin);
    fdWildcard.right = new FormAttachment(wbaSourceFileFolder, -margin);
    wWildcard.setLayoutData(fdWildcard);

    wlFields = new Label(wGeneralComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(middle, -margin);
    fdlFields.top = new FormAttachment(wWildcard, margin);
    wlFields.setLayoutData(fdlFields);

    // Buttons to the right of the screen...
    wbdSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdSourceFileFolder);
    wbdSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.FilenameDelete.Button"));
    wbdSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.FilenameDelete.Tooltip"));
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment(100, 0);
    fdbdSourceFileFolder.top = new FormAttachment(wlFields, margin);
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    wbeSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeSourceFileFolder);
    wbeSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.FilenameEdit.Button"));
    FormData fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment(100, 0);
    fdbeSourceFileFolder.left = new FormAttachment(wbdSourceFileFolder, 0, SWT.LEFT);
    fdbeSourceFileFolder.top = new FormAttachment(wbdSourceFileFolder, margin);
    wbeSourceFileFolder.setLayoutData(fdbeSourceFileFolder);

    ColumnInfo[] columnInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.SourceFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.IncludeSubDirs.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ActionEvalFilesMetrics.NO_YES)
        };

    columnInfos[0].setUsingVariables(true);
    columnInfos[0].setToolTip(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.SourceFileFolder.Tooltip"));
    columnInfos[1].setUsingVariables(true);
    columnInfos[1].setToolTip(
        BaseMessages.getString(PKG, "ActionEvalFilesMetrics.Fields.Wildcard.Tooltip"));

    wFields =
        new TableView(
            variables,
            wGeneralComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columnInfos,
            1,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wbeSourceFileFolder, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wFields.add(new String[] {wSourceFileFolder.getText(), wWildcard.getText()});
            wSourceFileFolder.setText("");

            wWildcard.setText("");
            wFields.removeEmptyRows();
            wFields.setRowNums();
            wFields.optWidth(true);
          }
        };
    wbaSourceFileFolder.addSelectionListener(selA);
    wSourceFileFolder.addSelectionListener(selA);

    // Delete files from the list of files...
    wbdSourceFileFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFields.getSelectionIndices();
            wFields.remove(idx);
            wFields.removeEmptyRows();
            wFields.setRowNums();
          }
        });

    // Edit the selected file & remove from the list...
    wbeSourceFileFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int idx = wFields.getSelectionIndex();
            if (idx >= 0) {
              String[] string = wFields.getItem(idx);
              wSourceFileFolder.setText(string[0]);
              wWildcard.setText(string[1]);
              wFields.remove(idx);
            }
            wFields.removeEmptyRows();
            wFields.setRowNums();
          }
        });

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);
  }

  private void refreshSourceFiles() {
    boolean useStaticFiles =
        (ActionEvalFilesMetrics.SourceFilesType.lookupDescriptions(wSourceFiles.getText())
            == ActionEvalFilesMetrics.SourceFilesType.FILES);
    wlFields.setEnabled(useStaticFiles);
    wFields.setEnabled(useStaticFiles);
    wbdSourceFileFolder.setEnabled(useStaticFiles);
    wbeSourceFileFolder.setEnabled(useStaticFiles);
    wbSourceFileFolder.setEnabled(useStaticFiles);
    wbaSourceFileFolder.setEnabled(useStaticFiles);
    wlSourceFileFolder.setEnabled(useStaticFiles);
    wSourceFileFolder.setEnabled(useStaticFiles);

    wlWildcard.setEnabled(useStaticFiles);
    wWildcard.setEnabled(useStaticFiles);
    wbSourceDirectory.setEnabled(useStaticFiles);

    boolean setResultWildcard =
        (ActionEvalFilesMetrics.SourceFilesType.lookupDescriptions(wSourceFiles.getText())
            == ActionEvalFilesMetrics.SourceFilesType.FILENAMES_RESULT);
    wlResultFilenamesWildcard.setEnabled(setResultWildcard);
    wResultFilenamesWildcard.setEnabled(setResultWildcard);

    boolean setResultFields =
        (ActionEvalFilesMetrics.SourceFilesType.lookupDescriptions(wSourceFiles.getText())
            == ActionEvalFilesMetrics.SourceFilesType.PREVIOUS_RESULT);
    wlResultFieldIncludeSubFolders.setEnabled(setResultFields);
    wResultFieldIncludeSubFolders.setEnabled(setResultFields);
    wlResultFieldFile.setEnabled(setResultFields);
    wResultFieldFile.setEnabled(setResultFields);
    wlResultFieldWildcard.setEnabled(setResultFields);
    wResultFieldWildcard.setEnabled(setResultFields);
  }

  private void refreshSize() {
    boolean useSize =
        (ActionEvalFilesMetrics.EvaluationType.lookupDescription(wEvaluationType.getText())
            == ActionEvalFilesMetrics.EvaluationType.SIZE);
    wlScale.setVisible(useSize);
    wScale.setVisible(useSize);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.NVL(action.getName(), ""));

    for (ActionEvalFilesMetrics.SourceFile sourceFile : action.getSourceFiles()) {
      TableItem ti = new TableItem(wFields.table, SWT.NONE);

      ti.setText(1, Const.NVL(sourceFile.getSourceFileFolder(), ""));
      ti.setText(2, Const.NVL(sourceFile.getSourceWildcard(), ""));
      ti.setText(3, Const.NVL(sourceFile.getSourceIncludeSubfolders(), ""));
    }
    wFields.optimizeTableView();

    wResultFilenamesWildcard.setText(Const.NVL(action.getResultFilenamesWildcard(), ""));
    wResultFieldFile.setText(Const.NVL(action.getResultFieldFile(), ""));
    wResultFieldWildcard.setText(Const.NVL(action.getResultFieldWildcard(), ""));
    wResultFieldIncludeSubFolders.setText(Const.NVL(action.getResultFieldIncludeSubFolders(), ""));

    wSourceFiles.setText(action.getSourceFilesType().getDescription());
    wEvaluationType.setText(action.getEvaluationType().getDescription());
    wScale.setText(action.getScale().getDescription());
    wSuccessNumberCondition.setText(action.getSuccessConditionType().getDescription());
    wCompareValue.setText(Const.NVL(action.getCompareValue(), ""));
    wMinValue.setText(Const.NVL(action.getMinValue(), ""));
    wMaxValue.setText(Const.NVL(action.getMaxValue(), ""));
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
    action.setResultFilenamesWildcard(wResultFilenamesWildcard.getText());
    action.setResultFieldFile(wResultFieldFile.getText());
    action.setResultFieldWildcard(wResultFieldWildcard.getText());
    action.setResultFieldIncludeSubFolders(wResultFieldIncludeSubFolders.getText());
    action.setSourceFilesType(
        ActionEvalFilesMetrics.SourceFilesType.lookupDescriptions(wSourceFiles.getText()));
    action.setEvaluationType(
        ActionEvalFilesMetrics.EvaluationType.lookupDescription(wEvaluationType.getText()));
    action.setScale(ActionEvalFilesMetrics.Scale.lookupDescription(wScale.getText()));
    action.setSuccessConditionType(
        ActionEvalFilesMetrics.SuccesConditionType.lookupDescription(
            wSuccessNumberCondition.getText()));
    action.setCompareValue(wCompareValue.getText());
    action.setMinValue(wMinValue.getText());
    action.setMaxValue(wMaxValue.getText());

    action.getSourceFiles().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      ActionEvalFilesMetrics.SourceFile sourceFile = new ActionEvalFilesMetrics.SourceFile();
      action.getSourceFiles().add(sourceFile);

      sourceFile.setSourceFileFolder(item.getText(1));
      sourceFile.setSourceWildcard(item.getText(2));
      sourceFile.setSourceIncludeSubfolders(item.getText(3));
    }
    dispose();
  }

  private void refresh() {
    boolean compareValue =
        (ActionEvalFilesMetrics.SuccesConditionType.lookupDescription(
                wSuccessNumberCondition.getText())
            != ActionEvalFilesMetrics.SuccesConditionType.BETWEEN);
    wlCompareValue.setVisible(compareValue);
    wCompareValue.setVisible(compareValue);
    wlMinValue.setVisible(!compareValue);
    wMinValue.setVisible(!compareValue);
    wlMaxValue.setVisible(!compareValue);
    wMaxValue.setVisible(!compareValue);
  }
}
