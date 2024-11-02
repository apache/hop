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

package org.apache.hop.workflow.actions.xml.xmlwellformed;

import java.util.ArrayList;
import java.util.List;
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
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.workflow.action.ActionDialog;
import org.apache.hop.ui.workflow.dialog.WorkflowDialog;
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
import org.eclipse.swt.widgets.Text;

/** This dialog allows you to edit the XML valid job entry settings. */
public class XmlWellFormedDialog extends ActionDialog {
  private static final Class<?> PKG = XmlWellFormedDialog.class;

  private static final String[] FILETYPES =
      new String[] {
        BaseMessages.getString(PKG, "ActionXMLWellFormed.Filetype.Xml"),
        BaseMessages.getString(PKG, "ActionXMLWellFormed.Filetype.All")
      };

  private Text wName;

  private Label wlSourceFileFolder;
  private Button wbSourceFileFolder;
  private Button wbSourceDirectory;

  private TextVar wSourceFileFolder;

  private Button wIncludeSubfolders;

  private XmlWellFormed action;

  private boolean changed;

  private Button wPrevious;

  private Label wlFields;

  private TableView wFields;

  private Label wlWildcard;
  private TextVar wWildcard;

  private Button wbdSourceFileFolder; // Delete
  private Button wbeSourceFileFolder; // Edit
  private Button wbaSourceFileFolder; // Add or change

  // Add File to result

  private CCombo wSuccessCondition;
  private CCombo wAddFilenameToResult;

  private Label wlNrErrorsLessThan;
  private TextVar wNrErrorsLessThan;

  public XmlWellFormedDialog(
      Shell parent, XmlWellFormed action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;

    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionXMLWellFormed.Name.Default"));
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
    shell.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons go at the very bottom
    //
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    BaseTransformDialog.positionBottomButtons(shell, new Button[] {wOk, wCancel}, margin, null);

    // Filename line
    Label wlName = new Label(shell, SWT.RIGHT);
    wlName.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Name.Label"));
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF GENERAL TAB ///
    // ////////////////////////

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // SETTINGS grouping?
    // ////////////////////////
    // START OF SETTINGS GROUP
    //

    Group wSettings = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSettings);
    wSettings.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Settings.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;
    wSettings.setLayout(groupLayout);

    Label wlIncludeSubfolders = new Label(wSettings, SWT.RIGHT);
    wlIncludeSubfolders.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.IncludeSubfolders.Label"));
    PropsUi.setLook(wlIncludeSubfolders);
    FormData fdlIncludeSubfolders = new FormData();
    fdlIncludeSubfolders.left = new FormAttachment(0, 0);
    fdlIncludeSubfolders.top = new FormAttachment(wName, margin);
    fdlIncludeSubfolders.right = new FormAttachment(middle, -margin);
    wlIncludeSubfolders.setLayoutData(fdlIncludeSubfolders);
    wIncludeSubfolders = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wIncludeSubfolders);
    wIncludeSubfolders.setToolTipText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.IncludeSubfolders.Tooltip"));
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

    // previous
    Label wlPrevious = new Label(wSettings, SWT.RIGHT);
    wlPrevious.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Previous.Label"));
    PropsUi.setLook(wlPrevious);
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment(0, 0);
    fdlPrevious.top = new FormAttachment(wlIncludeSubfolders, 2 * margin);
    fdlPrevious.right = new FormAttachment(middle, -margin);
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wSettings, SWT.CHECK);
    PropsUi.setLook(wPrevious);
    wPrevious.setSelection(action.isArgFromPrevious());
    wPrevious.setToolTipText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Previous.Tooltip"));
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment(middle, 0);
    fdPrevious.top = new FormAttachment(wlPrevious, 0, SWT.CENTER);
    fdPrevious.right = new FormAttachment(100, 0);
    wPrevious.setLayoutData(fdPrevious);
    wPrevious.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            RefreshArgFromPrevious();
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

    // SourceFileFolder line
    wlSourceFileFolder = new Label(wGeneralComp, SWT.RIGHT);
    wlSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SourceFileFolder.Label"));
    PropsUi.setLook(wlSourceFileFolder);
    FormData fdlSourceFileFolder = new FormData();
    fdlSourceFileFolder.left = new FormAttachment(0, 0);
    fdlSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin);
    fdlSourceFileFolder.right = new FormAttachment(middle, -margin);
    wlSourceFileFolder.setLayoutData(fdlSourceFileFolder);

    // Browse Source folders button ...
    wbSourceDirectory = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSourceDirectory);
    wbSourceDirectory.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.BrowseFolders.Label"));
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
        BaseMessages.getString(PKG, "ActionXMLWellFormed.BrowseFiles.Label"));
    FormData fdbSourceFileFolder = new FormData();
    fdbSourceFileFolder.right = new FormAttachment(wbSourceDirectory, -margin);
    fdbSourceFileFolder.top = new FormAttachment(wSettings, margin);
    wbSourceFileFolder.setLayoutData(fdbSourceFileFolder);

    // Browse Destination file add button ...
    wbaSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaSourceFileFolder);
    wbaSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.FilenameAdd.Button"));
    FormData fdbaSourceFileFolder = new FormData();
    fdbaSourceFileFolder.right = new FormAttachment(wbSourceFileFolder, -margin);
    fdbaSourceFileFolder.top = new FormAttachment(wSettings, margin);
    wbaSourceFileFolder.setLayoutData(fdbaSourceFileFolder);

    wSourceFileFolder = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SourceFileFolder.Tooltip"));

    PropsUi.setLook(wSourceFileFolder);
    wSourceFileFolder.addModifyListener(lsMod);
    FormData fdSourceFileFolder = new FormData();
    fdSourceFileFolder.left = new FormAttachment(middle, 0);
    fdSourceFileFolder.top = new FormAttachment(wSettings, 2 * margin);
    fdSourceFileFolder.right = new FormAttachment(wbaSourceFileFolder, -margin);
    wSourceFileFolder.setLayoutData(fdSourceFileFolder);

    // Whenever something changes, set the tooltip to the expanded version:
    wSourceFileFolder.addModifyListener(
        e -> wSourceFileFolder.setToolTipText(variables.resolve(wSourceFileFolder.getText())));
    wbSourceFileFolder.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wSourceFileFolder,
                variables,
                new String[] {"*.xml;*.XML", "*"},
                FILETYPES,
                true));

    // Wildcard
    wlWildcard = new Label(wGeneralComp, SWT.RIGHT);
    wlWildcard.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Wildcard.Label"));
    PropsUi.setLook(wlWildcard);
    FormData fdlWildcard = new FormData();
    fdlWildcard.left = new FormAttachment(0, 0);
    fdlWildcard.top = new FormAttachment(wSourceFileFolder, margin);
    fdlWildcard.right = new FormAttachment(middle, -margin);
    wlWildcard.setLayoutData(fdlWildcard);

    wWildcard = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wWildcard.setToolTipText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Wildcard.Tooltip"));
    PropsUi.setLook(wWildcard);
    wWildcard.addModifyListener(lsMod);
    FormData fdWildcard = new FormData();
    fdWildcard.left = new FormAttachment(middle, 0);
    fdWildcard.top = new FormAttachment(wSourceFileFolder, margin);
    fdWildcard.right = new FormAttachment(wbaSourceFileFolder, -margin);
    wWildcard.setLayoutData(fdWildcard);

    wlFields = new Label(wGeneralComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Fields.Label"));
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
        BaseMessages.getString(PKG, "ActionXMLWellFormed.FilenameDelete.Button"));
    wbdSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.FilenameDelete.Tooltip"));
    FormData fdbdSourceFileFolder = new FormData();
    fdbdSourceFileFolder.right = new FormAttachment(100, 0);
    fdbdSourceFileFolder.top = new FormAttachment(wlFields, margin);
    wbdSourceFileFolder.setLayoutData(fdbdSourceFileFolder);

    wbeSourceFileFolder = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeSourceFileFolder);
    wbeSourceFileFolder.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.FilenameEdit.Button"));
    wbeSourceFileFolder.setToolTipText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.FilenameEdit.Tooltip"));
    FormData fdbeSourceFileFolder = new FormData();
    fdbeSourceFileFolder.right = new FormAttachment(100, 0);
    fdbeSourceFileFolder.left = new FormAttachment(wbdSourceFileFolder, 0, SWT.LEFT);
    fdbeSourceFileFolder.top = new FormAttachment(wbdSourceFileFolder, margin);
    wbeSourceFileFolder.setLayoutData(fdbeSourceFileFolder);

    int rows =
        action.getSourceFileFolders() == null
            ? 1
            : (action.getSourceFileFolders().size() == 0
                ? 0
                : action.getSourceFileFolders().size());
    final int FieldsRows = rows;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionXMLWellFormed.Fields.SourceFileFolder.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ActionXMLWellFormed.Fields.Wildcard.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };

    colinf[0].setUsingVariables(true);
    colinf[0].setToolTip(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.Fields.SourceFileFolder.Tooltip"));
    colinf[1].setUsingVariables(true);
    colinf[1].setToolTip(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.Fields.Wildcard.Tooltip"));

    wFields =
        new TableView(
            variables,
            wGeneralComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(wbdSourceFileFolder, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    RefreshArgFromPrevious();

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

    // ///////////////////////////////////////////////////////////
    // / END OF GENERAL TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////////////////
    // START OF ADVANCED TAB ///
    // ///////////////////////////////////

    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.Tab.Advanced.Label"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);
    wAdvancedComp.setLayout(contentLayout);

    // SuccessOngrouping?
    // ////////////////////////
    // START OF SUCCESS ON GROUP///
    // /
    Group wSuccessOn = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wSuccessOn);
    wSuccessOn.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.SuccessOn.Group.Label"));

    FormLayout successongroupLayout = new FormLayout();
    successongroupLayout.marginWidth = 10;
    successongroupLayout.marginHeight = 10;

    wSuccessOn.setLayout(successongroupLayout);

    // Success Condition
    Label wlSuccessCondition = new Label(wSuccessOn, SWT.RIGHT);
    wlSuccessCondition.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SuccessCondition.Label"));
    PropsUi.setLook(wlSuccessCondition);
    FormData fdlSuccessCondition = new FormData();
    fdlSuccessCondition.left = new FormAttachment(0, 0);
    fdlSuccessCondition.right = new FormAttachment(middle, 0);
    fdlSuccessCondition.top = new FormAttachment(0, margin);
    wlSuccessCondition.setLayoutData(fdlSuccessCondition);
    wSuccessCondition = new CCombo(wSuccessOn, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SuccessWhenAllWorksFine.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SuccessWhenAtLeat.Label"));
    wSuccessCondition.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.SuccessWhenBadFormedLessThan.Label"));
    wSuccessCondition.select(0); // +1: starts at -1

    PropsUi.setLook(wSuccessCondition);
    FormData fdSuccessCondition = new FormData();
    fdSuccessCondition.left = new FormAttachment(middle, 0);
    fdSuccessCondition.top = new FormAttachment(0, margin);
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
        BaseMessages.getString(PKG, "ActionXMLWellFormed.NrBadFormedLessThan.Label"));
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
            BaseMessages.getString(PKG, "ActionXMLWellFormed.NrBadFormedLessThan.Tooltip"));
    PropsUi.setLook(wNrErrorsLessThan);
    wNrErrorsLessThan.addModifyListener(lsMod);
    FormData fdNrErrorsLessThan = new FormData();
    fdNrErrorsLessThan.left = new FormAttachment(middle, 0);
    fdNrErrorsLessThan.top = new FormAttachment(wSuccessCondition, margin);
    fdNrErrorsLessThan.right = new FormAttachment(100, -margin);
    wNrErrorsLessThan.setLayoutData(fdNrErrorsLessThan);

    FormData fdSuccessOn = new FormData();
    fdSuccessOn.left = new FormAttachment(0, margin);
    fdSuccessOn.top = new FormAttachment(0, margin);
    fdSuccessOn.right = new FormAttachment(100, -margin);
    wSuccessOn.setLayoutData(fdSuccessOn);
    // ///////////////////////////////////////////////////////////
    // / END OF Success ON GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFileResult = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionXMLWellFormed.FileResult.Group.Label"));

    FormLayout fileresultgroupLayout = new FormLayout();
    fileresultgroupLayout.marginWidth = 10;
    fileresultgroupLayout.marginHeight = 10;

    wFileResult.setLayout(fileresultgroupLayout);

    // Add Filenames to result filenames?
    Label wlAddFilenameToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFilenameToResult.setText(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.AddFilenameToResult.Label"));
    PropsUi.setLook(wlAddFilenameToResult);
    FormData fdlAddFilenameToResult = new FormData();
    fdlAddFilenameToResult.left = new FormAttachment(0, 0);
    fdlAddFilenameToResult.right = new FormAttachment(middle, 0);
    fdlAddFilenameToResult.top = new FormAttachment(0, margin);
    wlAddFilenameToResult.setLayoutData(fdlAddFilenameToResult);
    wAddFilenameToResult = new CCombo(wFileResult, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wAddFilenameToResult.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.AddAllFilenamesToResult.Label"));
    wAddFilenameToResult.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.AddOnlyWellFormedFilenames.Label"));
    wAddFilenameToResult.add(
        BaseMessages.getString(PKG, "ActionXMLWellFormed.AddOnlyBadFormedFilenames.Label"));
    wAddFilenameToResult.select(0); // +1: starts at -1

    PropsUi.setLook(wAddFilenameToResult);
    FormData fdAddFilenameToResult = new FormData();
    fdAddFilenameToResult.left = new FormAttachment(middle, 0);
    fdAddFilenameToResult.top = new FormAttachment(0, margin);
    fdAddFilenameToResult.right = new FormAttachment(100, 0);
    wAddFilenameToResult.setLayoutData(fdAddFilenameToResult);
    wAddFilenameToResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            // Do nothing
          }
        });

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wSuccessOn, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FilesResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(wAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF ADVANCED TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    activeSuccessCondition();
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void activeSuccessCondition() {
    wlNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
    wNrErrorsLessThan.setEnabled(wSuccessCondition.getSelectionIndex() != 0);
  }

  private void RefreshArgFromPrevious() {

    wlFields.setEnabled(!wPrevious.getSelection());
    wFields.setEnabled(!wPrevious.getSelection());
    wbdSourceFileFolder.setEnabled(!wPrevious.getSelection());
    wbeSourceFileFolder.setEnabled(!wPrevious.getSelection());
    wbSourceFileFolder.setEnabled(!wPrevious.getSelection());
    wbaSourceFileFolder.setEnabled(!wPrevious.getSelection());
    wlSourceFileFolder.setEnabled(!wPrevious.getSelection());
    wSourceFileFolder.setEnabled(!wPrevious.getSelection());

    wlWildcard.setEnabled(!wPrevious.getSelection());
    wWildcard.setEnabled(!wPrevious.getSelection());
    wbSourceDirectory.setEnabled(!wPrevious.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));

    if (action.getSourceFileFolders() != null) {
      for (int i = 0; i < action.getSourceFileFolders().size(); i++) {
        TableItem ti = wFields.table.getItem(i);
        if (action.getSourceFileFolders().get(i) != null) {
          ti.setText(1, action.getSourceFileFolders().get(i).getSourceFilefolder());
        }

        if (action.getSourceFileFolders().get(i).getWildcard() != null) {
          ti.setText(2, action.getSourceFileFolders().get(i).getWildcard());
        }
      }
      wFields.setRowNums();
      wFields.optWidth(true);
    }
    wPrevious.setSelection(action.isArgFromPrevious());
    wIncludeSubfolders.setSelection(action.isIncludeSubfolders());

    wNrErrorsLessThan.setText(Const.NVL(action.getNrErrorsLessThan(), "10"));

    if (action.getSuccessCondition() != null) {
      if (action
          .getSuccessCondition()
          .equals(XmlWellFormed.SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED)) {
        wSuccessCondition.select(1);
      } else if (action
          .getSuccessCondition()
          .equals(XmlWellFormed.SUCCESS_IF_BAD_FORMED_FILES_LESS)) {
        wSuccessCondition.select(2);
      } else {
        wSuccessCondition.select(0);
      }
    } else {
      wSuccessCondition.select(0);
    }

    if (action.getResultFilenames() != null) {
      if (action.getResultFilenames().equals(XmlWellFormed.ADD_WELL_FORMED_FILES_ONLY)) {
        wAddFilenameToResult.select(1);
      } else if (action.getResultFilenames().equals(XmlWellFormed.ADD_BAD_FORMED_FILES_ONLY)) {
        wAddFilenameToResult.select(2);
      } else {
        wAddFilenameToResult.select(0);
      }
    } else {
      wAddFilenameToResult.select(0);
    }

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
      mb.setText(BaseMessages.getString(PKG, "System.ActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());

    action.setIncludeSubfolders(wIncludeSubfolders.getSelection());
    action.setArgFromPrevious(wPrevious.getSelection());

    action.setNrErrorsLessThan(wNrErrorsLessThan.getText());

    if (wSuccessCondition.getSelectionIndex() == 1) {
      action.setSuccessCondition(XmlWellFormed.SUCCESS_IF_AT_LEAST_X_FILES_WELL_FORMED);
    } else if (wSuccessCondition.getSelectionIndex() == 2) {
      action.setSuccessCondition(XmlWellFormed.SUCCESS_IF_BAD_FORMED_FILES_LESS);
    } else {
      action.setSuccessCondition(XmlWellFormed.SUCCESS_IF_NO_ERRORS);
    }

    if (wAddFilenameToResult.getSelectionIndex() == 1) {
      action.setResultFilenames(XmlWellFormed.ADD_WELL_FORMED_FILES_ONLY);
    } else if (wAddFilenameToResult.getSelectionIndex() == 2) {
      action.setResultFilenames(XmlWellFormed.ADD_BAD_FORMED_FILES_ONLY);
    } else {
      action.setResultFilenames(XmlWellFormed.ADD_ALL_FILENAMES);
    }

    int nritems = wFields.nrNonEmpty();
    int nr = 0;
    for (int i = 0; i < nritems; i++) {
      String arg = wFields.getNonEmpty(i).getText(1);
      if (arg != null && arg.length() != 0) {
        nr++;
      }
    }
    String[] sourceFilefolder = new String[nr];
    String[] wildcard = new String[nr];
    nr = 0;
    List<XmlWellFormedField> fieldList = new ArrayList<XmlWellFormedField>();
    for (int i = 0; i < nritems; i++) {
      XmlWellFormedField field = new XmlWellFormedField();
      String source = wFields.getNonEmpty(i).getText(1);
      String wild = wFields.getNonEmpty(i).getText(2);
      if (source != null && source.length() != 0) {
        field.setSourceFilefolder(source);
        field.setWildcard(wild);
        fieldList.add(field);
        nr++;
      }
    }
    dispose();
  }
}
