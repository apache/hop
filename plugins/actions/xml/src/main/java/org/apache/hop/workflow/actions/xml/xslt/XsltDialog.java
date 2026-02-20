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

package org.apache.hop.workflow.actions.xml.xslt;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.xml.xslt.XsltMeta;
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

/** This dialog allows you to edit the XSLT job entry settings. */
public class XsltDialog extends ActionDialog {
  private static final Class<?> PKG = Xslt.class;

  private static final String[] FILETYPES_XML =
      new String[] {
        BaseMessages.getString(PKG, "ActionXSLT.Filetype.Xml"),
        BaseMessages.getString(PKG, "ActionXSLT.Filetype.All")
      };

  private static final String[] FILETYPES_XSL =
      new String[] {
        BaseMessages.getString(PKG, "ActionXSLT.Filetype.Xsl"),
        BaseMessages.getString(PKG, "ActionXSLT.Filetype.Xslt"),
        BaseMessages.getString(PKG, "ActionXSLT.Filetype.All")
      };
  public static final String CONST_SYSTEM_BUTTON_BROWSE = "System.Button.Browse";

  private Label wlxmlFilename;
  private Button wbxmlFilename;
  private TextVar wxmlFilename;

  private Label wlxslFilename;
  private Button wbxslFilename;
  private TextVar wxslFilename;

  private Label wlOutputFilename;
  private TextVar wOutputFilename;

  private Button wbMovetoDirectory;

  private CCombo wIfFileExists;

  private Xslt action;

  private boolean changed;

  private CCombo wXSLTFactory;

  private Button wPrevious;

  private Button wAddFileToResult;

  private TableView wFields;

  private TableView wOutputProperties;

  public XsltDialog(Shell parent, Xslt action, WorkflowMeta workflowMeta, IVariables variables) {
    super(parent, workflowMeta, variables);
    this.action = action;
    if (this.action.getName() == null) {
      this.action.setName(BaseMessages.getString(PKG, "ActionXSLT.Name.Default"));
    }
  }

  @Override
  public IAction open() {
    createShell(BaseMessages.getString(PKG, "ActionXSLT.Title"), action);
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
    wGeneralTab.setText(BaseMessages.getString(PKG, "ActionXSLT.Tab.General.Label"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // Files grouping?
    // ////////////////////////
    // START OF LOGGING GROUP///
    // /
    Group wFiles = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFiles);
    wFiles.setText(BaseMessages.getString(PKG, "ActionXSLT.Files.Group.Label"));

    FormLayout groupLayout = new FormLayout();
    groupLayout.marginWidth = 10;
    groupLayout.marginHeight = 10;

    wFiles.setLayout(groupLayout);

    Label wlPrevious = new Label(wFiles, SWT.RIGHT);
    wlPrevious.setText(BaseMessages.getString(PKG, "ActionXSLT.Previous.Label"));
    PropsUi.setLook(wlPrevious);
    FormData fdlPrevious = new FormData();
    fdlPrevious.left = new FormAttachment(0, 0);
    fdlPrevious.right = new FormAttachment(middle, -margin);
    wlPrevious.setLayoutData(fdlPrevious);
    wPrevious = new Button(wFiles, SWT.CHECK);
    PropsUi.setLook(wPrevious);
    wPrevious.setToolTipText(BaseMessages.getString(PKG, "ActionXSLT.Previous.ToolTip"));
    FormData fdPrevious = new FormData();
    fdPrevious.left = new FormAttachment(middle, 0);
    fdPrevious.top = new FormAttachment(wlPrevious, 0, SWT.CENTER);
    fdPrevious.right = new FormAttachment(100, 0);
    wPrevious.setLayoutData(fdPrevious);
    fdlPrevious.top = new FormAttachment(wPrevious, 0, SWT.CENTER);
    wPrevious.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {

            refreshArgFromPrevious();
          }
        });

    // Filename 1 line
    wlxmlFilename = new Label(wFiles, SWT.RIGHT);
    wlxmlFilename.setText(BaseMessages.getString(PKG, "ActionXSLT.xmlFilename.Label"));
    PropsUi.setLook(wlxmlFilename);
    FormData fdlxmlFilename = new FormData();
    fdlxmlFilename.left = new FormAttachment(0, 0);
    fdlxmlFilename.right = new FormAttachment(middle, -margin);
    wlxmlFilename.setLayoutData(fdlxmlFilename);
    wbxmlFilename = new Button(wFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbxmlFilename);
    wbxmlFilename.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbxmlFilename = new FormData();
    fdbxmlFilename.right = new FormAttachment(100, 0);
    fdbxmlFilename.top = new FormAttachment(wlPrevious, margin);
    wbxmlFilename.setLayoutData(fdbxmlFilename);
    wxmlFilename = new TextVar(variables, wFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wxmlFilename);
    wxmlFilename.addModifyListener(lsMod);
    FormData fdxmlFilename = new FormData();
    fdxmlFilename.left = new FormAttachment(middle, 0);
    fdxmlFilename.top = new FormAttachment(wbxmlFilename, 0, SWT.CENTER);
    fdxmlFilename.right = new FormAttachment(wbxmlFilename);
    wxmlFilename.setLayoutData(fdxmlFilename);
    fdlxmlFilename.top = new FormAttachment(wbxmlFilename, 0, SWT.CENTER);

    // Whenever something changes, set the tooltip to the expanded version:
    wxmlFilename.addModifyListener(
        e -> wxmlFilename.setToolTipText(variables.resolve(wxmlFilename.getText())));
    wbxmlFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wxmlFilename,
                variables,
                new String[] {"*.xml;*.XML", "*"},
                FILETYPES_XML,
                false));

    // Filename 2 line
    wlxslFilename = new Label(wFiles, SWT.RIGHT);
    wlxslFilename.setText(BaseMessages.getString(PKG, "ActionXSLT.xslFilename.Label"));
    PropsUi.setLook(wlxslFilename);
    FormData fdlxslFilename = new FormData();
    fdlxslFilename.left = new FormAttachment(0, 0);
    fdlxslFilename.right = new FormAttachment(middle, -margin);
    wlxslFilename.setLayoutData(fdlxslFilename);
    wbxslFilename = new Button(wFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbxslFilename);
    wbxslFilename.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbxslFilename = new FormData();
    fdbxslFilename.right = new FormAttachment(100, 0);
    fdbxslFilename.top = new FormAttachment(wxmlFilename, margin);
    wbxslFilename.setLayoutData(fdbxslFilename);
    wxslFilename = new TextVar(variables, wFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wxslFilename);
    wxslFilename.addModifyListener(lsMod);
    FormData fdxslFilename = new FormData();
    fdxslFilename.left = new FormAttachment(middle, 0);
    fdxslFilename.top = new FormAttachment(wbxslFilename, 0, SWT.CENTER);
    fdxslFilename.right = new FormAttachment(wbxslFilename);
    wxslFilename.setLayoutData(fdxslFilename);
    fdlxslFilename.top = new FormAttachment(wbxslFilename, 0, SWT.CENTER);

    // Whenever something changes, set the tooltip to the expanded version:
    wxslFilename.addModifyListener(
        e -> wxslFilename.setToolTipText(variables.resolve(wxslFilename.getText())));
    wbxslFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wxslFilename,
                variables,
                new String[] {"*.xsl;*.XSL", "*.xslt;*.XSLT", "*"},
                FILETYPES_XSL,
                false));

    // OutputFilename
    wlOutputFilename = new Label(wFiles, SWT.RIGHT);
    wlOutputFilename.setText(BaseMessages.getString(PKG, "ActionXSLT.OutputFilename.Label"));
    PropsUi.setLook(wlOutputFilename);
    FormData fdlOutputFilename = new FormData();
    fdlOutputFilename.left = new FormAttachment(0, 0);
    fdlOutputFilename.right = new FormAttachment(middle, -margin);
    wlOutputFilename.setLayoutData(fdlOutputFilename);

    // Browse folders button ...
    wbMovetoDirectory = new Button(wFiles, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbMovetoDirectory);
    wbMovetoDirectory.setText(BaseMessages.getString(PKG, CONST_SYSTEM_BUTTON_BROWSE));
    FormData fdbMovetoDirectory = new FormData();
    fdbMovetoDirectory.right = new FormAttachment(100, 0);
    fdbMovetoDirectory.top = new FormAttachment(wxslFilename, margin);
    wbMovetoDirectory.setLayoutData(fdbMovetoDirectory);
    wbMovetoDirectory.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wOutputFilename, variables));
    fdlOutputFilename.top = new FormAttachment(wbMovetoDirectory, 0, SWT.CENTER);

    wOutputFilename = new TextVar(variables, wFiles, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wOutputFilename);
    wOutputFilename.addModifyListener(lsMod);
    FormData fdOutputFilename = new FormData();
    fdOutputFilename.left = new FormAttachment(middle, 0);
    fdOutputFilename.top = new FormAttachment(wbMovetoDirectory, 0, SWT.CENTER);
    fdOutputFilename.right = new FormAttachment(wbMovetoDirectory);
    wOutputFilename.setLayoutData(fdOutputFilename);

    // Whenever something changes, set the tooltip to the expanded version:
    wOutputFilename.addModifyListener(
        e -> wOutputFilename.setToolTipText(variables.resolve(wOutputFilename.getText())));

    FormData fdFiles = new FormData();
    fdFiles.left = new FormAttachment(0, margin);
    fdFiles.top = new FormAttachment(0, margin);
    fdFiles.right = new FormAttachment(100, -margin);
    wFiles.setLayoutData(fdFiles);
    // ///////////////////////////////////////////////////////////
    // / END OF Files GROUP
    // ///////////////////////////////////////////////////////////

    // fileresult grouping?
    // ////////////////////////
    // START OF FILE RESULT GROUP///
    // /
    Group wFileResult = new Group(wGeneralComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wFileResult);
    wFileResult.setText(BaseMessages.getString(PKG, "ActionXSLT.FileResult.Group.Settings.Label"));

    FormLayout groupFilesResultLayout = new FormLayout();
    groupFilesResultLayout.marginWidth = 10;
    groupFilesResultLayout.marginHeight = 10;

    wFileResult.setLayout(groupFilesResultLayout);

    // XSLTFactory
    Label wlXSLTFactory = new Label(wFileResult, SWT.RIGHT);
    wlXSLTFactory.setText(BaseMessages.getString(PKG, "ActionXSLT.XSLTFactory.Label"));
    PropsUi.setLook(wlXSLTFactory);
    FormData fdlXSLTFactory = new FormData();
    fdlXSLTFactory.left = new FormAttachment(0, 0);
    fdlXSLTFactory.right = new FormAttachment(middle, -margin);
    wlXSLTFactory.setLayoutData(fdlXSLTFactory);
    wXSLTFactory = new CCombo(wFileResult, SWT.BORDER | SWT.READ_ONLY);
    wXSLTFactory.setEditable(true);
    PropsUi.setLook(wXSLTFactory);
    wXSLTFactory.addModifyListener(lsMod);
    FormData fdXSLTFactory = new FormData();
    fdXSLTFactory.left = new FormAttachment(middle, 0);
    fdXSLTFactory.top = new FormAttachment(wFiles, margin);
    fdXSLTFactory.right = new FormAttachment(100, 0);
    wXSLTFactory.setLayoutData(fdXSLTFactory);
    wXSLTFactory.add("JAXP");
    wXSLTFactory.add("SAXON");
    fdlXSLTFactory.top = new FormAttachment(wXSLTFactory, 0, SWT.CENTER);

    // IF File Exists
    Label wlIfFileExists = new Label(wFileResult, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ActionXSLT.IfFileExists.Label"));
    PropsUi.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.right = new FormAttachment(middle, -margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(wFileResult, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionXSLT.Create_NewFile_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionXSLT.Do_Nothing_IfFileExists.Label"));
    wIfFileExists.add(BaseMessages.getString(PKG, "ActionXSLT.Fail_IfFileExists.Label"));
    wIfFileExists.select(1); // +1: starts at -1
    fdlIfFileExists.top = new FormAttachment(wIfFileExists, 0, SWT.CENTER);

    PropsUi.setLook(wIfFileExists);
    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wXSLTFactory, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wXSLTFactory, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    // Add file to result
    Label wlAddFileToResult = new Label(wFileResult, SWT.RIGHT);
    wlAddFileToResult.setText(BaseMessages.getString(PKG, "ActionXSLT.AddFileToResult.Label"));
    PropsUi.setLook(wlAddFileToResult);
    FormData fdlAddFileToResult = new FormData();
    fdlAddFileToResult.left = new FormAttachment(0, 0);
    fdlAddFileToResult.right = new FormAttachment(middle, -margin);
    wlAddFileToResult.setLayoutData(fdlAddFileToResult);
    wAddFileToResult = new Button(wFileResult, SWT.CHECK);
    PropsUi.setLook(wAddFileToResult);
    wAddFileToResult.setToolTipText(
        BaseMessages.getString(PKG, "ActionXSLT.AddFileToResult.Tooltip"));
    FormData fdAddFileToResult = new FormData();
    fdAddFileToResult.left = new FormAttachment(middle, 0);
    fdAddFileToResult.top = new FormAttachment(wIfFileExists, margin);
    fdAddFileToResult.right = new FormAttachment(100, 0);
    wAddFileToResult.setLayoutData(fdAddFileToResult);
    wAddFileToResult.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            action.setChanged();
          }
        });
    fdlAddFileToResult.top = new FormAttachment(wAddFileToResult, 0, SWT.CENTER);

    FormData fdFileResult = new FormData();
    fdFileResult.left = new FormAttachment(0, margin);
    fdFileResult.top = new FormAttachment(wFiles, margin);
    fdFileResult.right = new FormAttachment(100, -margin);
    wFileResult.setLayoutData(fdFileResult);
    // ///////////////////////////////////////////////////////////
    // / END OF FileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, margin);
    fdGeneralComp.top = new FormAttachment(0, margin);
    fdGeneralComp.right = new FormAttachment(100, -margin);
    fdGeneralComp.bottom = new FormAttachment(100, -margin);
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
    wAdvancedTab.setText(BaseMessages.getString(PKG, "ActionXSLT.Tab.Advanced.Label"));

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout(advancedLayout);

    // Output properties
    Label wlOutputProperties = new Label(wAdvancedComp, SWT.NONE);
    wlOutputProperties.setText(BaseMessages.getString(PKG, "XsltDialog.OutputProperties.Label"));
    PropsUi.setLook(wlOutputProperties);
    FormData fdlOutputProperties = new FormData();
    fdlOutputProperties.left = new FormAttachment(0, 0);
    fdlOutputProperties.top = new FormAttachment(0, margin);
    wlOutputProperties.setLayoutData(fdlOutputProperties);

    final int OutputPropertiesRows = action.getOutputPropertyName().length;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "XsltDialog.ColumnInfo.OutputProperties.Name"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "XsltDialog.ColumnInfo.OutputProperties.Value"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    colinf[0].setComboValues(XsltMeta.outputProperties);
    colinf[1].setUsingVariables(true);

    wOutputProperties =
        new TableView(
            variables,
            wAdvancedComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            OutputPropertiesRows,
            lsMod,
            props);
    FormData fdOutputProperties = new FormData();
    fdOutputProperties.left = new FormAttachment(0, 0);
    fdOutputProperties.top = new FormAttachment(wlOutputProperties, margin);
    fdOutputProperties.right = new FormAttachment(100, -margin);
    fdOutputProperties.bottom = new FormAttachment(wlOutputProperties, 200);
    wOutputProperties.setLayoutData(fdOutputProperties);

    // Parameters

    Label wlFields = new Label(wAdvancedComp, SWT.NONE);
    wlFields.setText(BaseMessages.getString(PKG, "XsltDialog.Parameters.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.top = new FormAttachment(wOutputProperties, margin);
    wlFields.setLayoutData(fdlFields);

    final int FieldsRows = action.getParameterField().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "XsltDialog.ColumnInfo.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "XsltDialog.ColumnInfo.Parameter"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
        };
    colinf[1].setUsingVariables(true);
    colinf[0].setUsingVariables(true);

    wFields =
        new TableView(
            variables,
            wAdvancedComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, margin);
    fdAdvancedComp.top = new FormAttachment(0, margin);
    fdAdvancedComp.right = new FormAttachment(100, -margin);
    fdAdvancedComp.bottom = new FormAttachment(100, -margin);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    PropsUi.setLook(wAdvancedComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Advanced TAB
    // ///////////////////////////////////////////////////////////

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, margin);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, -margin);
    fdTabFolder.bottom = new FormAttachment(wCancel, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    getData();
    focusActionName();
    refreshArgFromPrevious();
    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return action;
  }

  private void refreshArgFromPrevious() {
    wlxmlFilename.setEnabled(!wPrevious.getSelection());
    wxmlFilename.setEnabled(!wPrevious.getSelection());
    wbxmlFilename.setEnabled(!wPrevious.getSelection());
    wlxslFilename.setEnabled(!wPrevious.getSelection());
    wxslFilename.setEnabled(!wPrevious.getSelection());
    wbxslFilename.setEnabled(!wPrevious.getSelection());
    wlOutputFilename.setEnabled(!wPrevious.getSelection());
    wOutputFilename.setEnabled(!wPrevious.getSelection());
    wbMovetoDirectory.setEnabled(!wPrevious.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wName.setText(Const.nullToEmpty(action.getName()));
    wxmlFilename.setText(Const.nullToEmpty(action.getXmlfilename()));
    wxslFilename.setText(Const.nullToEmpty(action.getXslfilename()));
    wOutputFilename.setText(Const.nullToEmpty(action.getoutputfilename()));

    if (action.ifFileExists >= 0) {
      wIfFileExists.select(action.ifFileExists);
    } else {
      wIfFileExists.select(2); // NOTHING
    }

    wAddFileToResult.setSelection(action.isAddfiletoresult());
    wPrevious.setSelection(action.isFilenamesFromPrevious());
    if (action.getXSLTFactory() != null) {
      wXSLTFactory.setText(action.getXSLTFactory());
    } else {
      wXSLTFactory.setText("JAXP");
    }
    if (action.getParameterName() != null) {
      for (int i = 0; i < action.getParameterName().length; i++) {
        TableItem item = wFields.table.getItem(i);
        item.setText(1, Const.NVL(action.getParameterField()[i], ""));
        item.setText(2, Const.NVL(action.getParameterName()[i], ""));
      }
    }
    if (action.getOutputPropertyName() != null) {
      for (int i = 0; i < action.getOutputPropertyName().length; i++) {
        TableItem item = wOutputProperties.table.getItem(i);
        item.setText(1, Const.NVL(action.getOutputPropertyName()[i], ""));
        item.setText(2, Const.NVL(action.getOutputPropertyValue()[i], ""));
      }
    }
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
      mb.setText(BaseMessages.getString(PKG, "System.ActionNameMissing.Title"));
      mb.setMessage(BaseMessages.getString(PKG, "System.ActionNameMissing.Msg"));
      mb.open();
      return;
    }
    action.setName(wName.getText());
    action.setXmlfilename(wxmlFilename.getText());
    action.setXslfilename(wxslFilename.getText());
    action.setOutputfilename(wOutputFilename.getText());
    action.ifFileExists = wIfFileExists.getSelectionIndex();
    action.setFilenamesFromPrevious(wPrevious.getSelection());
    action.setAddfiletoresult(wAddFileToResult.getSelection());
    action.setXSLTFactory(wXSLTFactory.getText());

    int nrparams = wFields.nrNonEmpty();
    int nroutputprops = wOutputProperties.nrNonEmpty();
    action.allocate(nrparams, nroutputprops);

    for (int i = 0; i < nrparams; i++) {
      TableItem item = wFields.getNonEmpty(i);
      action.getParameterField()[i] = item.getText(1);
      action.getParameterName()[i] = item.getText(2);
    }
    for (int i = 0; i < nroutputprops; i++) {
      TableItem item = wOutputProperties.getNonEmpty(i);
      action.getOutputPropertyName()[i] = item.getText(1);
      action.getOutputPropertyValue()[i] = item.getText(2);
    }

    dispose();
  }
}
