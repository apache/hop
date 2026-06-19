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

package org.apache.hop.pipeline.transforms.yamlinput;

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Cursor;
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

public class YamlInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = YamlInputMeta.class;
  public static final String CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_TITLE =
      "YamlInputDialog.ErrorParsingData.DialogTitle";
  public static final String CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_MESSAGE =
      "YamlInputDialog.ErrorParsingData.DialogMessage";

  protected static final String[] NO_YES_DEC =
      new String[] {
        BaseMessages.getString(PKG, "System.Combo.No"),
        BaseMessages.getString(PKG, "System.Combo.Yes")
      };

  private Label wlFilename;
  private Label wlYamlIsAFile;
  // Browse: add file or directory
  private Button wbbFilename;
  // Delete
  private Button wbdFilename;
  // Edit
  private Button wbeFilename;
  // Add or change
  private Button wbaFilename;
  private TextVar wFilename;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Label wlFilemask;
  private TextVar wFilemask;

  private Button wbShowFiles;

  private Label wlYamlField;
  private CCombo wYAMLLField;
  private Button wYAMLStreamField;
  private Button wYAMLIsAFile;

  private Label wlInclFilename;
  private Button wInclFilename;
  private Button wAddResult;

  private Label wlInclFilenameField;
  private TextVar wInclFilenameField;
  private Label wlAddResult;
  private Button wInclRowNum;

  private Label wlInclRowNumField;
  private TextVar wInclRowNumField;

  private Label wlLimit;
  private Text wLimit;

  private TableView wFields;

  // ignore empty files flag
  private Label wlIgnoreEmptyFile;
  private Button wIgnoreEmptyFile;

  // do not fail if no files?
  private Label wlDoNotFailIfNoFile;
  private Button wDoNotFailIfNoFile;

  private final YamlInputMeta input;

  public YamlInputDialog(
      Shell parent, IVariables variables, YamlInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "YamlInputDialog.DialogTitle"));
    buildButtonBar().ok(e -> ok()).preview(e -> preview()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    // Buttons at the bottom
    //
    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB ///
    // ////////////////////////
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "YamlInputDialog.File.Tab"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // ///////////////////////////////
    // START OF Output Field GROUP //
    // ///////////////////////////////

    Group wOutputField = new Group(wFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOutputField);
    wOutputField.setText(BaseMessages.getString(PKG, "YamlInputDialog.wOutputField.Label"));

    FormLayout outPutFieldGroupLayout = new FormLayout();
    outPutFieldGroupLayout.marginWidth = 10;
    outPutFieldGroupLayout.marginHeight = 10;
    wOutputField.setLayout(outPutFieldGroupLayout);

    // Source is defined in a field
    Label wlXmlStreamField = new Label(wOutputField, SWT.RIGHT);
    wlXmlStreamField.setText(BaseMessages.getString(PKG, "YamlInputDialog.wlXmlStreamField.Label"));
    PropsUi.setLook(wlXmlStreamField);
    FormData fdlXMLStreamField = new FormData();
    fdlXMLStreamField.left = new FormAttachment(0, 0);
    fdlXMLStreamField.right = new FormAttachment(middle, 0);
    wlXmlStreamField.setLayoutData(fdlXMLStreamField);
    wYAMLStreamField = new Button(wOutputField, SWT.CHECK);
    PropsUi.setLook(wYAMLStreamField);
    wYAMLStreamField.setToolTipText(
        BaseMessages.getString(PKG, "YamlInputDialog.wYAMLStreamField.Tooltip"));
    FormData fdYAMLStreamField = new FormData();
    fdYAMLStreamField.left = new FormAttachment(wlXmlStreamField, margin);
    wYAMLStreamField.setLayoutData(fdYAMLStreamField);
    SelectionAdapter lsYamlStream =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            activateStreamField();
            input.setChanged();
          }
        };
    wYAMLStreamField.addSelectionListener(lsYamlStream);

    // Source is a filename
    wlYamlIsAFile = new Label(wOutputField, SWT.RIGHT);
    wlYamlIsAFile.setText(BaseMessages.getString(PKG, "YamlInputDialog.XMLIsAFile.Label"));
    PropsUi.setLook(wlYamlIsAFile);
    FormData fdlXMLIsAFile = new FormData();
    fdlXMLIsAFile.left = new FormAttachment(0, 0);
    fdlXMLIsAFile.top = new FormAttachment(wlXmlStreamField, margin);
    fdlXMLIsAFile.right = new FormAttachment(middle, 0);
    wlYamlIsAFile.setLayoutData(fdlXMLIsAFile);
    wYAMLIsAFile = new Button(wOutputField, SWT.CHECK);
    PropsUi.setLook(wYAMLIsAFile);
    wYAMLIsAFile.setToolTipText(BaseMessages.getString(PKG, "YamlInputDialog.XMLIsAFile.Tooltip"));
    FormData fdYAMLIsAFile = new FormData();
    fdYAMLIsAFile.left = new FormAttachment(wlYamlIsAFile, margin);
    fdYAMLIsAFile.top = new FormAttachment(wYAMLStreamField, margin);
    wYAMLIsAFile.setLayoutData(fdYAMLIsAFile);
    SelectionAdapter lsYamlIsFile =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            enableFileSettings();
          }
        };
    wYAMLIsAFile.addSelectionListener(lsYamlIsFile);

    // Get source from a field
    wlYamlField = new Label(wOutputField, SWT.RIGHT);
    wlYamlField.setText(BaseMessages.getString(PKG, "YamlInputDialog.wlYamlField.Label"));
    PropsUi.setLook(wlYamlField);
    FormData fdlXMLField = new FormData();
    fdlXMLField.left = new FormAttachment(0, 0);
    fdlXMLField.top = new FormAttachment(wlYamlIsAFile, margin);
    fdlXMLField.right = new FormAttachment(middle, 0);
    wlYamlField.setLayoutData(fdlXMLField);

    wYAMLLField = new CCombo(wOutputField, SWT.BORDER | SWT.READ_ONLY);
    wYAMLLField.setEditable(true);
    PropsUi.setLook(wYAMLLField);
    wYAMLLField.addModifyListener(lsMod);
    FormData fdXMLField = new FormData();
    fdXMLField.left = new FormAttachment(wlYamlField, margin);
    fdXMLField.top = new FormAttachment(wYAMLIsAFile, margin);
    fdXMLField.right = new FormAttachment(100, 0);
    wYAMLLField.setLayoutData(fdXMLField);
    wYAMLLField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do Nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setXMLStreamField();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    FormData fdOutputField = new FormData();
    fdOutputField.left = new FormAttachment(0, margin);
    fdOutputField.top = new FormAttachment(wFilenameList, margin);
    fdOutputField.right = new FormAttachment(100, -margin);
    wOutputField.setLayoutData(fdOutputField);

    // ///////////////////////////////////////////////////////////
    // / END OF Output Field GROUP
    // ///////////////////////////////////////////////////////////

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(wOutputField, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbFilename);
    wbbFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameBrowse.Button"));
    wbbFilename.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(wOutputField, margin);
    wbbFilename.setLayoutData(fdbFilename);

    wbaFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbaFilename);
    wbaFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameAdd.Button"));
    wbaFilename.setToolTipText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameAdd.Tooltip"));
    FormData fdbaFilename = new FormData();
    fdbaFilename.right = new FormAttachment(wbbFilename, -margin);
    fdbaFilename.top = new FormAttachment(wOutputField, margin);
    wbaFilename.setLayoutData(fdbaFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.right = new FormAttachment(wbaFilename, -margin);
    fdFilename.top = new FormAttachment(wOutputField, margin);
    wFilename.setLayoutData(fdFilename);

    wlFilemask = new Label(wFileComp, SWT.RIGHT);
    wlFilemask.setText(BaseMessages.getString(PKG, "YamlInputDialog.RegExp.Label"));
    PropsUi.setLook(wlFilemask);
    FormData fdlFilemask = new FormData();
    fdlFilemask.left = new FormAttachment(0, 0);
    fdlFilemask.top = new FormAttachment(wFilename, margin);
    fdlFilemask.right = new FormAttachment(middle, -margin);
    wlFilemask.setLayoutData(fdlFilemask);
    wFilemask = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilemask);
    wFilemask.addModifyListener(lsMod);
    FormData fdFilemask = new FormData();
    fdFilemask.left = new FormAttachment(middle, 0);
    fdFilemask.top = new FormAttachment(wFilename, margin);
    fdFilemask.right = new FormAttachment(100, 0);
    wFilemask.setLayoutData(fdFilemask);

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.RIGHT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wFilemask, margin);
    fdlFilenameList.right = new FormAttachment(middle, -margin);
    wlFilenameList.setLayoutData(fdlFilenameList);

    // Buttons to the right of the screen...
    wbdFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbdFilename);
    wbdFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameRemove.Button"));
    wbdFilename.setToolTipText(
        BaseMessages.getString(PKG, "YamlInputDialog.FilenameRemove.Tooltip"));
    FormData fdbdFilename = new FormData();
    fdbdFilename.right = new FormAttachment(100, 0);
    fdbdFilename.top = new FormAttachment(wFilemask, 40);
    wbdFilename.setLayoutData(fdbdFilename);

    wbeFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbeFilename);
    wbeFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameEdit.Button"));
    wbeFilename.setToolTipText(BaseMessages.getString(PKG, "YamlInputDialog.FilenameEdit.Tooltip"));
    FormData fdbeFilename = new FormData();
    fdbeFilename.right = new FormAttachment(100, 0);
    fdbeFilename.left = new FormAttachment(wbdFilename, 0, SWT.LEFT);
    fdbeFilename.top = new FormAttachment(wbdFilename, margin);
    wbeFilename.setLayoutData(fdbeFilename);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "YamlInputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, 0);
    wbShowFiles.setLayoutData(fdbShowFiles);

    ColumnInfo[] colInfo = new ColumnInfo[4];
    colInfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "YamlInputDialog.Files.Filename.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colInfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "YamlInputDialog.Files.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    colInfo[0].setUsingVariables(true);
    colInfo[1].setUsingVariables(true);
    colInfo[1].setToolTip(BaseMessages.getString(PKG, "YamlInputDialog.Files.Wildcard.Tooltip"));
    colInfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "YamlInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            NO_YES_DEC);
    colInfo[2].setToolTip(BaseMessages.getString(PKG, "YamlInputDialog.Required.Tooltip"));
    colInfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "YamlInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            NO_YES_DEC);
    colInfo[3].setToolTip(BaseMessages.getString(PKG, "YamlInputDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.SINGLE | SWT.BORDER,
            colInfo,
            2,
            lsMod,
            props);
    PropsUi.setLook(wFilenameList);
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(wbdFilename, -margin);
    fdFilenameList.top = new FormAttachment(wFilemask, margin);
    fdFilenameList.bottom = new FormAttachment(wbShowFiles, -margin);
    wFilenameList.setLayoutData(fdFilenameList);

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment(0, 0);
    fdFileComp.top = new FormAttachment(0, 0);
    fdFileComp.right = new FormAttachment(100, 0);
    fdFileComp.bottom = new FormAttachment(100, 0);
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    // ///////////////////////////////////////////////////////////
    // / END OF FILE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "YamlInputDialog.Content.Tab"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // ///////////////////////////////
    // START OF XmlConf Field GROUP //
    // ///////////////////////////////

    Group wXmlConf = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wXmlConf);
    wXmlConf.setText(BaseMessages.getString(PKG, "YamlInputDialog.wXmlConf.Label"));

    FormLayout xmlConfGroupLayout = new FormLayout();
    xmlConfGroupLayout.marginWidth = 10;
    xmlConfGroupLayout.marginHeight = 10;
    wXmlConf.setLayout(xmlConfGroupLayout);

    // Ignore Empty File
    wlIgnoreEmptyFile = new Label(wXmlConf, SWT.RIGHT);
    wlIgnoreEmptyFile.setText(BaseMessages.getString(PKG, "YamlInputDialog.IgnoreEmptyFile.Label"));
    PropsUi.setLook(wlIgnoreEmptyFile);
    FormData fdlIgnoreEmptyFile = new FormData();
    fdlIgnoreEmptyFile.left = new FormAttachment(0, 0);
    fdlIgnoreEmptyFile.right = new FormAttachment(middle, -margin);
    wlIgnoreEmptyFile.setLayoutData(fdlIgnoreEmptyFile);
    wIgnoreEmptyFile = new Button(wXmlConf, SWT.CHECK);
    PropsUi.setLook(wIgnoreEmptyFile);
    wIgnoreEmptyFile.setToolTipText(
        BaseMessages.getString(PKG, "YamlInputDialog.IgnoreEmptyFile.Tooltip"));
    FormData fdIgnoreEmptyFile = new FormData();
    fdIgnoreEmptyFile.left = new FormAttachment(middle, 0);
    wIgnoreEmptyFile.setLayoutData(fdIgnoreEmptyFile);
    wIgnoreEmptyFile.addSelectionListener(new ComponentSelectionListener(input));

    // do not fail if no files?
    wlDoNotFailIfNoFile = new Label(wXmlConf, SWT.RIGHT);
    wlDoNotFailIfNoFile.setText(
        BaseMessages.getString(PKG, "YamlInputDialog.doNotFailIfNoFile.Label"));
    PropsUi.setLook(wlDoNotFailIfNoFile);
    FormData fdlDoNotFailIfNoFile = new FormData();
    fdlDoNotFailIfNoFile.left = new FormAttachment(0, 0);
    fdlDoNotFailIfNoFile.top = new FormAttachment(wIgnoreEmptyFile, margin);
    fdlDoNotFailIfNoFile.right = new FormAttachment(middle, -margin);
    wlDoNotFailIfNoFile.setLayoutData(fdlDoNotFailIfNoFile);
    wDoNotFailIfNoFile = new Button(wXmlConf, SWT.CHECK);
    PropsUi.setLook(wDoNotFailIfNoFile);
    wDoNotFailIfNoFile.setToolTipText(
        BaseMessages.getString(PKG, "YamlInputDialog.doNotFailIfNoFile.Tooltip"));
    FormData fdDoNotFailIfNoFile = new FormData();
    fdDoNotFailIfNoFile.left = new FormAttachment(middle, 0);
    fdDoNotFailIfNoFile.top = new FormAttachment(wlDoNotFailIfNoFile, 0, SWT.CENTER);
    wDoNotFailIfNoFile.setLayoutData(fdDoNotFailIfNoFile);
    wDoNotFailIfNoFile.addSelectionListener(new ComponentSelectionListener(input));

    wlLimit = new Label(wXmlConf, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "YamlInputDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wDoNotFailIfNoFile, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wXmlConf, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    wLimit.addModifyListener(lsMod);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wDoNotFailIfNoFile, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    FormData fdXmlConf = new FormData();
    fdXmlConf.left = new FormAttachment(0, margin);
    fdXmlConf.top = new FormAttachment(0, margin);
    fdXmlConf.right = new FormAttachment(100, -margin);
    wXmlConf.setLayoutData(fdXmlConf);

    // ///////////////////////////////////////////////////////////
    // / END OF XmlConf Field GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF Additional Fields GROUP //
    // ///////////////////////////////

    Group wAdditionalFields = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAdditionalFields);
    wAdditionalFields.setText(
        BaseMessages.getString(PKG, "YamlInputDialog.wAdditionalFields.Label"));

    FormLayout additionalFieldsGroupLayout = new FormLayout();
    additionalFieldsGroupLayout.marginWidth = 10;
    additionalFieldsGroupLayout.marginHeight = 10;
    wAdditionalFields.setLayout(additionalFieldsGroupLayout);

    wlInclFilename = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclFilename.setText(BaseMessages.getString(PKG, "YamlInputDialog.InclFilename.Label"));
    PropsUi.setLook(wlInclFilename);
    FormData fdlInclFilename = new FormData();
    fdlInclFilename.left = new FormAttachment(0, 0);
    fdlInclFilename.right = new FormAttachment(middle, -margin);
    wlInclFilename.setLayoutData(fdlInclFilename);
    wInclFilename = new Button(wAdditionalFields, SWT.CHECK);
    PropsUi.setLook(wInclFilename);
    wInclFilename.setToolTipText(
        BaseMessages.getString(PKG, "YamlInputDialog.InclFilename.Tooltip"));
    FormData fdInclFilename = new FormData();
    fdInclFilename.left = new FormAttachment(middle, 0);
    wInclFilename.setLayoutData(fdInclFilename);
    wInclFilename.addSelectionListener(new ComponentSelectionListener(input));

    wlInclFilenameField = new Label(wAdditionalFields, SWT.LEFT);
    wlInclFilenameField.setText(
        BaseMessages.getString(PKG, "YamlInputDialog.InclFilenameField.Label"));
    PropsUi.setLook(wlInclFilenameField);
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment(wInclFilename, margin);
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField =
        new TextVar(variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclFilenameField);
    wInclFilenameField.addModifyListener(lsMod);
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment(wlInclFilenameField, margin);
    fdInclFilenameField.right = new FormAttachment(100, 0);
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclRowNum = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRowNum.setText(BaseMessages.getString(PKG, "YamlInputDialog.InclRownum.Label"));
    PropsUi.setLook(wlInclRowNum);
    FormData fdlInclRowNum = new FormData();
    fdlInclRowNum.left = new FormAttachment(0, 0);
    fdlInclRowNum.top = new FormAttachment(wInclFilenameField, margin);
    fdlInclRowNum.right = new FormAttachment(middle, -margin);
    wlInclRowNum.setLayoutData(fdlInclRowNum);
    wInclRowNum = new Button(wAdditionalFields, SWT.CHECK);
    PropsUi.setLook(wInclRowNum);
    wInclRowNum.setToolTipText(BaseMessages.getString(PKG, "YamlInputDialog.InclRownum.Tooltip"));
    FormData fdRowNum = new FormData();
    fdRowNum.left = new FormAttachment(middle, 0);
    fdRowNum.top = new FormAttachment(wlInclRowNum, 0, SWT.CENTER);
    wInclRowNum.setLayoutData(fdRowNum);
    wInclRowNum.addSelectionListener(new ComponentSelectionListener(input));

    wlInclRowNumField = new Label(wAdditionalFields, SWT.RIGHT);
    wlInclRowNumField.setText(BaseMessages.getString(PKG, "YamlInputDialog.InclRownumField.Label"));
    PropsUi.setLook(wlInclRowNumField);
    FormData fdlInclRowNumField = new FormData();
    fdlInclRowNumField.left = new FormAttachment(wInclRowNum, margin);
    fdlInclRowNumField.top = new FormAttachment(wInclFilenameField, margin);
    wlInclRowNumField.setLayoutData(fdlInclRowNumField);
    wInclRowNumField =
        new TextVar(variables, wAdditionalFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRowNumField);
    wInclRowNumField.addModifyListener(lsMod);
    FormData fdInclRowNumField = new FormData();
    fdInclRowNumField.left = new FormAttachment(wlInclRowNumField, margin);
    fdInclRowNumField.top = new FormAttachment(wInclFilenameField, margin);
    fdInclRowNumField.right = new FormAttachment(100, 0);
    wInclRowNumField.setLayoutData(fdInclRowNumField);

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment(0, margin);
    fdAdditionalFields.top = new FormAttachment(wXmlConf, margin);
    fdAdditionalFields.right = new FormAttachment(100, -margin);
    wAdditionalFields.setLayoutData(fdAdditionalFields);

    // ///////////////////////////////////////////////////////////
    // / END OF Additional Fields GROUP
    // ///////////////////////////////////////////////////////////

    // ///////////////////////////////
    // START OF AddFileResult GROUP //
    // ///////////////////////////////

    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAddFileResult);
    wAddFileResult.setText(BaseMessages.getString(PKG, "YamlInputDialog.wAddFileResult.Label"));

    FormLayout addFileResultGroupLayout = new FormLayout();
    addFileResultGroupLayout.marginWidth = 10;
    addFileResultGroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(addFileResultGroupLayout);

    wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "YamlInputDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "YamlInputDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wAdditionalFields, margin);
    fdAddFileResult.right = new FormAttachment(100, -margin);
    wAddFileResult.setLayoutData(fdAddFileResult);

    // ///////////////////////////////////////////////////////////
    // / END OF AddFileResult GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Fields tab...
    //
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "YamlInputDialog.Fields.Tab"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);
    PropsUi.setLook(wFieldsComp);

    wGet = new Button(wFieldsComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "YamlInputDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(50, 0);
    fdGet.bottom = new FormAttachment(100, 0);
    wGet.setLayoutData(fdGet);

    final int FieldsRows = input.getInputFields().size();

    ColumnInfo[] colInf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.XPath.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              3),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Group.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              YamlInputField.TRIM_TYPE_DESC,
              true)
        };

    colInf[0].setUsingVariables(true);
    colInf[0].setToolTip(
        BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.Name.Column.Tooltip"));
    colInf[1].setUsingVariables(true);
    colInf[1].setToolTip(
        BaseMessages.getString(PKG, "YamlInputDialog.FieldsTable.XPath.Column.Tooltip"));

    wFields =
        new TableView(
            variables,
            wFieldsComp,
            SWT.FULL_SELECTION | SWT.MULTI,
            colInf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wGet.addListener(SWT.Selection, e -> get());

    // Add the file to the list of files...
    SelectionAdapter selA =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wFilenameList.add(wFilename.getText(), wFilemask.getText());
            wFilename.setText("");
            wFilemask.setText("");
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
            wFilenameList.optWidth(true);
          }
        };
    wbaFilename.addSelectionListener(selA);
    wFilename.addSelectionListener(selA);

    // Delete files from the list of files...
    wbdFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int[] idx = wFilenameList.getSelectionIndices();
            wFilenameList.remove(idx);
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
          }
        });

    // Edit the selected file & remove from the list...
    wbeFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            int idx = wFilenameList.getSelectionIndex();
            if (idx >= 0) {
              String[] string = wFilenameList.getItem(idx);
              wFilename.setText(string[0]);
              wFilemask.setText(string[1]);
              wFilenameList.remove(idx);
            }
            wFilenameList.removeEmptyRows();
            wFilenameList.setRowNums();
          }
        });

    // Show the files that are selected at this time...
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            try {
              YamlInputMeta im = new YamlInputMeta();
              getInfo(im);
              FileInputList fileInputList = im.getFiles(variables);
              String[] files = fileInputList.getFileStrings();
              if (files != null && files.length > 0) {
                EnterSelectionDialog esd =
                    new EnterSelectionDialog(
                        shell,
                        files,
                        BaseMessages.getString(
                            PKG, "YamlInputDialog.FilesReadSelection.DialogTitle"),
                        BaseMessages.getString(
                            PKG, "YamlInputDialog.FilesReadSelection.DialogMessage"));
                esd.setViewOnly();
                esd.open();
              } else {
                MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
                mb.setMessage(
                    BaseMessages.getString(PKG, "YamlInputDialog.NoFileFound.DialogMessage"));
                mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
                mb.open();
              }
            } catch (Exception ex) {
              new ErrorDialog(
                  shell,
                  BaseMessages.getString(
                      PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_TITLE),
                  BaseMessages.getString(
                      PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_MESSAGE),
                  ex);
            }
          }
        });
    // Enable/disable the right fields to allow a filename to be added to each row...
    wInclFilename.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setIncludeFilename();
          }
        });

    // Enable/disable the right fields to allow a row number to be added to each row...
    wInclRowNum.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            setIncludeRowNum();
          }
        });

    // Whenever something changes, set the tooltip to the expanded version of the filename:
    wFilename.addModifyListener(e -> wFilename.setToolTipText(wFilename.getText()));

    // Listen to the Browse... button
    wbbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.yaml;*.YAML;*.yml;*.YML", "*"},
                new String[] {
                  "Yaml files", BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));
    wTabFolder.setSelection(0);

    getData(input);
    activateStreamField();
    setIncludeFilename();
    setIncludeRowNum();
    input.setChanged(changed);
    wFields.optWidth(true);

    focusTransformName();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setXMLStreamField() {
    try {
      wYAMLLField.removeAll();

      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        r.getFieldNames();

        for (int i = 0; i < r.getFieldNames().length; i++) {
          wYAMLLField.add(r.getFieldNames()[i]);
        }
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "YamlInputDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "YamlInputDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  private void activateStreamField() {
    wlYamlField.setEnabled(wYAMLStreamField.getSelection());
    wYAMLLField.setEnabled(wYAMLStreamField.getSelection());
    wlYamlIsAFile.setEnabled(wYAMLStreamField.getSelection());
    wYAMLIsAFile.setEnabled(wYAMLStreamField.getSelection());

    wlFilename.setEnabled(!wYAMLStreamField.getSelection());
    wbbFilename.setEnabled(!wYAMLStreamField.getSelection());
    wbaFilename.setEnabled(!wYAMLStreamField.getSelection());
    wFilename.setEnabled(!wYAMLStreamField.getSelection());
    wlFilemask.setEnabled(!wYAMLStreamField.getSelection());
    wFilemask.setEnabled(!wYAMLStreamField.getSelection());
    wlFilenameList.setEnabled(!wYAMLStreamField.getSelection());
    wbdFilename.setEnabled(!wYAMLStreamField.getSelection());
    wbeFilename.setEnabled(!wYAMLStreamField.getSelection());
    wbShowFiles.setEnabled(!wYAMLStreamField.getSelection());
    wlFilenameList.setEnabled(!wYAMLStreamField.getSelection());
    wFilenameList.setEnabled(!wYAMLStreamField.getSelection());
    wInclFilename.setEnabled(!wYAMLStreamField.getSelection());
    wlInclFilename.setEnabled(!wYAMLStreamField.getSelection());

    if (wYAMLStreamField.getSelection()) {
      wInclFilename.setSelection(false);
      wlInclFilenameField.setEnabled(false);
      wInclFilenameField.setEnabled(false);
    } else {
      wlInclFilenameField.setEnabled(wInclFilename.getSelection());
      wInclFilenameField.setEnabled(wInclFilename.getSelection());
    }

    wAddResult.setEnabled(!wYAMLStreamField.getSelection());
    wlAddResult.setEnabled(!wYAMLStreamField.getSelection());
    wLimit.setEnabled(!wYAMLStreamField.getSelection());
    wlLimit.setEnabled(!wYAMLStreamField.getSelection());
    wPreview.setEnabled(!wYAMLStreamField.getSelection());
    wGet.setEnabled(!wYAMLStreamField.getSelection());
    enableFileSettings();
  }

  private void enableFileSettings() {
    boolean active =
        !wYAMLStreamField.getSelection()
            || (wYAMLStreamField.getSelection() && wYAMLIsAFile.getSelection());
    wlIgnoreEmptyFile.setEnabled(active);
    wIgnoreEmptyFile.setEnabled(active);
    wlDoNotFailIfNoFile.setEnabled(active);
    wDoNotFailIfNoFile.setEnabled(active);
  }

  private void get() {
    YamlReader yaml = null;
    try {
      YamlInputMeta meta = new YamlInputMeta();
      getInfo(meta);

      FileInputList inputList = meta.getFiles(variables);

      if (!inputList.getFiles().isEmpty()) {
        wFields.removeAll();

        yaml = new YamlReader();
        yaml.loadFile(inputList.getFile(0));
        RowMeta row = yaml.getFields();

        for (int i = 0; i < row.size(); i++) {
          IValueMeta value = row.getValueMeta(i);

          TableItem item = new TableItem(wFields.table, SWT.NONE);
          item.setText(1, value.getName());
          item.setText(2, value.getName());
          item.setText(3, value.getTypeDesc());
        }
        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth(true);
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_TITLE),
          BaseMessages.getString(PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_MESSAGE),
          e);
    } finally {
      if (yaml != null) {
        try {
          yaml.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  public void setIncludeFilename() {
    wlInclFilenameField.setEnabled(wInclFilename.getSelection());
    wInclFilenameField.setEnabled(wInclFilename.getSelection());
  }

  public void setIncludeRowNum() {
    wlInclRowNumField.setEnabled(wInclRowNum.getSelection());
    wInclRowNumField.setEnabled(wInclRowNum.getSelection());
  }

  /**
   * Read the data from the TextFileInputMeta object and show it in this dialog.
   *
   * @param in The TextFileInputMeta object to obtain the data from.
   */
  public void getData(YamlInputMeta in) {
    int index = 0;
    for (YamlInputMeta.YamlFile file : in.getYamlFiles()) {
      TableItem item = wFilenameList.table.getItem(index++);
      item.setText(1, Const.NVL(file.getFilename(), ""));
      item.setText(2, Const.NVL(file.getFileMask(), ""));
      item.setText(3, file.isFileRequired() ? NO_YES_DEC[1] : NO_YES_DEC[0]);
      item.setText(4, file.isIncludingSubFolders() ? NO_YES_DEC[1] : NO_YES_DEC[0]);
    }
    wFilenameList.optimizeTableView();

    wInclFilename.setSelection(in.isIncludeFilename());
    wInclRowNum.setSelection(in.isIncludeRowNumber());
    wAddResult.setSelection(in.isAddingResultFile());
    wIgnoreEmptyFile.setSelection(in.isIgnoringEmptyFile());
    wDoNotFailIfNoFile.setSelection(in.isDoNotFailIfNoFile());
    wYAMLStreamField.setSelection(in.isInFields());
    wYAMLIsAFile.setSelection(in.isSourceFile());
    wYAMLLField.setText(Const.NVL(in.getYamlField(), ""));
    wInclFilenameField.setText(Const.NVL(in.getFilenameField(), ""));
    wInclRowNumField.setText(Const.NVL(in.getRowNumberField(), ""));
    wLimit.setText("" + in.getRowLimit());

    if (isDebug()) {
      logDebug(BaseMessages.getString(PKG, "YamlInputDialog.Log.GettingFieldsInfo"));
    }
    for (int i = 0; i < in.getInputFields().size(); i++) {
      YamlInputField field = in.getInputFields().get(i);
      TableItem item = wFields.table.getItem(i);
      String length = "" + field.getLength();
      String precision = "" + field.getPrecision();
      String decimalSymbol = field.getDecimalSymbol();

      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getPath(), ""));
      item.setText(3, Const.NVL(field.getTypeDesc(), ""));
      item.setText(4, Const.NVL(field.getFormat(), ""));
      if (!"-1".equals(length)) {
        item.setText(5, length);
      }
      if (!"-1".equals(precision)) {
        item.setText(6, precision);
      }
      item.setText(7, Const.NVL(field.getCurrencySymbol(), ""));
      if (decimalSymbol != null) {
        item.setText(8, decimalSymbol);
      }
      item.setText(9, Const.NVL(field.getGroupSymbol(), ""));
      item.setText(10, Const.NVL(field.getTrimTypeDesc(), ""));
    }
    wFields.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    try {
      getInfo(input);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_TITLE),
          BaseMessages.getString(PKG, CONST_YAML_INPUT_DIALOG_ERROR_PARSING_DATA_DIALOG_MESSAGE),
          e);
    }
    dispose();
  }

  private void getInfo(YamlInputMeta in) {
    // return value
    transformName = wTransformName.getText();

    // copy info to TextFileInputMeta class (input)
    in.setRowLimit(Const.toLong(wLimit.getText(), 0L));
    in.setFilenameField(wInclFilenameField.getText());
    in.setRowNumberField(wInclRowNumField.getText());
    in.setAddingResultFile(wAddResult.getSelection());
    in.setIncludeFilename(wInclFilename.getSelection());
    in.setIncludeRowNumber(wInclRowNum.getSelection());
    in.setIgnoringEmptyFile(wIgnoreEmptyFile.getSelection());
    in.setDoNotFailIfNoFile(wDoNotFailIfNoFile.getSelection());

    in.setInFields(wYAMLStreamField.getSelection());
    in.setSourceFile(wYAMLIsAFile.getSelection());
    in.setYamlField(wYAMLLField.getText());

    in.getYamlFiles().clear();
    for (TableItem item : wFilenameList.getNonEmptyItems()) {
      YamlInputMeta.YamlFile file = new YamlInputMeta.YamlFile();
      file.setFilename(item.getText(1));
      file.setFileMask(item.getText(2));
      file.setFileRequired(NO_YES_DEC[1].equalsIgnoreCase(item.getText(3)));
      file.setIncludingSubFolders(NO_YES_DEC[1].equalsIgnoreCase(item.getText(4)));
      in.getYamlFiles().add(file);
    }

    in.getInputFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      YamlInputField field = new YamlInputField();

      field.setName(item.getText(1));
      field.setPath(item.getText(2));
      field.setType(ValueMetaFactory.getIdForValueMeta(item.getText(3)));
      field.setFormat(item.getText(4));
      field.setLength(Const.toInt(item.getText(5), -1));
      field.setPrecision(Const.toInt(item.getText(6), -1));
      field.setCurrencySymbol(item.getText(7));
      field.setDecimalSymbol(item.getText(8));
      field.setGroupSymbol(item.getText(9));
      field.setTrimType(YamlInputField.getTrimTypeByDesc(item.getText(10)));

      in.getInputFields().add(field);
    }
  }

  // Preview the data
  private void preview() {
    try {
      // Create the XML input transform
      YamlInputMeta oneMeta = new YamlInputMeta();
      getInfo(oneMeta);

      EnterNumberDialog numberDialog =
          new EnterNumberDialog(
              shell,
              props.getDefaultPreviewSize(),
              BaseMessages.getString(PKG, "YamlInputDialog.NumberRows.DialogTitle"),
              BaseMessages.getString(PKG, "YamlInputDialog.NumberRows.DialogMessage"));

      int previewSize = numberDialog.open();
      if (previewSize > 0) {
        oneMeta.setRowLimit(previewSize);
        PipelineMeta previewMeta =
            PipelinePreviewFactory.generatePreviewPipeline(
                pipelineMeta.getMetadataProvider(), oneMeta, wTransformName.getText());

        PipelinePreviewProgressDialog progressDialog =
            new PipelinePreviewProgressDialog(
                shell,
                variables,
                previewMeta,
                new String[] {wTransformName.getText()},
                new int[] {previewSize});
        progressDialog.open();

        if (!progressDialog.isCancelled()) {
          Pipeline pipeline = progressDialog.getPipeline();
          String loggingText = progressDialog.getLoggingText();

          if (pipeline.getResult() != null && pipeline.getResult().getNrErrors() > 0) {
            EnterTextDialog etd =
                new EnterTextDialog(
                    shell,
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Title"),
                    BaseMessages.getString(PKG, "System.Dialog.PreviewError.Message"),
                    loggingText,
                    true);
            etd.setReadOnly();
            etd.open();
          }
          PreviewRowsDialog prd =
              new PreviewRowsDialog(
                  shell,
                  variables,
                  SWT.NONE,
                  wTransformName.getText(),
                  progressDialog.getPreviewRowsMeta(wTransformName.getText()),
                  progressDialog.getPreviewRows(wTransformName.getText()),
                  loggingText);
          prd.open();
        }
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "YamlInputDialog.ErrorPreviewingData.DialogTitle"),
          BaseMessages.getString(PKG, "YamlInputDialog.ErrorPreviewingData.DialogMessage"),
          e);
    }
  }
}
