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

package org.apache.hop.pipeline.transforms.excelinput;

import static org.apache.hop.pipeline.transforms.excelinput.ExcelInputMeta.EIFile;
import static org.apache.hop.pipeline.transforms.excelinput.ExcelInputMeta.EISheet;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.fileinput.FileInputList;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.spreadsheet.IKCell;
import org.apache.hop.core.spreadsheet.IKSheet;
import org.apache.hop.core.spreadsheet.IKWorkbook;
import org.apache.hop.core.spreadsheet.KCellType;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.PipelinePreviewFactory;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transforms.fileinput.text.DirectoryDialogButtonListenerFactory;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterListDialog;
import org.apache.hop.ui.core.dialog.EnterNumberDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.EnterTextDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.PreviewRowsDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.core.widget.VariableButtonListenerFactory;
import org.apache.hop.ui.pipeline.dialog.PipelinePreviewProgressDialog;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class ExcelInputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExcelInputMeta.class;

  private static final String CONST_COMBO_NO = "System.Combo.No";
  private static final String CONST_COMBO_YES = "System.Combo.Yes";
  private static final String CONST_ERROR_TITLE = "System.Dialog.Error.Title";
  private static final String CONST_BUTTON_BROWSE = "System.Button.Browse";
  private static final String CONST_BUTTON_VARIABLE = "System.Button.Variable";
  private static final String CONST_LABEL_EXTENSION = "System.Label.Extension";
  private static final String CONST_BROWSE_FOR_DIR = "System.Tooltip.BrowseForDir";
  private static final String CONST_BROWSE_TO_DIR = "System.Tooltip.VariableToDir";

  private static final String[] YES_NO_COMBO =
      new String[] {
        BaseMessages.getString(PKG, CONST_COMBO_NO), BaseMessages.getString(PKG, CONST_COMBO_YES)
      };

  private CTabFolder wTabFolder;

  private CTabItem wFileTab;
  private CTabItem wSheetTab;
  private CTabItem wFieldsTab;

  private Label wlStatusMessage;

  private Label wlFilenameList;
  private TableView wFilenameList;

  private Button wAccFilenames;

  private Label wlAccField;
  private CCombo wAccField;

  private Label wlAccTransform;
  private CCombo wAccTransform;

  private Button wbShowFiles;

  private TableView wSheetNameList;

  private Button wHeader;

  private Button wNoEmpty;

  private Button wStopOnEmpty;

  private Text wInclFilenameField;

  private Text wInclSheetNameField;

  private Text wInclRowNumField;

  private Text wInclSheetRowNumField;

  private Text wLimit;

  private CCombo wSpreadSheetType;

  private CCombo wEncoding;

  private Button wbGetFields;

  private TableView wFields;

  private Button wStrictTypes;

  private Button wErrorIgnored;

  private Label wlSkipErrorLines;

  private MetaSelectionLine<SchemaDefinition> wSchemaDefinition;

  private Button wIgnoreFields;

  private Button wSkipErrorLines;

  // New entries for intelligent error handling AKA replay functionality
  // Bad files destination directory
  private Label wlWarningDestDir;
  private Button wbbWarningDestDir; // Browse: add file or directory
  private Button wbvWarningDestDir; // Variable
  private TextVar wWarningDestDir;
  private Label wlWarningExt;
  private Text wWarningExt;

  // Error messages files destination directory
  private Label wlErrorDestDir;
  private Button wbbErrorDestDir; // Browse: add file or directory
  private Button wbvErrorDestDir; // Variable
  private TextVar wErrorDestDir;
  private Label wlErrorExt;
  private Text wErrorExt;

  // Line numbers files destination directory
  private Label wlLineNrDestDir;
  private Button wbbLineNrDestDir; // Browse: add file or directory
  private Button wbvLineNrDestDir; // Variable
  private TextVar wLineNrDestDir;
  private Label wlLineNrExt;
  private Text wLineNrExt;

  private final ExcelInputMeta input;
  private int middle;
  private int margin;
  private boolean gotEncodings = false;

  private Button wAddResult;

  private Text wShortFileFieldName;
  private Text wPathFieldName;

  private Text wIsHiddenName;
  private Text wLastModificationTimeName;
  private Text wUriName;
  private Text wRootUriName;
  private Text wExtensionFieldName;
  private Text wSizeFieldName;

  public ExcelInputDialog(
      Shell parent, IVariables variables, ExcelInputMeta transformMeta, PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ExcelInputDialog.DialogTitle"));

    middle = props.getMiddlePct();
    margin = PropsUi.getMargin();

    // Buttons at the bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wPreview = new Button(shell, SWT.PUSH);
    wPreview.setText(BaseMessages.getString(PKG, "ExcelInputDialog.PreviewRows.Button"));
    wPreview.addListener(SWT.Selection, e -> preview());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wPreview, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Status Message
    wlStatusMessage = new Label(shell, SWT.RIGHT);
    wlStatusMessage.setText("(This Space To Let)");
    wlStatusMessage.setForeground(shell.getDisplay().getSystemColor(SWT.COLOR_RED));
    PropsUi.setLook(wlStatusMessage);
    FormData fdlStatusMessage = new FormData();
    fdlStatusMessage.left = new FormAttachment(0, 0);
    fdlStatusMessage.top = new FormAttachment(wlTransformName, margin);
    fdlStatusMessage.right = new FormAttachment(middle, -margin);
    wlStatusMessage.setLayoutData(fdlStatusMessage);

    // Tabs
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    //
    // START OF FILE TAB /
    //
    wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "ExcelInputDialog.FileTab.TabTitle"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = PropsUi.getFormMargin();
    fileLayout.marginHeight = PropsUi.getFormMargin();
    wFileComp.setLayout(fileLayout);

    // spreadsheet engine type
    Label wlSpreadSheetType = new Label(wFileComp, SWT.RIGHT);
    wlSpreadSheetType.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.SpreadSheetType.Label"));
    PropsUi.setLook(wlSpreadSheetType);
    FormData fdlSpreadSheetType = new FormData();
    fdlSpreadSheetType.left = new FormAttachment(0, 0);
    fdlSpreadSheetType.right = new FormAttachment(middle, -margin);
    fdlSpreadSheetType.top = new FormAttachment(0, 0);

    wlSpreadSheetType.setLayoutData(fdlSpreadSheetType);
    wSpreadSheetType = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
    wSpreadSheetType.setEditable(true);
    PropsUi.setLook(wSpreadSheetType);
    FormData fdSpreadSheetType = new FormData();
    fdSpreadSheetType.left = new FormAttachment(middle, 0);
    fdSpreadSheetType.right = new FormAttachment(100, 0);
    fdSpreadSheetType.top = new FormAttachment(0, 0);
    wSpreadSheetType.setLayoutData(fdSpreadSheetType);
    for (SpreadSheetType type : SpreadSheetType.values()) {
      wSpreadSheetType.add(type.getDescription());
    }

    // Filename list line
    wlFilenameList = new Label(wFileComp, SWT.LEFT);
    wlFilenameList.setText(BaseMessages.getString(PKG, "ExcelInputDialog.FilenameList.Label"));
    PropsUi.setLook(wlFilenameList);
    FormData fdlFilenameList = new FormData();
    fdlFilenameList.left = new FormAttachment(0, 0);
    fdlFilenameList.top = new FormAttachment(wSpreadSheetType, margin);
    fdlFilenameList.right = new FormAttachment(100, 0);
    wlFilenameList.setLayoutData(fdlFilenameList);

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "ExcelInputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.bottom = new FormAttachment(100, -margin);
    wbShowFiles.setLayoutData(fdbShowFiles);

    // Accepting filenames group
    //
    Group gAccepting = new Group(wFileComp, SWT.SHADOW_ETCHED_IN);
    gAccepting.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AcceptingGroup.Label"));
    FormLayout acceptingLayout = new FormLayout();
    acceptingLayout.marginWidth = 3;
    acceptingLayout.marginHeight = 3;
    gAccepting.setLayout(acceptingLayout);
    PropsUi.setLook(gAccepting);

    // Accept filenames from previous transforms?
    //
    Label wlAccFilenames = new Label(gAccepting, SWT.RIGHT);
    wlAccFilenames.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AcceptFilenames.Label"));
    PropsUi.setLook(wlAccFilenames);
    FormData fdlAccFilenames = new FormData();
    fdlAccFilenames.top = new FormAttachment(0, margin);
    fdlAccFilenames.left = new FormAttachment(0, 0);
    fdlAccFilenames.right = new FormAttachment(middle, -margin);
    wlAccFilenames.setLayoutData(fdlAccFilenames);
    wAccFilenames = new Button(gAccepting, SWT.CHECK);
    wAccFilenames.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.AcceptFilenames.Tooltip"));
    PropsUi.setLook(wAccFilenames);
    FormData fdAccFilenames = new FormData();
    fdAccFilenames.top = new FormAttachment(wlAccFilenames, 0, SWT.CENTER);
    fdAccFilenames.left = new FormAttachment(middle, 0);
    fdAccFilenames.right = new FormAttachment(100, 0);
    wAccFilenames.setLayoutData(fdAccFilenames);
    wAccFilenames.addListener(SWT.Selection, e -> setFlags());

    // Which transform to read from?
    wlAccTransform = new Label(gAccepting, SWT.RIGHT);
    wlAccTransform.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AcceptTransform.Label"));
    PropsUi.setLook(wlAccTransform);
    FormData fdlAccTransform = new FormData();
    fdlAccTransform.top = new FormAttachment(wAccFilenames, margin);
    fdlAccTransform.left = new FormAttachment(0, 0);
    fdlAccTransform.right = new FormAttachment(middle, -margin);
    wlAccTransform.setLayoutData(fdlAccTransform);
    wAccTransform = new CCombo(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wAccTransform.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.AcceptTransform.Tooltip"));
    PropsUi.setLook(wAccTransform);
    FormData fdAccTransform = new FormData();
    fdAccTransform.top = new FormAttachment(wAccFilenames, margin);
    fdAccTransform.left = new FormAttachment(middle, 0);
    fdAccTransform.right = new FormAttachment(100, 0);
    wAccTransform.setLayoutData(fdAccTransform);

    // Which field?
    //
    wlAccField = new Label(gAccepting, SWT.RIGHT);
    wlAccField.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AcceptField.Label"));
    PropsUi.setLook(wlAccField);
    FormData fdlAccField = new FormData();
    fdlAccField.top = new FormAttachment(wAccTransform, margin);
    fdlAccField.left = new FormAttachment(0, 0);
    fdlAccField.right = new FormAttachment(middle, -margin);
    wlAccField.setLayoutData(fdlAccField);

    wAccField = new CCombo(gAccepting, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    IRowMeta previousFields;
    try {
      previousFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopTransformException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ExcelInputDialog.ErrorDialog.UnableToGetInputFields.Title"),
          BaseMessages.getString(
              PKG, "ExcelInputDialog.ErrorDialog.UnableToGetInputFields.Message"),
          e);
      previousFields = new RowMeta();
    }
    wAccField.setItems(previousFields.getFieldNames());
    wAccField.setToolTipText(BaseMessages.getString(PKG, "ExcelInputDialog.AcceptField.Tooltip"));

    PropsUi.setLook(wAccField);
    FormData fdAccField = new FormData();
    fdAccField.top = new FormAttachment(wAccTransform, margin);
    fdAccField.left = new FormAttachment(middle, 0);
    fdAccField.right = new FormAttachment(100, 0);
    wAccField.setLayoutData(fdAccField);

    // Fill in the source transforms...
    List<TransformMeta> prevTransforms =
        pipelineMeta.findPreviousTransforms(pipelineMeta.findTransform(transformName));
    for (TransformMeta prevTransform : prevTransforms) {
      wAccTransform.add(prevTransform.getName());
    }

    FormData fdAccepting = new FormData();
    fdAccepting.left = new FormAttachment(0, 0);
    fdAccepting.right = new FormAttachment(100, 0);
    fdAccepting.bottom = new FormAttachment(wbShowFiles, -margin * 2);
    gAccepting.setLayoutData(fdAccepting);

    ColumnInfo[] colinfo = new ColumnInfo[5];
    colinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.FileDir.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT_BUTTON,
            false);
    colinfo[0].setUsingVariables(true);
    colinfo[0].setTextVarButtonSelectionListener(getFileSelectionAdapter());
    colinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.Wildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[1].setToolTip(BaseMessages.getString(PKG, "ExcelInputDialog.Wildcard.Tooltip"));
    colinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.Files.ExcludeWildcard.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    colinfo[2].setUsingVariables(true);
    colinfo[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.Required.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[3].setToolTip(BaseMessages.getString(PKG, "ExcelInputDialog.Required.Tooltip"));
    colinfo[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.IncludeSubDirs.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            YES_NO_COMBO);
    colinfo[4].setToolTip(BaseMessages.getString(PKG, "ExcelInputDialog.IncludeSubDirs.Tooltip"));

    wFilenameList =
        new TableView(
            variables,
            wFileComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            colinfo,
            input.getFiles().size(),
            null,
            props);
    PropsUi.setLook(wFilenameList);
    FormData fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(0, 0);
    fdFilenameList.right = new FormAttachment(100, 0);
    fdFilenameList.top = new FormAttachment(wlFilenameList, margin);
    fdFilenameList.bottom = new FormAttachment(gAccepting, -margin);
    wFilenameList.setLayoutData(fdFilenameList);
    wFilenameList.addModifyListener(event -> checkAlerts());

    FormData fdFileComp = new FormData();
    fdFileComp.left = new FormAttachment(0, 0);
    fdFileComp.top = new FormAttachment(0, 0);
    fdFileComp.right = new FormAttachment(100, 0);
    fdFileComp.bottom = new FormAttachment(100, 0);
    wFileComp.setLayoutData(fdFileComp);

    wFileComp.layout();
    wFileTab.setControl(wFileComp);

    //
    // / END OF FILE TAB
    //
    //
    // START OF SHEET TAB /
    //
    wSheetTab = new CTabItem(wTabFolder, SWT.NONE);
    wSheetTab.setFont(GuiResource.getInstance().getFontDefault());
    wSheetTab.setText(BaseMessages.getString(PKG, "ExcelInputDialog.SheetsTab.TabTitle"));

    Composite wSheetComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSheetComp);

    FormLayout sheetLayout = new FormLayout();
    sheetLayout.marginWidth = PropsUi.getFormMargin();
    sheetLayout.marginHeight = PropsUi.getFormMargin();
    wSheetComp.setLayout(sheetLayout);

    Button wbGetSheets = new Button(wSheetComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGetSheets);
    wbGetSheets.setText(BaseMessages.getString(PKG, "ExcelInputDialog.GetSheets.Button"));
    FormData fdbGetSheets = new FormData();
    fdbGetSheets.left = new FormAttachment(middle, 0);
    fdbGetSheets.bottom = new FormAttachment(100, -margin);
    wbGetSheets.setLayoutData(fdbGetSheets);

    Label wlSheetnameList = new Label(wSheetComp, SWT.RIGHT);
    wlSheetnameList.setText(BaseMessages.getString(PKG, "ExcelInputDialog.SheetNameList.Label"));
    PropsUi.setLook(wlSheetnameList);
    FormData fdlSheetnameList = new FormData();
    fdlSheetnameList.left = new FormAttachment(0, 0);
    fdlSheetnameList.top = new FormAttachment(wSpreadSheetType, margin);
    fdlSheetnameList.right = new FormAttachment(middle, -margin);
    wlSheetnameList.setLayoutData(fdlSheetnameList);

    ColumnInfo[] shinfo = new ColumnInfo[3];
    shinfo[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.SheetName.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    shinfo[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.StartRow.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);
    shinfo[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "ExcelInputDialog.StartColumn.Column"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    wSheetNameList =
        new TableView(
            variables,
            wSheetComp,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            shinfo,
            input.getSheets().size(),
            null,
            props);
    PropsUi.setLook(wSheetNameList);
    fdFilenameList = new FormData();
    fdFilenameList.left = new FormAttachment(middle, 0);
    fdFilenameList.right = new FormAttachment(100, 0);
    fdFilenameList.top = new FormAttachment(0, 0);
    fdFilenameList.bottom = new FormAttachment(wbGetSheets, -margin);
    wSheetNameList.setLayoutData(fdFilenameList);
    wSheetNameList.addModifyListener(event -> checkAlerts());

    FormData fdSheetComp = new FormData();
    fdSheetComp.left = new FormAttachment(0, 0);
    fdSheetComp.top = new FormAttachment(0, 0);
    fdSheetComp.right = new FormAttachment(100, 0);
    fdSheetComp.bottom = new FormAttachment(100, 0);
    wSheetComp.setLayoutData(fdSheetComp);

    wSheetComp.layout();
    wSheetTab.setControl(wSheetComp);

    //
    // / END OF SHEET TAB
    //
    //
    // START OF CONTENT TAB/
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "ExcelInputDialog.ContentTab.TabTitle"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // Header checkbox
    Label wlHeader = new Label(wContentComp, SWT.RIGHT);
    wlHeader.setText(BaseMessages.getString(PKG, "ExcelInputDialog.Header.Label"));
    PropsUi.setLook(wlHeader);
    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment(0, 0);
    fdlHeader.top = new FormAttachment(0, 0);
    fdlHeader.right = new FormAttachment(middle, -margin);
    wlHeader.setLayoutData(fdlHeader);
    wHeader = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wHeader);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(middle, 0);
    fdHeader.top = new FormAttachment(wlHeader, 0, SWT.CENTER);
    fdHeader.right = new FormAttachment(100, 0);
    wHeader.setLayoutData(fdHeader);
    wHeader.addListener(SWT.Selection, e -> setFlags());

    Label wlNoEmpty = new Label(wContentComp, SWT.RIGHT);
    wlNoEmpty.setText(BaseMessages.getString(PKG, "ExcelInputDialog.NoEmpty.Label"));
    PropsUi.setLook(wlNoEmpty);
    FormData fdlNoEmpty = new FormData();
    fdlNoEmpty.left = new FormAttachment(0, 0);
    fdlNoEmpty.top = new FormAttachment(wHeader, margin);
    fdlNoEmpty.right = new FormAttachment(middle, -margin);
    wlNoEmpty.setLayoutData(fdlNoEmpty);
    wNoEmpty = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wNoEmpty);
    wNoEmpty.setToolTipText(BaseMessages.getString(PKG, "ExcelInputDialog.NoEmpty.Tooltip"));
    FormData fdNoEmpty = new FormData();
    fdNoEmpty.left = new FormAttachment(middle, 0);
    fdNoEmpty.top = new FormAttachment(wlNoEmpty, 0, SWT.CENTER);
    fdNoEmpty.right = new FormAttachment(100, 0);
    wNoEmpty.setLayoutData(fdNoEmpty);
    wNoEmpty.addSelectionListener(new ComponentSelectionListener(input));

    Label wlStopOnEmpty = new Label(wContentComp, SWT.RIGHT);
    wlStopOnEmpty.setText(BaseMessages.getString(PKG, "ExcelInputDialog.StopOnEmpty.Label"));
    PropsUi.setLook(wlStopOnEmpty);
    FormData fdlStopOnEmpty = new FormData();
    fdlStopOnEmpty.left = new FormAttachment(0, 0);
    fdlStopOnEmpty.top = new FormAttachment(wNoEmpty, margin);
    fdlStopOnEmpty.right = new FormAttachment(middle, -margin);
    wlStopOnEmpty.setLayoutData(fdlStopOnEmpty);
    wStopOnEmpty = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wStopOnEmpty);
    wStopOnEmpty.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.StopOnEmpty.Tooltip"));
    FormData fdStopOnEmpty = new FormData();
    fdStopOnEmpty.left = new FormAttachment(middle, 0);
    fdStopOnEmpty.top = new FormAttachment(wlStopOnEmpty, 0, SWT.CENTER);
    fdStopOnEmpty.right = new FormAttachment(100, 0);
    wStopOnEmpty.setLayoutData(fdStopOnEmpty);
    wStopOnEmpty.addSelectionListener(new ComponentSelectionListener(input));

    Label wlLimit = new Label(wContentComp, SWT.RIGHT);
    wlLimit.setText(BaseMessages.getString(PKG, "ExcelInputDialog.Limit.Label"));
    PropsUi.setLook(wlLimit);
    FormData fdlLimit = new FormData();
    fdlLimit.left = new FormAttachment(0, 0);
    fdlLimit.top = new FormAttachment(wStopOnEmpty, margin);
    fdlLimit.right = new FormAttachment(middle, -margin);
    wlLimit.setLayoutData(fdlLimit);
    wLimit = new Text(wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLimit);
    FormData fdLimit = new FormData();
    fdLimit.left = new FormAttachment(middle, 0);
    fdLimit.top = new FormAttachment(wStopOnEmpty, margin);
    fdLimit.right = new FormAttachment(100, 0);
    wLimit.setLayoutData(fdLimit);

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "ExcelInputDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wLimit, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wLimit, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          setEncodings();
          shell.setCursor(null);
          busy.dispose();
        });
    wEncoding.layout();

    //
    // START OF AddFileResult GROUP
    //
    Group wAddFileResult = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wAddFileResult);
    wAddFileResult.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AddFileResult.Label"));

    FormLayout addFileResultgroupLayout = new FormLayout();
    addFileResultgroupLayout.marginWidth = 10;
    addFileResultgroupLayout.marginHeight = 10;
    wAddFileResult.setLayout(addFileResultgroupLayout);

    Label wlAddResult = new Label(wAddFileResult, SWT.RIGHT);
    wlAddResult.setText(BaseMessages.getString(PKG, "ExcelInputDialog.AddResult.Label"));
    PropsUi.setLook(wlAddResult);
    FormData fdlAddResult = new FormData();
    fdlAddResult.left = new FormAttachment(0, 0);
    fdlAddResult.top = new FormAttachment(wEncoding, margin);
    fdlAddResult.right = new FormAttachment(middle, -margin);
    wlAddResult.setLayoutData(fdlAddResult);
    wAddResult = new Button(wAddFileResult, SWT.CHECK);
    PropsUi.setLook(wAddResult);
    wAddResult.setToolTipText(BaseMessages.getString(PKG, "ExcelInputDialog.AddResult.Tooltip"));
    FormData fdAddResult = new FormData();
    fdAddResult.left = new FormAttachment(middle, 0);
    fdAddResult.top = new FormAttachment(wlAddResult, 0, SWT.CENTER);
    wAddResult.setLayoutData(fdAddResult);
    wAddResult.addSelectionListener(new ComponentSelectionListener(input));

    FormData fdAddFileResult = new FormData();
    fdAddFileResult.left = new FormAttachment(0, margin);
    fdAddFileResult.top = new FormAttachment(wEncoding, margin);
    fdAddFileResult.right = new FormAttachment(100, -margin);
    wAddFileResult.setLayoutData(fdAddFileResult);

    //
    // / END OF AddFileResult GROUP
    //
    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    fdContentComp.bottom = new FormAttachment(100, 0);
    wContentComp.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    //
    // / END OF CONTENT TAB
    //
    //
    // / START OF CONTENT TAB
    //
    addErrorTab();

    // Fields tab...
    //
    wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "ExcelInputDialog.FieldsTab.TabTitle"));

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            fillFieldsLayoutFromSchema();
            input.setChanged();
          }
        };

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    wFieldsComp.setLayout(fieldsLayout);

    wSchemaDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SchemaDefinition.class,
            wFieldsComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "ExcelInputDialog.SchemaDefinition.Label"),
            BaseMessages.getString(PKG, "ExcelInputDialog.SchemaDefinition.Tooltip"));

    PropsUi.setLook(wSchemaDefinition);
    FormData fdSchemaDefinition = new FormData();
    fdSchemaDefinition.left = new FormAttachment(0, 0);
    fdSchemaDefinition.top = new FormAttachment(0, margin);
    fdSchemaDefinition.right = new FormAttachment(100, 0);
    wSchemaDefinition.setLayoutData(fdSchemaDefinition);

    try {
      wSchemaDefinition.fillItems();
    } catch (Exception e) {
      log.logError("Error getting schema definition items", e);
    }

    wSchemaDefinition.addSelectionListener(lsSelection);

    // Ignore manual schema
    //
    Label wlIgnoreFields = new Label(wFieldsComp, SWT.RIGHT);
    PropsUi.setLook(wlIgnoreFields);
    wlIgnoreFields.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.IgnoreTransformFields.Label"));
    FormData fdlIgnoreFields = new FormData();
    fdlIgnoreFields.left = new FormAttachment(0, 0);
    fdlIgnoreFields.right = new FormAttachment(middle, -margin);
    fdlIgnoreFields.top = new FormAttachment(wSchemaDefinition, margin);
    wlIgnoreFields.setLayoutData(fdlIgnoreFields);
    wIgnoreFields = new Button(wFieldsComp, SWT.CHECK | SWT.LEFT);
    PropsUi.setLook(wIgnoreFields);
    FormData fdIgnoreFields = new FormData();
    fdIgnoreFields.left = new FormAttachment(middle, 0);
    fdIgnoreFields.right = new FormAttachment(100, 0);
    fdIgnoreFields.top = new FormAttachment(wlIgnoreFields, 0, SWT.CENTER);
    wIgnoreFields.setLayoutData(fdIgnoreFields);
    wIgnoreFields.addListener(SWT.Selection, e -> setFlags());

    Group wManualSchemaDefinition = new Group(wFieldsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wManualSchemaDefinition);
    wManualSchemaDefinition.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.ManualSchemaDefinition.Label"));

    FormLayout manualSchemaDefinitionLayout = new FormLayout();
    manualSchemaDefinitionLayout.marginWidth = 10;
    manualSchemaDefinitionLayout.marginHeight = 10;
    wManualSchemaDefinition.setLayout(manualSchemaDefinitionLayout);

    wbGetFields = new Button(wManualSchemaDefinition, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGetFields);
    wbGetFields.setText(BaseMessages.getString(PKG, "ExcelInputDialog.GetFields.Button"));

    setButtonPositions(new Button[] {wbGetFields}, margin, null);

    final int FieldsRows = input.getFields().size();
    int fieldsWidth = 600;
    int fieldsHeight = 150;

    ColumnInfo[] colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Name.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Type.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Length.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Precision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.TrimType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              IValueMeta.TrimType.getDescriptions()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Repeat.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              BaseMessages.getString(PKG, CONST_COMBO_YES),
              BaseMessages.getString(PKG, CONST_COMBO_NO)),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Format.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Currency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Decimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelInputDialog.Grouping.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT)
        };

    colinf[5].setToolTip(BaseMessages.getString(PKG, "ExcelInputDialog.Repeat.Tooltip"));

    wFields =
        new TableView(
            variables,
            wManualSchemaDefinition,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            colinf,
            FieldsRows,
            null,
            props);
    wFields.setSize(fieldsWidth, fieldsHeight);
    wFields.addModifyListener(event -> checkAlerts());

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wbGetFields, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();

    FormData fdManualSchemaDefinitionComp = new FormData();
    fdManualSchemaDefinitionComp.left = new FormAttachment(0, 0);
    fdManualSchemaDefinitionComp.top = new FormAttachment(wIgnoreFields, 0);
    fdManualSchemaDefinitionComp.right = new FormAttachment(100, 0);
    fdManualSchemaDefinitionComp.bottom = new FormAttachment(100, 0);
    wManualSchemaDefinition.setLayoutData(fdManualSchemaDefinitionComp);

    wFieldsTab.setControl(wFieldsComp);
    PropsUi.setLook(wFieldsComp);

    addAdditionalFieldsTab();

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wlStatusMessage, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Show the files that are selected at this time...
    wbShowFiles.addListener(SWT.Selection, e -> showFiles());

    // Get a list of the sheet names.
    wbGetSheets.addListener(SWT.Selection, e -> getSheets());
    wbGetFields.addListener(SWT.Selection, e -> getFields());

    wTabFolder.setSelection(0);

    getData(input);
    wFields.optWidth(true);
    checkAlerts(); // resyncing after setup

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void fillFieldsLayoutFromSchema() {

    if (!wSchemaDefinition.isDisposed()) {
      final String schemaName = wSchemaDefinition.getText();

      MessageBox mb = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "ExcelInputDialog.Load.SchemaDefinition.Message", schemaName));
      mb.setText(BaseMessages.getString(PKG, "ExcelInputDialog.Load.SchemaDefinition.Title"));
      int answer = mb.open();

      if (answer == SWT.YES && !Utils.isEmpty(schemaName)) {
        try {
          SchemaDefinition schemaDefinition =
              (new SchemaDefinitionUtil()).loadSchemaDefinition(metadataProvider, schemaName);
          if (schemaDefinition != null) {
            IRowMeta r = schemaDefinition.getRowMeta();
            if (r != null) {
              String[] fieldNames = r.getFieldNames();
              if (fieldNames != null) {
                wFields.clearAll();
                for (int i = 0; i < fieldNames.length; i++) {
                  IValueMeta valueMeta = r.getValueMeta(i);
                  TableItem item = new TableItem(wFields.table, SWT.NONE);

                  item.setText(1, valueMeta.getName());
                  item.setText(2, ValueMetaFactory.getValueMetaName(valueMeta.getType()));
                  item.setText(
                      3, valueMeta.getLength() >= 0 ? Integer.toString(valueMeta.getLength()) : "");
                  item.setText(
                      4,
                      valueMeta.getPrecision() >= 0
                          ? Integer.toString(valueMeta.getPrecision())
                          : "");
                  item.setText(
                      5, Const.NVL(ValueMetaBase.getTrimTypeDesc(valueMeta.getTrimType()), ""));
                  item.setText(7, Const.NVL(valueMeta.getConversionMask(), ""));
                  final SchemaFieldDefinition schemaFieldDefinition =
                      schemaDefinition.getFieldDefinitions().get(i);
                  item.setText(8, Const.NVL(schemaFieldDefinition.getCurrencySymbol(), ""));
                  item.setText(9, Const.NVL(schemaFieldDefinition.getDecimalSymbol(), ""));
                  item.setText(10, Const.NVL(schemaFieldDefinition.getGroupingSymbol(), ""));
                }
              }
            }
          }
        } catch (HopTransformException | HopPluginException e) {

          // ignore any errors here.
        }

        wFields.removeEmptyRows();
        wFields.setRowNums();
        wFields.optWidth(true);
      }
    }
  }

  public void setFlags() {
    wbGetFields.setEnabled(wHeader.getSelection());

    boolean accept = wAccFilenames.getSelection();
    wlAccField.setEnabled(accept);
    wAccField.setEnabled(accept);
    wlAccTransform.setEnabled(accept);
    wAccTransform.setEnabled(accept);
    wlFilenameList.setEnabled(!accept);
    wFilenameList.setEnabled(!accept);
    wbShowFiles.setEnabled(!accept);

    // wPreview.setEnabled(!accept); // Keep this one: you can do preview on defined files in the
    // files section.

    // Error handling tab...
    wlSkipErrorLines.setEnabled(wErrorIgnored.getSelection());
    wSkipErrorLines.setEnabled(wErrorIgnored.getSelection());

    wlErrorDestDir.setEnabled(wErrorIgnored.getSelection());
    wErrorDestDir.setEnabled(wErrorIgnored.getSelection());
    wlErrorExt.setEnabled(wErrorIgnored.getSelection());
    wErrorExt.setEnabled(wErrorIgnored.getSelection());
    wbbErrorDestDir.setEnabled(wErrorIgnored.getSelection());
    wbvErrorDestDir.setEnabled(wErrorIgnored.getSelection());

    wlWarningDestDir.setEnabled(wErrorIgnored.getSelection());
    wWarningDestDir.setEnabled(wErrorIgnored.getSelection());
    wlWarningExt.setEnabled(wErrorIgnored.getSelection());
    wWarningExt.setEnabled(wErrorIgnored.getSelection());
    wbbWarningDestDir.setEnabled(wErrorIgnored.getSelection());
    wbvWarningDestDir.setEnabled(wErrorIgnored.getSelection());

    wlLineNrDestDir.setEnabled(wErrorIgnored.getSelection());
    wLineNrDestDir.setEnabled(wErrorIgnored.getSelection());
    wlLineNrExt.setEnabled(wErrorIgnored.getSelection());
    wLineNrExt.setEnabled(wErrorIgnored.getSelection());
    wbbLineNrDestDir.setEnabled(wErrorIgnored.getSelection());
    wbvLineNrDestDir.setEnabled(wErrorIgnored.getSelection());

    wFields.setEnabled(!wIgnoreFields.getSelection());
    wbGetFields.setEnabled(!wIgnoreFields.getSelection());
  }

  // Listen to the browse "..." button
  protected SelectionAdapter getFileSelectionAdapter() {
    return new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent event) {
        try {
          String path =
              wFilenameList.getActiveTableItem().getText(wFilenameList.getActiveTableColumn());
          FileObject fileObject = HopVfs.getFileObject(path);

          SpreadSheetType type =
              SpreadSheetType.getSpreadSheetTypeByDescription(wSpreadSheetType.getText());
          if (type == null) {
            return;
          }

          String[] extensions =
              switch (type) {
                case SAX_POI -> new String[] {"*.xlsx;*.XLSX;*.xlsm;*.XLSM", "*"};
                case ODS -> new String[] {"*.ods;*.ODS;", "*"};
                case POI -> new String[] {"*.xls;*.XLS;*.xlsx;*.XLSX;*.xlsm;*.XLSM", "*"};
              };

          path =
              BaseDialog.presentFileDialog(
                  shell,
                  null,
                  variables,
                  fileObject,
                  extensions,
                  new String[] {
                    BaseMessages.getString(PKG, "ExcelInputDialog.FilterNames.ExcelFiles"),
                    BaseMessages.getString(PKG, "System.FileType.AllFiles")
                  },
                  true);

          if (path != null) {
            wFilenameList.getActiveTableItem().setText(wFilenameList.getActiveTableColumn(), path);
          }
        } catch (HopFileException e) {
          log.logError("Error selecting file or directory", e);
        }
      }
    };
  }

  /**
   * Read the data from the ExcelInputMeta object and show it in this dialog.
   *
   * @param meta The ExcelInputMeta object to obtain the data from.
   */
  public void getData(ExcelInputMeta meta) {
    for (int i = 0; i < meta.getFiles().size(); i++) {
      EIFile file = meta.getFiles().get(i);
      TableItem item = wFilenameList.table.getItem(i);
      item.setText(1, Const.NVL(file.getName(), ""));
      item.setText(2, Const.NVL(file.getMask(), ""));
      item.setText(3, Const.NVL(file.getExcludeMask(), ""));
      item.setText(4, Const.NVL(file.getRequired(), ""));
      item.setText(5, Const.NVL(file.getIncludeSubFolders(), ""));
    }
    wFilenameList.optimizeTableView();

    wAccFilenames.setSelection(meta.isAcceptingFilenames());
    wSchemaDefinition.setText(Const.NVL(meta.getSchemaDefinition(), ""));
    wIgnoreFields.setSelection(meta.isIgnoreFields());
    if (meta.getAcceptingField() != null && !meta.getAcceptingField().isEmpty()) {
      wAccField.select(wAccField.indexOf(meta.getAcceptingField()));
    }
    if (meta.getAcceptingTransformName() != null && !meta.getAcceptingTransformName().isEmpty()) {
      wAccTransform.select(wAccTransform.indexOf(meta.getAcceptingTransformName()));
    }

    wHeader.setSelection(meta.isStartsWithHeader());
    wNoEmpty.setSelection(meta.isIgnoreEmptyRows());
    wStopOnEmpty.setSelection(meta.isStopOnEmpty());

    wInclFilenameField.setText(Const.NVL(meta.getFileField(), ""));
    wInclSheetNameField.setText(Const.NVL(meta.getSheetField(), ""));
    wInclSheetRowNumField.setText(Const.NVL(meta.getSheetRowNumberField(), ""));
    wInclRowNumField.setText(Const.NVL(meta.getRowNumberField(), ""));
    wLimit.setText("" + meta.getRowLimit());
    wEncoding.setText(Const.NVL(meta.getEncoding(), ""));
    wSpreadSheetType.setText(meta.getSpreadSheetType().getDescription());
    wAddResult.setSelection(meta.isAddResultFile());

    if (isDebug()) {
      logDebug("getting fields info...");
    }
    for (int i = 0; i < meta.getFields().size(); i++) {
      ExcelInputField f = meta.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      String field = f.getName();
      String type = f.getTypeDesc();
      String length = "" + f.getLength();
      String prec = "" + f.getPrecision();
      String trim = f.getTrimType().getDescription();
      String rep =
          f.isRepeat()
              ? BaseMessages.getString(PKG, CONST_COMBO_YES)
              : BaseMessages.getString(PKG, CONST_COMBO_NO);
      String format = f.getFormat();
      String currency = f.getCurrencySymbol();
      String decimal = f.getDecimalSymbol();
      String grouping = f.getGroupSymbol();

      item.setText(1, Const.NVL(field, ""));
      item.setText(2, Const.NVL(type, ""));
      item.setText(3, Const.NVL(length, ""));
      item.setText(4, Const.NVL(prec, ""));
      item.setText(5, Const.NVL(trim, ""));
      item.setText(6, Const.NVL(rep, ""));
      item.setText(7, Const.NVL(format, ""));
      item.setText(8, Const.NVL(currency, ""));
      item.setText(9, Const.NVL(decimal, ""));
      item.setText(10, Const.NVL(grouping, ""));
    }

    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);

    logDebug("getting sheets info...");
    for (int i = 0; i < meta.getSheets().size(); i++) {
      EISheet sheet = meta.getSheets().get(i);
      TableItem item = wSheetNameList.table.getItem(i);
      String sheetname = sheet.getName();
      String startrow = "" + sheet.getStartRow();
      String startcol = "" + sheet.getStartColumn();

      item.setText(1, Const.NVL(sheetname, ""));
      item.setText(2, Const.NVL(startrow, ""));
      item.setText(3, Const.NVL(startcol, ""));
    }
    wSheetNameList.optimizeTableView();

    // Error handling fields...
    wErrorIgnored.setSelection(meta.isErrorIgnored());
    wStrictTypes.setSelection(meta.isStrictTypes());
    wSkipErrorLines.setSelection(meta.isErrorLineSkipped());

    wWarningDestDir.setText(Const.NVL(meta.getWarningFilesDestinationDirectory(), ""));
    wWarningExt.setText(Const.NVL(meta.getBadLineFilesExtension(), ""));
    wErrorDestDir.setText(Const.NVL(meta.getErrorFilesDestinationDirectory(), ""));
    wErrorExt.setText(Const.NVL(meta.getErrorFilesExtension(), ""));
    wLineNrDestDir.setText(Const.NVL(meta.getLineNumberFilesDestinationDirectory(), ""));
    wLineNrExt.setText(Const.NVL(meta.getLineNumberFilesExtension(), ""));
    wPathFieldName.setText(Const.NVL(meta.getPathFieldName(), ""));
    wShortFileFieldName.setText(Const.NVL(meta.getShortFileFieldName(), ""));
    wPathFieldName.setText(Const.NVL(meta.getPathFieldName(), ""));
    wIsHiddenName.setText(Const.NVL(meta.getHiddenFieldName(), ""));
    wLastModificationTimeName.setText(Const.NVL(meta.getLastModificationTimeFieldName(), ""));
    wUriName.setText(Const.NVL(meta.getUriNameFieldName(), ""));
    wRootUriName.setText(Const.NVL(meta.getRootUriNameFieldName(), ""));
    wExtensionFieldName.setText(Const.NVL(meta.getExtensionFieldName(), ""));
    wSizeFieldName.setText(Const.NVL(meta.getSizeFieldName(), ""));

    setFlags();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    getInfo(input);
    input.setChanged();
    dispose();
  }

  private void getInfo(ExcelInputMeta meta) {
    TransformMeta currentTransformMeta = pipelineMeta.findTransform(transformName);
    transformName = wTransformName.getText(); // return value

    // copy info to Meta class (input)
    meta.setRowLimit(Const.toLong(wLimit.getText(), 0));
    meta.setEncoding(wEncoding.getText());
    meta.setSchemaDefinition(wSchemaDefinition.getText());
    meta.setIgnoreFields(wIgnoreFields.getSelection());
    meta.setSpreadSheetType(SpreadSheetType.values()[wSpreadSheetType.getSelectionIndex()]);
    meta.setFileField(wInclFilenameField.getText());
    meta.setSheetField(wInclSheetNameField.getText());
    meta.setSheetRowNumberField(wInclSheetRowNumField.getText());
    meta.setRowNumberField(wInclRowNumField.getText());

    meta.setAddResultFile(wAddResult.getSelection());

    meta.setStartsWithHeader(wHeader.getSelection());
    meta.setIgnoreEmptyRows(wNoEmpty.getSelection());
    meta.setStopOnEmpty(wStopOnEmpty.getSelection());

    meta.setAcceptingFilenames(wAccFilenames.getSelection());
    meta.setAcceptingField(wAccField.getText());
    meta.setAcceptingTransformName(wAccTransform.getText());
    meta.searchInfoAndTargetTransforms(pipelineMeta.findPreviousTransforms(currentTransformMeta));

    meta.getSheets().clear();
    for (TableItem item : wSheetNameList.getNonEmptyItems()) {
      EISheet sheet = new EISheet();
      sheet.setName(item.getText(1));
      sheet.setStartRow(Const.toInt(item.getText(2), 0));
      sheet.setStartColumn(Const.toInt(item.getText(3), 0));
      meta.getSheets().add(sheet);
    }

    meta.getFields().clear();
    for (TableItem item : wFields.getNonEmptyItems()) {
      ExcelInputField field = new ExcelInputField();
      field.setName(item.getText(1));
      field.setType(item.getText(2));
      String slength = item.getText(3);
      String sprec = item.getText(4);
      field.setTrimType(IValueMeta.TrimType.lookupDescription(item.getText(5)));
      field.setRepeat(
          BaseMessages.getString(PKG, CONST_COMBO_YES).equalsIgnoreCase(item.getText(6)));
      field.setLength(Const.toInt(slength, -1));
      field.setPrecision(Const.toInt(sprec, -1));
      field.setFormat(item.getText(7));
      field.setCurrencySymbol(item.getText(8));
      field.setDecimalSymbol(item.getText(9));
      field.setGroupSymbol(item.getText(10));
      meta.getFields().add(field);
    }

    meta.getFiles().clear();
    for (TableItem item : wFilenameList.getNonEmptyItems()) {
      EIFile file = new EIFile();
      file.setName(item.getText(1));
      file.setMask(item.getText(2));
      file.setExcludeMask(item.getText(3));
      file.setRequired(item.getText(4));
      file.setIncludeSubFolders(item.getText(5));
      meta.getFiles().add(file);
    }

    // Error handling fields...
    meta.setStrictTypes(wStrictTypes.getSelection());
    meta.setErrorIgnored(wErrorIgnored.getSelection());
    meta.setErrorLineSkipped(wSkipErrorLines.getSelection());

    meta.setWarningFilesDestinationDirectory(wWarningDestDir.getText());
    meta.setBadLineFilesExtension(wWarningExt.getText());
    meta.setErrorFilesDestinationDirectory(wErrorDestDir.getText());
    meta.setErrorFilesExtension(wErrorExt.getText());
    meta.setLineNumberFilesDestinationDirectory(wLineNrDestDir.getText());
    meta.setLineNumberFilesExtension(wLineNrExt.getText());
    meta.setShortFileFieldName(wShortFileFieldName.getText());
    meta.setPathFieldName(wPathFieldName.getText());
    meta.setHiddenFieldName(wIsHiddenName.getText());
    meta.setLastModificationTimeFieldName(wLastModificationTimeName.getText());
    meta.setUriNameFieldName(wUriName.getText());
    meta.setRootUriNameFieldName(wRootUriName.getText());
    meta.setExtensionFieldName(wExtensionFieldName.getText());
    meta.setSizeFieldName(wSizeFieldName.getText());
  }

  private void addErrorTab() {
    //
    // START OF ERROR TAB /
    // /
    CTabItem wErrorTab = new CTabItem(wTabFolder, SWT.NONE);
    wErrorTab.setFont(GuiResource.getInstance().getFontDefault());
    wErrorTab.setText(BaseMessages.getString(PKG, "ExcelInputDialog.ErrorTab.TabTitle"));

    FormLayout errorLayout = new FormLayout();
    errorLayout.marginWidth = 3;
    errorLayout.marginHeight = 3;

    Composite wErrorComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wErrorComp);
    wErrorComp.setLayout(errorLayout);

    // ERROR HANDLING...
    // ErrorIgnored?
    // ERROR HANDLING...
    Label wlStrictTypes = new Label(wErrorComp, SWT.RIGHT);
    wlStrictTypes.setText(BaseMessages.getString(PKG, "ExcelInputDialog.StrictTypes.Label"));
    PropsUi.setLook(wlStrictTypes);
    FormData fdlStrictTypes = new FormData();
    fdlStrictTypes.left = new FormAttachment(0, 0);
    fdlStrictTypes.top = new FormAttachment(0, margin);
    fdlStrictTypes.right = new FormAttachment(middle, -margin);
    wlStrictTypes.setLayoutData(fdlStrictTypes);
    wStrictTypes = new Button(wErrorComp, SWT.CHECK);
    PropsUi.setLook(wStrictTypes);
    wStrictTypes.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.StrictTypes.Tooltip"));
    FormData fdStrictTypes = new FormData();
    fdStrictTypes.left = new FormAttachment(middle, 0);
    fdStrictTypes.top = new FormAttachment(wlStrictTypes, 0, SWT.CENTER);
    wStrictTypes.setLayoutData(fdStrictTypes);
    Control previous = wStrictTypes;
    wStrictTypes.addSelectionListener(new ComponentSelectionListener(input));

    // ErrorIgnored?
    Label wlErrorIgnored = new Label(wErrorComp, SWT.RIGHT);
    wlErrorIgnored.setText(BaseMessages.getString(PKG, "ExcelInputDialog.ErrorIgnored.Label"));
    PropsUi.setLook(wlErrorIgnored);
    FormData fdlErrorIgnored = new FormData();
    fdlErrorIgnored.left = new FormAttachment(0, 0);
    fdlErrorIgnored.top = new FormAttachment(previous, margin);
    fdlErrorIgnored.right = new FormAttachment(middle, -margin);
    wlErrorIgnored.setLayoutData(fdlErrorIgnored);
    wErrorIgnored = new Button(wErrorComp, SWT.CHECK);
    PropsUi.setLook(wErrorIgnored);
    wErrorIgnored.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.ErrorIgnored.Tooltip"));
    FormData fdErrorIgnored = new FormData();
    fdErrorIgnored.left = new FormAttachment(middle, 0);
    fdErrorIgnored.top = new FormAttachment(wlErrorIgnored, 0, SWT.CENTER);
    wErrorIgnored.setLayoutData(fdErrorIgnored);
    previous = wErrorIgnored;
    wErrorIgnored.addListener(SWT.Selection, e -> setFlags());

    // Skip error lines?
    wlSkipErrorLines = new Label(wErrorComp, SWT.RIGHT);
    wlSkipErrorLines.setText(BaseMessages.getString(PKG, "ExcelInputDialog.SkipErrorLines.Label"));
    PropsUi.setLook(wlSkipErrorLines);
    FormData fdlSkipErrorLines = new FormData();
    fdlSkipErrorLines.left = new FormAttachment(0, 0);
    fdlSkipErrorLines.top = new FormAttachment(previous, margin);
    fdlSkipErrorLines.right = new FormAttachment(middle, -margin);
    wlSkipErrorLines.setLayoutData(fdlSkipErrorLines);
    wSkipErrorLines = new Button(wErrorComp, SWT.CHECK);
    PropsUi.setLook(wSkipErrorLines);
    wSkipErrorLines.setToolTipText(
        BaseMessages.getString(PKG, "ExcelInputDialog.SkipErrorLines.Tooltip"));
    FormData fdSkipErrorLines = new FormData();
    fdSkipErrorLines.left = new FormAttachment(middle, 0);
    fdSkipErrorLines.top = new FormAttachment(wlSkipErrorLines, 0, SWT.CENTER);
    wSkipErrorLines.setLayoutData(fdSkipErrorLines);
    wSkipErrorLines.addSelectionListener(new ComponentSelectionListener(input));

    previous = wSkipErrorLines;

    // Bad lines files directory + extension

    // WarningDestDir line
    wlWarningDestDir = new Label(wErrorComp, SWT.RIGHT);
    wlWarningDestDir.setText(BaseMessages.getString(PKG, "ExcelInputDialog.WarningDestDir.Label"));
    PropsUi.setLook(wlWarningDestDir);
    FormData fdlWarningDestDir = new FormData();
    fdlWarningDestDir.left = new FormAttachment(0, 0);
    fdlWarningDestDir.top = new FormAttachment(previous, margin * 4);
    fdlWarningDestDir.right = new FormAttachment(middle, -margin);
    wlWarningDestDir.setLayoutData(fdlWarningDestDir);

    wbbWarningDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbWarningDestDir);
    wbbWarningDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_BROWSE));
    wbbWarningDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_FOR_DIR));
    FormData fdbWarningDestDir = new FormData();
    fdbWarningDestDir.right = new FormAttachment(100, 0);
    fdbWarningDestDir.top = new FormAttachment(previous, margin * 4);
    wbbWarningDestDir.setLayoutData(fdbWarningDestDir);

    wbvWarningDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbvWarningDestDir);
    wbvWarningDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_VARIABLE));
    wbvWarningDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_TO_DIR));
    FormData fdbvWarningDestDir = new FormData();
    fdbvWarningDestDir.right = new FormAttachment(wbbWarningDestDir, -margin);
    fdbvWarningDestDir.top = new FormAttachment(previous, margin * 4);
    wbvWarningDestDir.setLayoutData(fdbvWarningDestDir);

    wWarningExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wWarningExt);
    FormData fdWarningDestExt = new FormData();
    fdWarningDestExt.left = new FormAttachment(wbvWarningDestDir, -150);
    fdWarningDestExt.right = new FormAttachment(wbvWarningDestDir, -margin);
    fdWarningDestExt.top = new FormAttachment(previous, margin * 4);
    wWarningExt.setLayoutData(fdWarningDestExt);

    wlWarningExt = new Label(wErrorComp, SWT.RIGHT);
    wlWarningExt.setText(BaseMessages.getString(PKG, CONST_LABEL_EXTENSION));
    PropsUi.setLook(wlWarningExt);
    FormData fdlWarningDestExt = new FormData();
    fdlWarningDestExt.top = new FormAttachment(previous, margin * 4);
    fdlWarningDestExt.right = new FormAttachment(wWarningExt, -margin);
    wlWarningExt.setLayoutData(fdlWarningDestExt);

    wWarningDestDir = new TextVar(variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wWarningDestDir);
    FormData fdWarningDestDir = new FormData();
    fdWarningDestDir.left = new FormAttachment(middle, 0);
    fdWarningDestDir.right = new FormAttachment(wlWarningExt, -margin);
    fdWarningDestDir.top = new FormAttachment(previous, margin * 4);
    wWarningDestDir.setLayoutData(fdWarningDestDir);

    // Listen to the Browse... button
    wbbWarningDestDir.addListener(
        SWT.Selection, e -> BaseDialog.presentDirectoryDialog(shell, wWarningDestDir, variables));

    // Listen to the Variable... button
    wbvWarningDestDir.addSelectionListener(
        VariableButtonListenerFactory.getSelectionAdapter(shell, wWarningDestDir, variables));

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wWarningDestDir.addModifyListener(getModifyListenerTooltipText(variables, wWarningDestDir));

    // Error lines files directory + extention
    previous = wWarningDestDir;

    // ErrorDestDir line
    wlErrorDestDir = new Label(wErrorComp, SWT.RIGHT);
    wlErrorDestDir.setText(BaseMessages.getString(PKG, "ExcelInputDialog.ErrorDestDir.Label"));
    PropsUi.setLook(wlErrorDestDir);
    FormData fdlErrorDestDir = new FormData();
    fdlErrorDestDir.left = new FormAttachment(0, 0);
    fdlErrorDestDir.top = new FormAttachment(previous, margin);
    fdlErrorDestDir.right = new FormAttachment(middle, -margin);
    wlErrorDestDir.setLayoutData(fdlErrorDestDir);

    wbbErrorDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbErrorDestDir);
    wbbErrorDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_BROWSE));
    wbbErrorDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_FOR_DIR));
    FormData fdbErrorDestDir = new FormData();
    fdbErrorDestDir.right = new FormAttachment(100, 0);
    fdbErrorDestDir.top = new FormAttachment(previous, margin);
    wbbErrorDestDir.setLayoutData(fdbErrorDestDir);

    wbvErrorDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbvErrorDestDir);
    wbvErrorDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_VARIABLE));
    wbvErrorDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_TO_DIR));
    FormData fdbvErrorDestDir = new FormData();
    fdbvErrorDestDir.right = new FormAttachment(wbbErrorDestDir, -margin);
    fdbvErrorDestDir.top = new FormAttachment(previous, margin);
    wbvErrorDestDir.setLayoutData(fdbvErrorDestDir);

    wErrorExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorExt);
    FormData fdErrorDestExt = new FormData();
    fdErrorDestExt.left = new FormAttachment(wbvErrorDestDir, -150);
    fdErrorDestExt.right = new FormAttachment(wbvErrorDestDir, -margin);
    fdErrorDestExt.top = new FormAttachment(previous, margin);
    wErrorExt.setLayoutData(fdErrorDestExt);

    wlErrorExt = new Label(wErrorComp, SWT.RIGHT);
    wlErrorExt.setText(BaseMessages.getString(PKG, CONST_LABEL_EXTENSION));
    PropsUi.setLook(wlErrorExt);
    FormData fdlErrorDestExt = new FormData();
    fdlErrorDestExt.top = new FormAttachment(previous, margin);
    fdlErrorDestExt.right = new FormAttachment(wErrorExt, -margin);
    wlErrorExt.setLayoutData(fdlErrorDestExt);

    wErrorDestDir = new TextVar(variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wErrorDestDir);
    FormData fdErrorDestDir = new FormData();
    fdErrorDestDir.left = new FormAttachment(middle, 0);
    fdErrorDestDir.right = new FormAttachment(wlErrorExt, -margin);
    fdErrorDestDir.top = new FormAttachment(previous, margin);
    wErrorDestDir.setLayoutData(fdErrorDestDir);

    // Listen to the Browse... button
    wbbErrorDestDir.addSelectionListener(
        DirectoryDialogButtonListenerFactory.getSelectionAdapter(shell, wErrorDestDir));

    // Listen to the Variable... button
    wbvErrorDestDir.addSelectionListener(
        VariableButtonListenerFactory.getSelectionAdapter(shell, wErrorDestDir, variables));

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wErrorDestDir.addModifyListener(getModifyListenerTooltipText(variables, wErrorDestDir));

    // Line numbers files directory + extention
    previous = wErrorDestDir;

    // LineNrDestDir line
    wlLineNrDestDir = new Label(wErrorComp, SWT.RIGHT);
    wlLineNrDestDir.setText(BaseMessages.getString(PKG, "ExcelInputDialog.LineNrDestDir.Label"));
    PropsUi.setLook(wlLineNrDestDir);
    FormData fdlLineNrDestDir = new FormData();
    fdlLineNrDestDir.left = new FormAttachment(0, 0);
    fdlLineNrDestDir.top = new FormAttachment(previous, margin);
    fdlLineNrDestDir.right = new FormAttachment(middle, -margin);
    wlLineNrDestDir.setLayoutData(fdlLineNrDestDir);

    wbbLineNrDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbLineNrDestDir);
    wbbLineNrDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_BROWSE));
    wbbLineNrDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_FOR_DIR));
    FormData fdbLineNrDestDir = new FormData();
    fdbLineNrDestDir.right = new FormAttachment(100, 0);
    fdbLineNrDestDir.top = new FormAttachment(previous, margin);
    wbbLineNrDestDir.setLayoutData(fdbLineNrDestDir);

    wbvLineNrDestDir = new Button(wErrorComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbvLineNrDestDir);
    wbvLineNrDestDir.setText(BaseMessages.getString(PKG, CONST_BUTTON_VARIABLE));
    wbvLineNrDestDir.setToolTipText(BaseMessages.getString(PKG, CONST_BROWSE_TO_DIR));
    FormData fdbvLineNrDestDir = new FormData();
    fdbvLineNrDestDir.right = new FormAttachment(wbbLineNrDestDir, -margin);
    fdbvLineNrDestDir.top = new FormAttachment(previous, margin);
    wbvLineNrDestDir.setLayoutData(fdbvLineNrDestDir);

    wLineNrExt = new Text(wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLineNrExt);
    FormData fdLineNrDestExt = new FormData();
    fdLineNrDestExt.left = new FormAttachment(wbvLineNrDestDir, -150);
    fdLineNrDestExt.right = new FormAttachment(wbvLineNrDestDir, -margin);
    fdLineNrDestExt.top = new FormAttachment(previous, margin);
    wLineNrExt.setLayoutData(fdLineNrDestExt);

    wlLineNrExt = new Label(wErrorComp, SWT.RIGHT);
    wlLineNrExt.setText(BaseMessages.getString(PKG, CONST_LABEL_EXTENSION));
    PropsUi.setLook(wlLineNrExt);
    FormData fdlLineNrDestExt = new FormData();
    fdlLineNrDestExt.top = new FormAttachment(previous, margin);
    fdlLineNrDestExt.right = new FormAttachment(wLineNrExt, -margin);
    wlLineNrExt.setLayoutData(fdlLineNrDestExt);

    wLineNrDestDir = new TextVar(variables, wErrorComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLineNrDestDir);
    FormData fdLineNrDestDir = new FormData();
    fdLineNrDestDir.left = new FormAttachment(middle, 0);
    fdLineNrDestDir.right = new FormAttachment(wlLineNrExt, -margin);
    fdLineNrDestDir.top = new FormAttachment(previous, margin);
    wLineNrDestDir.setLayoutData(fdLineNrDestDir);

    // Listen to the Browse... button
    wbbLineNrDestDir.addSelectionListener(
        DirectoryDialogButtonListenerFactory.getSelectionAdapter(shell, wLineNrDestDir));

    // Listen to the Variable... button
    wbvLineNrDestDir.addSelectionListener(
        VariableButtonListenerFactory.getSelectionAdapter(shell, wLineNrDestDir, variables));

    // Whenever something changes, set the tooltip to the expanded version of the directory:
    wLineNrDestDir.addModifyListener(getModifyListenerTooltipText(variables, wLineNrDestDir));

    wErrorComp.layout();
    wErrorTab.setControl(wErrorComp);

    //
    // / END OF CONTENT TAB
    //
  }

  /**
   * Preview the data generated by this transform. This generates a pipeline using this transform &
   * a dummy and previews it.
   */
  private void preview() {
    // Create the excel reader transform...
    ExcelInputMeta oneMeta = new ExcelInputMeta();
    getInfo(oneMeta);

    if (oneMeta.isAcceptingFilenames()) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "ExcelInputDialog.Dialog.SpecifyASampleFile.Message")); // Nothing
      // found
      // that
      // matches
      // your
      // criteria
      mb.setText(
          BaseMessages.getString(
              PKG, "ExcelInputDialog.Dialog.SpecifyASampleFile.Title")); // Sorry!
      mb.open();
      return;
    }

    EnterNumberDialog numberDialog =
        new EnterNumberDialog(
            shell,
            props.getDefaultPreviewSize(),
            BaseMessages.getString(PKG, "ExcelInputDialog.PreviewSize.DialogTitle"),
            BaseMessages.getString(PKG, "ExcelInputDialog.PreviewSize.DialogMessage"));
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

      Pipeline pipeline = progressDialog.getPipeline();
      String loggingText = progressDialog.getLoggingText();

      if (!progressDialog.isCancelled()
          && pipeline.getResult() != null
          && pipeline.getResult().getNrErrors() > 0) {
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

  /**
   * Get the names of the sheets from the Excel workbooks and let the user select some or all of
   * them.
   */
  public void getSheets() {
    List<String> sheetNames = new ArrayList<>();

    ExcelInputMeta info = new ExcelInputMeta();
    getInfo(info);

    FileInputList fileList = info.getFileList(variables);
    for (FileObject fileObject : fileList.getFiles()) {
      try {
        IKWorkbook workbook =
            WorkbookFactory.getWorkbook(
                info.getSpreadSheetType(),
                HopVfs.getFilename(fileObject),
                info.getEncoding(),
                variables);

        int nrSheets = workbook.getNumberOfSheets();
        for (int j = 0; j < nrSheets; j++) {
          IKSheet sheet = workbook.getSheet(j);
          String sheetname = sheet.getName();

          if (Const.indexOfString(sheetname, sheetNames) < 0) {
            sheetNames.add(sheetname);
          }
        }

        workbook.close();
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, CONST_ERROR_TITLE),
            BaseMessages.getString(
                PKG,
                "ExcelInputDialog.ErrorReadingFile.DialogMessage",
                HopVfs.getFilename(fileObject)),
            e);
      }
    }

    // Put it in an array:
    String[] lst = sheetNames.toArray(new String[0]);

    // Let the user select the sheet-names...
    EnterListDialog esd = new EnterListDialog(shell, SWT.NONE, lst);
    String[] selection = esd.open();
    if (selection != null) {
      for (String s : selection) {
        wSheetNameList.add(s, "");
      }
      wSheetNameList.removeEmptyRows();
      wSheetNameList.setRowNums();
      wSheetNameList.optWidth(true);
      checkAlerts();
    }
  }

  /** Get the list of fields in the Excel workbook and put the result in the fields table view. */
  public void getFields() {
    IRowMeta fields = new RowMeta();

    ExcelInputMeta info = new ExcelInputMeta();
    getInfo(info);

    int clearFields = SWT.YES;
    if (wFields.nrNonEmpty() > 0) {
      MessageBox messageBox =
          new MessageBox(shell, SWT.YES | SWT.NO | SWT.CANCEL | SWT.ICON_QUESTION);
      messageBox.setMessage(
          BaseMessages.getString(PKG, "ExcelInputDialog.ClearFieldList.DialogMessage"));
      messageBox.setText(
          BaseMessages.getString(PKG, "ExcelInputDialog.ClearFieldList.DialogTitle"));
      clearFields = messageBox.open();
      if (clearFields == SWT.CANCEL) {
        return;
      }
    }

    FileInputList fileList = info.getFileList(variables);
    for (FileObject file : fileList.getFiles()) {
      try {
        IKWorkbook workbook =
            WorkbookFactory.getWorkbook(
                info.getSpreadSheetType(), HopVfs.getFilename(file), info.getEncoding(), variables);
        processingWorkbook(fields, info, workbook);
        workbook.close();
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, CONST_ERROR_TITLE),
            BaseMessages.getString(
                PKG,
                "ExcelInputDialog.ErrorReadingFile2.DialogMessage",
                HopVfs.getFilename(file),
                e.toString()),
            e);
      }
    }

    if (!fields.isEmpty()) {
      if (clearFields == SWT.YES) {
        wFields.clearAll(false);
      }
      for (int j = 0; j < fields.size(); j++) {
        IValueMeta field = fields.getValueMeta(j);
        wFields.add(field.getName(), field.getTypeDesc(), "", "", "none", "N");
      }
      wFields.removeEmptyRows();
      wFields.setRowNums();
      wFields.optWidth(true);
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_WARNING);
      mb.setMessage(
          BaseMessages.getString(PKG, "ExcelInputDialog.UnableToFindFields.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "ExcelInputDialog.UnableToFindFields.DialogTitle"));
      mb.open();
    }
    checkAlerts();
  }

  /**
   * Processing excel workbook, filling fields
   *
   * @param fields IRowMeta for filling fields
   * @param meta ExcelInputMeta
   * @param workbook excel workbook for processing
   * @throws HopPluginException In case something goes wrong
   */
  private void processingWorkbook(IRowMeta fields, ExcelInputMeta meta, IKWorkbook workbook)
      throws HopPluginException {
    int nrSheets = workbook.getNumberOfSheets();
    for (int j = 0; j < nrSheets; j++) {
      IKSheet sheet = workbook.getSheet(j);

      // See if it's a selected sheet:
      int sheetIndex;
      if (meta.readAllSheets()) {
        sheetIndex = 0;
      } else {
        sheetIndex = Const.indexOfString(sheet.getName(), meta.getSheetsNames());
      }
      if (sheetIndex >= 0) {
        // We suppose it's the complete range we're looking for...
        //
        EISheet sh = meta.getSheets().get(sheetIndex);

        int rowNr = sh.getStartRow();
        int startCol = sh.getStartColumn();

        boolean stop = false;
        for (int colnr = startCol; !stop; colnr++) {
          try {
            String fieldName = null;
            int fieldType;

            IKCell cell = sheet.getCell(colnr, rowNr);
            if (cell == null) {
              stop = true;
            } else {
              if (cell.getType() != KCellType.EMPTY) {
                // We found a field.
                fieldName = cell.getContents();
              }

              IKCell below = sheet.getCell(colnr, rowNr + 1);

              if (below != null) {
                if (below.getType() == KCellType.BOOLEAN) {
                  fieldType = IValueMeta.TYPE_BOOLEAN;
                } else if (below.getType() == KCellType.DATE) {
                  fieldType = IValueMeta.TYPE_DATE;
                } else if (below.getType() == KCellType.LABEL) {
                  fieldType = IValueMeta.TYPE_STRING;
                } else if (below.getType() == KCellType.NUMBER) {
                  fieldType = IValueMeta.TYPE_NUMBER;
                } else {
                  fieldType = IValueMeta.TYPE_STRING;
                }
              } else {
                fieldType = IValueMeta.TYPE_STRING;
              }

              if (Utils.isEmpty(fieldName)) {
                stop = true;
              } else {
                IValueMeta field = ValueMetaFactory.createValueMeta(fieldName, fieldType);
                fields.addValueMeta(field);
              }
            }
          } catch (ArrayIndexOutOfBoundsException aioobe) {
            stop = true;
          }
        }
      }
    }
  }

  private void showFiles() {
    ExcelInputMeta eii = new ExcelInputMeta();
    getInfo(eii);
    String[] files = eii.getFilePaths(variables);
    if (files.length > 0) {
      EnterSelectionDialog esd =
          new EnterSelectionDialog(
              shell,
              files,
              BaseMessages.getString(PKG, "ExcelInputDialog.FilesRead.DialogTitle"),
              BaseMessages.getString(PKG, "ExcelInputDialog.FilesRead.DialogMessage"));
      esd.setViewOnly();
      esd.open();
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(BaseMessages.getString(PKG, "ExcelInputDialog.NoFilesFound.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, CONST_ERROR_TITLE));
      mb.open();
    }
  }

  private void setEncodings() {
    // Encoding of the text file:
    if (!gotEncodings) {
      gotEncodings = true;

      wEncoding.removeAll();

      List<Charset> values = new ArrayList<>(Charset.availableCharsets().values());
      for (Charset charSet : values) {
        wEncoding.add(charSet.displayName());
      }

      // Now select the default!
      String defEncoding = Const.getEnvironmentVariable("file.encoding", "UTF-8");
      int idx = Const.indexOfString(defEncoding, wEncoding.getItems());
      if (idx >= 0) {
        wEncoding.select(idx);
      }
    }
  }

  /**
   * It is perfectly permissible to put away an incomplete transform definition. However, to assist
   * the user in setting up the full kit, this method is invoked whenever data changes in the
   * dialog. It scans the dialog's model looking for missing and/or inconsistent data. Tabs needing
   * attention are visually flagged and attention messages are displayed in the statusMessage line
   * (a la Eclipse).
   *
   * <p>Since there's only one statusMessage line, messages are prioritized. As each higher-level
   * item is corrected, the next lower level message is displayed.
   */
  private void checkAlerts() {
    logDebug("checkAlerts");
    // # Check the fields tab. At least one field is required.
    // # Check the Sheets tab. At least one sheet is required.
    // # Check the Files tab.

    final boolean fieldsOk = wFields.nrNonEmpty() != 0;
    final boolean sheetsOk = wSheetNameList.nrNonEmpty() != 0;
    final boolean filesOk =
        wFilenameList.nrNonEmpty() != 0
            || (wAccFilenames.getSelection() && !Utils.isEmpty(wAccField.getText()));

    tagTab(!fieldsOk, wFieldsTab, BaseMessages.getString(PKG, "ExcelInputDialog.AddFields"));
    tagTab(!sheetsOk, wSheetTab, BaseMessages.getString(PKG, "ExcelInputDialog.AddSheets"));
    tagTab(!filesOk, wFileTab, BaseMessages.getString(PKG, "ExcelInputDialog.AddFilenames"));

    String msgText = ""; // Will clear status if no actions.

    // Assign the highest-priority action message.
    if (!fieldsOk) {
      msgText = (BaseMessages.getString(PKG, "ExcelInputDialog.AddFields"));
    } else if (!sheetsOk) {
      msgText = (BaseMessages.getString(PKG, "ExcelInputDialog.AddSheets"));
    } else if (!filesOk) {
      msgText = (BaseMessages.getString(PKG, "ExcelInputDialog.AddFilenames"));
    }
    wlStatusMessage.setText(msgText);

    wPreview.setEnabled(fieldsOk && sheetsOk && filesOk);
  }

  /**
   * Highlight (or not) tab to indicate if action is required.
   *
   * @param highlight <code>true</code> to highlight, <code>false</code> if not.
   * @param tabItem Tab to highlight
   * @param toolTip Tab text to explain the warning
   */
  private void tagTab(boolean highlight, CTabItem tabItem, String toolTip) {
    if (highlight) {
      tabItem.setImage(GuiResource.getInstance().getImageWarning());
      tabItem.setToolTipText(toolTip);
    } else {
      // Will clear the status if no warnings.
      tabItem.setImage(null);
      tabItem.setToolTipText(null);
    }
  }

  private void addAdditionalFieldsTab() {
    //
    // START OF ADDITIONAL FIELDS TAB /
    //
    CTabItem wAdditionalFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdditionalFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdditionalFieldsTab.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.AdditionalFieldsTab.TabTitle"));

    Composite wAdditionalFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdditionalFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wAdditionalFieldsComp.setLayout(fieldsLayout);

    Label wlInclFilenameField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclFilenameField.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.InclFilenameField.Label"));
    PropsUi.setLook(wlInclFilenameField);
    FormData fdlInclFilenameField = new FormData();
    fdlInclFilenameField.left = new FormAttachment(0, 0);
    fdlInclFilenameField.top = new FormAttachment(wTransformName, margin);
    fdlInclFilenameField.right = new FormAttachment(middle, -margin);
    wlInclFilenameField.setLayoutData(fdlInclFilenameField);
    wInclFilenameField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclFilenameField);
    FormData fdInclFilenameField = new FormData();
    fdInclFilenameField.left = new FormAttachment(middle, 0);
    fdInclFilenameField.top = new FormAttachment(wTransformName, margin);
    fdInclFilenameField.right = new FormAttachment(100, 0);
    wInclFilenameField.setLayoutData(fdInclFilenameField);

    Label wlInclSheetnameField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclSheetnameField.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.InclSheetnameField.Label"));
    PropsUi.setLook(wlInclSheetnameField);
    FormData fdlInclSheetnameField = new FormData();
    fdlInclSheetnameField.left = new FormAttachment(0, 0);
    fdlInclSheetnameField.top = new FormAttachment(wInclFilenameField, margin);
    fdlInclSheetnameField.right = new FormAttachment(middle, -margin);
    wlInclSheetnameField.setLayoutData(fdlInclSheetnameField);
    wInclSheetNameField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclSheetNameField);
    FormData fdInclSheetnameField = new FormData();
    fdInclSheetnameField.left = new FormAttachment(middle, 0);
    fdInclSheetnameField.top = new FormAttachment(wInclFilenameField, margin);
    fdInclSheetnameField.right = new FormAttachment(100, 0);
    wInclSheetNameField.setLayoutData(fdInclSheetnameField);

    Label wlInclSheetRownumField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclSheetRownumField.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.InclSheetRownumField.Label"));
    PropsUi.setLook(wlInclSheetRownumField);
    FormData fdlInclSheetRownumField = new FormData();
    fdlInclSheetRownumField.left = new FormAttachment(0, 0);
    fdlInclSheetRownumField.top = new FormAttachment(wInclSheetNameField, margin);
    fdlInclSheetRownumField.right = new FormAttachment(middle, -margin);
    wlInclSheetRownumField.setLayoutData(fdlInclSheetRownumField);
    wInclSheetRowNumField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclSheetRowNumField);
    FormData fdInclSheetRownumField = new FormData();
    fdInclSheetRownumField.left = new FormAttachment(middle, 0);
    fdInclSheetRownumField.top = new FormAttachment(wInclSheetNameField, margin);
    fdInclSheetRownumField.right = new FormAttachment(100, 0);
    wInclSheetRowNumField.setLayoutData(fdInclSheetRownumField);

    Label wlInclRownumField = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlInclRownumField.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.InclRownumField.Label"));
    PropsUi.setLook(wlInclRownumField);
    FormData fdlInclRownumField = new FormData();
    fdlInclRownumField.left = new FormAttachment(0, 0);
    fdlInclRownumField.top = new FormAttachment(wInclSheetRowNumField, margin);
    fdlInclRownumField.right = new FormAttachment(middle, -margin);
    wlInclRownumField.setLayoutData(fdlInclRownumField);
    wInclRowNumField = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wInclRowNumField);
    FormData fdInclRownumField = new FormData();
    fdInclRownumField.left = new FormAttachment(middle, 0);
    fdInclRownumField.top = new FormAttachment(wInclSheetRowNumField, margin);
    fdInclRownumField.right = new FormAttachment(100, 0);
    wInclRowNumField.setLayoutData(fdInclRownumField);

    // ShortFileFieldName line
    Label wlShortFileFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlShortFileFieldName.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.ShortFileFieldName.Label"));
    PropsUi.setLook(wlShortFileFieldName);
    FormData fdlShortFileFieldName = new FormData();
    fdlShortFileFieldName.left = new FormAttachment(0, 0);
    fdlShortFileFieldName.top = new FormAttachment(wInclRowNumField, margin);
    fdlShortFileFieldName.right = new FormAttachment(middle, -margin);
    wlShortFileFieldName.setLayoutData(fdlShortFileFieldName);

    wShortFileFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wShortFileFieldName);
    FormData fdShortFileFieldName = new FormData();
    fdShortFileFieldName.left = new FormAttachment(middle, 0);
    fdShortFileFieldName.right = new FormAttachment(100, -margin);
    fdShortFileFieldName.top = new FormAttachment(wInclRowNumField, margin);
    wShortFileFieldName.setLayoutData(fdShortFileFieldName);

    // ExtensionFieldName line
    Label wlExtensionFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlExtensionFieldName.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.ExtensionFieldName.Label"));
    PropsUi.setLook(wlExtensionFieldName);
    FormData fdlExtensionFieldName = new FormData();
    fdlExtensionFieldName.left = new FormAttachment(0, 0);
    fdlExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    fdlExtensionFieldName.right = new FormAttachment(middle, -margin);
    wlExtensionFieldName.setLayoutData(fdlExtensionFieldName);

    wExtensionFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wExtensionFieldName);
    FormData fdExtensionFieldName = new FormData();
    fdExtensionFieldName.left = new FormAttachment(middle, 0);
    fdExtensionFieldName.right = new FormAttachment(100, -margin);
    fdExtensionFieldName.top = new FormAttachment(wShortFileFieldName, margin);
    wExtensionFieldName.setLayoutData(fdExtensionFieldName);

    // PathFieldName line
    Label wlPathFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlPathFieldName.setText(BaseMessages.getString(PKG, "ExcelInputDialog.PathFieldName.Label"));
    PropsUi.setLook(wlPathFieldName);
    FormData fdlPathFieldName = new FormData();
    fdlPathFieldName.left = new FormAttachment(0, 0);
    fdlPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    fdlPathFieldName.right = new FormAttachment(middle, -margin);
    wlPathFieldName.setLayoutData(fdlPathFieldName);

    wPathFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wPathFieldName);
    FormData fdPathFieldName = new FormData();
    fdPathFieldName.left = new FormAttachment(middle, 0);
    fdPathFieldName.right = new FormAttachment(100, -margin);
    fdPathFieldName.top = new FormAttachment(wExtensionFieldName, margin);
    wPathFieldName.setLayoutData(fdPathFieldName);

    // SizeFieldName line
    Label wlSizeFieldName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlSizeFieldName.setText(BaseMessages.getString(PKG, "ExcelInputDialog.SizeFieldName.Label"));
    PropsUi.setLook(wlSizeFieldName);
    FormData fdlSizeFieldName = new FormData();
    fdlSizeFieldName.left = new FormAttachment(0, 0);
    fdlSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    fdlSizeFieldName.right = new FormAttachment(middle, -margin);
    wlSizeFieldName.setLayoutData(fdlSizeFieldName);

    wSizeFieldName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSizeFieldName);
    FormData fdSizeFieldName = new FormData();
    fdSizeFieldName.left = new FormAttachment(middle, 0);
    fdSizeFieldName.right = new FormAttachment(100, -margin);
    fdSizeFieldName.top = new FormAttachment(wPathFieldName, margin);
    wSizeFieldName.setLayoutData(fdSizeFieldName);

    // IsHiddenName line
    Label wlIsHiddenName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlIsHiddenName.setText(BaseMessages.getString(PKG, "ExcelInputDialog.IsHiddenName.Label"));
    PropsUi.setLook(wlIsHiddenName);
    FormData fdlIsHiddenName = new FormData();
    fdlIsHiddenName.left = new FormAttachment(0, 0);
    fdlIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    fdlIsHiddenName.right = new FormAttachment(middle, -margin);
    wlIsHiddenName.setLayoutData(fdlIsHiddenName);

    wIsHiddenName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wIsHiddenName);
    FormData fdIsHiddenName = new FormData();
    fdIsHiddenName.left = new FormAttachment(middle, 0);
    fdIsHiddenName.right = new FormAttachment(100, -margin);
    fdIsHiddenName.top = new FormAttachment(wSizeFieldName, margin);
    wIsHiddenName.setLayoutData(fdIsHiddenName);

    // LastModificationTimeName line
    Label wlLastModificationTimeName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlLastModificationTimeName.setText(
        BaseMessages.getString(PKG, "ExcelInputDialog.LastModificationTimeName.Label"));
    PropsUi.setLook(wlLastModificationTimeName);
    FormData fdlLastModificationTimeName = new FormData();
    fdlLastModificationTimeName.left = new FormAttachment(0, 0);
    fdlLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    fdlLastModificationTimeName.right = new FormAttachment(middle, -margin);
    wlLastModificationTimeName.setLayoutData(fdlLastModificationTimeName);

    wLastModificationTimeName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLastModificationTimeName);
    FormData fdLastModificationTimeName = new FormData();
    fdLastModificationTimeName.left = new FormAttachment(middle, 0);
    fdLastModificationTimeName.right = new FormAttachment(100, -margin);
    fdLastModificationTimeName.top = new FormAttachment(wIsHiddenName, margin);
    wLastModificationTimeName.setLayoutData(fdLastModificationTimeName);

    // UriName line
    Label wlUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlUriName.setText(BaseMessages.getString(PKG, "ExcelInputDialog.UriName.Label"));
    PropsUi.setLook(wlUriName);
    FormData fdlUriName = new FormData();
    fdlUriName.left = new FormAttachment(0, 0);
    fdlUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    fdlUriName.right = new FormAttachment(middle, -margin);
    wlUriName.setLayoutData(fdlUriName);

    wUriName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wUriName);
    FormData fdUriName = new FormData();
    fdUriName.left = new FormAttachment(middle, 0);
    fdUriName.right = new FormAttachment(100, -margin);
    fdUriName.top = new FormAttachment(wLastModificationTimeName, margin);
    wUriName.setLayoutData(fdUriName);

    // RootUriName line
    Label wlRootUriName = new Label(wAdditionalFieldsComp, SWT.RIGHT);
    wlRootUriName.setText(BaseMessages.getString(PKG, "ExcelInputDialog.RootUriName.Label"));
    PropsUi.setLook(wlRootUriName);
    FormData fdlRootUriName = new FormData();
    fdlRootUriName.left = new FormAttachment(0, 0);
    fdlRootUriName.top = new FormAttachment(wUriName, margin);
    fdlRootUriName.right = new FormAttachment(middle, -margin);
    wlRootUriName.setLayoutData(fdlRootUriName);

    wRootUriName = new Text(wAdditionalFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wRootUriName);
    FormData fdRootUriName = new FormData();
    fdRootUriName.left = new FormAttachment(middle, 0);
    fdRootUriName.right = new FormAttachment(100, -margin);
    fdRootUriName.top = new FormAttachment(wUriName, margin);
    wRootUriName.setLayoutData(fdRootUriName);

    FormData fdAdditionalFieldsComp = new FormData();
    fdAdditionalFieldsComp.left = new FormAttachment(0, 0);
    fdAdditionalFieldsComp.top = new FormAttachment(wTransformName, margin);
    fdAdditionalFieldsComp.right = new FormAttachment(100, 0);
    fdAdditionalFieldsComp.bottom = new FormAttachment(100, 0);
    wAdditionalFieldsComp.setLayoutData(fdAdditionalFieldsComp);

    wAdditionalFieldsComp.layout();
    wAdditionalFieldsTab.setControl(wAdditionalFieldsComp);

    //
    // / END OF ADDITIONAL FIELDS TAB
    //
  }
}
