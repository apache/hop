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

package org.apache.hop.pipeline.transforms.excelwriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.util.SchemaDefinitionUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.PasswordTextVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
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

public class ExcelWriterTransformDialog extends BaseTransformDialog {
  private static final Class<?> PKG = ExcelWriterTransformMeta.class; // For Translator

  private TextVar wFilename;
  private Label wlFilename;

  private CCombo wExtension;

  private Button wFileNameInField;

  private Label wlFileNameField;
  private ComboVar wFileNameField;

  private Button wStreamData;

  private Button wAddTransformNr;
  private Label wlAddTransformNr;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wProtectSheet;

  private Button wHeader;

  private Button wFooter;

  private Text wSplitEvery;
  private Label wlSplitEvery;

  private Button wTemplate;

  private Button wbTemplateFilename;
  private TextVar wTemplateFilename;

  private Button wbShowFiles;

  private TextVar wPassword;

  private TextVar wSheetname;

  private TableView wFields;

  private final ExcelWriterTransformMeta input;

  private Button wAddToResult;

  private Button wDoNotOpenNewFileInit;

  private Button wSpecifyFormat;
  private Label wlSpecifyFormat;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Button wAutoSize;

  private ColumnInfo[] colinf;

  private final List<String> inputFields = new ArrayList<>();

  private CCombo wIfFileExists;

  private CCombo wIfSheetExists;

  private TextVar wTemplateSheetname;

  private TextVar wStartingCell;

  private CCombo wRowWritingMethod;

  private Button wTemplateSheet;

  private Button wTemplateSheetHide;

  private Button wAppendLines;

  private Text wSkipRows;

  private Text wEmptyRows;

  private Button wOmitHeader;

  private TextVar wProtectedBy;

  protected Button wCreateParentFolder;

  private Button wMakeActiveSheet;
  private Button wForceFormulaRecalculation;
  private Button wLeaveExistingStylesUnchanged;

  private boolean gotPreviousFields = false;

  private MetaSelectionLine<SchemaDefinition> wSchemaDefinition;

  private static final String LABEL_FORMATXLSX = "ExcelWriterDialog.FormatXLSX.Label";
  private static final String LABEL_FORMATXLS = "ExcelWriterDialog.FormatXLS.Label";

  public ExcelWriterTransformDialog(
      Shell parent,
      IVariables variables,
      ExcelWriterTransformMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    SelectionAdapter lsSel =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.DialogTitle"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons go at the bottom
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.Label.TransformName"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.top = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.H_SCROLL | SWT.V_SCROLL);

    CTabFolder wTabFolder = new CTabFolder(sc, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB///
    // /
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.FileTab.TabTitle"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    Group fileGroup = new Group(wFileComp, SWT.SHADOW_NONE);
    PropsUi.setLook(fileGroup);
    fileGroup.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.fileGroup.Label"));

    FormLayout fileGroupgroupLayout = new FormLayout();
    fileGroupgroupLayout.marginWidth = 10;
    fileGroupgroupLayout.marginHeight = 10;
    fileGroup.setLayout(fileGroupgroupLayout);

    // Filename line
    wlFilename = new Label(fileGroup, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(0, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    Button wbFilename = new Button(fileGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(0, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, fileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    wFilename.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Filename.Tooltip"));
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(0, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    // Extension line
    Label wlExtension = new Label(fileGroup, SWT.RIGHT);
    wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
    PropsUi.setLook(wlExtension);
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment(0, 0);
    fdlExtension.top = new FormAttachment(wFilename, margin);
    fdlExtension.right = new FormAttachment(middle, -margin);
    wlExtension.setLayoutData(fdlExtension);
    wExtension = new CCombo(fileGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.READ_ONLY);

    String xlsLabel = BaseMessages.getString(PKG, LABEL_FORMATXLS);
    String xlsxLabel = BaseMessages.getString(PKG, LABEL_FORMATXLSX);
    wExtension.setItems(new String[] {xlsLabel, xlsxLabel});
    wExtension.setData(xlsLabel, "xls");
    wExtension.setData(xlsxLabel, "xlsx");

    PropsUi.setLook(wExtension);
    wExtension.addModifyListener(lsMod);

    wExtension.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableExtension();
          }
        });

    wExtension.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Extension.Tooltip"));

    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment(middle, 0);
    fdExtension.top = new FormAttachment(wFilename, margin);
    fdExtension.right = new FormAttachment(wbFilename, -margin);
    wExtension.setLayoutData(fdExtension);

    // Create Parent Folder
    Label wlCreateParentFolder = new Label(fileGroup, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(wExtension, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(fileGroup, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.CreateParentFolder.Tooltip"));
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
            input.setChanged();
          }
        });

    // Do not open new File at Init
    Label wlDoNotOpenNewFileInit = new Label(fileGroup, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.DoNotOpenNewFileInit.Label"));
    PropsUi.setLook(wlDoNotOpenNewFileInit);
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
    fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreateParentFolder, 2 * margin, margin);
    fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button(fileGroup, SWT.CHECK);
    wDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.DoNotOpenNewFileInit.Tooltip"));
    PropsUi.setLook(wDoNotOpenNewFileInit);
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
    fdDoNotOpenNewFileInit.top = new FormAttachment(wlDoNotOpenNewFileInit, 0, SWT.CENTER);
    fdDoNotOpenNewFileInit.right = new FormAttachment(100, 0);
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener(lsSel);

    // FileNameInField line
    Label wlFileNameInField = new Label(fileGroup, SWT.RIGHT);
    wlFileNameInField.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.FileNameInField.Label"));
    PropsUi.setLook(wlFileNameInField);
    FormData fdlFileNameInField = new FormData();
    fdlFileNameInField.left = new FormAttachment(0, 0);
    fdlFileNameInField.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    fdlFileNameInField.right = new FormAttachment(middle, -margin);
    wlFileNameInField.setLayoutData(fdlFileNameInField);
    wFileNameInField = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wFileNameInField);
    FormData fdFileNameInField = new FormData();
    fdFileNameInField.left = new FormAttachment(middle, 0);
    fdFileNameInField.top = new FormAttachment(wlFileNameInField, 0, SWT.CENTER);
    fdFileNameInField.right = new FormAttachment(100, 0);
    wFileNameInField.setLayoutData(fdFileNameInField);
    wFileNameInField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            activeFileNameField();
          }
        });

    // FileNameField Line
    wlFileNameField = new Label(fileGroup, SWT.RIGHT);
    wlFileNameField.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.FileNameField.Label"));
    PropsUi.setLook(wlFileNameField);
    FormData fdlFileNameField = new FormData();
    fdlFileNameField.left = new FormAttachment(0, 0);
    fdlFileNameField.right = new FormAttachment(middle, -margin);
    fdlFileNameField.top = new FormAttachment(wFileNameInField, margin);
    wlFileNameField.setLayoutData(fdlFileNameField);

    wFileNameField = new ComboVar(variables, fileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFileNameField);
    wFileNameField.addModifyListener(lsMod);
    FormData fdFileNameField = new FormData();
    fdFileNameField.left = new FormAttachment(middle, 0);
    fdFileNameField.top = new FormAttachment(wFileNameInField, margin);
    fdFileNameField.right = new FormAttachment(100, 0);
    wFileNameField.setLayoutData(fdFileNameField);
    wFileNameField.setEnabled(false);
    wFileNameField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focustLost event
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            getFields();
            shell.setCursor(null);
            busy.dispose();
          }
        });
    /* End */

    Label wlStreamData = new Label(fileGroup, SWT.RIGHT);
    wlStreamData.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.StreamData.Label"));
    PropsUi.setLook(wlStreamData);
    FormData fdlStreamData = new FormData();
    fdlStreamData.left = new FormAttachment(0, 0);
    fdlStreamData.top = new FormAttachment(wFileNameField, margin);
    fdlStreamData.right = new FormAttachment(middle, -margin);
    wlStreamData.setLayoutData(fdlStreamData);
    wStreamData = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wStreamData);
    FormData fdStreamData = new FormData();
    fdStreamData.left = new FormAttachment(middle, 0);
    fdStreamData.top = new FormAttachment(wlStreamData, 0, SWT.CENTER);
    fdStreamData.right = new FormAttachment(100, 0);
    wStreamData.setLayoutData(fdStreamData);
    wStreamData.addSelectionListener(lsSel);

    // split every x rows
    wlSplitEvery = new Label(fileGroup, SWT.RIGHT);
    wlSplitEvery.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.SplitEvery.Label"));
    PropsUi.setLook(wlSplitEvery);
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment(0, 0);
    fdlSplitEvery.top = new FormAttachment(wStreamData, margin);
    fdlSplitEvery.right = new FormAttachment(middle, -margin);
    wlSplitEvery.setLayoutData(fdlSplitEvery);
    wSplitEvery = new Text(fileGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSplitEvery);
    wSplitEvery.addModifyListener(lsMod);
    wSplitEvery.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.SplitEvery.Tooltip"));
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment(middle, 0);
    fdSplitEvery.top = new FormAttachment(wStreamData, margin);
    fdSplitEvery.right = new FormAttachment(100, 0);
    wSplitEvery.setLayoutData(fdSplitEvery);

    // Create multi-part file?
    wlAddTransformNr = new Label(fileGroup, SWT.RIGHT);
    wlAddTransformNr.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AddTransformnr.Label"));
    PropsUi.setLook(wlAddTransformNr);
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment(0, 0);
    fdlAddTransformNr.top = new FormAttachment(wSplitEvery, margin);
    fdlAddTransformNr.right = new FormAttachment(middle, -margin);
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wAddTransformNr);
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment(middle, 0);
    fdAddTransformNr.top = new FormAttachment(wlAddTransformNr, 0, SWT.CENTER);
    fdAddTransformNr.right = new FormAttachment(100, 0);
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener(lsSel);

    // Create multi-part file?
    wlAddDate = new Label(fileGroup, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wAddTransformNr, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(lsSel);
    // Create multi-part file?
    wlAddTime = new Label(fileGroup, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wAddDate, margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(lsSel);

    // Specify date time format?
    wlSpecifyFormat = new Label(fileGroup, SWT.RIGHT);
    wlSpecifyFormat.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wAddTime, margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(fileGroup, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.SpecifyFormat.Tooltip"));
    FormData fdSpecifyFormat = new FormData();
    fdSpecifyFormat.left = new FormAttachment(middle, 0);
    fdSpecifyFormat.top = new FormAttachment(wlSpecifyFormat, 0, SWT.CENTER);
    fdSpecifyFormat.right = new FormAttachment(100, 0);
    wSpecifyFormat.setLayoutData(fdSpecifyFormat);
    wSpecifyFormat.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setDateTimeFormat();
          }
        });

    // Prepare a list of possible DateTimeFormats...
    String[] dats = Const.getDateFormats();

    // DateTimeFormat
    wlDateTimeFormat = new Label(fileGroup, SWT.RIGHT);
    wlDateTimeFormat.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wSpecifyFormat, 2 * margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(fileGroup, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    PropsUi.setLook(wDateTimeFormat);
    wDateTimeFormat.addModifyListener(lsMod);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, 0);
    fdDateTimeFormat.top = new FormAttachment(wSpecifyFormat, 2 * margin);
    fdDateTimeFormat.right = new FormAttachment(100, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    wbShowFiles = new Button(fileGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.top = new FormAttachment(wDateTimeFormat, margin * 3);
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            ExcelWriterTransformMeta tfoi = new ExcelWriterTransformMeta();
            getInfo(tfoi);
            String[] files = tfoi.getFiles(variables);
            if (files != null && files.length > 0) {
              EnterSelectionDialog esd =
                  new EnterSelectionDialog(
                      shell,
                      files,
                      BaseMessages.getString(
                          PKG, "ExcelWriterDialog.SelectOutputFiles.DialogTitle"),
                      BaseMessages.getString(
                          PKG, "ExcelWriterDialog.SelectOutputFiles.DialogMessage"));
              esd.setViewOnly();
              esd.open();
            } else {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setMessage(
                  BaseMessages.getString(PKG, "ExcelWriterDialog.NoFilesFound.DialogMessage"));
              mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
              mb.open();
            }
          }
        });

    // If output file exists line
    Label wlIfFileExists = new Label(fileGroup, SWT.RIGHT);
    wlIfFileExists.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.IfFileExists.Label"));
    PropsUi.setLook(wlIfFileExists);
    FormData fdlIfFileExists = new FormData();
    fdlIfFileExists.left = new FormAttachment(0, 0);
    fdlIfFileExists.top = new FormAttachment(wbShowFiles, 2 * margin, margin);
    fdlIfFileExists.right = new FormAttachment(middle, -margin);
    wlIfFileExists.setLayoutData(fdlIfFileExists);
    wIfFileExists = new CCombo(fileGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.READ_ONLY);

    String createNewLabel =
        BaseMessages.getString(PKG, "ExcelWriterDialog.IfFileExists.CreateNew.Label");
    String reuseLabel = BaseMessages.getString(PKG, "ExcelWriterDialog.IfFileExists.Reuse.Label");
    wIfFileExists.setItems(new String[] {createNewLabel, reuseLabel});
    wIfFileExists.setData(createNewLabel, ExcelWriterTransformMeta.IF_FILE_EXISTS_CREATE_NEW);
    wIfFileExists.setData(reuseLabel, ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE);

    PropsUi.setLook(wIfFileExists);
    wIfFileExists.addModifyListener(lsMod);
    wIfFileExists.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.IfFileExists.Tooltip"));

    FormData fdIfFileExists = new FormData();
    fdIfFileExists.left = new FormAttachment(middle, 0);
    fdIfFileExists.top = new FormAttachment(wbShowFiles, 2 * margin, margin);
    fdIfFileExists.right = new FormAttachment(100, 0);
    wIfFileExists.setLayoutData(fdIfFileExists);

    // Add File to the result files name
    Label wlAddToResult = new Label(fileGroup, SWT.RIGHT);
    wlAddToResult.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(wIfFileExists, 2 * margin, margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(fileGroup, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdAddToResult.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdAddToResult);
    wAddToResult.addSelectionListener(lsSel);

    FormData fsFileGroup = new FormData();
    fsFileGroup.left = new FormAttachment(0, margin);
    fsFileGroup.top = new FormAttachment(0, margin);
    fsFileGroup.right = new FormAttachment(100, -margin);
    fileGroup.setLayoutData(fsFileGroup);

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

    // END OF FILE GROUP

    // ////////////////////////
    // START OF SHEET & TEMPLATE TAB
    // /
    CTabItem wSheetTemplateTab = new CTabItem(wTabFolder, SWT.NONE);
    wSheetTemplateTab.setFont(GuiResource.getInstance().getFontDefault());
    wSheetTemplateTab.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.SheeTemplateTab.TabTitle"));

    Composite wSheetTemplateComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wSheetTemplateComp);

    FormLayout sheetTemplateLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wSheetTemplateComp.setLayout(sheetTemplateLayout);

    Group sheetGroup = new Group(wSheetTemplateComp, SWT.SHADOW_NONE);
    PropsUi.setLook(sheetGroup);
    sheetGroup.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.sheetGroup.Label"));

    FormLayout sheetGroupLayout = new FormLayout();
    sheetGroupLayout.marginWidth = 10;
    sheetGroupLayout.marginHeight = 10;
    sheetGroup.setLayout(sheetGroupLayout);

    // Sheet name line
    Label wlSheetname = new Label(sheetGroup, SWT.RIGHT);
    wlSheetname.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Sheetname.Label"));
    PropsUi.setLook(wlSheetname);
    FormData fdlSheetname = new FormData();
    fdlSheetname.left = new FormAttachment(0, 0);
    fdlSheetname.top = new FormAttachment(0, margin);
    fdlSheetname.right = new FormAttachment(middle, -margin);
    wlSheetname.setLayoutData(fdlSheetname);
    wSheetname = new TextVar(variables, sheetGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSheetname.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Sheetname.Tooltip"));
    PropsUi.setLook(wSheetname);
    wSheetname.addModifyListener(lsMod);
    FormData fdSheetname = new FormData();
    fdSheetname.left = new FormAttachment(middle, 0);
    fdSheetname.top = new FormAttachment(0, margin);
    fdSheetname.right = new FormAttachment(100, 0);
    wSheetname.setLayoutData(fdSheetname);

    // Make sheet active Sheet Line
    Label wlMakeActiveSheet = new Label(sheetGroup, SWT.RIGHT);
    wlMakeActiveSheet.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.MakeActiveSheet.Label"));
    PropsUi.setLook(wlMakeActiveSheet);
    FormData fdlMakeActiveSheet = new FormData();
    fdlMakeActiveSheet.left = new FormAttachment(0, 0);
    fdlMakeActiveSheet.top = new FormAttachment(wSheetname, margin);
    fdlMakeActiveSheet.right = new FormAttachment(middle, -margin);
    wlMakeActiveSheet.setLayoutData(fdlMakeActiveSheet);
    wMakeActiveSheet = new Button(sheetGroup, SWT.CHECK);
    wMakeActiveSheet.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.MakeActiveSheet.Tooltip"));
    PropsUi.setLook(wMakeActiveSheet);
    FormData fdMakeActiveSheet = new FormData();
    fdMakeActiveSheet.left = new FormAttachment(middle, 0);
    fdMakeActiveSheet.top = new FormAttachment(wlMakeActiveSheet, 0, SWT.CENTER);
    fdMakeActiveSheet.right = new FormAttachment(100, 0);
    wMakeActiveSheet.setLayoutData(fdMakeActiveSheet);
    wMakeActiveSheet.addSelectionListener(lsSel);

    // If output sheet exists line
    Label wlIfSheetExists = new Label(sheetGroup, SWT.RIGHT);
    wlIfSheetExists.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.IfSheetExists.Label"));
    PropsUi.setLook(wlIfSheetExists);
    FormData fdlIfSheetExists = new FormData();
    fdlIfSheetExists.left = new FormAttachment(0, 0);
    fdlIfSheetExists.top = new FormAttachment(wMakeActiveSheet, margin);
    fdlIfSheetExists.right = new FormAttachment(middle, -margin);
    wlIfSheetExists.setLayoutData(fdlIfSheetExists);
    wIfSheetExists = new CCombo(sheetGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.READ_ONLY);

    String replaceSheetNewLabel =
        BaseMessages.getString(PKG, "ExcelWriterDialog.IfSheetExists.CreateNew.Label");
    String reuseSheetLabel =
        BaseMessages.getString(PKG, "ExcelWriterDialog.IfSheetExists.Reuse.Label");
    wIfSheetExists.setItems(new String[] {replaceSheetNewLabel, reuseSheetLabel});
    wIfSheetExists.setData(
        replaceSheetNewLabel, ExcelWriterTransformMeta.IF_SHEET_EXISTS_CREATE_NEW);
    wIfSheetExists.setData(reuseSheetLabel, ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE);

    PropsUi.setLook(wIfSheetExists);
    wIfSheetExists.addModifyListener(lsMod);
    wIfSheetExists.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.IfSheetExists.Tooltip"));

    FormData fdIfSheetExists = new FormData();
    fdIfSheetExists.left = new FormAttachment(middle, 0);
    fdIfSheetExists.top = new FormAttachment(wMakeActiveSheet, margin);
    fdIfSheetExists.right = new FormAttachment(100, 0);
    wIfSheetExists.setLayoutData(fdIfSheetExists);

    // Protect Sheet?
    Label wlProtectSheet = new Label(sheetGroup, SWT.RIGHT);
    wlProtectSheet.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.ProtectSheet.Label"));
    PropsUi.setLook(wlProtectSheet);
    FormData fdlProtectSheet = new FormData();
    fdlProtectSheet.left = new FormAttachment(0, 0);
    fdlProtectSheet.top = new FormAttachment(wIfSheetExists, margin);
    fdlProtectSheet.right = new FormAttachment(middle, -margin);
    wlProtectSheet.setLayoutData(fdlProtectSheet);
    wProtectSheet = new Button(sheetGroup, SWT.CHECK);
    wProtectSheet.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.ProtectSheet.Tooltip"));
    PropsUi.setLook(wProtectSheet);
    FormData fdProtectSheet = new FormData();
    fdProtectSheet.left = new FormAttachment(middle, 0);
    fdProtectSheet.top = new FormAttachment(wlProtectSheet, 0, SWT.CENTER);
    fdProtectSheet.right = new FormAttachment(100, 0);
    wProtectSheet.setLayoutData(fdProtectSheet);
    wProtectSheet.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enablePassword();
          }
        });

    // Protected by line
    Label wlProtectedBy = new Label(sheetGroup, SWT.RIGHT);
    wlProtectedBy.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.ProtectedBy.Label"));
    PropsUi.setLook(wlProtectedBy);
    FormData fdlProtectedBy = new FormData();
    fdlProtectedBy.left = new FormAttachment(0, 0);
    fdlProtectedBy.top = new FormAttachment(wProtectSheet, margin);
    fdlProtectedBy.right = new FormAttachment(middle, -margin);
    wlProtectedBy.setLayoutData(fdlProtectedBy);
    wProtectedBy = new TextVar(variables, sheetGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wProtectedBy.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.ProtectedBy.Tooltip"));
    PropsUi.setLook(wProtectedBy);

    wProtectedBy.addModifyListener(lsMod);
    FormData fdProtectedBy = new FormData();
    fdProtectedBy.left = new FormAttachment(middle, 0);
    fdProtectedBy.top = new FormAttachment(wProtectSheet, margin);
    fdProtectedBy.right = new FormAttachment(100, 0);
    wProtectedBy.setLayoutData(fdProtectedBy);

    // Password line
    Label wlPassword = new Label(sheetGroup, SWT.RIGHT);
    wlPassword.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Password.Label"));
    PropsUi.setLook(wlPassword);
    FormData fdlPassword = new FormData();
    fdlPassword.left = new FormAttachment(0, 0);
    fdlPassword.top = new FormAttachment(wProtectedBy, margin);
    fdlPassword.right = new FormAttachment(middle, -margin);
    wlPassword.setLayoutData(fdlPassword);
    wPassword = new PasswordTextVar(variables, sheetGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wPassword.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Password.Tooltip"));
    PropsUi.setLook(wPassword);
    wPassword.addModifyListener(lsMod);
    FormData fdPassword = new FormData();
    fdPassword.left = new FormAttachment(middle, 0);
    fdPassword.top = new FormAttachment(wProtectedBy, margin);
    fdPassword.right = new FormAttachment(100, 0);
    wPassword.setLayoutData(fdPassword);

    FormData fsSheetGroup = new FormData();
    fsSheetGroup.left = new FormAttachment(0, margin);
    fsSheetGroup.top = new FormAttachment(fileGroup, margin);
    fsSheetGroup.right = new FormAttachment(100, -margin);
    sheetGroup.setLayoutData(fsSheetGroup);

    // END OF SHEET GROUP

    // ///////////////////////////////
    // START OF Template Group GROUP //
    // ///////////////////////////////

    Group wTemplateGroup = new Group(wSheetTemplateComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wTemplateGroup);
    wTemplateGroup.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateGroup.Label"));

    FormLayout templateGroupGroupLayout = new FormLayout();
    templateGroupGroupLayout.marginWidth = 10;
    templateGroupGroupLayout.marginHeight = 10;
    wTemplateGroup.setLayout(templateGroupGroupLayout);

    // Use template
    Label wlTemplate = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplate.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Template.Label"));
    PropsUi.setLook(wlTemplate);
    FormData fdlTemplate = new FormData();
    fdlTemplate.left = new FormAttachment(0, 0);
    fdlTemplate.top = new FormAttachment(0, margin);
    fdlTemplate.right = new FormAttachment(middle, -margin);
    wlTemplate.setLayoutData(fdlTemplate);
    wTemplate = new Button(wTemplateGroup, SWT.CHECK);
    PropsUi.setLook(wTemplate);
    FormData fdTemplate = new FormData();
    fdTemplate.left = new FormAttachment(middle, 0);
    fdTemplate.top = new FormAttachment(wlTemplate, 0, SWT.CENTER);
    fdTemplate.right = new FormAttachment(100, 0);
    wTemplate.setLayoutData(fdTemplate);
    wTemplate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableTemplate();
          }
        });
    wTemplate.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Template.Tooltip"));

    // TemplateFilename line
    Label wlTemplateFilename = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplateFilename.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateFilename.Label"));
    PropsUi.setLook(wlTemplateFilename);
    FormData fdlTemplateFilename = new FormData();
    fdlTemplateFilename.left = new FormAttachment(0, 0);
    fdlTemplateFilename.top = new FormAttachment(wTemplate, margin);
    fdlTemplateFilename.right = new FormAttachment(middle, -margin);
    wlTemplateFilename.setLayoutData(fdlTemplateFilename);

    wbTemplateFilename = new Button(wTemplateGroup, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTemplateFilename);
    wbTemplateFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbTemplateFilename = new FormData();
    fdbTemplateFilename.right = new FormAttachment(100, 0);
    fdbTemplateFilename.top = new FormAttachment(wTemplate, 0);
    wbTemplateFilename.setLayoutData(fdbTemplateFilename);

    wTemplateFilename = new TextVar(variables, wTemplateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTemplateFilename);
    wTemplateFilename.addModifyListener(lsMod);
    FormData fdTemplateFilename = new FormData();
    fdTemplateFilename.left = new FormAttachment(middle, 0);
    fdTemplateFilename.top = new FormAttachment(wTemplate, margin);
    fdTemplateFilename.right = new FormAttachment(wbTemplateFilename, -margin);
    wTemplateFilename.setLayoutData(fdTemplateFilename);

    // Use template sheet
    Label wlTemplateSheet = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplateSheet.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateSheet.Label"));
    PropsUi.setLook(wlTemplateSheet);
    FormData fdlTemplateSheet = new FormData();
    fdlTemplateSheet.left = new FormAttachment(0, 0);
    fdlTemplateSheet.top = new FormAttachment(wTemplateFilename, margin);
    fdlTemplateSheet.right = new FormAttachment(middle, -margin);
    wlTemplateSheet.setLayoutData(fdlTemplateSheet);
    wTemplateSheet = new Button(wTemplateGroup, SWT.CHECK);
    wTemplateSheet.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateSheet.Tooltip"));

    PropsUi.setLook(wTemplateSheet);
    FormData fdTemplateSheet = new FormData();
    fdTemplateSheet.left = new FormAttachment(middle, 0);
    fdTemplateSheet.top = new FormAttachment(wlTemplateSheet, 0, SWT.CENTER);
    fdTemplateSheet.right = new FormAttachment(100, 0);
    wTemplateSheet.setLayoutData(fdTemplateSheet);
    wTemplateSheet.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableTemplateSheet();
          }
        });

    // TemplateSheetname line
    Label wlTemplateSheetname = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplateSheetname.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateSheetname.Label"));
    PropsUi.setLook(wlTemplateSheetname);
    FormData fdlTemplateSheetname = new FormData();
    fdlTemplateSheetname.left = new FormAttachment(0, 0);
    fdlTemplateSheetname.top = new FormAttachment(wTemplateSheet, margin);
    fdlTemplateSheetname.right = new FormAttachment(middle, -margin);
    wlTemplateSheetname.setLayoutData(fdlTemplateSheetname);

    wTemplateSheetname = new TextVar(variables, wTemplateGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTemplateSheetname);
    wTemplateSheetname.addModifyListener(lsMod);
    FormData fdTemplateSheetname = new FormData();
    fdTemplateSheetname.left = new FormAttachment(middle, 0);
    fdTemplateSheetname.top = new FormAttachment(wTemplateSheet, margin);
    fdTemplateSheetname.right = new FormAttachment(wbTemplateFilename, -margin);
    wTemplateSheetname.setLayoutData(fdTemplateSheetname);

    // Hide Template Sheet
    Label wlTemplateSheetHide = new Label(wTemplateGroup, SWT.RIGHT);
    wlTemplateSheetHide.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateSheetHide.Label"));
    PropsUi.setLook(wlTemplateSheetHide);
    FormData fdlTemplateSheetHide = new FormData();
    fdlTemplateSheetHide.left = new FormAttachment(0, 0);
    fdlTemplateSheetHide.top = new FormAttachment(wTemplateSheetname, margin);
    fdlTemplateSheetHide.right = new FormAttachment(middle, -margin);
    wlTemplateSheetHide.setLayoutData(fdlTemplateSheetHide);
    wTemplateSheetHide = new Button(wTemplateGroup, SWT.CHECK);
    wTemplateSheetHide.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.TemplateSheetHide.Tooltip"));
    PropsUi.setLook(wTemplateSheetHide);
    FormData fdTemplateSheetHide = new FormData();
    fdTemplateSheetHide.left = new FormAttachment(middle, 0);
    fdTemplateSheetHide.top = new FormAttachment(wlTemplateSheetHide, 0, SWT.CENTER);
    fdTemplateSheetHide.right = new FormAttachment(100, 0);
    wTemplateSheetHide.setLayoutData(fdTemplateSheetHide);
    wTemplateSheetHide.addSelectionListener(lsSel);

    FormData fdTemplateGroup = new FormData();
    fdTemplateGroup.left = new FormAttachment(0, margin);
    fdTemplateGroup.top = new FormAttachment(sheetGroup, margin);
    fdTemplateGroup.right = new FormAttachment(100, -margin);
    wTemplateGroup.setLayoutData(fdTemplateGroup);

    FormData fdSheetTemplateComp = new FormData();
    fdSheetTemplateComp.left = new FormAttachment(0, 0);
    fdSheetTemplateComp.top = new FormAttachment(0, 0);
    fdSheetTemplateComp.right = new FormAttachment(100, 0);
    wSheetTemplateComp.setLayoutData(fdSheetTemplateComp);

    wSheetTemplateComp.layout();
    wSheetTemplateTab.setControl(wSheetTemplateComp);

    // ///////////////////////////////////////////////////////////
    // END OF SHEET & TEMPLATE TAB
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF CONTENT TAB///
    // /
    CTabItem wContentTab = new CTabItem(wTabFolder, SWT.NONE);
    wContentTab.setFont(GuiResource.getInstance().getFontDefault());
    wContentTab.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.ContentTab.TabTitle"));

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;
    wContentComp.setLayout(contentLayout);

    Group wContentGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wContentGroup);
    wContentGroup.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.ContentGroup.Label"));

    FormLayout contentGroupGroupLayout = new FormLayout();
    contentGroupGroupLayout.marginWidth = 10;
    contentGroupGroupLayout.marginHeight = 10;
    wContentGroup.setLayout(contentGroupGroupLayout);

    // starting cell
    Label wlStartingCell = new Label(wContentGroup, SWT.RIGHT);
    wlStartingCell.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.StartingCell.Label"));
    PropsUi.setLook(wlStartingCell);
    FormData fdlStartingCell = new FormData();
    fdlStartingCell.left = new FormAttachment(0, 0);
    fdlStartingCell.top = new FormAttachment(wIfSheetExists, margin);
    fdlStartingCell.right = new FormAttachment(middle, -margin);
    wlStartingCell.setLayoutData(fdlStartingCell);
    wStartingCell = new TextVar(variables, wContentGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wStartingCell.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.StartingCell.Tooltip"));
    PropsUi.setLook(wStartingCell);
    wStartingCell.addModifyListener(lsMod);
    FormData fdStartingCell = new FormData();
    fdStartingCell.left = new FormAttachment(middle, 0);
    fdStartingCell.top = new FormAttachment(wIfSheetExists, margin);
    fdStartingCell.right = new FormAttachment(100, 0);
    wStartingCell.setLayoutData(fdStartingCell);

    // row writing method line
    Label wlRowWritingMethod = new Label(wContentGroup, SWT.RIGHT);
    wlRowWritingMethod.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.RowWritingMethod.Label"));
    PropsUi.setLook(wlRowWritingMethod);
    FormData fdlRowWritingMethod = new FormData();
    fdlRowWritingMethod.left = new FormAttachment(0, 0);
    fdlRowWritingMethod.top = new FormAttachment(wStartingCell, margin);
    fdlRowWritingMethod.right = new FormAttachment(middle, -margin);
    wlRowWritingMethod.setLayoutData(fdlRowWritingMethod);
    wRowWritingMethod =
        new CCombo(wContentGroup, SWT.LEFT | SWT.BORDER | SWT.SINGLE | SWT.READ_ONLY);

    String overwriteLabel =
        BaseMessages.getString(PKG, "ExcelWriterDialog.RowWritingMethod.Overwrite.Label");
    String pushDownLabel =
        BaseMessages.getString(PKG, "ExcelWriterDialog.RowWritingMethod.PushDown.Label");
    wRowWritingMethod.setItems(new String[] {overwriteLabel, pushDownLabel});
    wRowWritingMethod.setData(overwriteLabel, ExcelWriterTransformMeta.ROW_WRITE_OVERWRITE);
    wRowWritingMethod.setData(pushDownLabel, ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN);
    wRowWritingMethod.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.RowWritingMethod.Tooltip"));

    PropsUi.setLook(wRowWritingMethod);
    wRowWritingMethod.addModifyListener(lsMod);

    FormData fdRowWritingMethod = new FormData();
    fdRowWritingMethod.left = new FormAttachment(middle, 0);
    fdRowWritingMethod.top = new FormAttachment(wStartingCell, margin);
    fdRowWritingMethod.right = new FormAttachment(100, 0);
    wRowWritingMethod.setLayoutData(fdRowWritingMethod);

    Label wlHeader = new Label(wContentGroup, SWT.RIGHT);
    wlHeader.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Header.Label"));
    PropsUi.setLook(wlHeader);
    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment(0, 0);
    fdlHeader.top = new FormAttachment(wRowWritingMethod, margin);
    fdlHeader.right = new FormAttachment(middle, -margin);
    wlHeader.setLayoutData(fdlHeader);
    wHeader = new Button(wContentGroup, SWT.CHECK);
    PropsUi.setLook(wHeader);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(middle, 0);
    fdHeader.top = new FormAttachment(wlHeader, 0, SWT.CENTER);
    fdHeader.right = new FormAttachment(100, 0);
    wHeader.setLayoutData(fdHeader);
    wHeader.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Header.Tooltip"));
    wHeader.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableHeader();
          }
        });

    Label wlFooter = new Label(wContentGroup, SWT.RIGHT);
    wlFooter.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Footer.Label"));
    PropsUi.setLook(wlFooter);
    FormData fdlFooter = new FormData();
    fdlFooter.left = new FormAttachment(0, 0);
    fdlFooter.top = new FormAttachment(wHeader, margin);
    fdlFooter.right = new FormAttachment(middle, -margin);
    wlFooter.setLayoutData(fdlFooter);
    wFooter = new Button(wContentGroup, SWT.CHECK);
    PropsUi.setLook(wFooter);
    FormData fdFooter = new FormData();
    fdFooter.left = new FormAttachment(middle, 0);
    fdFooter.top = new FormAttachment(wlFooter, 0, SWT.CENTER);
    fdFooter.right = new FormAttachment(100, 0);
    wFooter.setLayoutData(fdFooter);
    wFooter.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.Footer.Tooltip"));
    wFooter.addSelectionListener(lsSel);

    // auto size columns?
    Label wlAutoSize = new Label(wContentGroup, SWT.RIGHT);
    wlAutoSize.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AutoSize.Label"));
    PropsUi.setLook(wlAutoSize);
    FormData fdlAutoSize = new FormData();
    fdlAutoSize.left = new FormAttachment(0, 0);
    fdlAutoSize.top = new FormAttachment(wFooter, margin);
    fdlAutoSize.right = new FormAttachment(middle, -margin);
    wlAutoSize.setLayoutData(fdlAutoSize);
    wAutoSize = new Button(wContentGroup, SWT.CHECK);
    PropsUi.setLook(wAutoSize);
    wAutoSize.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.AutoSize.Tooltip"));
    FormData fdAutoSize = new FormData();
    fdAutoSize.left = new FormAttachment(middle, 0);
    fdAutoSize.top = new FormAttachment(wlAutoSize, 0, SWT.CENTER);
    fdAutoSize.right = new FormAttachment(100, 0);
    wAutoSize.setLayoutData(fdAutoSize);
    wAutoSize.addSelectionListener(lsSel);

    // force formula recalculation?
    Label wlForceFormulaRecalculation = new Label(wContentGroup, SWT.RIGHT);
    wlForceFormulaRecalculation.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.ForceFormulaRecalculation.Label"));
    PropsUi.setLook(wlForceFormulaRecalculation);
    FormData fdlForceFormulaRecalculation = new FormData();
    fdlForceFormulaRecalculation.left = new FormAttachment(0, 0);
    fdlForceFormulaRecalculation.top = new FormAttachment(wAutoSize, margin);
    fdlForceFormulaRecalculation.right = new FormAttachment(middle, -margin);
    wlForceFormulaRecalculation.setLayoutData(fdlForceFormulaRecalculation);
    wForceFormulaRecalculation = new Button(wContentGroup, SWT.CHECK);
    PropsUi.setLook(wForceFormulaRecalculation);
    wForceFormulaRecalculation.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.ForceFormulaRecalculation.Tooltip"));
    FormData fdForceFormulaRecalculation = new FormData();
    fdForceFormulaRecalculation.left = new FormAttachment(middle, 0);
    fdForceFormulaRecalculation.top =
        new FormAttachment(wlForceFormulaRecalculation, 0, SWT.CENTER);
    fdForceFormulaRecalculation.right = new FormAttachment(100, 0);
    wForceFormulaRecalculation.setLayoutData(fdForceFormulaRecalculation);
    wForceFormulaRecalculation.addSelectionListener(lsSel);

    // leave existing styles alone?
    Label wlLeaveExistingStylesUnchanged = new Label(wContentGroup, SWT.RIGHT);
    wlLeaveExistingStylesUnchanged.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.LeaveExistingStylesUnchanged.Label"));
    PropsUi.setLook(wlLeaveExistingStylesUnchanged);
    FormData fdlLeaveExistingStylesUnchanged = new FormData();
    fdlLeaveExistingStylesUnchanged.left = new FormAttachment(0, 0);
    fdlLeaveExistingStylesUnchanged.top = new FormAttachment(wForceFormulaRecalculation, margin);
    fdlLeaveExistingStylesUnchanged.right = new FormAttachment(middle, -margin);
    wlLeaveExistingStylesUnchanged.setLayoutData(fdlLeaveExistingStylesUnchanged);
    wLeaveExistingStylesUnchanged = new Button(wContentGroup, SWT.CHECK);
    PropsUi.setLook(wLeaveExistingStylesUnchanged);
    wLeaveExistingStylesUnchanged.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.LeaveExistingStylesUnchanged.Tooltip"));
    FormData fdLeaveExistingStylesUnchanged = new FormData();
    fdLeaveExistingStylesUnchanged.left = new FormAttachment(middle, 0);
    fdLeaveExistingStylesUnchanged.top =
        new FormAttachment(wlLeaveExistingStylesUnchanged, 0, SWT.CENTER);
    fdLeaveExistingStylesUnchanged.right = new FormAttachment(100, 0);
    wLeaveExistingStylesUnchanged.setLayoutData(fdLeaveExistingStylesUnchanged);
    wLeaveExistingStylesUnchanged.addSelectionListener(lsSel);

    FormData fdContentGroup = new FormData();
    fdContentGroup.left = new FormAttachment(0, margin);
    fdContentGroup.top = new FormAttachment(wTemplateGroup, margin);
    fdContentGroup.right = new FormAttachment(100, -margin);
    wContentGroup.setLayoutData(fdContentGroup);

    // / END OF CONTENT GROUP

    Group writeToExistingGroup = new Group(wContentComp, SWT.SHADOW_NONE);
    PropsUi.setLook(writeToExistingGroup);
    writeToExistingGroup.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.writeToExistingGroup.Label"));

    FormLayout writeToExistingGroupgroupLayout = new FormLayout();
    writeToExistingGroupgroupLayout.marginWidth = 10;
    writeToExistingGroupgroupLayout.marginHeight = 10;
    writeToExistingGroup.setLayout(writeToExistingGroupgroupLayout);

    // Use AppendLines
    Label wlAppendLines = new Label(writeToExistingGroup, SWT.RIGHT);
    wlAppendLines.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.AppendLines.Label"));
    PropsUi.setLook(wlAppendLines);
    FormData fdlAppendLines = new FormData();
    fdlAppendLines.left = new FormAttachment(0, 0);
    fdlAppendLines.top = new FormAttachment(0, margin);
    fdlAppendLines.right = new FormAttachment(middle, -margin);
    wlAppendLines.setLayoutData(fdlAppendLines);
    wAppendLines = new Button(writeToExistingGroup, SWT.CHECK);
    wAppendLines.setToolTipText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.AppendLines.Tooltip"));
    PropsUi.setLook(wAppendLines);
    FormData fdAppendLines = new FormData();
    fdAppendLines.left = new FormAttachment(middle, 0);
    fdAppendLines.top = new FormAttachment(wlAppendLines, 0, SWT.CENTER);
    fdAppendLines.right = new FormAttachment(100, 0);
    wAppendLines.setLayoutData(fdAppendLines);
    wAppendLines.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            enableAppend();
          }
        });

    // SkipRows line
    Label wlSkipRows = new Label(writeToExistingGroup, SWT.RIGHT);
    wlSkipRows.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.SkipRows.Label"));
    PropsUi.setLook(wlSkipRows);
    FormData fdlSkipRows = new FormData();
    fdlSkipRows.left = new FormAttachment(0, 0);
    fdlSkipRows.top = new FormAttachment(wAppendLines, margin);
    fdlSkipRows.right = new FormAttachment(middle, -margin);
    wlSkipRows.setLayoutData(fdlSkipRows);

    wSkipRows = new Text(writeToExistingGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wSkipRows.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.SkipRows.Tooltip"));
    PropsUi.setLook(wSkipRows);
    wSkipRows.addModifyListener(lsMod);
    FormData fdSkipRows = new FormData();
    fdSkipRows.left = new FormAttachment(middle, 0);
    fdSkipRows.top = new FormAttachment(wAppendLines, margin);
    fdSkipRows.right = new FormAttachment(100, 0);
    wSkipRows.setLayoutData(fdSkipRows);

    // EmptyRows line
    Label wlEmptyRows = new Label(writeToExistingGroup, SWT.RIGHT);
    wlEmptyRows.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.EmptyRows.Label"));
    PropsUi.setLook(wlEmptyRows);
    FormData fdlEmptyRows = new FormData();
    fdlEmptyRows.left = new FormAttachment(0, 0);
    fdlEmptyRows.top = new FormAttachment(wSkipRows, margin);
    fdlEmptyRows.right = new FormAttachment(middle, -margin);
    wlEmptyRows.setLayoutData(fdlEmptyRows);

    wEmptyRows = new Text(writeToExistingGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEmptyRows.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.EmptyRows.Tooltip"));
    PropsUi.setLook(wEmptyRows);
    wEmptyRows.addModifyListener(lsMod);
    FormData fdEmptyRows = new FormData();
    fdEmptyRows.left = new FormAttachment(middle, 0);
    fdEmptyRows.top = new FormAttachment(wSkipRows, margin);
    fdEmptyRows.right = new FormAttachment(100, 0);
    wEmptyRows.setLayoutData(fdEmptyRows);

    // Use AppendLines
    Label wlOmitHeader = new Label(writeToExistingGroup, SWT.RIGHT);
    wlOmitHeader.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.OmitHeader.Label"));
    PropsUi.setLook(wlOmitHeader);
    FormData fdlOmitHeader = new FormData();
    fdlOmitHeader.left = new FormAttachment(0, 0);
    fdlOmitHeader.top = new FormAttachment(wEmptyRows, margin);
    fdlOmitHeader.right = new FormAttachment(middle, -margin);
    wlOmitHeader.setLayoutData(fdlOmitHeader);
    wOmitHeader = new Button(writeToExistingGroup, SWT.CHECK);
    wOmitHeader.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.OmitHeader.Tooltip"));
    PropsUi.setLook(wOmitHeader);
    FormData fdOmitHeader = new FormData();
    fdOmitHeader.left = new FormAttachment(middle, 0);
    fdOmitHeader.top = new FormAttachment(wlOmitHeader, 0, SWT.CENTER);
    fdOmitHeader.right = new FormAttachment(100, 0);
    wOmitHeader.setLayoutData(fdOmitHeader);
    wOmitHeader.addSelectionListener(lsSel);

    FormData fdWriteToExistingGroup = new FormData();
    fdWriteToExistingGroup.left = new FormAttachment(0, margin);
    fdWriteToExistingGroup.top = new FormAttachment(wContentGroup, margin);
    fdWriteToExistingGroup.right = new FormAttachment(100, -margin);
    writeToExistingGroup.setLayoutData(fdWriteToExistingGroup);

    FormData fdContentComp = new FormData();
    fdContentComp.left = new FormAttachment(0, 0);
    fdContentComp.top = new FormAttachment(0, 0);
    fdContentComp.right = new FormAttachment(100, 0);
    wContentGroup.setLayoutData(fdContentComp);

    wContentComp.layout();
    wContentTab.setControl(wContentComp);

    // ///////////////////////////////////////////////////////////
    // / END OF Write to existing Group GROUP
    // ///////////////////////////////////////////////////////////

    // ////////////////////////
    // START OF FIELDS TAB///
    // /
    CTabItem wFieldTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldTab.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.OutputFieldsTab.TabTitle"));

    FormLayout fieldLayout = new FormLayout();
    fieldLayout.marginWidth = 3;
    fieldLayout.marginHeight = 3;

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            fillFieldsLayoutFromSchema();
            input.setChanged();
          }
        };

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    wFieldsComp.setLayout(fieldLayout);

    Group fieldGroup = new Group(wFieldsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(fieldGroup);
    fieldGroup.setText(
        BaseMessages.getString(PKG, "ExcelWriterDialog.ManualSchemaDefinition.Label"));

    FormLayout fieldGroupGroupLayout = new FormLayout();
    fieldGroupGroupLayout.marginWidth = 10;
    fieldGroupGroupLayout.marginHeight = 10;
    fieldGroup.setLayout(fieldGroupGroupLayout);

    wGet = new Button(fieldGroup, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));

    Button wMinWidth = new Button(fieldGroup, SWT.PUSH);
    wMinWidth.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.MinWidth.Button"));
    wMinWidth.setToolTipText(BaseMessages.getString(PKG, "ExcelWriterDialog.MinWidth.Tooltip"));

    setButtonPositions(new Button[] {wGet, wMinWidth}, margin, null);

    final int FieldsRows = input.getOutputFields().size();

    // Prepare a list of possible formats, filtering reserved internal formats away

    List<String> allFormats = Arrays.asList(BuiltinFormats.getAll());
    List<String> nonReservedFormats = new ArrayList<>(allFormats.size());

    for (String format : allFormats) {
      if (!format.startsWith("reserved")) {
        nonReservedFormats.add(format);
      }
    }

    wSchemaDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SchemaDefinition.class,
            wFieldsComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "ExcelWriterDialog.SchemaDefinition.Label"),
            BaseMessages.getString(PKG, "ExcelWriterDialog.SchemaDefinition.Tooltip"));

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

    Collections.sort(nonReservedFormats);
    String[] formats = nonReservedFormats.toArray(new String[0]);

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.NameColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.TypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.FormatColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              formats),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.UseStyleCell.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.TitleColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.UseTitleStyleCell.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.FormulaField.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"N", "Y"},
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.HyperLinkField.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.CommentField.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ExcelWriterDialog.CommentAuthor.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false)
        };

    wFields =
        new TableView(
            variables,
            fieldGroup,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            colinf,
            FieldsRows,
            lsMod,
            props);

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(wGet, -margin);
    wFields.setLayoutData(fdFields);
    wFields.addModifyListener(lsMod);

    // Search the fields in the background

    final Runnable runnable =
        () -> {
          TransformMeta transformMeta = pipelineMeta.findTransform(transformName);
          if (transformMeta != null) {
            try {
              IRowMeta row = pipelineMeta.getPrevTransformFields(variables, transformMeta);

              // Remember these fields...
              for (int i = 0; i < row.size(); i++) {
                inputFields.add(row.getValueMeta(i).getName());
              }
              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    FormData fdFieldGroup = new FormData();
    fdFieldGroup.left = new FormAttachment(0, margin);
    fdFieldGroup.top = new FormAttachment(wSchemaDefinition, margin);
    fdFieldGroup.bottom = new FormAttachment(100, 0);
    fdFieldGroup.right = new FormAttachment(100, -margin);
    fieldGroup.setLayoutData(fdFieldGroup);

    FormData fdFieldComp = new FormData();
    fdFieldComp.left = new FormAttachment(0, 0);
    fdFieldComp.top = new FormAttachment(0, 0);
    fdFieldComp.right = new FormAttachment(100, 0);
    fdFieldComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldComp);

    wFieldsComp.layout();
    wFieldTab.setControl(wFieldsComp);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wTransformName, margin);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -2 * margin);
    sc.setLayoutData(fdSc);

    sc.setContent(wTabFolder);

    // ///////////////////////////////////////////////////////////
    // / END OF CONTENT TAB
    // ///////////////////////////////////////////////////////////

    // Add listeners
    wGet.addListener(SWT.Selection, e -> get());
    wMinWidth.addListener(SWT.Selection, e -> setMinimalWidth());

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e ->
            wFilename.setToolTipText(
                variables.resolve(wFilename.getText())
                    + "\n\n"
                    + BaseMessages.getString(PKG, "ExcelWriterDialog.Filename.Tooltip")));
    wTemplateFilename.addModifyListener(
        e -> wTemplateFilename.setToolTipText(variables.resolve(wTemplateFilename.getText())));

    wSheetname.addModifyListener(
        e ->
            wSheetname.setToolTipText(
                variables.resolve(wSheetname.getText())
                    + "\n\n"
                    + BaseMessages.getString(PKG, "ExcelWriterDialog.Sheetname.Tooltip")));

    wTemplateSheetname.addModifyListener(
        e -> wTemplateSheetname.setToolTipText(variables.resolve(wTemplateSheetname.getText())));

    wStartingCell.addModifyListener(
        e ->
            wStartingCell.setToolTipText(
                variables.resolve(wStartingCell.getText())
                    + "\n\n"
                    + BaseMessages.getString(PKG, "ExcelWriterDialog.StartingCell.Tooltip")));

    wPassword.addModifyListener(
        e ->
            wPassword.setToolTipText(
                BaseMessages.getString(PKG, "ExcelWriterDialog.Password.Tooltip")));

    wProtectedBy.addModifyListener(
        e ->
            wProtectedBy.setToolTipText(
                variables.resolve(wProtectedBy.getText())
                    + "\n\n"
                    + BaseMessages.getString(PKG, "ExcelWriterDialog.ProtectedBy.Tooltip")));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wFilename,
                variables,
                new String[] {"*.xls", "*.xlsx", "*.*"},
                new String[] {
                  BaseMessages.getString(PKG, LABEL_FORMATXLS),
                  BaseMessages.getString(PKG, LABEL_FORMATXLSX),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));
    wbTemplateFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wTemplateFilename,
                variables,
                new String[] {"*.xls", "*.xlsx", "*.*"},
                new String[] {
                  BaseMessages.getString(PKG, LABEL_FORMATXLS),
                  BaseMessages.getString(PKG, LABEL_FORMATXLSX),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wTabFolder.setSelection(0);

    getData();
    activeFileNameField();
    enableExtension();
    enableAppend();
    enableHeader();
    enableTemplateSheet();
    input.setChanged(changed);

    // artificially reduce table size
    for (int t = 0; t < wFields.table.getColumnCount(); t++) {
      wFields.table.getColumn(t).setWidth(20);
    }

    wFields.layout();
    wFields.pack();

    // determine scrollable area
    sc.setMinSize(wTabFolder.computeSize(SWT.DEFAULT, SWT.DEFAULT));
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);

    // restore optimal column widths
    wFields.optWidth(true);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void fillFieldsLayoutFromSchema() {

    if (!wSchemaDefinition.isDisposed()) {
      final String schemaName = wSchemaDefinition.getText();

      MessageBox mb = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "ExcelWriterDialog.Load.SchemaDefinition.Message", schemaName));
      mb.setText(BaseMessages.getString(PKG, "ExcelWriterDialog.Load.SchemaDefinition.Title"));
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
                  item.setText(3, Const.NVL(valueMeta.getConversionMask(), ""));
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

  private void activeFileNameField() {
    wlFileNameField.setEnabled(wFileNameInField.getSelection());
    wFileNameField.setEnabled(wFileNameInField.getSelection());
    wlFilename.setEnabled(!wFileNameInField.getSelection());
    wFilename.setEnabled(!wFileNameInField.getSelection());

    if (wFileNameInField.getSelection()) {
      if (!wDoNotOpenNewFileInit.getSelection()) {
        wDoNotOpenNewFileInit.setSelection(true);
      }
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
      wSpecifyFormat.setSelection(false);
      wAddTransformNr.setSelection(false);
    }

    wlSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    wSpecifyFormat.setEnabled(!wFileNameInField.getSelection());

    wAddTransformNr.setEnabled(!wFileNameInField.getSelection());
    wlAddTransformNr.setEnabled(!wFileNameInField.getSelection());
    if (wFileNameInField.getSelection()) {
      wSplitEvery.setText("0");
    }
    wSplitEvery.setEnabled(!wFileNameInField.getSelection());
    wlSplitEvery.setEnabled(!wFileNameInField.getSelection());
    wbShowFiles.setEnabled(!wFileNameInField.getSelection());

    setDateTimeFormat();
  }

  private void enableAppend() {
    wSplitEvery.setEnabled(!wAppendLines.getSelection());
  }

  private void enableHeader() {
    wOmitHeader.setEnabled(wHeader.getSelection());
  }

  private void setDateTimeFormat() {
    if (wSpecifyFormat.getSelection()) {
      wAddDate.setSelection(false);
      wAddTime.setSelection(false);
    }

    wDateTimeFormat.setEnabled(wSpecifyFormat.getSelection() && !wFileNameInField.getSelection());
    wlDateTimeFormat.setEnabled(wSpecifyFormat.getSelection() && !wFileNameInField.getSelection());
    wAddDate.setEnabled(!(wFileNameInField.getSelection() || wSpecifyFormat.getSelection()));
    wlAddDate.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
    wAddTime.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
    wlAddTime.setEnabled(!(wSpecifyFormat.getSelection() || wFileNameInField.getSelection()));
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
    colinf[7].setComboValues(fieldNames);
    colinf[8].setComboValues(fieldNames);
    colinf[9].setComboValues(fieldNames);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    ExcelWriterFileField file = input.getFile();
    ExcelWriterTemplateField template = input.getTemplate();

    if (file.getFileName() != null) {
      wFilename.setText(file.getFileName());
    }
    wDoNotOpenNewFileInit.setSelection(file.isDoNotOpenNewFileInit());
    if (file.getExtension() != null) {

      if (file.getExtension().equals("xlsx")) {
        wExtension.select(1);
      } else {
        wExtension.select(0);
      }
    }

    wFileNameInField.setSelection(file.isFileNameInField());

    if (file.isFileNameInField() && file.getFileNameField() != null) {
      wFileNameField.setText(file.getFileNameField());
    }

    wSchemaDefinition.setText(Const.NVL(input.getSchemaDefinition(), ""));
    wStreamData.setSelection(file.isStreamingData());
    wSplitEvery.setText("" + file.getSplitEvery());
    wEmptyRows.setText("" + input.getAppendEmpty());
    wSkipRows.setText("" + input.getAppendOffset());
    wAppendLines.setSelection(input.isAppendLines());
    wHeader.setSelection(input.isHeaderEnabled());
    wFooter.setSelection(input.isFooterEnabled());
    wOmitHeader.setSelection(input.isAppendOmitHeader());
    wForceFormulaRecalculation.setSelection(input.isForceFormulaRecalculation());
    wLeaveExistingStylesUnchanged.setSelection(input.isLeaveExistingStylesUnchanged());

    if (input.getStartingCell() != null) {
      wStartingCell.setText(input.getStartingCell());
    }

    wAddDate.setSelection(file.isDateInFilename());
    wAddTime.setSelection(file.isTimeInFilename());

    if (file.getDateTimeFormat() != null) {
      wDateTimeFormat.setText(file.getDateTimeFormat());
    }

    wSpecifyFormat.setSelection(file.isSpecifyFormat());
    wCreateParentFolder.setSelection(file.isCreateParentFolder());
    wAddToResult.setSelection(input.isAddToResultFilenames());
    wAutoSize.setSelection(file.isAutosizecolums());
    wIfFileExists.select(
        ExcelWriterTransformMeta.IF_FILE_EXISTS_REUSE.equals(file.getIfFileExists()) ? 1 : 0);
    wIfSheetExists.select(
        ExcelWriterTransformMeta.IF_SHEET_EXISTS_REUSE.equals(file.getIfSheetExists()) ? 1 : 0);
    wRowWritingMethod.select(
        ExcelWriterTransformMeta.ROW_WRITE_PUSH_DOWN.equals(input.getRowWritingMethod()) ? 1 : 0);

    wAddTransformNr.setSelection(file.isTransformNrInFilename());
    wMakeActiveSheet.setSelection(input.isMakeSheetActive());
    wTemplate.setSelection(template.isTemplateEnabled());
    wTemplateSheet.setSelection(template.isTemplateSheetEnabled());

    if (template.getTemplateFileName() != null) {
      wTemplateFilename.setText(template.getTemplateFileName());
    }

    if (template.getTemplateSheetName() != null) {
      wTemplateSheetname.setText(template.getTemplateSheetName());
    }

    if (file.getSheetname() != null) {
      wSheetname.setText(file.getSheetname());
    } else {
      wSheetname.setText(BaseMessages.getString(PKG, "ExcelWriterMeta.Tab.Sheetname.Text"));
    }
    wTemplateSheetHide.setSelection(template.isTemplateSheetHidden());
    wProtectSheet.setSelection(file.isProtectsheet());

    enablePassword();
    enableTemplate();

    if (file.getPassword() != null) {
      wPassword.setText(file.getPassword());
    }
    if (file.getProtectedBy() != null) {
      wProtectedBy.setText(file.getProtectedBy());
    }

    logDebug("Getting fields info...");

    for (int i = 0; i < input.getOutputFields().size(); i++) {
      ExcelWriterOutputField field = input.getOutputFields().get(i);

      TableItem item = wFields.table.getItem(i);
      if (field.getName() != null) {
        item.setText(1, field.getName());
      }
      item.setText(2, field.getType());

      if (field.getFormat() != null) {
        item.setText(3, field.getFormat());
      }
      if (field.getStyleCell() != null) {
        item.setText(4, field.getStyleCell());
      }
      if (field.getTitle() != null) {
        item.setText(5, field.getTitle());
      }
      if (field.getTitleStyleCell() != null) {
        item.setText(6, field.getTitleStyleCell());
      }
      if (field.isFormula()) {
        item.setText(7, "Y");
      } else {
        item.setText(7, "N");
      }

      if (field.getHyperlinkField() != null) {
        item.setText(8, field.getHyperlinkField());
      }
      if (field.getCommentField() != null) {
        item.setText(9, field.getCommentField());
      }
      if (field.getCommentAuthorField() != null) {
        item.setText(10, field.getCommentAuthorField());
      }
    }

    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void getFields() {
    if (!gotPreviousFields) {
      try {
        String field = wFileNameField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wFileNameField.setItems(r.getFieldNames());
        }
        if (field != null) {
          wFileNameField.setText(field);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "ExcelWriterDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "ExcelWriterDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void cancel() {
    transformName = null;

    input.setChanged(backupChanged);

    dispose();
  }

  private void getInfo(ExcelWriterTransformMeta tfoi) {

    ExcelWriterFileField file = tfoi.getFile();
    ExcelWriterTemplateField template = tfoi.getTemplate();

    tfoi.setSchemaDefinition(wSchemaDefinition.getText());
    file.setFileName(wFilename.getText());
    file.setCreateParentFolder(wCreateParentFolder.getSelection());
    file.setStreamingData(wStreamData.getSelection());
    file.setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());
    file.setFileNameInField(wFileNameInField.getSelection());
    file.setFileNameField(wFileNameField.getText());
    tfoi.setAppendOmitHeader(wOmitHeader.getSelection());
    file.setExtension((String) wExtension.getData(wExtension.getText()));
    file.setSplitEvery(Const.toInt(wSplitEvery.getText(), 0));
    tfoi.setAppendOffset(Const.toInt(wSkipRows.getText(), 0));
    tfoi.setAppendEmpty(Const.toInt(wEmptyRows.getText(), 0));
    tfoi.setAppendLines(wAppendLines.getSelection());
    tfoi.setHeaderEnabled(wHeader.getSelection());
    tfoi.setFooterEnabled(wFooter.getSelection());
    tfoi.setStartingCell(wStartingCell.getText());
    file.setTransformNrInFilename(wAddTransformNr.getSelection());
    file.setDateInFilename(wAddDate.getSelection());
    file.setTimeInFilename(wAddTime.getSelection());
    file.setIfFileExists((String) wIfFileExists.getData(wIfFileExists.getText()));
    file.setIfSheetExists((String) wIfSheetExists.getData(wIfSheetExists.getText()));
    tfoi.setRowWritingMethod((String) wRowWritingMethod.getData(wRowWritingMethod.getText()));
    tfoi.setForceFormulaRecalculation(wForceFormulaRecalculation.getSelection());
    tfoi.setLeaveExistingStylesUnchanged(wLeaveExistingStylesUnchanged.getSelection());

    file.setDateTimeFormat(wDateTimeFormat.getText());
    file.setSpecifyFormat(wSpecifyFormat.getSelection());
    file.setAutosizecolums(wAutoSize.getSelection());

    tfoi.setAddToResultFilenames(wAddToResult.getSelection());

    tfoi.setMakeSheetActive(wMakeActiveSheet.getSelection());
    file.setProtectsheet(wProtectSheet.getSelection());
    file.setProtectedBy(wProtectedBy.getText());
    file.setPassword(wPassword.getText());

    template.setTemplateEnabled(wTemplate.getSelection());
    template.setTemplateSheetEnabled(wTemplateSheet.getSelection());
    template.setTemplateFileName(wTemplateFilename.getText());
    template.setTemplateSheetName(wTemplateSheetname.getText());
    template.setTemplateSheetHidden(wTemplateSheetHide.getSelection());

    if (wSheetname.getText() != null) {
      file.setSheetname(wSheetname.getText());
    } else {
      file.setSheetname(BaseMessages.getString(PKG, "ExcelWriterMeta.Tab.Sheetname.Text"));
    }

    int nrFields = wFields.nrNonEmpty();
    input.getOutputFields().clear();

    for (int i = 0; i < nrFields; i++) {
      ExcelWriterOutputField field = new ExcelWriterOutputField();

      TableItem item = wFields.getNonEmpty(i);
      field.setName(item.getText(1));
      field.setType(item.getText(2));
      field.setFormat(item.getText(3));
      field.setStyleCell(item.getText(4));
      field.setTitle(item.getText(5));
      field.setTitleStyleCell(item.getText(6));
      field.setFormula(item.getText(7).equalsIgnoreCase("Y"));
      field.setHyperlinkField(item.getText(8));
      field.setCommentField(item.getText(9));
      field.setCommentAuthorField(item.getText(10));
      tfoi.getOutputFields().add(field);
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    getInfo(input);

    dispose();
  }

  private void enablePassword() {
    wPassword.setEnabled(wProtectSheet.getSelection());
    wProtectedBy.setEnabled(wProtectSheet.getSelection());
  }

  private void enableTemplate() {
    wbTemplateFilename.setEnabled(wTemplate.getSelection());
    wTemplateFilename.setEnabled(wTemplate.getSelection());
  }

  private void enableTemplateSheet() {
    wTemplateSheetname.setEnabled(wTemplateSheet.getSelection());
    wTemplateSheetHide.setEnabled(wTemplateSheet.getSelection());
  }

  private void enableExtension() {
    wProtectSheet.setEnabled(wExtension.getSelectionIndex() == 0);
    if (wExtension.getSelectionIndex() == 0) {
      wPassword.setEnabled(wProtectSheet.getSelection());
      wProtectedBy.setEnabled(wProtectSheet.getSelection());
      wStreamData.setEnabled(false);
    } else {
      wPassword.setEnabled(false);
      wProtectedBy.setEnabled(false);
      wStreamData.setEnabled(true);
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.isNumber()) {
                if (v.getLength() > 0) {
                  int le = v.getLength();
                  int pr = v.getPrecision();

                  if (v.getPrecision() <= 0) {
                    pr = 0;
                  }

                  String mask = "";
                  for (int m = 0; m < le - pr; m++) {
                    mask += "0";
                  }
                  if (pr > 0) {
                    mask += ".";
                  }
                  for (int m = 0; m < pr; m++) {
                    mask += "0";
                  }
                  tableItem.setText(3, mask);
                }
              }
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 5}, new int[] {2}, 0, 0, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"),
          ke);
    }
  }

  /** Sets the output width to minimal width... */
  public void setMinimalWidth() {
    int nrNonEmptyFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrNonEmptyFields; i++) {
      TableItem item = wFields.getNonEmpty(i);

      int type = ValueMetaFactory.getIdForValueMeta(item.getText(2));
      switch (type) {
        case IValueMeta.TYPE_STRING:
          item.setText(3, "");
          break;
        case IValueMeta.TYPE_INTEGER:
          item.setText(3, "0");
          break;
        case IValueMeta.TYPE_NUMBER:
          item.setText(3, "0.#####");
          break;
        case IValueMeta.TYPE_DATE:
          break;
        default:
          break;
      }
    }
    wFields.optWidth(true);
  }
}
