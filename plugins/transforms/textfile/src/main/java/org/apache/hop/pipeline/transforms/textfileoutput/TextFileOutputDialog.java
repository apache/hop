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

package org.apache.hop.pipeline.transforms.textfileoutput;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.compress.CompressionProviderFactory;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.staticschema.metadata.SchemaDefinition;
import org.apache.hop.staticschema.metadata.SchemaFieldDefinition;
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
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
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
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class TextFileOutputDialog extends BaseTransformDialog {
  private static final Class<?> PKG = TextFileOutputMeta.class;

  private Label wlFilename;
  private Button wbFilename;
  private TextVar wFilename;

  protected Button wServletOutput;

  private Label wlExtension;
  private TextVar wExtension;

  private Label wlAddTransformNr;
  private Button wAddTransformNr;

  private Label wlAddPartnr;
  private Button wAddPartnr;

  private Label wlAddDate;
  private Button wAddDate;

  private Label wlAddTime;
  private Button wAddTime;

  private Button wbShowFiles;

  private Button wFileNameInField;

  private Label wlFileNameField;
  private ComboVar wFileNameField;
  /* END */

  private Label wlAppend;
  private Button wAppend;

  private TextVar wSeparator;

  private TextVar wEnclosure;

  private TextVar wEndedLine;

  private Button wEnclForced;

  private Button wDisableEnclosureFix;

  private Button wHeader;

  private Button wFooter;

  private CCombo wFormat;

  private CCombo wCompression;

  private CCombo wEncoding;

  private Button wPad;

  private Button wFastDump;

  private Label wlSplitEvery;
  private TextVar wSplitEvery;

  private TableView wFields;

  protected TextFileOutputMeta input;

  private boolean gotEncodings = false;

  private Label wlAddToResult;
  private Button wAddToResult;

  private Label wlDoNotOpenNewFileInit;
  private Button wDoNotOpenNewFileInit;

  private Label wlDateTimeFormat;
  private CCombo wDateTimeFormat;

  private Label wlSpecifyFormat;
  private Button wSpecifyFormat;

  protected Label wlCreateParentFolder;
  protected Button wCreateParentFolder;

  private MetaSelectionLine<SchemaDefinition> wSchemaDefinition;
  private Button wIgnoreFields;

  private ColumnInfo[] colinf;

  private final List<String> inputFields = new ArrayList<>();

  private boolean gotPreviousFields = false;

  public TextFileOutputDialog(
      Shell parent,
      IVariables variables,
      TextFileOutputMeta transformMeta,
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

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(getDialogTitle());

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

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
    wTransformName.addModifyListener(lsMod);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Buttons at the bottom first
    //
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // START OF FILE TAB///
    // /
    CTabItem wFileTab = new CTabItem(wTabFolder, SWT.NONE);
    wFileTab.setFont(GuiResource.getInstance().getFontDefault());
    wFileTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FileTab.TabTitle"));

    Composite wFileComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFileComp);

    FormLayout fileLayout = new FormLayout();
    fileLayout.marginWidth = 3;
    fileLayout.marginHeight = 3;
    wFileComp.setLayout(fileLayout);

    // Filename line
    wlFilename = new Label(wFileComp, SWT.RIGHT);
    wlFilename.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Filename.Label"));
    PropsUi.setLook(wlFilename);
    FormData fdlFilename = new FormData();
    fdlFilename.left = new FormAttachment(0, 0);
    fdlFilename.top = new FormAttachment(0, margin);
    fdlFilename.right = new FormAttachment(middle, -margin);
    wlFilename.setLayoutData(fdlFilename);

    wbFilename = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbFilename);
    wbFilename.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbFilename = new FormData();
    fdbFilename.right = new FormAttachment(100, 0);
    fdbFilename.top = new FormAttachment(0, 0);
    wbFilename.setLayoutData(fdbFilename);

    wFilename = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFilename);
    wFilename.addModifyListener(lsMod);
    FormData fdFilename = new FormData();
    fdFilename.left = new FormAttachment(middle, 0);
    fdFilename.top = new FormAttachment(0, margin);
    fdFilename.right = new FormAttachment(wbFilename, -margin);
    wFilename.setLayoutData(fdFilename);

    Control topControl = addAdditionalComponentIfNeed(middle, margin, wFileComp, wFilename);

    // Output to servlet (browser, ws)
    //
    Label wlServletOutput = new Label(wFileComp, SWT.RIGHT);
    wlServletOutput.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.ServletOutput.Label"));
    PropsUi.setLook(wlServletOutput);
    FormData fdlServletOutput = new FormData();
    fdlServletOutput.left = new FormAttachment(0, 0);
    fdlServletOutput.top = new FormAttachment(topControl, margin);
    fdlServletOutput.right = new FormAttachment(middle, -margin);
    wlServletOutput.setLayoutData(fdlServletOutput);
    wServletOutput = new Button(wFileComp, SWT.CHECK);
    wServletOutput.setToolTipText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.ServletOutput.Tooltip"));
    PropsUi.setLook(wServletOutput);
    FormData fdServletOutput = new FormData();
    fdServletOutput.left = new FormAttachment(middle, 0);
    fdServletOutput.top = new FormAttachment(wlServletOutput, 0, SWT.CENTER);
    fdServletOutput.right = new FormAttachment(100, 0);
    wServletOutput.setLayoutData(fdServletOutput);
    wServletOutput.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setFlagsServletOption();
          }
        });

    // Create Parent Folder
    wlCreateParentFolder = new Label(wFileComp, SWT.RIGHT);
    wlCreateParentFolder.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.CreateParentFolder.Label"));
    PropsUi.setLook(wlCreateParentFolder);
    FormData fdlCreateParentFolder = new FormData();
    fdlCreateParentFolder.left = new FormAttachment(0, 0);
    fdlCreateParentFolder.top = new FormAttachment(wServletOutput, margin);
    fdlCreateParentFolder.right = new FormAttachment(middle, -margin);
    wlCreateParentFolder.setLayoutData(fdlCreateParentFolder);
    wCreateParentFolder = new Button(wFileComp, SWT.CHECK);
    wCreateParentFolder.setToolTipText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.CreateParentFolder.Tooltip"));
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

    // Open new File at Init
    wlDoNotOpenNewFileInit = new Label(wFileComp, SWT.RIGHT);
    wlDoNotOpenNewFileInit.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.DoNotOpenNewFileInit.Label"));
    PropsUi.setLook(wlDoNotOpenNewFileInit);
    FormData fdlDoNotOpenNewFileInit = new FormData();
    fdlDoNotOpenNewFileInit.left = new FormAttachment(0, 0);
    fdlDoNotOpenNewFileInit.top = new FormAttachment(wCreateParentFolder, margin);
    fdlDoNotOpenNewFileInit.right = new FormAttachment(middle, -margin);
    wlDoNotOpenNewFileInit.setLayoutData(fdlDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit = new Button(wFileComp, SWT.CHECK);
    wDoNotOpenNewFileInit.setToolTipText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.DoNotOpenNewFileInit.Tooltip"));
    PropsUi.setLook(wDoNotOpenNewFileInit);
    FormData fdDoNotOpenNewFileInit = new FormData();
    fdDoNotOpenNewFileInit.left = new FormAttachment(middle, 0);
    fdDoNotOpenNewFileInit.top = new FormAttachment(wlDoNotOpenNewFileInit, 0, SWT.CENTER);
    fdDoNotOpenNewFileInit.right = new FormAttachment(100, 0);
    wDoNotOpenNewFileInit.setLayoutData(fdDoNotOpenNewFileInit);
    wDoNotOpenNewFileInit.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    /* next Lines */
    // FileNameInField line
    /* Additional fields */
    Label wlFileNameInField = new Label(wFileComp, SWT.RIGHT);
    wlFileNameInField.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.FileNameInField.Label"));
    PropsUi.setLook(wlFileNameInField);
    FormData fdlFileNameInField = new FormData();
    fdlFileNameInField.left = new FormAttachment(0, 0);
    fdlFileNameInField.top = new FormAttachment(wDoNotOpenNewFileInit, margin);
    fdlFileNameInField.right = new FormAttachment(middle, -margin);
    wlFileNameInField.setLayoutData(fdlFileNameInField);
    wFileNameInField = new Button(wFileComp, SWT.CHECK);
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
    wlFileNameField = new Label(wFileComp, SWT.RIGHT);
    wlFileNameField.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.FileNameField.Label"));
    PropsUi.setLook(wlFileNameField);
    FormData fdlFileNameField = new FormData();
    fdlFileNameField.left = new FormAttachment(0, 0);
    fdlFileNameField.right = new FormAttachment(middle, -margin);
    fdlFileNameField.top = new FormAttachment(wFileNameInField, margin);
    wlFileNameField.setLayoutData(fdlFileNameField);

    wFileNameField = new ComboVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
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
            // Do nothing
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

    // Extension line
    wlExtension = new Label(wFileComp, SWT.RIGHT);
    wlExtension.setText(BaseMessages.getString(PKG, "System.Label.Extension"));
    PropsUi.setLook(wlExtension);
    FormData fdlExtension = new FormData();
    fdlExtension.left = new FormAttachment(0, 0);
    fdlExtension.top = new FormAttachment(wFileNameField, margin);
    fdlExtension.right = new FormAttachment(middle, -margin);
    wlExtension.setLayoutData(fdlExtension);
    wExtension = new TextVar(variables, wFileComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wExtension.setText("");
    PropsUi.setLook(wExtension);
    wExtension.addModifyListener(lsMod);
    FormData fdExtension = new FormData();
    fdExtension.left = new FormAttachment(middle, 0);
    fdExtension.top = new FormAttachment(wFileNameField, margin);
    fdExtension.right = new FormAttachment(100, 0);
    wExtension.setLayoutData(fdExtension);

    // Create multi-part file?
    wlAddTransformNr = new Label(wFileComp, SWT.RIGHT);
    wlAddTransformNr.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.AddTransformnr.Label"));
    PropsUi.setLook(wlAddTransformNr);
    FormData fdlAddTransformNr = new FormData();
    fdlAddTransformNr.left = new FormAttachment(0, 0);
    fdlAddTransformNr.top = new FormAttachment(wExtension, margin);
    fdlAddTransformNr.right = new FormAttachment(middle, -margin);
    wlAddTransformNr.setLayoutData(fdlAddTransformNr);
    wAddTransformNr = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddTransformNr);
    FormData fdAddTransformNr = new FormData();
    fdAddTransformNr.left = new FormAttachment(middle, 0);
    fdAddTransformNr.top = new FormAttachment(wlAddTransformNr, 0, SWT.CENTER);
    fdAddTransformNr.right = new FormAttachment(100, 0);
    wAddTransformNr.setLayoutData(fdAddTransformNr);
    wAddTransformNr.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Create multi-part file?
    wlAddPartnr = new Label(wFileComp, SWT.RIGHT);
    wlAddPartnr.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddPartnr.Label"));
    PropsUi.setLook(wlAddPartnr);
    FormData fdlAddPartnr = new FormData();
    fdlAddPartnr.left = new FormAttachment(0, 0);
    fdlAddPartnr.top = new FormAttachment(wAddTransformNr, margin);
    fdlAddPartnr.right = new FormAttachment(middle, -margin);
    wlAddPartnr.setLayoutData(fdlAddPartnr);
    wAddPartnr = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddPartnr);
    FormData fdAddPartnr = new FormData();
    fdAddPartnr.left = new FormAttachment(middle, 0);
    fdAddPartnr.top = new FormAttachment(wlAddPartnr, 0, SWT.CENTER);
    fdAddPartnr.right = new FormAttachment(100, 0);
    wAddPartnr.setLayoutData(fdAddPartnr);
    wAddPartnr.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Create multi-part file?
    wlAddDate = new Label(wFileComp, SWT.RIGHT);
    wlAddDate.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddDate.Label"));
    PropsUi.setLook(wlAddDate);
    FormData fdlAddDate = new FormData();
    fdlAddDate.left = new FormAttachment(0, 0);
    fdlAddDate.top = new FormAttachment(wAddPartnr, margin);
    fdlAddDate.right = new FormAttachment(middle, -margin);
    wlAddDate.setLayoutData(fdlAddDate);
    wAddDate = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddDate);
    FormData fdAddDate = new FormData();
    fdAddDate.left = new FormAttachment(middle, 0);
    fdAddDate.top = new FormAttachment(wlAddDate, 0, SWT.CENTER);
    fdAddDate.right = new FormAttachment(100, 0);
    wAddDate.setLayoutData(fdAddDate);
    wAddDate.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    // Create multi-part file?
    wlAddTime = new Label(wFileComp, SWT.RIGHT);
    wlAddTime.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.AddTime.Label"));
    PropsUi.setLook(wlAddTime);
    FormData fdlAddTime = new FormData();
    fdlAddTime.left = new FormAttachment(0, 0);
    fdlAddTime.top = new FormAttachment(wAddDate, margin);
    fdlAddTime.right = new FormAttachment(middle, -margin);
    wlAddTime.setLayoutData(fdlAddTime);
    wAddTime = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wAddTime);
    FormData fdAddTime = new FormData();
    fdAddTime.left = new FormAttachment(middle, 0);
    fdAddTime.top = new FormAttachment(wlAddTime, 0, SWT.CENTER);
    fdAddTime.right = new FormAttachment(100, 0);
    wAddTime.setLayoutData(fdAddTime);
    wAddTime.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    // Specify date time format?
    wlSpecifyFormat = new Label(wFileComp, SWT.RIGHT);
    wlSpecifyFormat.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.SpecifyFormat.Label"));
    PropsUi.setLook(wlSpecifyFormat);
    FormData fdlSpecifyFormat = new FormData();
    fdlSpecifyFormat.left = new FormAttachment(0, 0);
    fdlSpecifyFormat.top = new FormAttachment(wAddTime, margin);
    fdlSpecifyFormat.right = new FormAttachment(middle, -margin);
    wlSpecifyFormat.setLayoutData(fdlSpecifyFormat);
    wSpecifyFormat = new Button(wFileComp, SWT.CHECK);
    PropsUi.setLook(wSpecifyFormat);
    wSpecifyFormat.setToolTipText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.SpecifyFormat.Tooltip"));
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

    // DateTimeFormat
    wlDateTimeFormat = new Label(wFileComp, SWT.RIGHT);
    wlDateTimeFormat.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.DateTimeFormat.Label"));
    PropsUi.setLook(wlDateTimeFormat);
    FormData fdlDateTimeFormat = new FormData();
    fdlDateTimeFormat.left = new FormAttachment(0, 0);
    fdlDateTimeFormat.top = new FormAttachment(wSpecifyFormat, margin);
    fdlDateTimeFormat.right = new FormAttachment(middle, -margin);
    wlDateTimeFormat.setLayoutData(fdlDateTimeFormat);
    wDateTimeFormat = new CCombo(wFileComp, SWT.BORDER | SWT.READ_ONLY);
    wDateTimeFormat.setEditable(true);
    PropsUi.setLook(wDateTimeFormat);
    wDateTimeFormat.addModifyListener(lsMod);
    FormData fdDateTimeFormat = new FormData();
    fdDateTimeFormat.left = new FormAttachment(middle, 0);
    fdDateTimeFormat.top = new FormAttachment(wSpecifyFormat, margin);
    fdDateTimeFormat.right = new FormAttachment(100, 0);
    wDateTimeFormat.setLayoutData(fdDateTimeFormat);
    String[] dats = Const.getDateFormats();
    for (String dat : dats) {
      wDateTimeFormat.add(dat);
    }

    wbShowFiles = new Button(wFileComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbShowFiles);
    wbShowFiles.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.ShowFiles.Button"));
    FormData fdbShowFiles = new FormData();
    fdbShowFiles.left = new FormAttachment(middle, 0);
    fdbShowFiles.top = new FormAttachment(wDateTimeFormat, margin * 2);
    wbShowFiles.setLayoutData(fdbShowFiles);
    wbShowFiles.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            TextFileOutputMeta tfoi = new TextFileOutputMeta();
            saveInfoInMeta(tfoi);
            String[] files = tfoi.getFiles(variables);
            if (files != null && files.length > 0) {
              EnterSelectionDialog esd =
                  new EnterSelectionDialog(
                      shell,
                      files,
                      BaseMessages.getString(
                          PKG, "TextFileOutputDialog.SelectOutputFiles.DialogTitle"),
                      BaseMessages.getString(
                          PKG, "TextFileOutputDialog.SelectOutputFiles.DialogMessage"));
              esd.setViewOnly();
              esd.open();
            } else {
              MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
              mb.setMessage(
                  BaseMessages.getString(PKG, "TextFileOutputDialog.NoFilesFound.DialogMessage"));
              mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
              mb.open();
            }
          }
        });

    // Add File to the result files name
    wlAddToResult = new Label(wFileComp, SWT.RIGHT);
    wlAddToResult.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.AddFileToResult.Label"));
    PropsUi.setLook(wlAddToResult);
    FormData fdlAddToResult = new FormData();
    fdlAddToResult.left = new FormAttachment(0, 0);
    fdlAddToResult.top = new FormAttachment(wbShowFiles, 2 * margin);
    fdlAddToResult.right = new FormAttachment(middle, -margin);
    wlAddToResult.setLayoutData(fdlAddToResult);
    wAddToResult = new Button(wFileComp, SWT.CHECK);
    wAddToResult.setToolTipText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.AddFileToResult.Tooltip"));
    PropsUi.setLook(wAddToResult);
    FormData fdAddToResult = new FormData();
    fdAddToResult.left = new FormAttachment(middle, 0);
    fdAddToResult.top = new FormAttachment(wlAddToResult, 0, SWT.CENTER);
    fdAddToResult.right = new FormAttachment(100, 0);
    wAddToResult.setLayoutData(fdAddToResult);
    SelectionAdapter lsSelR =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
          }
        };
    wAddToResult.addSelectionListener(lsSelR);

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
    wContentTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.ContentTab.TabTitle"));

    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = 3;
    contentLayout.marginHeight = 3;

    Composite wContentComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wContentComp);
    wContentComp.setLayout(contentLayout);

    // Append to end of file?
    wlAppend = new Label(wContentComp, SWT.RIGHT);
    wlAppend.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Append.Label"));
    PropsUi.setLook(wlAppend);
    FormData fdlAppend = new FormData();
    fdlAppend.left = new FormAttachment(0, 0);
    fdlAppend.top = new FormAttachment(0, 0);
    fdlAppend.right = new FormAttachment(middle, -margin);
    wlAppend.setLayoutData(fdlAppend);
    wAppend = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wAppend);
    FormData fdAppend = new FormData();
    fdAppend.left = new FormAttachment(middle, 0);
    fdAppend.top = new FormAttachment(wlAppend, 0, SWT.CENTER);
    fdAppend.right = new FormAttachment(100, 0);
    wAppend.setLayoutData(fdAppend);
    wAppend.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }

          private void headerDisabling() {
            wHeader.setSelection(false);
            wHeader.setEnabled(!wAppend.getSelection());
          }
        });

    Label wlSeparator = new Label(wContentComp, SWT.RIGHT);
    wlSeparator.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Separator.Label"));
    PropsUi.setLook(wlSeparator);
    FormData fdlSeparator = new FormData();
    fdlSeparator.left = new FormAttachment(0, 0);
    fdlSeparator.top = new FormAttachment(wAppend, margin);
    fdlSeparator.right = new FormAttachment(middle, -margin);
    wlSeparator.setLayoutData(fdlSeparator);

    Button wbSeparator = new Button(wContentComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSeparator);
    wbSeparator.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Separator.Button"));
    FormData fdbSeparator = new FormData();
    fdbSeparator.right = new FormAttachment(100, 0);
    fdbSeparator.top = new FormAttachment(wAppend, 0);
    wbSeparator.setLayoutData(fdbSeparator);
    wbSeparator.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent se) {
            wSeparator.getTextWidget().insert("\t");
          }
        });

    wSeparator = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeparator);
    wSeparator.addModifyListener(lsMod);
    FormData fdSeparator = new FormData();
    fdSeparator.left = new FormAttachment(middle, 0);
    fdSeparator.top = new FormAttachment(wAppend, margin);
    fdSeparator.right = new FormAttachment(wbSeparator, -margin);
    wSeparator.setLayoutData(fdSeparator);

    // Enclosure line...
    Label wlEnclosure = new Label(wContentComp, SWT.RIGHT);
    wlEnclosure.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Enclosure.Label"));
    PropsUi.setLook(wlEnclosure);
    FormData fdlEnclosure = new FormData();
    fdlEnclosure.left = new FormAttachment(0, 0);
    fdlEnclosure.top = new FormAttachment(wSeparator, margin);
    fdlEnclosure.right = new FormAttachment(middle, -margin);
    wlEnclosure.setLayoutData(fdlEnclosure);
    wEnclosure = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEnclosure);
    wEnclosure.addModifyListener(lsMod);
    FormData fdEnclosure = new FormData();
    fdEnclosure.left = new FormAttachment(middle, 0);
    fdEnclosure.top = new FormAttachment(wSeparator, margin);
    fdEnclosure.right = new FormAttachment(100, 0);
    wEnclosure.setLayoutData(fdEnclosure);

    Label wlEnclForced = new Label(wContentComp, SWT.RIGHT);
    wlEnclForced.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.EnclForced.Label"));
    PropsUi.setLook(wlEnclForced);
    FormData fdlEnclForced = new FormData();
    fdlEnclForced.left = new FormAttachment(0, 0);
    fdlEnclForced.top = new FormAttachment(wEnclosure, margin);
    fdlEnclForced.right = new FormAttachment(middle, -margin);
    wlEnclForced.setLayoutData(fdlEnclForced);
    wEnclForced = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wEnclForced);
    FormData fdEnclForced = new FormData();
    fdEnclForced.left = new FormAttachment(middle, 0);
    fdEnclForced.top = new FormAttachment(wlEnclForced, 0, SWT.CENTER);
    fdEnclForced.right = new FormAttachment(100, 0);
    wEnclForced.setLayoutData(fdEnclForced);
    wEnclForced.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlDisableEnclosureFix = new Label(wContentComp, SWT.RIGHT);
    wlDisableEnclosureFix.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.DisableEnclosureFix.Label"));
    PropsUi.setLook(wlDisableEnclosureFix);
    FormData fdlDisableEnclosureFix = new FormData();
    fdlDisableEnclosureFix.left = new FormAttachment(0, 0);
    fdlDisableEnclosureFix.top = new FormAttachment(wEnclForced, margin);
    fdlDisableEnclosureFix.right = new FormAttachment(middle, -margin);
    wlDisableEnclosureFix.setLayoutData(fdlDisableEnclosureFix);
    wDisableEnclosureFix = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wDisableEnclosureFix);
    FormData fdDisableEnclosureFix = new FormData();
    fdDisableEnclosureFix.left = new FormAttachment(middle, 0);
    fdDisableEnclosureFix.top = new FormAttachment(wlDisableEnclosureFix, 0, SWT.CENTER);
    fdDisableEnclosureFix.right = new FormAttachment(100, 0);
    wDisableEnclosureFix.setLayoutData(fdDisableEnclosureFix);
    wDisableEnclosureFix.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlHeader = new Label(wContentComp, SWT.RIGHT);
    wlHeader.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Header.Label"));
    PropsUi.setLook(wlHeader);
    FormData fdlHeader = new FormData();
    fdlHeader.left = new FormAttachment(0, 0);
    fdlHeader.top = new FormAttachment(wDisableEnclosureFix, margin);
    fdlHeader.right = new FormAttachment(middle, -margin);
    wlHeader.setLayoutData(fdlHeader);
    wHeader = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wHeader);
    FormData fdHeader = new FormData();
    fdHeader.left = new FormAttachment(middle, 0);
    fdHeader.top = new FormAttachment(wlHeader, 0, SWT.CENTER);
    fdHeader.right = new FormAttachment(100, 0);
    wHeader.setLayoutData(fdHeader);
    wHeader.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    Label wlFooter = new Label(wContentComp, SWT.RIGHT);
    wlFooter.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Footer.Label"));
    PropsUi.setLook(wlFooter);
    FormData fdlFooter = new FormData();
    fdlFooter.left = new FormAttachment(0, 0);
    fdlFooter.top = new FormAttachment(wHeader, margin);
    fdlFooter.right = new FormAttachment(middle, -margin);
    wlFooter.setLayoutData(fdlFooter);
    wFooter = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wFooter);
    FormData fdFooter = new FormData();
    fdFooter.left = new FormAttachment(middle, 0);
    fdFooter.top = new FormAttachment(wlFooter, 0, SWT.CENTER);
    fdFooter.right = new FormAttachment(100, 0);
    wFooter.setLayoutData(fdFooter);
    wFooter.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlFormat = new Label(wContentComp, SWT.RIGHT);
    wlFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Format.Label"));
    PropsUi.setLook(wlFormat);
    FormData fdlFormat = new FormData();
    fdlFormat.left = new FormAttachment(0, 0);
    fdlFormat.top = new FormAttachment(wFooter, margin);
    fdlFormat.right = new FormAttachment(middle, -margin);
    wlFormat.setLayoutData(fdlFormat);
    wFormat = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wFormat.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Format.Label"));
    PropsUi.setLook(wFormat);

    for (int i = 0; i < TextFileOutputMeta.formatMapperLineTerminator.length; i++) {
      // add e.g. TextFileOutputDialog.Format.DOS, .UNIX, .CR, .None
      wFormat.add(
          BaseMessages.getString(
              PKG,
              "TextFileOutputDialog.Format." + TextFileOutputMeta.formatMapperLineTerminator[i]));
    }
    wFormat.select(0);
    wFormat.addModifyListener(lsMod);
    FormData fdFormat = new FormData();
    fdFormat.left = new FormAttachment(middle, 0);
    fdFormat.top = new FormAttachment(wFooter, margin);
    fdFormat.right = new FormAttachment(100, 0);
    wFormat.setLayoutData(fdFormat);

    Label wlCompression = new Label(wContentComp, SWT.RIGHT);
    wlCompression.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Compression.Label"));
    PropsUi.setLook(wlCompression);
    FormData fdlCompression = new FormData();
    fdlCompression.left = new FormAttachment(0, 0);
    fdlCompression.top = new FormAttachment(wFormat, margin);
    fdlCompression.right = new FormAttachment(middle, -margin);
    wlCompression.setLayoutData(fdlCompression);
    wCompression = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wCompression.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Compression.Label"));
    PropsUi.setLook(wCompression);

    wCompression.setItems(CompressionProviderFactory.getInstance().getCompressionProviderNames());
    wCompression.addModifyListener(lsMod);
    FormData fdCompression = new FormData();
    fdCompression.left = new FormAttachment(middle, 0);
    fdCompression.top = new FormAttachment(wFormat, margin);
    fdCompression.right = new FormAttachment(100, 0);
    wCompression.setLayoutData(fdCompression);

    Label wlEncoding = new Label(wContentComp, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wCompression, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new CCombo(wContentComp, SWT.BORDER | SWT.READ_ONLY);
    wEncoding.setEditable(true);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wCompression, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do nothing
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            setEncodings();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    Label wlPad = new Label(wContentComp, SWT.RIGHT);
    wlPad.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Pad.Label"));
    PropsUi.setLook(wlPad);
    FormData fdlPad = new FormData();
    fdlPad.left = new FormAttachment(0, 0);
    fdlPad.top = new FormAttachment(wEncoding, margin);
    fdlPad.right = new FormAttachment(middle, -margin);
    wlPad.setLayoutData(fdlPad);
    wPad = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wPad);
    FormData fdPad = new FormData();
    fdPad.left = new FormAttachment(middle, 0);
    fdPad.top = new FormAttachment(wlPad, 0, SWT.CENTER);
    fdPad.right = new FormAttachment(100, 0);
    wPad.setLayoutData(fdPad);
    wPad.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    Label wlFastDump = new Label(wContentComp, SWT.RIGHT);
    wlFastDump.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FastDump.Label"));
    PropsUi.setLook(wlFastDump);
    FormData fdlFastDump = new FormData();
    fdlFastDump.left = new FormAttachment(0, 0);
    fdlFastDump.top = new FormAttachment(wPad, margin);
    fdlFastDump.right = new FormAttachment(middle, -margin);
    wlFastDump.setLayoutData(fdlFastDump);
    wFastDump = new Button(wContentComp, SWT.CHECK);
    PropsUi.setLook(wFastDump);
    FormData fdFastDump = new FormData();
    fdFastDump.left = new FormAttachment(middle, 0);
    fdFastDump.top = new FormAttachment(wlFastDump, 0, SWT.CENTER);
    fdFastDump.right = new FormAttachment(100, 0);
    wFastDump.setLayoutData(fdFastDump);
    wFastDump.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });

    wlSplitEvery = new Label(wContentComp, SWT.RIGHT);
    wlSplitEvery.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.SplitEvery.Label"));
    PropsUi.setLook(wlSplitEvery);
    FormData fdlSplitEvery = new FormData();
    fdlSplitEvery.left = new FormAttachment(0, 0);
    fdlSplitEvery.top = new FormAttachment(wFastDump, margin);
    fdlSplitEvery.right = new FormAttachment(middle, -margin);
    wlSplitEvery.setLayoutData(fdlSplitEvery);
    wSplitEvery = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSplitEvery);
    wSplitEvery.addModifyListener(lsMod);
    FormData fdSplitEvery = new FormData();
    fdSplitEvery.left = new FormAttachment(middle, 0);
    fdSplitEvery.top = new FormAttachment(wFastDump, margin);
    fdSplitEvery.right = new FormAttachment(100, 0);
    wSplitEvery.setLayoutData(fdSplitEvery);

    // Bruise:
    Label wlEndedLine = new Label(wContentComp, SWT.RIGHT);
    wlEndedLine.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.EndedLine.Label"));
    PropsUi.setLook(wlEndedLine);
    FormData fdlEndedLine = new FormData();
    fdlEndedLine.left = new FormAttachment(0, 0);
    fdlEndedLine.top = new FormAttachment(wSplitEvery, margin);
    fdlEndedLine.right = new FormAttachment(middle, -margin);
    wlEndedLine.setLayoutData(fdlEndedLine);
    wEndedLine = new TextVar(variables, wContentComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wEndedLine);
    wEndedLine.addModifyListener(lsMod);
    FormData fdEndedLine = new FormData();
    fdEndedLine.left = new FormAttachment(middle, 0);
    fdEndedLine.top = new FormAttachment(wSplitEvery, margin);
    fdEndedLine.right = new FormAttachment(100, 0);
    wEndedLine.setLayoutData(fdEndedLine);

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

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            fillFieldsLayoutFromSchema();
            input.setChanged();
          }
        };

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.FieldsTab.TabTitle"));

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);
    wFieldsComp.setLayout(fieldsLayout);

    Group fieldGroup = new Group(wFieldsComp, SWT.SHADOW_NONE);
    PropsUi.setLook(fieldGroup);
    fieldGroup.setText(
        BaseMessages.getString(PKG, "TextFileOutputDialog.ManualSchemaDefinition.Label"));

    FormLayout fieldGroupGroupLayout = new FormLayout();
    fieldGroupGroupLayout.marginWidth = 10;
    fieldGroupGroupLayout.marginHeight = 10;
    fieldGroup.setLayout(fieldGroupGroupLayout);

    wGet = new Button(fieldGroup, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "System.Button.GetFields"));
    wGet.setToolTipText(BaseMessages.getString(PKG, "System.Tooltip.GetFields"));

    Button wMinWidth = new Button(fieldGroup, SWT.PUSH);
    wMinWidth.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.MinWidth.Button"));
    wMinWidth.setToolTipText(BaseMessages.getString(PKG, "TextFileOutputDialog.MinWidth.Tooltip"));
    wMinWidth.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        });
    setButtonPositions(new Button[] {wGet, wMinWidth}, margin, null);

    wSchemaDefinition =
        new MetaSelectionLine<>(
            variables,
            metadataProvider,
            SchemaDefinition.class,
            wFieldsComp,
            SWT.NONE,
            BaseMessages.getString(PKG, "TextFileOutputDialog.SchemaDefinition.Label"),
            BaseMessages.getString(PKG, "TextFileOutputDialog.SchemaDefinition.Tooltip"));

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
        BaseMessages.getString(PKG, "TextFileOutputDialog.IgnoreTransformFields.Label"));
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

    wIgnoreFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            wFields.setEnabled(!wIgnoreFields.getSelection());
            wGet.setEnabled(!wIgnoreFields.getSelection());
            wMinWidth.setEnabled(!wIgnoreFields.getSelection());
            input.setChanged();
          }
        });

    final int FieldsRows = input.getOutputFields().length;

    colinf =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.NameColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.TypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames()),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.FormatColumn.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.LengthColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.PrecisionColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.CurrencyColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.DecimalColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.GroupColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.TrimTypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.trimTypeDesc,
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.NullColumn.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "TextFileOutputDialog.RoundingTypeColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.roundingTypeDesc,
              true),
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

    //
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
    fdFieldGroup.top = new FormAttachment(wIgnoreFields, margin);
    fdFieldGroup.right = new FormAttachment(100, -margin);
    fdFieldGroup.bottom = new FormAttachment(100, 0);
    fieldGroup.setLayoutData(fdFieldGroup);

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
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wGet.addListener(SWT.Selection, e -> get());
    wMinWidth.addListener(SWT.Selection, e -> setMinimalWidth());

    // Whenever something changes, set the tooltip to the expanded version:
    wFilename.addModifyListener(
        e -> wFilename.setToolTipText(variables.resolve(wFilename.getText())));

    wbFilename.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                true,
                shell,
                wFilename,
                variables,
                new String[] {"*.txt", "*.csv", "*"},
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.TextFiles"),
                  BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wTabFolder.setSelection(0);

    getData();

    activeFileNameField();
    enableParentFolder();
    input.setChanged(changed);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void fillFieldsLayoutFromSchema() {

    if (!wSchemaDefinition.isDisposed()) {
      final String schemaName = wSchemaDefinition.getText();

      MessageBox mb = new MessageBox(shell, SWT.ICON_QUESTION | SWT.NO | SWT.YES);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "TextFileOutputDialog.Load.SchemaDefinition.Message", schemaName));
      mb.setText(BaseMessages.getString(PKG, "TextFileOutputDialog.Load.SchemaDefinition.Title"));
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
                  item.setText(
                      4, valueMeta.getLength() >= 0 ? Integer.toString(valueMeta.getLength()) : "");
                  item.setText(
                      5,
                      valueMeta.getPrecision() >= 0
                          ? Integer.toString(valueMeta.getPrecision())
                          : "");
                  final SchemaFieldDefinition schemaFieldDefinition =
                      schemaDefinition.getFieldDefinitions().get(i);
                  item.setText(6, Const.NVL(schemaFieldDefinition.getCurrencySymbol(), ""));
                  item.setText(7, Const.NVL(schemaFieldDefinition.getDecimalSymbol(), ""));
                  item.setText(8, Const.NVL(schemaFieldDefinition.getGroupingSymbol(), ""));
                  item.setText(
                      9, Const.NVL(ValueMetaBase.getTrimTypeDesc(valueMeta.getTrimType()), ""));
                  item.setText(10, Const.NVL(schemaFieldDefinition.getIfNullValue(), ""));
                  item.setText(
                      11,
                      ValueMetaBase.getRoundingTypeDesc(schemaFieldDefinition.getRoundingType()));
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

  protected String getDialogTitle() {
    return BaseMessages.getString(PKG, "TextFileOutputDialog.DialogTitle");
  }

  protected Control addAdditionalComponentIfNeed(
      int middle, int margin, Composite wFileComp, Composite topComp) {
    return topComp;
  }

  protected void setFlagsServletOption() {
    boolean enableFilename = !wServletOutput.getSelection();
    wlFilename.setEnabled(enableFilename);
    wFilename.setEnabled(enableFilename);
    wlDoNotOpenNewFileInit.setEnabled(enableFilename);
    wDoNotOpenNewFileInit.setEnabled(enableFilename);
    wlCreateParentFolder.setEnabled(enableFilename);
    wCreateParentFolder.setEnabled(enableFilename);
    wlFileNameField.setEnabled(enableFilename);
    wFileNameField.setEnabled(enableFilename);
    wlExtension.setEnabled(enableFilename);
    wExtension.setEnabled(enableFilename);
    wlSplitEvery.setEnabled(enableFilename);
    wSplitEvery.setEnabled(enableFilename);
    wlAddDate.setEnabled(enableFilename);
    wAddDate.setEnabled(enableFilename);
    wlAddTime.setEnabled(enableFilename);
    wAddTime.setEnabled(enableFilename);
    wlDateTimeFormat.setEnabled(enableFilename);
    wDateTimeFormat.setEnabled(enableFilename);
    wlSpecifyFormat.setEnabled(enableFilename);
    wSpecifyFormat.setEnabled(enableFilename);
    wlAppend.setEnabled(enableFilename);
    wAppend.setEnabled(enableFilename);
    wlAddTransformNr.setEnabled(enableFilename);
    wAddTransformNr.setEnabled(enableFilename);
    wlAddPartnr.setEnabled(enableFilename);
    wAddPartnr.setEnabled(enableFilename);
    wbShowFiles.setEnabled(enableFilename);
    wlAddToResult.setEnabled(enableFilename);
    wAddToResult.setEnabled(enableFilename);
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
      wAddPartnr.setSelection(false);
    }

    wlDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    wDoNotOpenNewFileInit.setEnabled(!wFileNameInField.getSelection());
    wlSpecifyFormat.setEnabled(!wFileNameInField.getSelection());
    wSpecifyFormat.setEnabled(!wFileNameInField.getSelection());

    wAddTransformNr.setEnabled(!wFileNameInField.getSelection());
    wlAddTransformNr.setEnabled(!wFileNameInField.getSelection());
    wAddPartnr.setEnabled(!wFileNameInField.getSelection());
    wlAddPartnr.setEnabled(!wFileNameInField.getSelection());
    if (wFileNameInField.getSelection()) {
      wSplitEvery.setText("0");
    }
    wSplitEvery.setEnabled(!wFileNameInField.getSelection());
    wlSplitEvery.setEnabled(!wFileNameInField.getSelection());
    if (wFileNameInField.getSelection()) {
      wEndedLine.setText("");
    }
    wEndedLine.setEnabled(!wFileNameInField.getSelection());
    wbShowFiles.setEnabled(!wFileNameInField.getSelection());
    wbFilename.setEnabled(!wFileNameInField.getSelection());

    setDateTimeFormat();
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    colinf[0].setComboValues(fieldNames);
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
            BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(PKG, "TextFileOutputDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (input.getFileName() != null) {
      wFilename.setText(input.getFileName());
    }
    wServletOutput.setSelection(input.isServletOutput());
    wSchemaDefinition.setText(Const.NVL(input.getSchemaDefinition(), ""));
    wIgnoreFields.setSelection(input.isIgnoreFields());
    setFlagsServletOption();
    wDoNotOpenNewFileInit.setSelection(input.isDoNotOpenNewFileInit());
    wCreateParentFolder.setSelection(input.isCreateParentFolder());
    wExtension.setText(Const.NVL(input.getExtension(), ""));
    wSeparator.setText(Const.NVL(input.getSeparator(), ""));
    wEnclosure.setText(Const.NVL(input.getEnclosure(), ""));

    if (input.getFileFormat() != null) {
      wFormat.select(0); // default if not found: CR+LF
      for (int i = 0; i < TextFileOutputMeta.formatMapperLineTerminator.length; i++) {
        if (input
            .getFileFormat()
            .equalsIgnoreCase(TextFileOutputMeta.formatMapperLineTerminator[i])) {
          wFormat.select(i);
        }
      }
    }
    if (input.getFileCompression() != null) {
      wCompression.setText(input.getFileCompression());
    }
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    if (input.getEndedLine() != null) {
      wEndedLine.setText(input.getEndedLine());
    }

    wFileNameInField.setSelection(input.isFileNameInField());
    if (input.getFileNameField() != null) {
      wFileNameField.setText(input.getFileNameField());
    }

    wSplitEvery.setText(Const.NVL(input.getSplitEveryRows(), ""));

    wEnclForced.setSelection(input.isEnclosureForced());
    wDisableEnclosureFix.setSelection(input.isEnclosureFixDisabled());
    wHeader.setSelection(input.isHeaderEnabled());
    wFooter.setSelection(input.isFooterEnabled());
    wAddDate.setSelection(input.isDateInFilename());
    wAddTime.setSelection(input.isTimeInFilename());
    wDateTimeFormat.setText(Const.NVL(input.getDateTimeFormat(), ""));
    wSpecifyFormat.setSelection(input.isSpecifyingFormat());

    wAppend.setSelection(input.isFileAppended());
    wAddTransformNr.setSelection(input.isTransformNrInFilename());
    wAddPartnr.setSelection(input.isPartNrInFilename());
    wPad.setSelection(input.isPadded());
    wFastDump.setSelection(input.isFastDump());
    wAddToResult.setSelection(input.isAddToResultFiles());

    logDebug("getting fields info...");

    for (int i = 0; i < input.getOutputFields().length; i++) {
      TextFileField field = input.getOutputFields()[i];

      TableItem item = wFields.table.getItem(i);
      if (field.getName() != null) {
        item.setText(1, field.getName());
      }
      item.setText(2, field.getTypeDesc());
      if (field.getFormat() != null) {
        item.setText(3, field.getFormat());
      }
      if (field.getLength() >= 0) {
        item.setText(4, "" + field.getLength());
      }
      if (field.getPrecision() >= 0) {
        item.setText(5, "" + field.getPrecision());
      }
      if (field.getCurrencySymbol() != null) {
        item.setText(6, field.getCurrencySymbol());
      }
      if (field.getDecimalSymbol() != null) {
        item.setText(7, field.getDecimalSymbol());
      }
      if (field.getGroupingSymbol() != null) {
        item.setText(8, field.getGroupingSymbol());
      }
      String trim = field.getTrimTypeDesc();
      if (trim != null) {
        item.setText(9, trim);
      }
      if (field.getNullString() != null) {
        item.setText(10, field.getNullString());
      }
      String roundingType = ValueMetaBase.getRoundingTypeDesc(field.getRoundingType());
      if (roundingType != null) {
        item.setText(11, roundingType);
      }
    }

    wFields.optWidth(true);

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;

    input.setChanged(backupChanged);

    dispose();
  }

  protected void saveInfoInMeta(TextFileOutputMeta tfoi) {
    tfoi.setFileName(wFilename.getText());
    tfoi.setSchemaDefinition(wSchemaDefinition.getText());
    tfoi.setIgnoreFields(wIgnoreFields.getSelection());
    tfoi.setServletOutput(wServletOutput.getSelection());
    tfoi.setCreateParentFolder(wCreateParentFolder.getSelection());
    tfoi.setDoNotOpenNewFileInit(wDoNotOpenNewFileInit.getSelection());
    tfoi.setFileFormat(TextFileOutputMeta.formatMapperLineTerminator[wFormat.getSelectionIndex()]);
    tfoi.setFileCompression(wCompression.getText());
    tfoi.setEncoding(wEncoding.getText());
    tfoi.setSeparator(wSeparator.getText());
    tfoi.setEnclosure(wEnclosure.getText());
    tfoi.setExtension(wExtension.getText());
    tfoi.setSplitEveryRows(wSplitEvery.getText());
    tfoi.setEndedLine(wEndedLine.getText());

    tfoi.setFileNameField(wFileNameField.getText());
    tfoi.setFileNameInField(wFileNameInField.getSelection());

    tfoi.setEnclosureForced(wEnclForced.getSelection());
    tfoi.setEnclosureFixDisabled(wDisableEnclosureFix.getSelection());
    tfoi.setHeaderEnabled(wHeader.getSelection());
    tfoi.setFooterEnabled(wFooter.getSelection());
    tfoi.setFileAppended(wAppend.getSelection());
    tfoi.setTransformNrInFilename(wAddTransformNr.getSelection());
    tfoi.setPartNrInFilename(wAddPartnr.getSelection());
    tfoi.setDateInFilename(wAddDate.getSelection());
    tfoi.setTimeInFilename(wAddTime.getSelection());
    tfoi.setDateTimeFormat(wDateTimeFormat.getText());
    tfoi.setSpecifyingFormat(wSpecifyFormat.getSelection());
    tfoi.setPadded(wPad.getSelection());
    tfoi.setAddToResultFiles(wAddToResult.getSelection());
    tfoi.setFastDump(wFastDump.getSelection());

    int i;

    int nrFields = wFields.nrNonEmpty();

    tfoi.allocate(nrFields);

    for (i = 0; i < nrFields; i++) {
      TextFileField field = new TextFileField();

      TableItem item = wFields.getNonEmpty(i);
      field.setName(item.getText(1));
      field.setType(item.getText(2));
      field.setFormat(item.getText(3));
      field.setLength(Const.toInt(item.getText(4), -1));
      field.setPrecision(Const.toInt(item.getText(5), -1));
      field.setCurrencySymbol(item.getText(6));
      field.setDecimalSymbol(item.getText(7));
      field.setGroupingSymbol(item.getText(8));
      field.setTrimType(ValueMetaBase.getTrimTypeByDesc(item.getText(9)));
      field.setNullString(item.getText(10));
      field.setRoundingType(ValueMetaBase.getRoundingTypeCode(item.getText(11)));
      tfoi.getOutputFields()[i] = field;
    }
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    saveInfoInMeta(input);

    dispose();
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.isNumeric()) {
                // currency symbol
                tableItem.setText(6, Const.NVL(v.getCurrencySymbol(), ""));

                // decimal and grouping
                tableItem.setText(7, Const.NVL(v.getDecimalSymbol(), ""));
                tableItem.setText(8, Const.NVL(v.getGroupingSymbol(), ""));
              }

              // trim type
              tableItem.setText(9, Const.NVL(ValueMetaBase.getTrimTypeDesc(v.getTrimType()), ""));

              // conversion mask
              if (!Utils.isEmpty(v.getConversionMask())) {
                tableItem.setText(3, v.getConversionMask());
              } else {
                if (v.isNumber() && v.getLength() > 0) {
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
            r, wFields, 1, new int[] {1}, new int[] {2}, 4, 5, listener);
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

      item.setText(4, "");
      item.setText(5, "");
      item.setText(9, ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_BOTH));

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

    for (int i = 0; i < input.getOutputFields().length; i++) {
      input.getOutputFields()[i].setTrimType(IValueMeta.TRIM_TYPE_BOTH);
    }

    wFields.optWidth(true);
  }

  protected void enableParentFolder() {
    // it is enabled always in this implementation
  }
}
