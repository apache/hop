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

package org.apache.hop.pipeline.transforms.creditcardvalidator;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ComponentSelectionListener;
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
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class CreditCardValidatorDialog extends BaseTransformDialog {
  private static final Class<?> PKG = CreditCardValidatorMeta.class;

  private boolean gotPreviousFields = false;

  private CCombo wFieldName;

  private TextVar wResult;
  private TextVar wFileType;

  private TextVar wNotValidMsg;

  private Button wgetOnlyDigits;

  private Button wUseBinDatabase;

  private TextVar wBinFileName;

  private Button wbbBinFileName;

  private TextVar wBinDelimiter;

  private TextVar wBinEnclosure;

  private ComboVar wBinEncoding;

  private Button wBinHeaderPresent;

  private CCombo wBinCsvColumn;

  private Button wbGetColumns;

  private TableView wOutputFields;

  private List<String> binCsvColumns = new ArrayList<>();

  private final CreditCardValidatorMeta input;

  public CreditCardValidatorDialog(
      Shell parent,
      IVariables variables,
      CreditCardValidatorMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "CreditCardValidatorDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder);

    addFieldsTab(wTabFolder, lsMod);
    addBinDatabaseTab(wTabFolder, lsMod);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    getData();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addFieldsTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.Fields.Tab"));

    ScrolledComposite sc = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // filename field
    Label wlFieldName = new Label(wContent, SWT.RIGHT);
    wlFieldName.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.FieldName.Label"));
    PropsUi.setLook(wlFieldName);
    FormData fdlFieldName = new FormData();
    fdlFieldName.left = new FormAttachment(0, 0);
    fdlFieldName.right = new FormAttachment(middle, -margin);
    fdlFieldName.top = new FormAttachment(0, margin);
    wlFieldName.setLayoutData(fdlFieldName);

    wFieldName = new CCombo(wContent, SWT.BORDER | SWT.READ_ONLY);
    PropsUi.setLook(wFieldName);
    wFieldName.addModifyListener(lsMod);
    FormData fdFieldName = new FormData();
    fdFieldName.left = new FormAttachment(middle, 0);
    fdFieldName.top = new FormAttachment(0, margin);
    fdFieldName.right = new FormAttachment(100, 0);
    wFieldName.setLayoutData(fdFieldName);
    wFieldName.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost
          }

          @Override
          public void focusGained(FocusEvent e) {
            Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
            shell.setCursor(busy);
            get();
            shell.setCursor(null);
            busy.dispose();
          }
        });

    // get only digits?
    Label wlgetOnlyDigits = new Label(wContent, SWT.RIGHT);
    wlgetOnlyDigits.setText(BaseMessages.getString(PKG, "CreditCardValidator.getOnlyDigits.Label"));
    PropsUi.setLook(wlgetOnlyDigits);
    FormData fdlgetOnlyDigits = new FormData();
    fdlgetOnlyDigits.left = new FormAttachment(0, 0);
    fdlgetOnlyDigits.top = new FormAttachment(wlFieldName, margin);
    fdlgetOnlyDigits.right = new FormAttachment(middle, -margin);
    wlgetOnlyDigits.setLayoutData(fdlgetOnlyDigits);
    wgetOnlyDigits = new Button(wContent, SWT.CHECK);
    PropsUi.setLook(wgetOnlyDigits);
    wgetOnlyDigits.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidator.getOnlyDigits.Tooltip"));
    FormData fdgetOnlyDigits = new FormData();
    fdgetOnlyDigits.left = new FormAttachment(middle, 0);
    fdgetOnlyDigits.top = new FormAttachment(wFieldName, margin);
    wgetOnlyDigits.setLayoutData(fdgetOnlyDigits);
    wgetOnlyDigits.addSelectionListener(new ComponentSelectionListener(input));

    // ///////////////////////////////
    // START OF Output Fields GROUP //
    // ///////////////////////////////

    Group wOutputFields = new Group(wContent, SWT.SHADOW_NONE);
    PropsUi.setLook(wOutputFields);
    wOutputFields.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputFields.Label"));

    FormLayout outputFieldsgroupLayout = new FormLayout();
    outputFieldsgroupLayout.marginWidth = 10;
    outputFieldsgroupLayout.marginHeight = 10;
    wOutputFields.setLayout(outputFieldsgroupLayout);

    // Result fieldname ...
    Label wlResult = new Label(wOutputFields, SWT.RIGHT);
    wlResult.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.ResultField.Label"));
    PropsUi.setLook(wlResult);
    FormData fdlResult = new FormData();
    fdlResult.left = new FormAttachment(0, 0);
    fdlResult.right = new FormAttachment(middle, -margin);
    fdlResult.top = new FormAttachment(wgetOnlyDigits, margin);
    wlResult.setLayoutData(fdlResult);

    wResult = new TextVar(variables, wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wResult.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.ResultField.Tooltip"));
    PropsUi.setLook(wResult);
    wResult.addModifyListener(lsMod);
    FormData fdResult = new FormData();
    fdResult.left = new FormAttachment(wlResult, margin);
    fdResult.top = new FormAttachment(wgetOnlyDigits, margin);
    fdResult.right = new FormAttachment(100, 0);
    wResult.setLayoutData(fdResult);

    // FileType fieldname ...
    Label wlCardType = new Label(wOutputFields, SWT.RIGHT);
    wlCardType.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.CardType.Label"));
    PropsUi.setLook(wlCardType);
    FormData fdlCardType = new FormData();
    fdlCardType.left = new FormAttachment(0, 0);
    fdlCardType.right = new FormAttachment(middle, -margin);
    fdlCardType.top = new FormAttachment(wResult, margin);
    wlCardType.setLayoutData(fdlCardType);

    wFileType = new TextVar(variables, wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wFileType.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.CardType.Tooltip"));
    PropsUi.setLook(wFileType);
    wFileType.addModifyListener(lsMod);
    FormData fdCardType = new FormData();
    fdCardType.left = new FormAttachment(wlCardType, margin);
    fdCardType.top = new FormAttachment(wResult, margin);
    fdCardType.right = new FormAttachment(100, 0);
    wFileType.setLayoutData(fdCardType);

    // UnvalidMsg fieldname ...
    Label wlNotValidMsg = new Label(wOutputFields, SWT.RIGHT);
    wlNotValidMsg.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.NotValidMsg.Label"));
    PropsUi.setLook(wlNotValidMsg);
    FormData fdlNotValidMsg = new FormData();
    fdlNotValidMsg.left = new FormAttachment(0, 0);
    fdlNotValidMsg.right = new FormAttachment(middle, -margin);
    fdlNotValidMsg.top = new FormAttachment(wFileType, margin);
    wlNotValidMsg.setLayoutData(fdlNotValidMsg);

    wNotValidMsg = new TextVar(variables, wOutputFields, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wNotValidMsg.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.NotValidMsg.Tooltip"));
    PropsUi.setLook(wNotValidMsg);
    wNotValidMsg.addModifyListener(lsMod);
    FormData fdNotValidMsg = new FormData();
    fdNotValidMsg.left = new FormAttachment(wlNotValidMsg, margin);
    fdNotValidMsg.top = new FormAttachment(wFileType, margin);
    fdNotValidMsg.right = new FormAttachment(100, 0);
    wNotValidMsg.setLayoutData(fdNotValidMsg);

    FormData fdAdditionalFields = new FormData();
    fdAdditionalFields.left = new FormAttachment(0, margin);
    fdAdditionalFields.top = new FormAttachment(wgetOnlyDigits, margin);
    fdAdditionalFields.right = new FormAttachment(100, -margin);
    wOutputFields.setLayoutData(fdAdditionalFields);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    wFieldsTab.setControl(sc);
  }

  private void addBinDatabaseTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wBinTab = new CTabItem(wTabFolder, SWT.NONE);
    wBinTab.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.BinDatabase.Tab"));

    ScrolledComposite sc = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    Group wBinDatabase = new Group(wContent, SWT.SHADOW_NONE);
    PropsUi.setLook(wBinDatabase);
    wBinDatabase.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.BinDatabase.Label"));

    FormLayout binDatabaseGroupLayout = new FormLayout();
    binDatabaseGroupLayout.marginWidth = 10;
    binDatabaseGroupLayout.marginHeight = 10;
    wBinDatabase.setLayout(binDatabaseGroupLayout);

    // Use BIN database checkbox
    Label wlUseBin = new Label(wBinDatabase, SWT.RIGHT);
    wlUseBin.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.UseBinDatabase.Label"));
    PropsUi.setLook(wlUseBin);
    FormData fdlUseBin = new FormData();
    fdlUseBin.left = new FormAttachment(0, 0);
    fdlUseBin.right = new FormAttachment(middle, -margin);
    fdlUseBin.top = new FormAttachment(0, margin);
    wlUseBin.setLayoutData(fdlUseBin);

    wUseBinDatabase = new Button(wBinDatabase, SWT.CHECK);
    PropsUi.setLook(wUseBinDatabase);
    wUseBinDatabase.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.UseBinDatabase.Tooltip"));
    FormData fdUseBin = new FormData();
    fdUseBin.left = new FormAttachment(middle, 0);
    fdUseBin.top = new FormAttachment(0, margin);
    wUseBinDatabase.setLayoutData(fdUseBin);
    wUseBinDatabase.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setBinDatabaseEnabled();
          }
        });

    // File name with browse
    Label wlBinFileName = new Label(wBinDatabase, SWT.RIGHT);
    wlBinFileName.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.BinFileName.Label"));
    PropsUi.setLook(wlBinFileName);
    FormData fdlBinFileName = new FormData();
    fdlBinFileName.left = new FormAttachment(0, 0);
    fdlBinFileName.right = new FormAttachment(middle, -margin);
    fdlBinFileName.top = new FormAttachment(wUseBinDatabase, margin);
    wlBinFileName.setLayoutData(fdlBinFileName);

    wbbBinFileName = new Button(wBinDatabase, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbbBinFileName);
    wbbBinFileName.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbbBinFileName.setToolTipText(
        BaseMessages.getString(PKG, "System.Tooltip.BrowseForFileOrDirAndAdd"));
    FormData fdbbBinFileName = new FormData();
    fdbbBinFileName.right = new FormAttachment(100, 0);
    fdbbBinFileName.top = new FormAttachment(wUseBinDatabase, margin);
    wbbBinFileName.setLayoutData(fdbbBinFileName);
    wbbBinFileName.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell,
                wBinFileName,
                variables,
                new String[] {"*.csv", "*.*"},
                new String[] {
                  BaseMessages.getString(PKG, "System.FileType.CSVFiles"),
                  BaseMessages.getString(PKG, "System.FileType.AllFiles")
                },
                true));

    wBinFileName = new TextVar(variables, wBinDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBinFileName.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.BinFileName.Tooltip"));
    PropsUi.setLook(wBinFileName);
    wBinFileName.addModifyListener(lsMod);
    FormData fdBinFileName = new FormData();
    fdBinFileName.left = new FormAttachment(middle, 0);
    fdBinFileName.top = new FormAttachment(wUseBinDatabase, margin);
    fdBinFileName.right = new FormAttachment(wbbBinFileName, -margin);
    wBinFileName.setLayoutData(fdBinFileName);

    // Delimiter
    Label wlBinDelimiter = new Label(wBinDatabase, SWT.RIGHT);
    wlBinDelimiter.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.Delimiter.Label"));
    PropsUi.setLook(wlBinDelimiter);
    FormData fdlBinDelimiter = new FormData();
    fdlBinDelimiter.left = new FormAttachment(0, 0);
    fdlBinDelimiter.right = new FormAttachment(middle, -margin);
    fdlBinDelimiter.top = new FormAttachment(wBinFileName, margin);
    wlBinDelimiter.setLayoutData(fdlBinDelimiter);

    wBinDelimiter = new TextVar(variables, wBinDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBinDelimiter);
    wBinDelimiter.addModifyListener(lsMod);
    FormData fdBinDelimiter = new FormData();
    fdBinDelimiter.left = new FormAttachment(middle, 0);
    fdBinDelimiter.top = new FormAttachment(wBinFileName, margin);
    fdBinDelimiter.right = new FormAttachment(100, 0);
    wBinDelimiter.setLayoutData(fdBinDelimiter);

    // Enclosure
    Label wlBinEnclosure = new Label(wBinDatabase, SWT.RIGHT);
    wlBinEnclosure.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.Enclosure.Label"));
    PropsUi.setLook(wlBinEnclosure);
    FormData fdlBinEnclosure = new FormData();
    fdlBinEnclosure.left = new FormAttachment(0, 0);
    fdlBinEnclosure.right = new FormAttachment(middle, -margin);
    fdlBinEnclosure.top = new FormAttachment(wBinDelimiter, margin);
    wlBinEnclosure.setLayoutData(fdlBinEnclosure);

    wBinEnclosure = new TextVar(variables, wBinDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBinEnclosure);
    wBinEnclosure.addModifyListener(lsMod);
    FormData fdBinEnclosure = new FormData();
    fdBinEnclosure.left = new FormAttachment(middle, 0);
    fdBinEnclosure.top = new FormAttachment(wBinDelimiter, margin);
    fdBinEnclosure.right = new FormAttachment(100, 0);
    wBinEnclosure.setLayoutData(fdBinEnclosure);

    // Encoding
    Label wlBinEncoding = new Label(wBinDatabase, SWT.RIGHT);
    wlBinEncoding.setText(BaseMessages.getString(PKG, "CreditCardValidatorDialog.Encoding.Label"));
    PropsUi.setLook(wlBinEncoding);
    FormData fdlBinEncoding = new FormData();
    fdlBinEncoding.left = new FormAttachment(0, 0);
    fdlBinEncoding.right = new FormAttachment(middle, -margin);
    fdlBinEncoding.top = new FormAttachment(wBinEnclosure, margin);
    wlBinEncoding.setLayoutData(fdlBinEncoding);

    wBinEncoding = new ComboVar(variables, wBinDatabase, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBinEncoding);
    wBinEncoding.addModifyListener(lsMod);
    FormData fdBinEncoding = new FormData();
    fdBinEncoding.left = new FormAttachment(middle, 0);
    fdBinEncoding.top = new FormAttachment(wBinEnclosure, margin);
    fdBinEncoding.right = new FormAttachment(100, 0);
    wBinEncoding.setLayoutData(fdBinEncoding);

    // Header present
    Label wlBinHeader = new Label(wBinDatabase, SWT.RIGHT);
    wlBinHeader.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.HeaderPresent.Label"));
    PropsUi.setLook(wlBinHeader);
    FormData fdlBinHeader = new FormData();
    fdlBinHeader.left = new FormAttachment(0, 0);
    fdlBinHeader.right = new FormAttachment(middle, -margin);
    fdlBinHeader.top = new FormAttachment(wBinEncoding, margin);
    wlBinHeader.setLayoutData(fdlBinHeader);

    wBinHeaderPresent = new Button(wBinDatabase, SWT.CHECK);
    PropsUi.setLook(wBinHeaderPresent);
    wBinHeaderPresent.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.HeaderPresent.Tooltip"));
    FormData fdBinHeader = new FormData();
    fdBinHeader.left = new FormAttachment(middle, 0);
    fdBinHeader.top = new FormAttachment(wBinEncoding, margin);
    wBinHeaderPresent.setLayoutData(fdBinHeader);
    wBinHeaderPresent.addSelectionListener(new ComponentSelectionListener(input));
    wBinHeaderPresent.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (!wBinHeaderPresent.getSelection() && hasHeaderMappings()) {
              MessageBox mb = new MessageBox(shell, SWT.YES | SWT.NO | SWT.ICON_WARNING);
              mb.setText(
                  BaseMessages.getString(
                      PKG, "CreditCardValidatorDialog.HeaderDisabledWarning.Title"));
              mb.setMessage(
                  BaseMessages.getString(
                      PKG, "CreditCardValidatorDialog.HeaderDisabledWarning.Message"));
              if (mb.open() == SWT.NO) {
                wBinHeaderPresent.setSelection(true);
              } else {
                clearHeaderMappings();
              }
            }
            setBinDatabaseEnabled();
          }
        });

    // BIN CSV column
    Label wlBinCsvColumn = new Label(wBinDatabase, SWT.RIGHT);
    wlBinCsvColumn.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.BinCsvColumn.Label"));
    PropsUi.setLook(wlBinCsvColumn);
    FormData fdlBinCsvColumn = new FormData();
    fdlBinCsvColumn.left = new FormAttachment(0, 0);
    fdlBinCsvColumn.right = new FormAttachment(middle, -margin);
    fdlBinCsvColumn.top = new FormAttachment(wBinHeaderPresent, margin);
    wlBinCsvColumn.setLayoutData(fdlBinCsvColumn);

    wBinCsvColumn = new CCombo(wBinDatabase, SWT.BORDER);
    wBinCsvColumn.setEditable(true);
    PropsUi.setLook(wBinCsvColumn);
    wBinCsvColumn.addModifyListener(lsMod);
    FormData fdBinCsvColumn = new FormData();
    fdBinCsvColumn.left = new FormAttachment(middle, 0);
    fdBinCsvColumn.top = new FormAttachment(wBinHeaderPresent, margin);
    fdBinCsvColumn.right = new FormAttachment(100, 0);
    wBinCsvColumn.setLayoutData(fdBinCsvColumn);
    wBinCsvColumn.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Disable focuslost
          }

          @Override
          public void focusGained(FocusEvent e) {
            loadBinCsvColumns();
          }
        });

    // Get fields button
    wbGetColumns = new Button(wBinDatabase, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbGetColumns);
    wbGetColumns.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.GetColumns.Button"));
    wbGetColumns.setToolTipText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.GetColumns.Tooltip"));
    FormData fdbGetColumns = new FormData();
    fdbGetColumns.right = new FormAttachment(100, 0);
    fdbGetColumns.top = new FormAttachment(wBinCsvColumn, margin);
    wbGetColumns.setLayoutData(fdbGetColumns);
    wbGetColumns.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getFields();
          }
        });

    // Extra output fields
    Label wlOutputFields = new Label(wBinDatabase, SWT.RIGHT);
    wlOutputFields.setText(
        BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputFieldList.Label"));
    PropsUi.setLook(wlOutputFields);
    FormData fdlOutputFields = new FormData();
    fdlOutputFields.left = new FormAttachment(0, 0);
    fdlOutputFields.right = new FormAttachment(middle, -margin);
    fdlOutputFields.top = new FormAttachment(wBinCsvColumn, margin);
    wlOutputFields.setLayoutData(fdlOutputFields);

    ColumnInfo[] outputColInfos =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputName.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false,
              120),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputColumn.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0],
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputType.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaFactory.getValueMetaNames(),
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputFormat.Column"),
              ColumnInfo.COLUMN_TYPE_FORMAT,
              2),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputLength.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputPrecision.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputCurrency.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputDecimal.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputGroup.Column"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "CreditCardValidatorDialog.OutputTrim.Column"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              ValueMetaBase.trimTypeDesc,
              true),
        };
    outputColInfos[1].setComboValueSupplier(() -> binCsvColumns.toArray(new String[0]));
    wOutputFields =
        new TableView(
            variables,
            wBinDatabase,
            SWT.FULL_SELECTION | SWT.MULTI | SWT.BORDER,
            outputColInfos,
            input.getOutputFields().size(),
            lsMod,
            props);
    PropsUi.setLook(wOutputFields);
    FormData fdOutputFields = new FormData();
    fdOutputFields.left = new FormAttachment(middle, 0);
    fdOutputFields.top = new FormAttachment(wBinCsvColumn, margin);
    fdOutputFields.right = new FormAttachment(wbGetColumns, -margin);
    fdOutputFields.bottom = new FormAttachment(100, -margin);
    wOutputFields.setLayoutData(fdOutputFields);

    FormData fdBinDatabase = new FormData();
    fdBinDatabase.left = new FormAttachment(0, margin);
    fdBinDatabase.top = new FormAttachment(0, margin);
    fdBinDatabase.right = new FormAttachment(100, -margin);
    fdBinDatabase.bottom = new FormAttachment(100, -margin);
    wBinDatabase.setLayoutData(fdBinDatabase);

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

    wBinTab.setControl(sc);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wFieldName.setText(Const.NVL(input.getFieldName(), ""));
    wgetOnlyDigits.setSelection(input.isOnlyDigits());
    wResult.setText(Const.NVL(input.getResultFieldName(), ""));
    wFileType.setText(Const.NVL(input.getCardType(), ""));
    wNotValidMsg.setText(Const.NVL(input.getNotValidMessage(), ""));
    wUseBinDatabase.setSelection(input.isUseBinDatabase());
    wBinFileName.setText(Const.NVL(input.getBinFileName(), ""));
    wBinDelimiter.setText(Const.NVL(input.getBinDelimiter(), ","));
    wBinEnclosure.setText(Const.NVL(input.getBinEnclosure(), "\""));
    String encoding = Const.NVL(input.getBinEncoding(), "UTF-8");
    wBinEncoding.setItems(ConstUi.getEncodings());
    wBinEncoding.setText(encoding);
    wBinHeaderPresent.setSelection(input.isBinHeaderPresent());
    wBinCsvColumn.setText(Const.NVL(input.getBinCsvColumn(), ""));
    populateOutputFieldsTable();
    setBinDatabaseEnabled();
    loadBinCsvColumns();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }
    input.setFieldName(wFieldName.getText());
    input.setOnlyDigits(wgetOnlyDigits.getSelection());
    input.setResultFieldName(wResult.getText());
    input.setCardType(wFileType.getText());
    input.setNotValidMessage(wNotValidMsg.getText());
    input.setUseBinDatabase(wUseBinDatabase.getSelection());
    input.setBinFileName(wBinFileName.getText());
    input.setBinDelimiter(wBinDelimiter.getText());
    input.setBinEnclosure(wBinEnclosure.getText());
    input.setBinEncoding(wBinEncoding.getText());
    input.setBinHeaderPresent(wBinHeaderPresent.getSelection());
    input.setBinCsvColumn(wBinCsvColumn.getText());
    readOutputFieldsTable();
    // return value
    transformName = wTransformName.getText();

    dispose();
  }

  private void populateOutputFieldsTable() {
    wOutputFields.clearAll();
    for (BinOutputField outputField : input.getOutputFields()) {
      wOutputFields.add(
          Const.NVL(outputField.getName(), ""),
          Const.NVL(outputField.getColumn(), ""),
          ValueMetaFactory.getValueMetaName(outputField.getType()),
          Const.NVL(outputField.getFormat(), ""),
          Integer.toString(outputField.getLength()),
          Integer.toString(outputField.getPrecision()),
          Const.NVL(outputField.getCurrency(), ""),
          Const.NVL(outputField.getDecimal(), ""),
          Const.NVL(outputField.getGroup(), ""),
          ValueMetaBase.getTrimTypeDesc(outputField.getTrimType()));
    }
    wOutputFields.removeEmptyRows();
    wOutputFields.setRowNums();
    wOutputFields.optWidth(true);
  }

  private void readOutputFieldsTable() {
    List<BinOutputField> outputFields = new ArrayList<>();
    int rows = wOutputFields.nrNonEmpty();
    for (int i = 0; i < rows; i++) {
      TableItem item = wOutputFields.getNonEmpty(i);
      String name = item.getText(1);
      String column = item.getText(2);
      if (Utils.isEmpty(name) && Utils.isEmpty(column)) {
        continue;
      }
      BinOutputField outputField = new BinOutputField(name, column);
      outputField.setType(ValueMetaFactory.getIdForValueMeta(item.getText(3)));
      outputField.setFormat(item.getText(4));
      outputField.setLength(Const.toInt(item.getText(5), -1));
      outputField.setPrecision(Const.toInt(item.getText(6), -1));
      outputField.setCurrency(item.getText(7));
      outputField.setDecimal(item.getText(8));
      outputField.setGroup(item.getText(9));
      outputField.setTrimType(ValueMetaBase.getTrimTypeByDesc(item.getText(10)));
      outputFields.add(outputField);
    }
    input.setOutputFields(outputFields);
  }

  private void setBinDatabaseEnabled() {
    boolean enabled = wUseBinDatabase.getSelection();
    boolean hasHeader = enabled && wBinHeaderPresent.getSelection();
    wBinFileName.setEnabled(enabled);
    wbbBinFileName.setEnabled(enabled);
    wBinDelimiter.setEnabled(enabled);
    wBinEnclosure.setEnabled(enabled);
    wBinEncoding.setEnabled(enabled);
    wBinHeaderPresent.setEnabled(enabled);
    wBinCsvColumn.setEnabled(hasHeader);
    wbGetColumns.setEnabled(hasHeader);
    wOutputFields.setEnabled(enabled);
  }

  private boolean hasHeaderMappings() {
    if (!Utils.isEmpty(wBinCsvColumn.getText())) {
      return true;
    }
    int rows = wOutputFields.nrNonEmpty();
    for (int i = 0; i < rows; i++) {
      TableItem item = wOutputFields.getNonEmpty(i);
      if (!Utils.isEmpty(item.getText(2))) {
        return true;
      }
    }
    return false;
  }

  private void clearHeaderMappings() {
    wBinCsvColumn.setText("");
    int rows = wOutputFields.nrNonEmpty();
    for (int i = 0; i < rows; i++) {
      TableItem item = wOutputFields.getNonEmpty(i);
      item.setText(2, "");
    }
    wOutputFields.removeEmptyRows();
    wOutputFields.setRowNums();
    wOutputFields.optWidth(true);
  }

  private void loadBinCsvColumns() {
    if (!wBinHeaderPresent.getSelection()) {
      return;
    }
    readHeader(
        header -> {
          binCsvColumns.clear();
          for (String column : header) {
            binCsvColumns.add(column);
          }
          String current = wBinCsvColumn.getText();
          wBinCsvColumn.removeAll();
          wBinCsvColumn.setItems(binCsvColumns.toArray(new String[0]));
          wBinCsvColumn.setText(current);
        });
  }

  private void getFields() {
    if (!wBinHeaderPresent.getSelection()) {
      return;
    }
    readHeader(
        header -> {
          binCsvColumns.clear();
          for (String column : header) {
            binCsvColumns.add(column);
          }
          String current = wBinCsvColumn.getText();
          wBinCsvColumn.removeAll();
          wBinCsvColumn.setItems(binCsvColumns.toArray(new String[0]));
          wBinCsvColumn.setText(current);

          for (String column : header) {
            wOutputFields.add(
                column,
                column,
                ValueMetaFactory.getValueMetaName(IValueMeta.TYPE_STRING),
                "",
                "-1",
                "-1",
                "",
                "",
                "",
                ValueMetaBase.getTrimTypeDesc(IValueMeta.TRIM_TYPE_NONE));
          }
          wOutputFields.removeEmptyRows();
          wOutputFields.setRowNums();
          wOutputFields.optWidth(true);
        });
  }

  private void readHeader(java.util.function.Consumer<String[]> consumer) {
    String fileName = variables.resolve(wBinFileName.getText());
    if (Utils.isEmpty(fileName)) {
      return;
    }
    String delimiter = variables.resolve(wBinDelimiter.getText());
    String enclosure = variables.resolve(wBinEnclosure.getText());
    String encoding = variables.resolve(wBinEncoding.getText());
    Charset charset;
    try {
      charset = Charset.forName(Utils.isEmpty(encoding) ? "UTF-8" : encoding);
    } catch (Exception e) {
      charset = StandardCharsets.UTF_8;
    }
    try (InputStream in = HopVfs.getInputStream(fileName, variables);
        CSVReader reader =
            new CSVReaderBuilder(new InputStreamReader(in, charset))
                .withCSVParser(
                    new CSVParserBuilder()
                        .withSeparator(Utils.isEmpty(delimiter) ? ',' : delimiter.charAt(0))
                        .withQuoteChar(Utils.isEmpty(enclosure) ? '"' : enclosure.charAt(0))
                        .build())
                .build()) {
      String[] header = reader.readNext();
      if (header != null) {
        consumer.accept(header);
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "CreditCardValidatorDialog.GetColumns.DialogTitle"),
          BaseMessages.getString(PKG, "CreditCardValidatorDialog.GetColumns.ErrorMessage"),
          e);
    }
  }

  private void get() {
    if (!gotPreviousFields) {
      try {
        String columnName = wFieldName.getText();
        wFieldName.removeAll();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          r.getFieldNames();

          for (int i = 0; i < r.getFieldNames().length; i++) {
            wFieldName.add(r.getFieldNames()[i]);
          }
        }
        wFieldName.setText(columnName);
        gotPreviousFields = true;
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "CreditCardValidatorDialog.FailedToGetFields.DialogTitle"),
            BaseMessages.getString(
                PKG, "CreditCardValidatorDialog.FailedToGetFields.DialogMessage"),
            ke);
      }
    }
  }
}
