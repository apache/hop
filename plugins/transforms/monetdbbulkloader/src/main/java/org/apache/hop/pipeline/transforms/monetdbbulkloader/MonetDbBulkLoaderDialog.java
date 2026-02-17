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
package org.apache.hop.pipeline.transforms.monetdbbulkloader;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.FontMetrics;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class MonetDbBulkLoaderDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      MonetDbBulkLoaderMeta.class; // for i18n purposes, needed by Translator2!!

  private CTabFolder wTabFolder;

  //
  // General Settings tab - Widgets and FormData
  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private TextVar wBufferSize;

  private TextVar wLogFile;

  private Button wTruncate;

  private Button wFullyQuoteSQL;

  private Combo wFieldSeparator;

  private Combo wFieldEnclosure;

  private Combo wNULLrepresentation;

  private Combo wEncoding;

  private TableView wReturn;

  private final MonetDbBulkLoaderMeta input;

  private ColumnInfo[] ciReturn;

  private final List<String> inputFields = new ArrayList<>();

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  // Commonly used field delimiters (separators)
  private static final String[] fieldSeparators = {"", "|", ","};
  // Commonly used enclosure characters
  private static final String[] fieldEnclosures = {"", "\""};

  // In MonetDB when streaming fields over, you can specify how to alert the database of a truly
  // empty field i.e. NULL
  // The user can put anything they want or leave it blank.
  private static final String[] nullRepresentations = {"", "null"};

  // These should not be translated, they are required to exist on all
  // platforms according to the documentation of "Charset".
  private static final String[] encodings = {
    "", "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"
  };

  private static final String[] ALL_FILETYPES =
      new String[] {BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Filetype.All")};

  public MonetDbBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      MonetDbBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).sql(e -> create()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent arg0) {
            setTableFieldCombo();
          }
        };
    changed = input.hasChanged();

    //
    // Dialog Box Contents (Organized from dialog top to bottom, dialog left to right.)
    //
    // Connection line
    //
    // Connection line
    wConnection = addConnectionLine(shell, wSpacer, input.getDatabaseMeta(), lsMod);

    // //////////////////////////////////////////////
    // Prepare the Folder that will contain tabs. //
    // //////////////////////////////////////////////
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // General Settings tab //
    // ////////////////////////

    CTabItem wGeneralSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralSettingsTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralSettingsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.GeneralSettings.Label"));

    Composite wGeneralSettingsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralSettingsComp);

    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = 3;
    tabLayout.marginHeight = 3;
    wGeneralSettingsComp.setLayout(tabLayout);

    wGeneralSettingsComp.layout();
    wGeneralSettingsTab.setControl(wGeneralSettingsComp);

    // ////////////////////////////////
    // MonetDB Settings tab //
    // ////////////////////////////////

    CTabItem wMonetDBmclientSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wMonetDBmclientSettingsTab.setFont(GuiResource.getInstance().getFontDefault());
    wMonetDBmclientSettingsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.Label"));

    Composite wMonetDBmclientSettingsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wMonetDBmclientSettingsComp);
    wMonetDBmclientSettingsComp.setLayout(tabLayout);
    wMonetDBmclientSettingsComp.layout();
    wMonetDBmclientSettingsTab.setControl(wMonetDBmclientSettingsComp);

    Group wMonetDBmclientParamGroup = new Group(wMonetDBmclientSettingsComp, SWT.SHADOW_IN);
    wMonetDBmclientParamGroup.setText(
        BaseMessages.getString(
            PKG, "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup"));
    PropsUi.setLook(wMonetDBmclientParamGroup);
    wMonetDBmclientParamGroup.setLayout(tabLayout);
    wMonetDBmclientParamGroup.layout();

    // /////////////////////
    // Output Fields tab //
    // /////////////////////

    CTabItem wOutputFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOutputFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wOutputFieldsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.OutputFields"));

    Composite wOutputFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wOutputFieldsComp);

    wOutputFieldsComp.setLayout(tabLayout);

    wOutputFieldsComp.layout();
    wOutputFieldsTab.setControl(wOutputFieldsComp);

    // Activate the "General Settings" tab
    wTabFolder.setSelection(0);

    wTabFolder.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            wTabFolder.layout(true, true);
          }
        });

    //
    // Schema line (General Settings tab)
    //
    Label wlSchema = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    wSchema = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);

    //
    // Table line (General Settings tab)
    //
    Label wlTable = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);

    Button wbTable = new Button(wGeneralSettingsComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Browse.Button"));

    wTable = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);

    //
    // Buffer size line (General Settings tab)
    //
    Label wlBufferSize = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlBufferSize.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.BufferSize.Label"));
    PropsUi.setLook(wlBufferSize);

    wBufferSize = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBufferSize);
    wBufferSize.addModifyListener(lsMod);

    //
    // Log file line (General Settings tab)
    //
    Label wlLogFile = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlLogFile.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.LogFile.Label"));
    PropsUi.setLook(wlLogFile);

    Button wbLogFile = new Button(wGeneralSettingsComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbLogFile);
    wbLogFile.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Browse.Button"));

    wLogFile = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLogFile);
    wLogFile.addModifyListener(lsMod);

    //
    // Truncate before loading check box (General Settings tab)
    //
    Label wlTruncate = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlTruncate.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Truncate.Label"));
    wlTruncate.setToolTipText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Truncate.Tooltip"));
    PropsUi.setLook(wlTruncate);
    wTruncate = new Button(wGeneralSettingsComp, SWT.CHECK);
    PropsUi.setLook(wTruncate);
    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            input.setTruncate(wTruncate.getSelection());
          }
        };
    wTruncate.addSelectionListener(lsSelMod);

    //
    // Fully Quote SQL during the run. (This setting will persist into the database connection
    // definition.)
    //
    Label wlFullyQuoteSQL = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlFullyQuoteSQL.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.FullyQuoteSQL.Label"));
    wlFullyQuoteSQL.setToolTipText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.FullyQuoteSQL.Tooltip"));
    PropsUi.setLook(wlFullyQuoteSQL);

    wFullyQuoteSQL = new Button(wGeneralSettingsComp, SWT.CHECK);
    PropsUi.setLook(wFullyQuoteSQL);
    SelectionAdapter lsFullyQuoteSQL =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent arg0) {
            input.setChanged();
            input.getDatabaseMeta().setQuoteAllFields(wFullyQuoteSQL.getSelection());
          }
        };
    wFullyQuoteSQL.addSelectionListener(lsFullyQuoteSQL);

    // /////////////////////////////////////////////////////////
    // MonetDB API Settings tab widget declarations follow
    // /////////////////////////////////////////////////////////

    // (Sub-group within the "MonetDB mclient Settings" tab)
    // Widgets for setting the parameters that are sent to the mclient software when the transform
    // executes
    //
    // MonetDB API Settings tab - Widgets and FormData
    Label wlFieldSeparator = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlFieldSeparator.setText(
        BaseMessages.getString(
            PKG,
            "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup.FieldSeparator.Label"));
    PropsUi.setLook(wlFieldSeparator);

    wFieldSeparator = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wFieldSeparator.setItems(fieldSeparators);
    PropsUi.setLook(wFieldSeparator);
    wFieldSeparator.addModifyListener(lsMod);

    Label wlFieldEnclosure = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlFieldEnclosure.setText(
        BaseMessages.getString(
            PKG,
            "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup.FieldEnclosure.Label"));
    PropsUi.setLook(wlFieldEnclosure);

    wFieldEnclosure = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wFieldEnclosure.setItems(fieldEnclosures);
    wFieldEnclosure.addModifyListener(lsMod);
    PropsUi.setLook(wFieldEnclosure);

    Label wlNULLrepresentation = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlNULLrepresentation.setText(
        BaseMessages.getString(
            PKG,
            "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup.NULLrepresentation.Label"));
    PropsUi.setLook(wlNULLrepresentation);

    wNULLrepresentation =
        new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wNULLrepresentation.setItems(nullRepresentations);
    wNULLrepresentation.addModifyListener(lsMod);
    PropsUi.setLook(wNULLrepresentation);
    //
    // Control encoding line (MonetDB API Settings tab -> Parameter Group)
    //
    // The drop down is editable as it may happen an encoding may not be present
    // on one machine, but you may want to use it on your execution server
    //
    Label wlEncoding = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);

    wEncoding = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEncoding.setToolTipText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Encoding.Tooltip"));
    wEncoding.setItems(encodings);
    PropsUi.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);

    // The field Table
    //
    // Output Fields tab - Widgets and FormData
    Label wlReturn = new Label(wOutputFieldsComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Fields.Label"));
    PropsUi.setLook(wlReturn);

    int upInsCols = 3;
    int upInsRows = (input.getFieldTable() != null ? input.getFieldTable().length : 1);

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.ColumnInfo.FormatOK"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              "Y", "N",
            },
            true);
    tableFieldColumns.add(ciReturn[0]);
    wReturn =
        new TableView(
            variables,
            wOutputFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            lsMod,
            props);
    wReturn.optWidth(true);

    // wReturn.table.pack(); // Force columns to take up the size they need. Make it easy for the
    // user to see what
    // values are in the field.

    Button wGetLU = new Button(wOutputFieldsComp, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.GetFields.Label"));

    Button wDoMapping = new Button(wOutputFieldsComp, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.EditMapping.Label"));

    wDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());

    Button wClearDBCache = new Button(wOutputFieldsComp, SWT.PUSH);
    wClearDBCache.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.ClearDbCache"));
    wClearDBCache.addListener(SWT.Selection, e -> clearDbCache());

    //
    // Visual Layout Definition
    //
    // FormLayout (org.eclipse.swt.layout.FormLayout) is being used to compose the dialog box.
    // The layout works by creating FormAttachments for each side of the widget and storing them in
    // the layout data.
    // An attachment 'attaches' a specific side of the widget either to a position in the parent
    // Composite or to another
    // widget within the layout.

    //
    // Tabs will appear below the "Transform Name" area and above the buttons at the bottom of the
    // dialog.
    //
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wConnection, margin + 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Database Schema Line - (General Settings Tab)
    //
    // Database Schema (Label layout) - first row: attach to top of tab composite
    FormData fdlSchema = new FormData();
    fdlSchema.top = new FormAttachment(0, margin);
    fdlSchema.left = new FormAttachment(0, margin);
    fdlSchema.right = new FormAttachment(middle, -margin);
    wlSchema.setLayoutData(fdlSchema);

    // Database schema (Edit box layout)
    FormData fdSchema = new FormData();
    fdSchema.top = new FormAttachment(wlSchema, 0, SWT.CENTER);
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.right = new FormAttachment(100, -margin);
    wSchema.setLayoutData(fdSchema);

    // Target Table Line - (General Settings Tab)
    //
    // Target table (Label layout)
    FormData fdlTable = new FormData();
    fdlTable.top = new FormAttachment(wSchema, margin);
    fdlTable.left = new FormAttachment(0, margin);
    fdlTable.right = new FormAttachment(middle, -margin);
    wlTable.setLayoutData(fdlTable);

    // Target table Browse button - right edge, vertically centered with label
    FormData fdbTable = new FormData();
    fdbTable.top = new FormAttachment(wlTable, 0, SWT.CENTER);
    fdbTable.right = new FormAttachment(100, 0);
    wbTable.setLayoutData(fdbTable);

    // Target table (Edit box) - between label and browse button
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(wlTable, 0, SWT.CENTER);
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Buffer size (Label layout)
    FormData fdlBufferSize = new FormData();
    fdlBufferSize.top = new FormAttachment(wTable, margin);
    fdlBufferSize.left = new FormAttachment(0, margin);
    fdlBufferSize.right = new FormAttachment(middle, -margin);
    wlBufferSize.setLayoutData(fdlBufferSize);

    FormData fdBufferSize = new FormData();
    fdBufferSize.top = new FormAttachment(wTable, margin);
    fdBufferSize.left = new FormAttachment(middle, 0);
    fdBufferSize.right = new FormAttachment(100, -margin);
    wBufferSize.setLayoutData(fdBufferSize);

    FormData fdlLogFile = new FormData();
    fdlLogFile.top = new FormAttachment(wBufferSize, margin);
    fdlLogFile.left = new FormAttachment(0, margin);
    fdlLogFile.right = new FormAttachment(middle, -margin);
    wlLogFile.setLayoutData(fdlLogFile);

    // Log file Browse button - right edge, vertically centered with label
    FormData fdbLogFile = new FormData();
    fdbLogFile.top = new FormAttachment(wlLogFile, 0, SWT.CENTER);
    fdbLogFile.right = new FormAttachment(100, 0);
    wbLogFile.setLayoutData(fdbLogFile);

    FormData fdLogFile = new FormData();
    fdLogFile.left = new FormAttachment(middle, 0);
    fdLogFile.top = new FormAttachment(wlLogFile, 0, SWT.CENTER);
    fdLogFile.right = new FormAttachment(wbLogFile, -margin);
    wLogFile.setLayoutData(fdLogFile);

    FormData fdlTruncate = new FormData();
    fdlTruncate.top = new FormAttachment(wLogFile, margin);
    fdlTruncate.left = new FormAttachment(0, margin);
    fdlTruncate.right = new FormAttachment(middle, -margin);
    wlTruncate.setLayoutData(fdlTruncate);

    FormData fdTruncate = new FormData();
    fdTruncate.top = new FormAttachment(wlTruncate, 0, SWT.CENTER);
    fdTruncate.left = new FormAttachment(middle, 0);
    fdTruncate.right = new FormAttachment(100, -margin);
    wTruncate.setLayoutData(fdTruncate);

    FormData fdlFullyQuoteSQL = new FormData();
    fdlFullyQuoteSQL.top = new FormAttachment(wlTruncate, margin);
    fdlFullyQuoteSQL.left = new FormAttachment(0, margin);
    fdlFullyQuoteSQL.right = new FormAttachment(middle, -margin);
    wlFullyQuoteSQL.setLayoutData(fdlFullyQuoteSQL);

    FormData fdFullyQuoteSQL = new FormData();
    fdFullyQuoteSQL.top = new FormAttachment(wlFullyQuoteSQL, 0, SWT.CENTER);
    fdFullyQuoteSQL.left = new FormAttachment(middle, 0);
    fdFullyQuoteSQL.right = new FormAttachment(100, -margin);
    wFullyQuoteSQL.setLayoutData(fdFullyQuoteSQL);
    //
    // MonetDB Settings tab layout
    //

    //
    // mclient parameter grouping (Group composite) - attach to top of tab composite
    FormData fdgMonetDBmclientParamGroup = new FormData();
    fdgMonetDBmclientParamGroup.top = new FormAttachment(0, margin * 3);
    fdgMonetDBmclientParamGroup.left = new FormAttachment(0, margin);
    fdgMonetDBmclientParamGroup.right = new FormAttachment(100, -margin);
    wMonetDBmclientParamGroup.setLayoutData(fdgMonetDBmclientParamGroup);

    // Combo width: ~20 characters (use existing control for font metrics, avoid stray widget on
    // shell)
    GC gc = new GC(wSchema);
    FontMetrics fm = gc.getFontMetrics();
    int charWidth = fm.getAverageCharWidth();
    int fieldWidth = 20 * charWidth;
    gc.dispose();

    FormData fdlFieldSeparator = new FormData();
    fdlFieldSeparator.top = new FormAttachment(0, 3 * margin);
    fdlFieldSeparator.left = new FormAttachment(0, 3 * margin);
    fdlFieldSeparator.right = new FormAttachment(middle, -margin);
    wlFieldSeparator.setLayoutData(fdlFieldSeparator);

    FormData fdFieldSeparator = new FormData();
    fdFieldSeparator.top = new FormAttachment(0, 3 * margin);
    fdFieldSeparator.left = new FormAttachment(middle, 0);
    fdFieldSeparator.width = fieldWidth;
    wFieldSeparator.setLayoutData(fdFieldSeparator);

    FormData fdlFieldEnclosure = new FormData();
    fdlFieldEnclosure.top = new FormAttachment(wFieldSeparator, margin);
    fdlFieldEnclosure.left = new FormAttachment(0, 3 * margin);
    fdlFieldEnclosure.right = new FormAttachment(middle, -margin);
    wlFieldEnclosure.setLayoutData(fdlFieldEnclosure);

    FormData fdFieldEnclosure = new FormData();
    fdFieldEnclosure.top = new FormAttachment(wFieldSeparator, margin);
    fdFieldEnclosure.left = new FormAttachment(middle, 0);
    fdFieldEnclosure.width = fieldWidth;
    wFieldEnclosure.setLayoutData(fdFieldEnclosure);

    FormData fdlNULLrepresentation = new FormData();
    fdlNULLrepresentation.top = new FormAttachment(wFieldEnclosure, margin);
    fdlNULLrepresentation.left = new FormAttachment(0, 3 * margin);
    fdlNULLrepresentation.right = new FormAttachment(middle, -margin);
    wlNULLrepresentation.setLayoutData(fdlNULLrepresentation);

    FormData fdNULLrepresentation = new FormData();
    fdNULLrepresentation.top = new FormAttachment(wFieldEnclosure, margin);
    fdNULLrepresentation.left = new FormAttachment(middle, 0);
    fdNULLrepresentation.width = fieldWidth;
    wNULLrepresentation.setLayoutData(fdNULLrepresentation);

    FormData fdlEncoding = new FormData();
    fdlEncoding.top = new FormAttachment(wNULLrepresentation, margin);
    fdlEncoding.left = new FormAttachment(0, 3 * margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);

    FormData fdEncoding = new FormData();
    fdEncoding.top = new FormAttachment(wNULLrepresentation, margin);
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.width = fieldWidth;
    wEncoding.setLayoutData(fdEncoding);

    //
    // Output Fields tab layout - attach to top of tab composite (0, margin)
    //
    FormData fdlReturn = new FormData();
    fdlReturn.top = new FormAttachment(0, 5 * margin);
    fdlReturn.left = new FormAttachment(0, margin);
    wlReturn.setLayoutData(fdlReturn);

    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(0, margin);
    fdDoMapping.right = new FormAttachment(100, -margin);
    wDoMapping.setLayoutData(fdDoMapping);

    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(0, margin);
    fdGetLU.right = new FormAttachment(wDoMapping, -margin);
    wGetLU.setLayoutData(fdGetLU);

    FormData fdClearDBCache = new FormData();
    fdClearDBCache.top = new FormAttachment(0, margin);
    fdClearDBCache.right = new FormAttachment(wGetLU, -margin);
    wClearDBCache.setLayoutData(fdClearDBCache);

    FormData fdReturn = new FormData();
    fdReturn.top = new FormAttachment(wGetLU, 3 * margin);
    fdReturn.left = new FormAttachment(0, margin);
    fdReturn.right = new FormAttachment(100, -margin);
    fdReturn.bottom = new FormAttachment(100, 0);
    wReturn.setLayoutData(fdReturn);

    //
    // Layout section ends

    //
    // Search the fields in the background
    //

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

    wbLogFile.addListener(
        SWT.Selection,
        e ->
            BaseDialog.presentFileDialog(
                shell, wLogFile, variables, new String[] {"*"}, ALL_FILETYPES, true));

    wbTable.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            getTableName();
          }
        });

    getData();
    setTableFieldCombo();
    input.setChanged(changed);

    setSize();
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void clearDbCache() {
    DbCache.getInstance().clear(input.getDbConnectionName());
    MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
    mb.setMessage(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.ClearedDbCacheMsg"));
    mb.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.ClearedDbCacheTitle"));
    mb.open();
  }

  protected void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
            final String tableName = wTable.getText();
            final String connectionName = wConnection.getText();
            final String schemaName = wSchema.getText();

            // clear
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            if (!Utils.isEmpty(tableName)) {
              DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
              if (databaseMeta != null) {
                try (Database db = new Database(loggingObject, variables, databaseMeta)) {
                  db.connect();

                  String schemaTable =
                      databaseMeta.getQuotedSchemaTableCombination(
                          variables, schemaName, tableName);
                  IRowMeta r = db.getTableFields(schemaTable);
                  if (null != r) {
                    String[] fieldNames = r.getFieldNames();
                    if (null != fieldNames) {
                      for (ColumnInfo colInfo : tableFieldColumns) {
                        colInfo.setComboValues(fieldNames);
                      }
                    }
                  }
                } catch (Exception e) {
                  for (ColumnInfo colInfo : tableFieldColumns) {
                    colInfo.setComboValues(new String[] {});
                  }
                  // ignore any errors here. drop downs will not be
                  // filled, but no problem for the user
                }
              }
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /**
   * Copy information from the meta-data input to the dialog fields.
   *
   * <p>This method is called each time the dialog is opened.
   */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Log.GettingKeyInfo"));
    }

    if (input.getFieldTable() != null) {
      for (int i = 0; i < input.getFieldTable().length; i++) {
        TableItem item = wReturn.table.getItem(i);
        if (input.getFieldTable()[i] != null) {
          item.setText(1, input.getFieldTable()[i]);
        }
        if (input.getFieldStream()[i] != null) {
          item.setText(2, input.getFieldStream()[i]);
        }
        item.setText(3, input.getFieldFormatOk()[i] ? "Y" : "N");
      }
    }

    if (input.getDatabaseMeta() != null) {
      wConnection.setText(input.getDatabaseMeta().getName());
    }
    // General Settings Tab values from transform meta-data configuration.
    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    wBufferSize.setText("" + input.getBufferSize());
    if (input.getLogFile() != null) {
      wLogFile.setText(input.getLogFile());
    }
    wTruncate.setSelection(input.isTruncate());
    wFullyQuoteSQL.setSelection(input.isFullyQuoteSQL());

    // MonetDB mclient Settings tab
    if (input.getFieldSeparator() != null) {
      wFieldSeparator.setText(input.getFieldSeparator());
    }
    if (input.getFieldEnclosure() != null) {
      wFieldEnclosure.setText(input.getFieldEnclosure());
    }
    if (input.getNullRepresentation() != null) {
      wNULLrepresentation.setText(input.getNullRepresentation());
    }
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }

    wReturn.setRowNums();
    wReturn.optWidth(true);
  }

  protected void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    // return fields
    if (ciReturn != null) {
      ciReturn[1].setComboValues(fieldNames);
    }
  }

  /*
   * When the OK button is pressed, this method is called to take all values from the dialog and save them in the transform
   * meta data.
   */
  protected void getInfo(MonetDbBulkLoaderMeta inf) {
    int nrfields = wReturn.nrNonEmpty();

    inf.allocate(nrfields);

    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Log.FoundFields", "" + nrfields));
    }

    for (int i = 0; i < nrfields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      inf.getFieldTable()[i] = item.getText(1);
      inf.getFieldStream()[i] = item.getText(2);
      inf.getFieldFormatOk()[i] = "Y".equalsIgnoreCase(item.getText(3));
    }
    // General Settings Tab values from transform meta-data configuration.
    inf.setDbConnectionName(wConnection.getText());
    inf.setSchemaName(wSchema.getText());
    inf.setTableName(wTable.getText());
    inf.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText(), variables));
    inf.setBufferSize(wBufferSize.getText());
    inf.setLogFile(wLogFile.getText());
    inf.setTruncate(wTruncate.getSelection());
    inf.setFullyQuoteSQL(wFullyQuoteSQL.getSelection());

    // MonetDB API Settings tab
    inf.setFieldSeparator(wFieldSeparator.getText());
    inf.setFieldEnclosure(wFieldEnclosure.getText());
    inf.setNullRepresentation(wNULLrepresentation.getText());
    inf.setEncoding(wEncoding.getText());

    transformName = wTransformName.getText(); // return value
  }

  protected void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);

    dispose();
  }

  protected void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta != null) {
      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Log.LookingAtConnection")
                + databaseMeta.toString());
      }

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {

    // Determine the source and target fields...
    //
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MonetDBBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "MonetDBBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    // refresh data
    input.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText(), variables));
    input.setTableName(variables.resolve(wTable.getText()));
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MonetDBBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "MonetDBBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName();
    }

    // Create the existing mapping list...
    //
    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wReturn.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wReturn.getNonEmpty(i);
      String source = item.getText(2);
      String target = item.getText(1);

      int sourceIndex = sourceFields.indexOfValue(source);
      if (sourceIndex < 0) {
        missingSourceFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      int targetIndex = targetFields.indexOfValue(target);
      if (targetIndex < 0) {
        missingTargetFields
            .append(Const.CR)
            .append("   ")
            .append(source)
            .append(" --> ")
            .append(target);
      }
      if (sourceIndex < 0 || targetIndex < 0) {
        continue;
      }

      SourceToTargetMapping mapping = new SourceToTargetMapping(sourceIndex, targetIndex);
      mappings.add(mapping);
    }

    // show a confirm dialog if some missing field was found
    //
    if (!missingSourceFields.isEmpty() || !missingTargetFields.isEmpty()) {

      String message = "";
      if (!missingSourceFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "MonetDBBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "MonetDBBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(
                  PKG, "MonetDBBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;
      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(
                  PKG, "MonetDBBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.OK | SWT.CANCEL);
      boolean goOn = (answer & SWT.OK) != 0;
      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            MonetDbBulkLoaderDialog.this.shell,
            sourceFields.getFieldNames(),
            targetFields.getFieldNames(),
            mappings);
    mappings = d.open();

    // mappings == null if the user pressed cancel
    //
    if (mappings != null) {
      // Clear and re-populate!
      //
      wReturn.table.removeAll();
      wReturn.table.setItemCount(mappings.size());
      for (int i = 0; i < mappings.size(); i++) {
        SourceToTargetMapping mapping = mappings.get(i);
        TableItem item = wReturn.table.getItem(i);
        item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
        item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
      }
      wReturn.setRowNums();
      wReturn.optWidth(true);
    }
  }

  /*
   * Runs when the "Get Fields" button is pressed on the Output Fields dialog tab.
   */
  private void getUpdate() {
    try {

      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.getType() == IValueMeta.TYPE_DATE) {
                // The default is : format is OK for dates, see if this sticks later on...
                //
                tableItem.setText(3, "Y");
              } else {
                tableItem.setText(3, "Y"); // default is OK too...
              }
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      MonetDbBulkLoaderMeta info = new MonetDbBulkLoaderMeta();
      getInfo(info);

      String name = transformName; // new name might not yet be linked to other transforms!

      SqlStatement sql = info.getTableDdl(variables, pipelineMeta, name, false, null, false);
      if (!sql.hasError()) {
        if (sql.hasSql()) {
          SqlEditor sqledit =
              new SqlEditor(
                  shell,
                  SWT.NONE,
                  variables,
                  info.getDatabaseMeta(),
                  DbCache.getInstance(),
                  sql.getSql());
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(
              BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.NoSQLNeeds.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.NoSQLNeeds.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.SQLError.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.CouldNotBuildSQL.DialogTitle"),
          BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.CouldNotBuildSQL.DialogMessage"),
          ke);
    }
  }
}
