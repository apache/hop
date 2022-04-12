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

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.*;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.FontMetrics;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;

import java.util.List;
import java.util.*;

public class MonetDbBulkLoaderDialog extends BaseTransformDialog implements ITransformDialog {
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

  private final Map<String, Integer> inputFields = new HashMap<>();

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
      Shell parent, IVariables variables, Object in, PipelineMeta pipelineMeta, String sname) {
    super(parent, variables, (BaseTransformMeta) in, pipelineMeta, sname);
    input = (MonetDbBulkLoaderMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    props.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent arg0) {
            setTableFieldCombo();
          }
        };
    changed = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Shell.Title"));

    // The right side of all the labels is available as a user-defined percentage:
    // props.getMiddlePct()
    int middle = props.getMiddlePct();
    int margin = Const.MARGIN; // Default 4 pixel margin around components.

    //
    // OK (Button), Cancel (Button) and SQL (Button)
    // - these appear at the bottom of the dialog window.
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wSql = new Button(shell, SWT.PUSH);
    wSql.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.SQL.Button"));
    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    setButtonPositions(new Button[] {wOk, wSql, wCancel}, margin, null);

    //
    // Dialog Box Contents (Organized from dialog top to bottom, dialog left to right.)
    // Label - Transform name

    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Transformname.Label"));
    props.setLook(
        wlTransformName); // Puts the user-selected background color and font on the widget.

    // Text box for editing the transform name
    wTransformName = new Text(shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    props.setLook(wTransformName);
    wTransformName.addModifyListener(lsMod);

    //
    // Connection line
    //
    // Connection line
    wConnection = addConnectionLine(shell, wTransformName, input.getDatabaseMeta(), lsMod);

    // //////////////////////////////////////////////
    // Prepare the Folder that will contain tabs. //
    // //////////////////////////////////////////////
    wTabFolder = new CTabFolder(shell, SWT.BORDER);
    props.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    // ////////////////////////
    // General Settings tab //
    // ////////////////////////

    CTabItem wGeneralSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralSettingsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.GeneralSettings.Label"));

    Composite wGeneralSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wGeneralSettingsComp);

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
    wMonetDBmclientSettingsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.Label"));

    Composite wMonetDBmclientSettingsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wMonetDBmclientSettingsComp);
    wMonetDBmclientSettingsComp.setLayout(tabLayout);
    wMonetDBmclientSettingsComp.layout();
    wMonetDBmclientSettingsTab.setControl(wMonetDBmclientSettingsComp);

    Group wMonetDBmclientParamGroup = new Group(wMonetDBmclientSettingsComp, SWT.SHADOW_IN);
    wMonetDBmclientParamGroup.setText(
        BaseMessages.getString(
            PKG, "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup"));
    props.setLook(wMonetDBmclientParamGroup);
    wMonetDBmclientParamGroup.setLayout(tabLayout);
    wMonetDBmclientParamGroup.layout();

    // /////////////////////
    // Output Fields tab //
    // /////////////////////

    CTabItem wOutputFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wOutputFieldsTab.setText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Tab.OutputFields"));

    Composite wOutputFieldsComp = new Composite(wTabFolder, SWT.NONE);
    props.setLook(wOutputFieldsComp);

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
    props.setLook(wlSchema);
    wSchema = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);

    //
    // Table line (General Settings tab)
    //
    Label wlTable = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.TargetTable.Label"));
    props.setLook(wlTable);

    Button wbTable = new Button(wGeneralSettingsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Browse.Button"));

    wTable = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);

    //
    // Buffer size line (General Settings tab)
    //
    Label wlBufferSize = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlBufferSize.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.BufferSize.Label"));
    props.setLook(wlBufferSize);

    wBufferSize = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wBufferSize);
    wBufferSize.addModifyListener(lsMod);

    //
    // Log file line (General Settings tab)
    //
    Label wlLogFile = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlLogFile.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.LogFile.Label"));
    props.setLook(wlLogFile);

    Button wbLogFile = new Button(wGeneralSettingsComp, SWT.PUSH | SWT.CENTER);
    props.setLook(wbLogFile);
    wbLogFile.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Browse.Button"));

    wLogFile = new TextVar(variables, wGeneralSettingsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    props.setLook(wLogFile);
    wLogFile.addModifyListener(lsMod);

    //
    // Truncate before loading check box (General Settings tab)
    //
    Label wlTruncate = new Label(wGeneralSettingsComp, SWT.RIGHT);
    wlTruncate.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Truncate.Label"));
    props.setLook(wlTruncate);
    wTruncate = new Button(wGeneralSettingsComp, SWT.CHECK);
    props.setLook(wTruncate);
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
    props.setLook(wlFullyQuoteSQL);

    wFullyQuoteSQL = new Button(wGeneralSettingsComp, SWT.CHECK);
    props.setLook(wFullyQuoteSQL);
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
    props.setLook(wlFieldSeparator);

    wFieldSeparator = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wFieldSeparator.setItems(fieldSeparators);
    props.setLook(wFieldSeparator);
    wFieldSeparator.addModifyListener(lsMod);

    Label wlFieldEnclosure = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlFieldEnclosure.setText(
        BaseMessages.getString(
            PKG,
            "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup.FieldEnclosure.Label"));
    props.setLook(wlFieldEnclosure);

    wFieldEnclosure = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wFieldEnclosure.setItems(fieldEnclosures);
    wFieldEnclosure.addModifyListener(lsMod);

    Label wlNULLrepresentation = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlNULLrepresentation.setText(
        BaseMessages.getString(
            PKG,
            "MonetDBBulkLoaderDialog.Tab.MonetDBmclientSettings.ParameterGroup.NULLrepresentation.Label"));
    props.setLook(wlNULLrepresentation);

    wNULLrepresentation =
        new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.CENTER | SWT.BORDER);
    wNULLrepresentation.setItems(nullRepresentations);
    wNULLrepresentation.addModifyListener(lsMod);

    //
    // Control encoding line (MonetDB API Settings tab -> Parameter Group)
    //
    // The drop down is editable as it may happen an encoding may not be present
    // on one machine, but you may want to use it on your execution server
    //
    Label wlEncoding = new Label(wMonetDBmclientParamGroup, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Encoding.Label"));
    props.setLook(wlEncoding);

    wEncoding = new Combo(wMonetDBmclientParamGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEncoding.setToolTipText(
        BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Encoding.Tooltip"));
    wEncoding.setItems(encodings);
    props.setLook(wEncoding);
    wEncoding.addModifyListener(lsMod);

    // The field Table
    //
    // Output Fields tab - Widgets and FormData
    Label wlReturn = new Label(wOutputFieldsComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "MonetDBBulkLoaderDialog.Fields.Label"));
    props.setLook(wlReturn);

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
    // Transform name (Label and Edit Box)
    // - Location: top of the dialog box
    //
    fdlTransformName = new FormData();
    fdlTransformName.top = new FormAttachment(0, 15);
    fdlTransformName.left = new FormAttachment(0, margin);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    wlTransformName.setLayoutData(fdlTransformName);

    fdTransformName = new FormData();
    fdTransformName.top = new FormAttachment(wlTransformName, 0, SWT.CENTER);
    fdTransformName.left = new FormAttachment(wlTransformName, margin);
    fdTransformName.right =
        new FormAttachment(100, -margin); // 100% of the form component (length of edit box)
    wTransformName.setLayoutData(fdTransformName);

    //
    // Tabs will appear below the "Transform Name" area and above the buttons at the bottom of the
    // dialog.
    //
    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wConnection, margin + 20);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Database Schema Line - (General Settings Tab)
    //
    // Database Schema (Label layout)
    FormData fdlSchema = new FormData();
    fdlSchema.top = new FormAttachment(wTabFolder, margin);
    fdlSchema.left = new FormAttachment(wGeneralSettingsComp, margin);
    fdlSchema.right = new FormAttachment(middle, -margin);
    wlSchema.setLayoutData(fdlSchema);

    // Database schema (Edit box layout)
    FormData fdSchema = new FormData();
    fdSchema.top = new FormAttachment(wlSchema, 0, SWT.CENTER);
    fdSchema.left = new FormAttachment(middle, margin);
    fdSchema.right = new FormAttachment(100, -margin);
    wSchema.setLayoutData(fdSchema);

    // Target Table Line - (General Settings Tab)
    //
    // Target table (Label layout)
    // - tied to the left wall of the general settings tab (composite)
    FormData fdlTable = new FormData();
    fdlTable.top = new FormAttachment(wSchema, margin);
    fdlTable.left = new FormAttachment(0, margin);
    fdlTable.right = new FormAttachment(middle, -margin);
    wlTable.setLayoutData(fdlTable);

    // Target table browse (Button layout)
    // - tied to the right wall of the general settings tab (composite)
    FormData fdbTable = new FormData();
    fdbTable.top = new FormAttachment(wlTable, 0, SWT.CENTER);
    fdbTable.right = new FormAttachment(100, -margin);
    wbTable.setLayoutData(fdbTable);

    // Target table (Edit box layout)
    // Between the label and button.
    // - tied to the right edge of the general Browse tables button
    FormData fdTable = new FormData();
    fdTable.top = new FormAttachment(wSchema, margin);
    fdTable.left = new FormAttachment(middle, margin);
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
    fdBufferSize.left = new FormAttachment(middle, margin);
    fdBufferSize.right = new FormAttachment(100, -margin);
    wBufferSize.setLayoutData(fdBufferSize);

    FormData fdlLogFile = new FormData();
    fdlLogFile.top = new FormAttachment(wBufferSize, margin);
    fdlLogFile.left = new FormAttachment(0, margin);
    fdlLogFile.right = new FormAttachment(middle, -margin);
    wlLogFile.setLayoutData(fdlLogFile);

    // Log file Browse (button)
    FormData fdbLogFile = new FormData();
    fdbLogFile.top = new FormAttachment(wBufferSize, 0);
    fdbLogFile.right = new FormAttachment(100, -margin);
    wbLogFile.setLayoutData(fdbLogFile);

    FormData fdLogFile = new FormData();
    fdLogFile.left = new FormAttachment(middle, margin);
    fdLogFile.top = new FormAttachment(wBufferSize, margin);
    fdLogFile.right = new FormAttachment(wbLogFile, -margin);
    fdLogFile.right = new FormAttachment(middle, -margin);
    wLogFile.setLayoutData(fdLogFile);

    FormData fdlTruncate = new FormData();
    fdlTruncate.top = new FormAttachment(wLogFile, margin * 2);
    fdlTruncate.left = new FormAttachment(0, margin);
    fdlTruncate.right = new FormAttachment(middle, -margin);
    wlTruncate.setLayoutData(fdlTruncate);

    FormData fdTruncate = new FormData();
    fdTruncate.top = new FormAttachment(wlTruncate, 0, SWT.CENTER);
    fdTruncate.left = new FormAttachment(wlTruncate, margin);
    fdTruncate.right = new FormAttachment(100, -margin);
    wTruncate.setLayoutData(fdTruncate);

    FormData fdlFullyQuoteSQL = new FormData();
    fdlFullyQuoteSQL.top = new FormAttachment(wlTruncate, margin * 2);
    fdlFullyQuoteSQL.left = new FormAttachment(0, margin);
    fdlFullyQuoteSQL.right = new FormAttachment(middle, -margin);
    wlFullyQuoteSQL.setLayoutData(fdlFullyQuoteSQL);

    FormData fdFullyQuoteSQL = new FormData();
    fdFullyQuoteSQL.top = new FormAttachment(wlFullyQuoteSQL, 0, SWT.CENTER);
    fdFullyQuoteSQL.left = new FormAttachment(wlFullyQuoteSQL, margin);
    fdFullyQuoteSQL.right = new FormAttachment(100, -margin);
    wFullyQuoteSQL.setLayoutData(fdFullyQuoteSQL);
    //
    // MonetDB Settings tab layout
    //

    //
    // mclient parameter grouping (Group composite)
    // - Visually we make it clear what is being fed to mclient as parameters.
    FormData fdgMonetDBmclientParamGroup = new FormData();
    fdgMonetDBmclientParamGroup.top = new FormAttachment(wMonetDBmclientSettingsComp, margin * 3);
    fdgMonetDBmclientParamGroup.left = new FormAttachment(0, margin);
    fdgMonetDBmclientParamGroup.right = new FormAttachment(100, -margin);
    wMonetDBmclientParamGroup.setLayoutData(fdgMonetDBmclientParamGroup);

    // Figure out font width in pixels, then set the combo boxes to a standard width of 20
    // characters.
    Text text = new Text(shell, SWT.NONE);
    GC gc = new GC(text);
    FontMetrics fm = gc.getFontMetrics();
    int charWidth = fm.getAverageCharWidth();
    int fieldWidth = text.computeSize(charWidth * 20, SWT.DEFAULT).x;
    gc.dispose();
    text.dispose();

    FormData fdlFieldSeparator = new FormData();
    fdlFieldSeparator.top = new FormAttachment(wMonetDBmclientSettingsComp, 3 * margin);
    fdlFieldSeparator.left = new FormAttachment(0, 3 * margin);
    fdlFieldSeparator.right = new FormAttachment(middle, -margin);
    wlFieldSeparator.setLayoutData(fdlFieldSeparator);

    FormData fdFieldSeparator = new FormData();
    fdFieldSeparator.top = new FormAttachment(wMonetDBmclientSettingsComp, 3 * margin);
    fdFieldSeparator.left = new FormAttachment(middle, margin);
    fdFieldSeparator.width = fieldWidth;
    wFieldSeparator.setLayoutData(fdFieldSeparator);

    FormData fdlFieldEnclosure = new FormData();
    fdlFieldEnclosure.top = new FormAttachment(wFieldSeparator, 2 * margin);
    fdlFieldEnclosure.left = new FormAttachment(0, 3 * margin);
    fdlFieldEnclosure.right = new FormAttachment(middle, -margin);
    wlFieldEnclosure.setLayoutData(fdlFieldEnclosure);

    FormData fdFieldEnclosure = new FormData();
    fdFieldEnclosure.top = new FormAttachment(wFieldSeparator, 2 * margin);
    fdFieldEnclosure.left = new FormAttachment(middle, margin);
    fdFieldEnclosure.width = fieldWidth;
    wFieldEnclosure.setLayoutData(fdFieldEnclosure);

    FormData fdlNULLrepresentation = new FormData();
    fdlNULLrepresentation.top = new FormAttachment(wFieldEnclosure, 2 * margin);
    fdlNULLrepresentation.left = new FormAttachment(0, 3 * margin);
    fdlNULLrepresentation.right = new FormAttachment(middle, -margin);
    wlNULLrepresentation.setLayoutData(fdlNULLrepresentation);

    FormData fdNULLrepresentation = new FormData();
    fdNULLrepresentation.top = new FormAttachment(wFieldEnclosure, 2 * margin);
    fdNULLrepresentation.left = new FormAttachment(middle, margin);
    fdNULLrepresentation.width = fieldWidth;
    wNULLrepresentation.setLayoutData(fdNULLrepresentation);

    // Stream encoding parameter sent to mclient (Label layout)
    FormData fdlEncoding = new FormData();
    fdlEncoding.top = new FormAttachment(wNULLrepresentation, 2 * margin);
    fdlEncoding.left = new FormAttachment(0, 3 * margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);

    FormData fdEncoding = new FormData();
    fdEncoding.top = new FormAttachment(wNULLrepresentation, 2 * margin);
    fdEncoding.left = new FormAttachment(middle, margin);
    fdEncoding.width = fieldWidth;
    wEncoding.setLayoutData(fdEncoding);

    //
    // Output Fields tab layout
    //
    // Label at the top left of the tab
    FormData fdlReturn = new FormData();
    fdlReturn.top = new FormAttachment(wOutputFieldsComp, 5 * margin);
    fdlReturn.left = new FormAttachment(0, margin);
    wlReturn.setLayoutData(fdlReturn);

    // button right, top of the tab
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wOutputFieldsComp, 2 * margin);
    fdDoMapping.right = new FormAttachment(100, -margin);
    wDoMapping.setLayoutData(fdDoMapping);

    // to the left of the button above
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(wOutputFieldsComp, 2 * margin);
    fdGetLU.right = new FormAttachment(wDoMapping, -margin);
    wGetLU.setLayoutData(fdGetLU);

    // to the left of the button above
    FormData fdClearDBCache = new FormData();
    fdClearDBCache.top = new FormAttachment(wOutputFieldsComp, 2 * margin);
    fdClearDBCache.right = new FormAttachment(wGetLU, -margin);
    wClearDBCache.setLayoutData(fdClearDBCache);

    // Table of results
    FormData fdReturn = new FormData();
    fdReturn.top = new FormAttachment(wGetLU, 3 * margin);
    fdReturn.left = new FormAttachment(0, margin);
    fdReturn.right = new FormAttachment(100, -margin);
    fdReturn.bottom = new FormAttachment(100, -2 * margin);
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
                inputFields.put(row.getValueMeta(i).getName(), i);
              }

              setComboBoxes();
            } catch (HopException e) {
              logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
            }
          }
        };
    new Thread(runnable).start();

    wbLogFile.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            FileDialog dialog = new FileDialog(shell, SWT.OPEN);
            dialog.setFilterExtensions(new String[] {"*"});
            if (wLogFile.getText() != null) {
              dialog.setFileName(wLogFile.getText());
            }
            dialog.setFilterNames(ALL_FILETYPES);
            if (dialog.open() != null) {
              wLogFile.setText(
                  dialog.getFilterPath() + Const.FILE_SEPARATOR + dialog.getFileName());
            }
          }
        });

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wGetLU.addListener(SWT.Selection, e -> getUpdate());
    wSql.addListener(SWT.Selection, e -> create());
    wCancel.addListener(SWT.Selection, e -> cancel());

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
              DatabaseMeta ci = pipelineMeta.findDatabase(connectionName);
              if (ci != null) {
                Database db = new Database(loggingObject, variables, ci);
                try {
                  db.connect();

                  String schemaTable =
                      ci.getQuotedSchemaTableCombination(variables, schemaName, tableName);
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
                } finally {
                  try {
                    if (db != null) {
                      db.disconnect();
                    }
                  } catch (Exception ignored) {
                    // ignore any errors here.
                    db = null;
                  }
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

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  protected void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    final Map<String, Integer> fields = new HashMap<>();

    // Add the currentMeta fields...
    fields.putAll(inputFields);

    Set<String> keySet = fields.keySet();
    List<String> entries = new ArrayList<>(keySet);

    String[] fieldNames = entries.toArray(new String[entries.size()]);
    Const.sortStrings(fieldNames);
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
    // CHECKSTYLE:Indentation:OFF
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
    inf.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText()));
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
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName);
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
    input.setDatabaseMeta(pipelineMeta.findDatabase(wConnection.getText()));
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
    if (missingSourceFields.length() > 0 || missingTargetFields.length() > 0) {

      String message = "";
      if (missingSourceFields.length() > 0) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "MonetDBBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (missingTargetFields.length() > 0) {
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
