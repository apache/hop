/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hop.pipeline.transforms.orabulkloader;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

/** Dialog class for the Oracle bulk loader transformation. */
public class OraBulkLoaderDialog extends BaseTransformDialog {
  private static final Class<?> PKG =
      OraBulkLoaderDialog.class; // for i18n purposes, needed by Translator2!!
  public static final String CONST_ORA_BULK_LOADER_DIALOG_DATE_MASK_LABEL =
      "OraBulkLoaderDialog.DateMask.Label";
  public static final String CONST_ORA_BULK_LOADER_DIALOG_DATE_TIME_MASK_LABEL =
      "OraBulkLoaderDialog.DateTimeMask.Label";
  public static final String CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON =
      "OraBulkLoaderDialog.Browse.Button";

  private MetaSelectionLine<DatabaseMeta> wConnection;
  private TextVar wSchema;
  private TextVar wTable;
  private TextVar wSqlldr;
  private CCombo wLoadMethod;
  private CCombo wLoadAction;
  private TextVar wMaxErrors;
  private TextVar wCommit;
  private TextVar wBindSize;
  private TextVar wReadSize;
  private TableView wReturn;
  private TextVar wControlFile;
  private TextVar wDataFile;
  private TextVar wLogFile;
  private TextVar wBadFile;
  private TextVar wDiscardFile;
  private Combo wEncoding;
  private Combo wCharacterSetName;
  private Button wDirectPath;
  private Button wEraseFiles;
  private Button wFailOnWarning;
  private Button wFailOnError;
  private Button wParallel;
  private TextVar wAltRecordTerm;

  private OraBulkLoaderMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] ciReturn;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  // These should not be translated, they are required to exist on all
  // platforms according to the documentation of "Charset".
  private static String[] encodings = {
    "", "US-ASCII", "ISO-8859-1", "UTF-8", "UTF-16BE", "UTF-16LE", "UTF-16"
  };

  private static String[] characterSetNames = {
    "", "US7ASCII", "WE8ISO8859P1", "UTF8",
  };

  private static final String[] ALL_FILETYPES =
      new String[] {BaseMessages.getString(PKG, "OraBulkLoaderDialog.Filetype.All")};

  public OraBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      OraBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  public String open() {
    createShell(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).sql(e -> create()).cancel(e -> cancel()).build();

    CTabFolder wTabFolder;

    ModifyListener lsMod = e -> input.setChanged();
    ModifyListener lsTableMod =
        arg0 -> {
          input.setChanged();
          setTableFieldCombo();
        };

    changed = input.hasChanged();

    ScrolledComposite sc = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(sc);
    FormData fdSc = new FormData();
    fdSc.left = new FormAttachment(0, 0);
    fdSc.top = new FormAttachment(wSpacer, 0);
    fdSc.right = new FormAttachment(100, 0);
    fdSc.bottom = new FormAttachment(wOk, -margin);
    sc.setLayoutData(fdSc);
    sc.setLayout(new FillLayout());

    Composite wContent = new Composite(sc, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    // Tab folder
    wTabFolder = new CTabFolder(wContent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(0, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);

    CTabItem wBulkLoaderTab = new CTabItem(wTabFolder, SWT.NONE);
    wBulkLoaderTab.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.BulkLoader.Label"));

    Composite wBulkLoaderComposite = new Composite(wTabFolder, SWT.NONE);
    FormLayout fdBulkLoaderLayout = new FormLayout();
    fdBulkLoaderLayout.marginWidth = 3;
    fdBulkLoaderLayout.marginHeight = 3;
    wBulkLoaderComposite.setLayout(fdBulkLoaderLayout);
    PropsUi.setLook(wBulkLoaderComposite);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    wBulkLoaderComposite.setLayoutData(fdComp);

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTableFieldCombo();
          }
        };

    // Connection line
    wConnection = addConnectionLine(wBulkLoaderComposite, null, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    wbSchema.addListener(SWT.Selection, e -> getSchemaNames());
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsTableMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    wbTable.addListener(SWT.Selection, e -> getTableName());
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wTable = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsTableMod);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Sqlldr line...
    Label wlSqlldr = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlSqlldr.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Sqlldr.Label"));
    PropsUi.setLook(wlSqlldr);
    FormData fdlSqlldr = new FormData();
    fdlSqlldr.left = new FormAttachment(0, 0);
    fdlSqlldr.right = new FormAttachment(middle, -margin);
    fdlSqlldr.top = new FormAttachment(wTable, margin);
    wlSqlldr.setLayoutData(fdlSqlldr);

    Button wbSqlldr = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSqlldr);
    wbSqlldr.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbSqlldr = new FormData();
    fdbSqlldr.right = new FormAttachment(100, 0);
    fdbSqlldr.top = new FormAttachment(wTable, margin);
    wbSqlldr.setLayoutData(fdbSqlldr);
    wSqlldr = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSqlldr);
    wSqlldr.addModifyListener(lsMod);
    FormData fdSqlldr = new FormData();
    fdSqlldr.left = new FormAttachment(middle, 0);
    fdSqlldr.top = new FormAttachment(wTable, margin);
    fdSqlldr.right = new FormAttachment(wbSqlldr, -margin);
    wSqlldr.setLayoutData(fdSqlldr);
    wbSqlldr.setData(wSqlldr);

    // Load Method line
    Label wlLoadMethod = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlLoadMethod.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.LoadMethod.Label"));
    PropsUi.setLook(wlLoadMethod);
    FormData fdlLoadMethod = new FormData();
    fdlLoadMethod.left = new FormAttachment(0, 0);
    fdlLoadMethod.right = new FormAttachment(middle, -margin);
    fdlLoadMethod.top = new FormAttachment(wSqlldr, margin);
    wlLoadMethod.setLayoutData(fdlLoadMethod);
    wLoadMethod = new CCombo(wBulkLoaderComposite, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wLoadMethod.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.AutoEndLoadMethod.Label"));
    wLoadMethod.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.ManualLoadMethod.Label"));
    wLoadMethod.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.AutoConcLoadMethod.Label"));
    wLoadMethod.select(0); // +1: starts at -1
    wLoadMethod.addModifyListener(lsMod);

    PropsUi.setLook(wLoadMethod);
    FormData fdLoadMethod = new FormData();
    fdLoadMethod.left = new FormAttachment(middle, 0);
    fdLoadMethod.top = new FormAttachment(wSqlldr, margin);
    fdLoadMethod.right = new FormAttachment(100, 0);
    wLoadMethod.setLayoutData(fdLoadMethod);

    fdLoadMethod = new FormData();
    fdLoadMethod.left = new FormAttachment(middle, 0);
    fdLoadMethod.top = new FormAttachment(wSqlldr, margin);
    fdLoadMethod.right = new FormAttachment(100, 0);
    wLoadMethod.setLayoutData(fdLoadMethod);

    // Load Action line
    Label wlLoadAction = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlLoadAction.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.LoadAction.Label"));
    PropsUi.setLook(wlLoadAction);
    FormData fdlLoadAction = new FormData();
    fdlLoadAction.left = new FormAttachment(0, 0);
    fdlLoadAction.right = new FormAttachment(middle, -margin);
    fdlLoadAction.top = new FormAttachment(wLoadMethod, margin);
    wlLoadAction.setLayoutData(fdlLoadAction);
    wLoadAction = new CCombo(wBulkLoaderComposite, SWT.SINGLE | SWT.READ_ONLY | SWT.BORDER);
    wLoadAction.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.AppendLoadAction.Label"));
    wLoadAction.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.InsertLoadAction.Label"));
    wLoadAction.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.ReplaceLoadAction.Label"));
    wLoadAction.add(BaseMessages.getString(PKG, "OraBulkLoaderDialog.TruncateLoadAction.Label"));
    wLoadAction.select(0); // +1: starts at -1
    wLoadAction.addModifyListener(lsMod);

    PropsUi.setLook(wLoadAction);
    FormData fdLoadAction = new FormData();
    fdLoadAction.left = new FormAttachment(middle, 0);
    fdLoadAction.top = new FormAttachment(wLoadMethod, margin);
    fdLoadAction.right = new FormAttachment(100, 0);
    wLoadAction.setLayoutData(fdLoadAction);

    // MaxErrors file line
    Label wlMaxErrors = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlMaxErrors.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.MaxErrors.Label"));
    PropsUi.setLook(wlMaxErrors);
    FormData fdlMaxErrors = new FormData();
    fdlMaxErrors.left = new FormAttachment(0, 0);
    fdlMaxErrors.top = new FormAttachment(wLoadAction, margin);
    fdlMaxErrors.right = new FormAttachment(middle, -margin);
    wlMaxErrors.setLayoutData(fdlMaxErrors);
    wMaxErrors = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxErrors);
    wMaxErrors.addModifyListener(lsMod);
    FormData fdMaxErrors = new FormData();
    fdMaxErrors.left = new FormAttachment(middle, 0);
    fdMaxErrors.top = new FormAttachment(wLoadAction, margin);
    fdMaxErrors.right = new FormAttachment(100, 0);
    wMaxErrors.setLayoutData(fdMaxErrors);

    // Commmit/batch file line
    Label wlCommit = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Commit.Label"));
    PropsUi.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment(0, 0);
    fdlCommit.top = new FormAttachment(wMaxErrors, margin);
    fdlCommit.right = new FormAttachment(middle, -margin);
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommit);
    wCommit.addModifyListener(lsMod);
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment(middle, 0);
    fdCommit.top = new FormAttachment(wMaxErrors, margin);
    fdCommit.right = new FormAttachment(100, 0);
    wCommit.setLayoutData(fdCommit);

    // Bind size line
    Label wlBindSize = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlBindSize.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.BindSize.Label"));
    PropsUi.setLook(wlBindSize);
    FormData fdlBindSize = new FormData();
    fdlBindSize.left = new FormAttachment(0, 0);
    fdlBindSize.top = new FormAttachment(wCommit, margin);
    fdlBindSize.right = new FormAttachment(middle, -margin);
    wlBindSize.setLayoutData(fdlBindSize);
    wBindSize = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBindSize);
    wBindSize.addModifyListener(lsMod);
    FormData fdBindSize = new FormData();
    fdBindSize.left = new FormAttachment(middle, 0);
    fdBindSize.top = new FormAttachment(wCommit, margin);
    fdBindSize.right = new FormAttachment(100, 0);
    wBindSize.setLayoutData(fdBindSize);

    // Read size line
    Label wlReadSize = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlReadSize.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.ReadSize.Label"));
    PropsUi.setLook(wlReadSize);
    FormData fdlReadSize = new FormData();
    fdlReadSize.left = new FormAttachment(0, 0);
    fdlReadSize.top = new FormAttachment(wBindSize, margin);
    fdlReadSize.right = new FormAttachment(middle, -margin);
    wlReadSize.setLayoutData(fdlReadSize);
    wReadSize = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wReadSize);
    wReadSize.addModifyListener(lsMod);
    FormData fdReadSize = new FormData();
    fdReadSize.left = new FormAttachment(middle, 0);
    fdReadSize.top = new FormAttachment(wBindSize, margin);
    fdReadSize.right = new FormAttachment(100, 0);
    wReadSize.setLayoutData(fdReadSize);

    // Control file line
    Label wlControlFile = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlControlFile.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.ControlFile.Label"));
    PropsUi.setLook(wlControlFile);
    FormData fdlControlFile = new FormData();
    fdlControlFile.left = new FormAttachment(0, 0);
    fdlControlFile.top = new FormAttachment(wReadSize, margin);
    fdlControlFile.right = new FormAttachment(middle, -margin);
    wlControlFile.setLayoutData(fdlControlFile);
    Button wbControlFile = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbControlFile);
    wbControlFile.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbControlFile = new FormData();
    fdbControlFile.right = new FormAttachment(100, 0);
    fdbControlFile.top = new FormAttachment(wReadSize, margin);
    wbControlFile.setLayoutData(fdbControlFile);
    wControlFile = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wControlFile);
    wControlFile.addModifyListener(lsMod);
    FormData fdControlFile = new FormData();
    fdControlFile.left = new FormAttachment(middle, 0);
    fdControlFile.top = new FormAttachment(wReadSize, margin);
    fdControlFile.right = new FormAttachment(wbControlFile, -margin);
    wControlFile.setLayoutData(fdControlFile);
    wbControlFile.setData(wControlFile);

    // Data file line
    Label wlDataFile = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlDataFile.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.DataFile.Label"));
    PropsUi.setLook(wlDataFile);
    FormData fdlDataFile = new FormData();
    fdlDataFile.left = new FormAttachment(0, 0);
    fdlDataFile.top = new FormAttachment(wControlFile, margin);
    fdlDataFile.right = new FormAttachment(middle, -margin);
    wlDataFile.setLayoutData(fdlDataFile);
    Button wbDataFile = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDataFile);
    wbDataFile.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbDataFile = new FormData();
    fdbDataFile.right = new FormAttachment(100, 0);
    fdbDataFile.top = new FormAttachment(wControlFile, margin);
    wbDataFile.setLayoutData(fdbDataFile);
    wDataFile = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDataFile);
    wDataFile.addModifyListener(lsMod);
    FormData fdDataFile = new FormData();
    fdDataFile.left = new FormAttachment(middle, 0);
    fdDataFile.top = new FormAttachment(wControlFile, margin);
    fdDataFile.right = new FormAttachment(wbDataFile, -margin);
    wDataFile.setLayoutData(fdDataFile);
    wbDataFile.setData(wDataFile);

    // Log file line
    Label wlLogFile = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlLogFile.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.LogFile.Label"));
    PropsUi.setLook(wlLogFile);
    FormData fdlLogFile = new FormData();
    fdlLogFile.left = new FormAttachment(0, 0);
    fdlLogFile.top = new FormAttachment(wDataFile, margin);
    fdlLogFile.right = new FormAttachment(middle, -margin);
    wlLogFile.setLayoutData(fdlLogFile);
    Button wbLogFile = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbLogFile);
    wbLogFile.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbLogFile = new FormData();
    fdbLogFile.right = new FormAttachment(100, 0);
    fdbLogFile.top = new FormAttachment(wDataFile, margin);
    wbLogFile.setLayoutData(fdbLogFile);
    wLogFile = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wLogFile);
    wLogFile.addModifyListener(lsMod);
    FormData fdLogFile = new FormData();
    fdLogFile.left = new FormAttachment(middle, 0);
    fdLogFile.top = new FormAttachment(wDataFile, margin);
    fdLogFile.right = new FormAttachment(wbLogFile, -margin);
    wLogFile.setLayoutData(fdLogFile);
    wbLogFile.setData(wLogFile);

    // Bad file line
    Label wlBadFile = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlBadFile.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.BadFile.Label"));
    PropsUi.setLook(wlBadFile);
    FormData fdlBadFile = new FormData();
    fdlBadFile.left = new FormAttachment(0, 0);
    fdlBadFile.top = new FormAttachment(wLogFile, margin);
    fdlBadFile.right = new FormAttachment(middle, -margin);
    wlBadFile.setLayoutData(fdlBadFile);
    Button wbBadFile = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbBadFile);
    wbBadFile.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbBadFile = new FormData();
    fdbBadFile.right = new FormAttachment(100, 0);
    fdbBadFile.top = new FormAttachment(wLogFile, margin);
    wbBadFile.setLayoutData(fdbBadFile);
    wBadFile = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wBadFile);
    wBadFile.addModifyListener(lsMod);
    FormData fdBadFile = new FormData();
    fdBadFile.left = new FormAttachment(middle, 0);
    fdBadFile.top = new FormAttachment(wLogFile, margin);
    fdBadFile.right = new FormAttachment(wbBadFile, -margin);
    wBadFile.setLayoutData(fdBadFile);
    wbBadFile.setData(wBadFile);

    // Discard file line
    Label wlDiscardFile = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlDiscardFile.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.DiscardFile.Label"));
    PropsUi.setLook(wlDiscardFile);
    FormData fdlDiscardFile = new FormData();
    fdlDiscardFile.left = new FormAttachment(0, 0);
    fdlDiscardFile.top = new FormAttachment(wBadFile, margin);
    fdlDiscardFile.right = new FormAttachment(middle, -margin);
    wlDiscardFile.setLayoutData(fdlDiscardFile);
    Button wbDiscardFile = new Button(wBulkLoaderComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbDiscardFile);
    wbDiscardFile.setText(BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_BROWSE_BUTTON));
    FormData fdbDiscardFile = new FormData();
    fdbDiscardFile.right = new FormAttachment(100, 0);
    fdbDiscardFile.top = new FormAttachment(wBadFile, margin);
    wbDiscardFile.setLayoutData(fdbDiscardFile);
    wDiscardFile = new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDiscardFile);
    wDiscardFile.addModifyListener(lsMod);
    FormData fdDiscardFile = new FormData();
    fdDiscardFile.left = new FormAttachment(middle, 0);
    fdDiscardFile.top = new FormAttachment(wBadFile, margin);
    fdDiscardFile.right = new FormAttachment(wbDiscardFile, -margin);
    wDiscardFile.setLayoutData(fdDiscardFile);
    wDiscardFile.setData(wDiscardFile);

    //
    // Control encoding line
    //
    // The drop down is editable as it may happen an encoding may not be present
    // on one machine, but you may want to use it on your execution server
    //
    Label wlEncoding = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlEncoding.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Encoding.Label"));
    PropsUi.setLook(wlEncoding);
    FormData fdlEncoding = new FormData();
    fdlEncoding.left = new FormAttachment(0, 0);
    fdlEncoding.top = new FormAttachment(wDiscardFile, margin);
    fdlEncoding.right = new FormAttachment(middle, -margin);
    wlEncoding.setLayoutData(fdlEncoding);
    wEncoding = new Combo(wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wEncoding.setToolTipText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Encoding.Tooltip"));
    wEncoding.setItems(encodings);
    PropsUi.setLook(wEncoding);
    FormData fdEncoding = new FormData();
    fdEncoding.left = new FormAttachment(middle, 0);
    fdEncoding.top = new FormAttachment(wDiscardFile, margin);
    fdEncoding.right = new FormAttachment(100, 0);
    wEncoding.setLayoutData(fdEncoding);
    wEncoding.addModifyListener(lsMod);

    // Oracle character set name line
    Label wlCharacterSetName = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlCharacterSetName.setText(
        BaseMessages.getString(PKG, "OraBulkLoaderDialog.CharacterSetName.Label"));
    PropsUi.setLook(wlCharacterSetName);
    FormData fdlCharacterSetName = new FormData();
    fdlCharacterSetName.left = new FormAttachment(0, 0);
    fdlCharacterSetName.top = new FormAttachment(wEncoding, margin);
    fdlCharacterSetName.right = new FormAttachment(middle, -margin);
    wlCharacterSetName.setLayoutData(fdlCharacterSetName);
    wCharacterSetName = new Combo(wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wCharacterSetName.setToolTipText(
        BaseMessages.getString(PKG, "OraBulkLoaderDialog.CharacterSetName.Tooltip"));
    wCharacterSetName.setItems(characterSetNames);
    PropsUi.setLook(wCharacterSetName);
    FormData fdCharacterSetName = new FormData();
    fdCharacterSetName.left = new FormAttachment(middle, 0);
    fdCharacterSetName.top = new FormAttachment(wEncoding, margin);
    fdCharacterSetName.right = new FormAttachment(100, 0);
    wCharacterSetName.setLayoutData(fdCharacterSetName);
    wCharacterSetName.addModifyListener(lsMod);

    // Alternate Record Terminator
    Label wlAltRecordTerm = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlAltRecordTerm.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.AltRecordTerm.Label"));
    PropsUi.setLook(wlAltRecordTerm);
    FormData fdlAltRecordTerm = new FormData();
    fdlAltRecordTerm.left = new FormAttachment(0, 0);
    fdlAltRecordTerm.top = new FormAttachment(wCharacterSetName, margin);
    fdlAltRecordTerm.right = new FormAttachment(middle, -margin);
    wlAltRecordTerm.setLayoutData(fdlAltRecordTerm);
    wAltRecordTerm =
        new TextVar(variables, wBulkLoaderComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wAltRecordTerm);
    FormData fdAltRecordTerm = new FormData();
    fdAltRecordTerm.left = new FormAttachment(middle, 0);
    fdAltRecordTerm.top = new FormAttachment(wCharacterSetName, margin);
    fdAltRecordTerm.right = new FormAttachment(100, 0);
    wAltRecordTerm.setLayoutData(fdAltRecordTerm);
    wAltRecordTerm.addModifyListener(lsMod);

    // DirectPath line
    Label wlDirectPath = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlDirectPath.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.DirectPath.Label"));
    PropsUi.setLook(wlDirectPath);
    FormData fdlDirectPath = new FormData();
    fdlDirectPath.left = new FormAttachment(0, 0);
    fdlDirectPath.top = new FormAttachment(wAltRecordTerm, margin);
    fdlDirectPath.right = new FormAttachment(middle, -margin);
    wlDirectPath.setLayoutData(fdlDirectPath);
    wDirectPath = new Button(wBulkLoaderComposite, SWT.CHECK);
    PropsUi.setLook(wDirectPath);
    FormData fdDirectPath = new FormData();
    fdDirectPath.left = new FormAttachment(middle, 0);
    fdDirectPath.top = new FormAttachment(wAltRecordTerm, margin);
    fdDirectPath.right = new FormAttachment(100, 0);
    wDirectPath.setLayoutData(fdDirectPath);
    wDirectPath.addListener(
        SWT.Selection,
        e -> {
          input.setChanged();
          // Parallel loading is only possible with a direct path option...
          //
          if (!wDirectPath.getSelection()) {
            wParallel.setSelection(false);
          }
          wParallel.setEnabled(wDirectPath.getSelection());
        });

    // Erase files line
    Label wlEraseFiles = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlEraseFiles.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.EraseFiles.Label"));
    PropsUi.setLook(wlEraseFiles);
    FormData fdlEraseFiles = new FormData();
    fdlEraseFiles.left = new FormAttachment(0, 0);
    fdlEraseFiles.top = new FormAttachment(wDirectPath, margin);
    fdlEraseFiles.right = new FormAttachment(middle, -margin);
    wlEraseFiles.setLayoutData(fdlEraseFiles);
    wEraseFiles = new Button(wBulkLoaderComposite, SWT.CHECK);
    PropsUi.setLook(wEraseFiles);
    FormData fdEraseFiles = new FormData();
    fdEraseFiles.left = new FormAttachment(middle, 0);
    fdEraseFiles.top = new FormAttachment(wDirectPath, margin);
    fdEraseFiles.right = new FormAttachment(100, 0);
    wEraseFiles.setLayoutData(fdEraseFiles);
    wEraseFiles.addListener(SWT.Selection, e -> input.setChanged());

    // Fail on warning line
    Label wlFailOnWarning = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlFailOnWarning.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.FailOnWarning.Label"));
    PropsUi.setLook(wlFailOnWarning);
    FormData fdlFailOnWarning = new FormData();
    fdlFailOnWarning.left = new FormAttachment(0, 0);
    fdlFailOnWarning.top = new FormAttachment(wEraseFiles, margin);
    fdlFailOnWarning.right = new FormAttachment(middle, -margin);
    wlFailOnWarning.setLayoutData(fdlFailOnWarning);
    wFailOnWarning = new Button(wBulkLoaderComposite, SWT.CHECK);
    PropsUi.setLook(wFailOnWarning);
    FormData fdFailOnWarning = new FormData();
    fdFailOnWarning.left = new FormAttachment(middle, 0);
    fdFailOnWarning.top = new FormAttachment(wEraseFiles, margin);
    fdFailOnWarning.right = new FormAttachment(100, 0);
    wFailOnWarning.setLayoutData(fdFailOnWarning);
    wFailOnWarning.addListener(SWT.Selection, e -> input.setChanged());

    // Fail on error line
    Label wlFailOnError = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlFailOnError.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.FailOnError.Label"));
    PropsUi.setLook(wlFailOnError);
    FormData fdlFailOnError = new FormData();
    fdlFailOnError.left = new FormAttachment(0, 0);
    fdlFailOnError.top = new FormAttachment(wFailOnWarning, margin);
    fdlFailOnError.right = new FormAttachment(middle, -margin);
    wlFailOnError.setLayoutData(fdlFailOnError);
    wFailOnError = new Button(wBulkLoaderComposite, SWT.CHECK);
    PropsUi.setLook(wFailOnError);
    FormData fdFailOnError = new FormData();
    fdFailOnError.left = new FormAttachment(middle, 0);
    fdFailOnError.top = new FormAttachment(wFailOnWarning, margin);
    fdFailOnError.right = new FormAttachment(100, 0);
    wFailOnError.setLayoutData(fdFailOnError);
    wFailOnError.addListener(SWT.Selection, e -> input.setChanged());

    // Fail on error line
    Label wlParallel = new Label(wBulkLoaderComposite, SWT.RIGHT);
    wlParallel.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Parallel.Label"));
    PropsUi.setLook(wlParallel);
    FormData fdlParallel = new FormData();
    fdlParallel.left = new FormAttachment(0, 0);
    fdlParallel.top = new FormAttachment(wFailOnError, margin);
    fdlParallel.right = new FormAttachment(middle, -margin);
    wlParallel.setLayoutData(fdlParallel);
    wParallel = new Button(wBulkLoaderComposite, SWT.CHECK);
    PropsUi.setLook(wParallel);
    FormData fdParallel = new FormData();
    fdParallel.left = new FormAttachment(middle, 0);
    fdParallel.top = new FormAttachment(wFailOnError, margin);
    fdParallel.right = new FormAttachment(100, 0);
    wParallel.setLayoutData(fdParallel);
    wParallel.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            // Parallel loading is only possible with a direct path option...
            //
            if (wParallel.getSelection()) {
              wDirectPath.setSelection(true);
            }
          }
        });

    fdComp.bottom = new FormAttachment(wParallel, margin);
    wBulkLoaderComposite.pack();
    wBulkLoaderComposite.setLayoutData(fdComp);

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Fields.Label"));

    Composite wFieldsComposite = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComposite);

    FormLayout fFieldsLayout = new FormLayout();
    fFieldsLayout.marginWidth = 3;
    fFieldsLayout.marginHeight = 3;
    wFieldsComposite.setLayout(fFieldsLayout);

    FormData fdDataComp = new FormData();
    fdDataComp.left = new FormAttachment(0, 0);
    fdDataComp.top = new FormAttachment(0, 0);
    fdDataComp.right = new FormAttachment(100, 0);
    fdDataComp.bottom = new FormAttachment(100, 0);
    wFieldsComposite.setLayoutData(fdDataComp);

    int upInsCols = 3;
    int upInsRows = (input.getMappings() != null ? input.getMappings().size() : 1);

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "OraBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "OraBulkLoaderDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "OraBulkLoaderDialog.ColumnInfo.DateMask"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {
              "",
              BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_MASK_LABEL),
              BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_TIME_MASK_LABEL)
            },
            true);
    tableFieldColumns.add(ciReturn[0]);
    wReturn =
        new TableView(
            variables,
            wFieldsComposite,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            lsMod,
            props);

    Button wGetLU = new Button(wFieldsComposite, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.GetFields.Label"));
    wGetLU.addListener(SWT.Selection, e -> getUpdate());
    FormData fdGetLU = new FormData();
    fdGetLU.top = new FormAttachment(0, 0);
    fdGetLU.right = new FormAttachment(100, 0);
    fdGetLU.left = new FormAttachment(wReturn, margin);
    wGetLU.setLayoutData(fdGetLU);

    Button wDoMapping = new Button(wFieldsComposite, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.EditMapping.Label"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetLU, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);
    wDoMapping.addListener(SWT.Selection, e -> generateMappings());

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(0, 0);
    fdReturn.right = new FormAttachment(wDoMapping, -margin);
    fdReturn.bottom = new FormAttachment(100, 0);
    wReturn.setLayoutData(fdReturn);

    wBulkLoaderTab.setControl(wBulkLoaderComposite);
    wFieldsTab.setControl(wFieldsComposite);

    wTabFolder.layout(true, true);
    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    sc.setContent(wContent);
    sc.setExpandHorizontal(true);
    sc.setExpandVertical(true);
    sc.setMinWidth(bounds.width);
    sc.setMinHeight(bounds.height);

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

    SelectionListener fileSelectionListener =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            if (e.widget == null || !(e.widget.getData() instanceof TextVar)) {
              return;
            }
            TextVar wText = (TextVar) e.widget.getData();
            FileObject fileObject = null;
            if (!StringUtil.isEmpty(wText.getText())) {
              try {
                fileObject = HopVfs.getFileObject(variables.resolve(wText.getText()));
              } catch (HopFileException ignored) {
              }
            }
            String filePath =
                BaseDialog.presentFileDialog(
                    false, shell, wText, fileObject, new String[] {"*"}, ALL_FILETYPES, false);
            if (!StringUtil.isEmpty(filePath)) {
              wText.setText(filePath);
            }
          }
        };

    wbSqlldr.addSelectionListener(fileSelectionListener);
    wbControlFile.addSelectionListener(fileSelectionListener);
    wbDataFile.addSelectionListener(fileSelectionListener);
    wbLogFile.addSelectionListener(fileSelectionListener);
    wbBadFile.addSelectionListener(fileSelectionListener);
    wbDiscardFile.addSelectionListener(fileSelectionListener);

    getData();
    wTabFolder.setSelection(0);
    setTableFieldCombo();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciReturn[1].setComboValues(fieldNames);
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
            final String tableName = wTable.getText(),
                connectionName = wConnection.getText(),
                schemaName = wSchema.getText();

            // clear
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            if (!Utils.isEmpty(tableName)) {
              DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
              if (databaseMeta != null) {
                Database db = new Database(loggingObject, variables, databaseMeta);

                try {
                  db.connect();

                  String schemaTable =
                      databaseMeta.getQuotedSchemaTableCombination(
                          variables, schemaName, tableName);
                  IRowMeta rowMeta = db.getTableFields(schemaTable);
                  if (null != rowMeta) {
                    String[] fieldNames = rowMeta.getFieldNames();
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

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Log.GettingKeyInfo"));
    }

    wMaxErrors.setText("" + input.getMaxErrors());
    wCommit.setText("" + input.getCommitSize());
    wBindSize.setText("" + input.getBindSize());
    wReadSize.setText("" + input.getReadSize());

    for (int i = 0; i < input.getMappings().size(); i++) {
      TableItem item = wReturn.table.getItem(i);
      OraBulkLoaderMappingMeta mapping = input.getMappings().get(i);

      if (mapping.getFieldTable() != null) {
        item.setText(1, mapping.getFieldTable());
      }
      if (mapping.getFieldStream() != null) {
        item.setText(2, mapping.getFieldStream());
      }
      String dateMask = mapping.getDateMask();
      if (dateMask != null) {
        if (OraBulkLoaderMeta.DATE_MASK_DATE.equals(dateMask)) {
          item.setText(
              3, BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_MASK_LABEL));
        } else if (OraBulkLoaderMeta.DATE_MASK_DATETIME.equals(dateMask)) {
          item.setText(
              3, BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_TIME_MASK_LABEL));
        } else {
          item.setText(3, "");
        }
      } else {
        item.setText(3, "");
      }
    }

    if (input.getConnection() != null) {
      wConnection.setText(input.getConnection());
    }
    if (input.getSchemaName() != null) {
      wSchema.setText(input.getSchemaName());
    }
    if (input.getTableName() != null) {
      wTable.setText(input.getTableName());
    }
    if (input.getSqlldr() != null) {
      wSqlldr.setText(input.getSqlldr());
    }
    if (input.getControlFile() != null) {
      wControlFile.setText(input.getControlFile());
    }
    if (input.getDataFile() != null) {
      wDataFile.setText(input.getDataFile());
    }
    if (input.getLogFile() != null) {
      wLogFile.setText(input.getLogFile());
    }
    if (input.getBadFile() != null) {
      wBadFile.setText(input.getBadFile());
    }
    if (input.getDiscardFile() != null) {
      wDiscardFile.setText(input.getDiscardFile());
    }
    if (input.getEncoding() != null) {
      wEncoding.setText(input.getEncoding());
    }
    if (input.getCharacterSetName() != null) {
      wCharacterSetName.setText(input.getCharacterSetName());
    }
    if (input.getAltRecordTerm() != null) {
      wAltRecordTerm.setText(input.getAltRecordTerm());
    }
    wDirectPath.setSelection(input.isDirectPath());
    wEraseFiles.setSelection(input.isEraseFiles());
    wFailOnError.setSelection(input.isFailOnError());
    wParallel.setSelection(input.isParallel());
    wFailOnWarning.setSelection(input.isFailOnWarning());

    String method = input.getLoadMethod();
    if (OraBulkLoaderMeta.METHOD_AUTO_END.equals(method)) {
      wLoadMethod.select(0);
    } else if (OraBulkLoaderMeta.METHOD_MANUAL.equals(method)) {
      wLoadMethod.select(1);
    } else if (OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT.equals(method)) {
      wLoadMethod.select(2);
    } else {
      if (log.isDebug()) {
        logDebug("Internal error: load_method set to default 'auto at end'");
      }
      wLoadMethod.select(0);
    }

    String action = input.getLoadAction();
    if (OraBulkLoaderMeta.ACTION_APPEND.equals(action)) {
      wLoadAction.select(0);
    } else if (OraBulkLoaderMeta.ACTION_INSERT.equals(action)) {
      wLoadAction.select(1);
    } else if (OraBulkLoaderMeta.ACTION_REPLACE.equals(action)) {
      wLoadAction.select(2);
    } else if (OraBulkLoaderMeta.ACTION_TRUNCATE.equals(action)) {
      wLoadAction.select(3);
    } else {
      if (log.isDebug()) {
        logDebug("Internal error: load_action set to default 'append'");
      }
      wLoadAction.select(0);
    }

    wReturn.setRowNums();
    wReturn.optWidth(true);
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(OraBulkLoaderMeta meta) {
    meta.setMaxErrors(wMaxErrors.getText());
    meta.setCommitSize(wCommit.getText());
    meta.setBindSize(wBindSize.getText());
    meta.setReadSize(wReadSize.getText());

    int nrfields = wReturn.nrNonEmpty();
    if (log.isDebug()) {
      logDebug(BaseMessages.getString(PKG, "OraBulkLoaderDialog.Log.FoundFields", "" + nrfields));
    }

    List<OraBulkLoaderMappingMeta> mappings = new ArrayList<>();
    for (int i = 0; i < nrfields; i++) {
      OraBulkLoaderMappingMeta mapping = new OraBulkLoaderMappingMeta();

      TableItem item = wReturn.getNonEmpty(i);
      mapping.setFieldTable(item.getText(1));
      mapping.setFieldStream(item.getText(2));

      if (BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_MASK_LABEL)
          .equals(item.getText(3))) {
        mapping.setDateMask(OraBulkLoaderMeta.DATE_MASK_DATE);
      } else if (BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_TIME_MASK_LABEL)
          .equals(item.getText(3))) {
        mapping.setDateMask(OraBulkLoaderMeta.DATE_MASK_DATETIME);
      }

      mappings.add(mapping);
    }

    meta.setMappings(mappings);
    meta.setSchemaName(wSchema.getText());
    meta.setTableName(wTable.getText());
    meta.setConnection(wConnection.getText());
    meta.setSqlldr(wSqlldr.getText());
    meta.setControlFile(wControlFile.getText());
    meta.setDataFile(wDataFile.getText());
    meta.setLogFile(wLogFile.getText());
    meta.setBadFile(wBadFile.getText());
    meta.setDiscardFile(wDiscardFile.getText());
    meta.setEncoding(wEncoding.getText());
    meta.setCharacterSetName(wCharacterSetName.getText());
    meta.setAltRecordTerm(wAltRecordTerm.getText());
    meta.setDirectPath(wDirectPath.getSelection());
    meta.setEraseFiles(wEraseFiles.getSelection());
    meta.setFailOnError(wFailOnError.getSelection());
    meta.setParallel(wParallel.getSelection());
    meta.setFailOnWarning(wFailOnWarning.getSelection());

    /*
     * Set the load method
     */
    String method = wLoadMethod.getText();
    if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.AutoConcLoadMethod.Label")
        .equals(method)) {
      meta.setLoadMethod(OraBulkLoaderMeta.METHOD_AUTO_CONCURRENT);
    } else if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.AutoEndLoadMethod.Label")
        .equals(method)) {
      meta.setLoadMethod(OraBulkLoaderMeta.METHOD_AUTO_END);
    } else if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.ManualLoadMethod.Label")
        .equals(method)) {
      meta.setLoadMethod(OraBulkLoaderMeta.METHOD_MANUAL);
    } else {
      if (log.isDebug()) {
        logDebug(
            "Internal error: load_method set to default 'auto concurrent', value found '"
                + method
                + "'.");
      }
      meta.setLoadMethod(OraBulkLoaderMeta.METHOD_AUTO_END);
    }

    /*
     * Set the load action
     */
    String action = wLoadAction.getText();
    if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.AppendLoadAction.Label").equals(action)) {
      meta.setLoadAction(OraBulkLoaderMeta.ACTION_APPEND);
    } else if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.InsertLoadAction.Label")
        .equals(action)) {
      meta.setLoadAction(OraBulkLoaderMeta.ACTION_INSERT);
    } else if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.ReplaceLoadAction.Label")
        .equals(action)) {
      meta.setLoadAction(OraBulkLoaderMeta.ACTION_REPLACE);
    } else if (BaseMessages.getString(PKG, "OraBulkLoaderDialog.TruncateLoadAction.Label")
        .equals(action)) {
      meta.setLoadAction(OraBulkLoaderMeta.ACTION_TRUNCATE);
    } else {
      if (log.isDebug()) {
        logDebug(
            "Internal error: load_action set to default 'append', value found '" + action + "'.");
      }
      meta.setLoadAction(OraBulkLoaderMeta.ACTION_APPEND);
    }

    transformName = wTransformName.getText();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);

    if (input.getConnection() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
  }

  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      Database database = new Database(loggingObject, variables, databaseMeta);
      try {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "OraBulkLoaderDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG, "OraBulkLoaderDialog.AvailableSchemas.Message", wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "OraBulkLoaderDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "OraBulkLoaderDialog.ErrorGettingSchemas"),
            e);
      } finally {
        database.disconnect();
      }
    }
  }

  private void getTableName() {

    String connectionName = wConnection.getText();
    if (StringUtils.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);

    if (databaseMeta != null) {
      if (log.isDebug()) {
        logDebug(
            BaseMessages.getString(PKG, "OraBulkLoaderDialog.Log.LookingAtConnection")
                + databaseMeta.toString());
      }

      DatabaseExplorerDialog dialog =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      dialog.setSelectedSchemaAndTable(
          variables.resolve(wSchema.getText()), variables.resolve(wTable.getText()));
      if (dialog.open()) {
        wSchema.setText(Const.NVL(dialog.getSchemaName(), ""));
        wTable.setText(Const.NVL(dialog.getTableName(), ""));
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevInfoFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              if (v.getType() == IValueMeta.TYPE_DATE) {
                // The default is date mask.
                tableItem.setText(
                    3, BaseMessages.getString(PKG, CONST_ORA_BULK_LOADER_DIALOG_DATE_MASK_LABEL));
              } else {
                tableItem.setText(3, "");
              }
              return true;
            };

        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      OraBulkLoaderMeta info = new OraBulkLoaderMeta();
      getInfo(info);
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(info.getConnection(), variables);
      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
          new TransformMeta(
              BaseMessages.getString(PKG, "OraBulkLoaderDialog.TransformMeta.Title"), name, info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

      SqlStatement sql =
          info.getSqlStatements(variables, pipelineMeta, transformMeta, prev, metadataProvider);
      if (!sql.hasError()) {
        if (sql.hasSql()) {
          SqlEditor sqledit =
              new SqlEditor(
                  shell, SWT.NONE, variables, databaseMeta, DbCache.getInstance(), sql.getSql());
          sqledit.open();
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
          mb.setMessage(
              BaseMessages.getString(PKG, "OraBulkLoaderDialog.NoSQLNeeds.DialogMessage"));
          mb.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.NoSQLNeeds.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, "OraBulkLoaderDialog.SQLError.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.CouldNotBuildSQL.DialogTitle"),
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.CouldNotBuildSQL.DialogMessage"),
          ke);
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
              PKG, "OraBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "OraBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }
    // refresh data
    input.setConnection(wConnection.getText());
    input.setTableName(variables.resolve(wTable.getText()));
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "OraBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "OraBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    String[] inputNames = new String[sourceFields.size()];
    for (int i = 0; i < sourceFields.size(); i++) {
      IValueMeta value = sourceFields.getValueMeta(i);
      inputNames[i] = value.getName() + "            (" + value.getOrigin() + ")";
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
                    "OraBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "OraBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(PKG, "OraBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;

      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(PKG, "OraBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.OK | SWT.CANCEL);
      boolean goOn = (answer & SWT.OK) != 0;

      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            OraBulkLoaderDialog.this.shell,
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
}
