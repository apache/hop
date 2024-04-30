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

package org.apache.hop.pipeline.transforms.dimensionlookup;

import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLField;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLFields;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DLKey;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.DimensionUpdateType;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.StartDateAlternative;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod.AUTO_INCREMENT;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod.SEQUENCE;
import static org.apache.hop.pipeline.transforms.dimensionlookup.DimensionLookupMeta.TechnicalKeyCreationMethod.TABLE_MAXIMUM;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DimensionLookupDialog extends BaseTransformDialog implements ITransformDialog {
  private static final Class<?> PKG = DimensionLookupMeta.class; // For Translator

  private CTabFolder wTabFolder;

  private CTabItem wFieldsTab;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TextVar wSchema;

  private TextVar wTable;

  private Label wlCommit;
  private Text wCommit;

  private Button wUseCache;

  private Label wlPreloadCache;
  private Button wPreloadCache;

  private Label wlCacheSize;
  private Text wCacheSize;

  private Combo wTk;

  private Label wlTkRename;
  private Text wTkRename;

  private Button wAutoIncrement;

  private Button wTableMax;

  private Button wSeqButton;
  private Text wSeq;

  private Label wlVersion;
  private Combo wVersion;

  private Combo wDateField;

  private Combo wFromDate;

  private Button wUseAltStartDate;
  private Combo wAltStartDate;
  private Combo wAltStartDateField;

  private Label wlMinYear;
  private Text wMinYear;

  private Combo wToDate;

  private Label wlMaxYear;
  private Text wMaxYear;

  private Button wUpdate;

  private TableView wKey;

  private TableView wUpIns;

  private final DimensionLookupMeta input;

  private DatabaseMeta databaseMeta;

  private ColumnInfo[] fieldColumns;

  private ColumnInfo[] keyColumns;

  private final List<String> inputFields = new ArrayList<>();

  private boolean gotPreviousFields = false;

  private boolean gotTableFields = false;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public DimensionLookupDialog(
      Shell parent, IVariables variables, Object in, PipelineMeta tr, String sname) {
    super(
        parent, variables, (BaseTransformMeta<DimensionLookup, DimensionLookupData>) in, tr, sname);
    input = (DimensionLookupMeta) in;
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    databaseMeta = input.getDatabaseMeta();

    shell.setLayout(props.createFormLayout());
    shell.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    Composite mainComposite = shell;
    PropsUi.setLook(mainComposite);

    mainComposite.setLayout(props.createFormLayout());

    // TransformName line
    wlTransformName = new Label(mainComposite, SWT.RIGHT);
    wlTransformName.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.TransformName.Label"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
    wlTransformName.setLayoutData(fdlTransformName);
    wTransformName = new Text(mainComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wTransformName.setText(transformName);
    PropsUi.setLook(wTransformName);
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment(middle, 0);
    fdTransformName.top = new FormAttachment(0, margin);
    fdTransformName.right = new FormAttachment(100, 0);
    wTransformName.setLayoutData(fdTransformName);

    // Update the dimension?
    Label wlUpdate = new Label(mainComposite, SWT.RIGHT);
    wlUpdate.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Update.Label"));
    PropsUi.setLook(wlUpdate);
    FormData fdlUpdate = new FormData();
    fdlUpdate.left = new FormAttachment(0, 0);
    fdlUpdate.right = new FormAttachment(middle, -margin);
    fdlUpdate.top = new FormAttachment(wTransformName, margin);
    wlUpdate.setLayoutData(fdlUpdate);
    wUpdate = new Button(mainComposite, SWT.CHECK);
    PropsUi.setLook(wUpdate);
    FormData fdUpdate = new FormData();
    fdUpdate.left = new FormAttachment(middle, 0);
    fdUpdate.top = new FormAttachment(wlUpdate, 0, SWT.CENTER);
    fdUpdate.right = new FormAttachment(100, 0);
    wUpdate.setLayoutData(fdUpdate);

    // Clicking on update changes the options in the update combo boxes!
    wUpdate.addListener(
        SWT.Selection,
        e -> {
          input.setUpdate(!input.isUpdate());
          setFlags();
        });

    // Connection line
    wConnection = addConnectionLine(mainComposite, wUpdate, input.getDatabaseMeta(), null);
    wConnection.addListener(SWT.FocusOut, e -> setTableFieldCombo());
    wConnection.addListener(
        SWT.Modify,
        e -> {
          // We have new content: change ci connection:
          databaseMeta = wConnection.loadSelectedElement();
          setFlags();
        });

    // Schema line...
    Label wlSchema = new Label(mainComposite, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(mainComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addListener(SWT.Selection, e -> getSchemaNames());

    wSchema = new TextVar(variables, mainComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);
    wSchema.addListener(SWT.Modify, e -> setTableFieldCombo());

    // Table line...
    Label wlTable = new Label(mainComposite, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(mainComposite, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addListener(SWT.Selection, e -> getTableName());

    wTable = new TextVar(variables, mainComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);
    wTable.addListener(SWT.Modify, e -> setTableFieldCombo());

    // Commit size ...
    wlCommit = new Label(mainComposite, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Commit.Label"));
    PropsUi.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment(0, 0);
    fdlCommit.right = new FormAttachment(middle, -margin);
    fdlCommit.top = new FormAttachment(wTable, margin);
    wlCommit.setLayoutData(fdlCommit);
    wCommit = new Text(mainComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommit);
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment(middle, 0);
    fdCommit.top = new FormAttachment(wTable, margin);
    fdCommit.right = new FormAttachment(100, 0);
    wCommit.setLayoutData(fdCommit);

    // Use Cache?
    Label wlUseCache = new Label(mainComposite, SWT.RIGHT);
    wlUseCache.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.UseCache.Label"));
    PropsUi.setLook(wlUseCache);
    FormData fdlUseCache = new FormData();
    fdlUseCache.left = new FormAttachment(0, 0);
    fdlUseCache.right = new FormAttachment(middle, -margin);
    fdlUseCache.top = new FormAttachment(wCommit, margin);
    wlUseCache.setLayoutData(fdlUseCache);
    wUseCache = new Button(mainComposite, SWT.CHECK);
    PropsUi.setLook(wUseCache);
    wUseCache.addListener(SWT.Selection, e -> setFlags());
    FormData fdUseCache = new FormData();
    fdUseCache.left = new FormAttachment(middle, 0);
    fdUseCache.top = new FormAttachment(wlUseCache, 0, SWT.CENTER);
    fdUseCache.right = new FormAttachment(100, 0);
    wUseCache.setLayoutData(fdUseCache);

    // Preload cache?
    wlPreloadCache = new Label(mainComposite, SWT.RIGHT);
    wlPreloadCache.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.PreloadCache.Label"));
    PropsUi.setLook(wlPreloadCache);
    FormData fdlPreloadCache = new FormData();
    fdlPreloadCache.left = new FormAttachment(0, 0);
    fdlPreloadCache.right = new FormAttachment(middle, -margin);
    fdlPreloadCache.top = new FormAttachment(wUseCache, margin);
    wlPreloadCache.setLayoutData(fdlPreloadCache);
    wPreloadCache = new Button(mainComposite, SWT.CHECK);
    PropsUi.setLook(wPreloadCache);
    wPreloadCache.addListener(SWT.Selection, e -> setFlags());
    FormData fdPreloadCache = new FormData();
    fdPreloadCache.left = new FormAttachment(middle, 0);
    fdPreloadCache.top = new FormAttachment(wlPreloadCache, 0, SWT.CENTER);
    fdPreloadCache.right = new FormAttachment(100, 0);
    wPreloadCache.setLayoutData(fdPreloadCache);

    // Cache size ...
    wlCacheSize = new Label(mainComposite, SWT.RIGHT);
    wlCacheSize.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.CacheSize.Label"));
    PropsUi.setLook(wlCacheSize);
    FormData fdlCacheSize = new FormData();
    fdlCacheSize.left = new FormAttachment(0, 0);
    fdlCacheSize.right = new FormAttachment(middle, -margin);
    fdlCacheSize.top = new FormAttachment(wPreloadCache, margin);
    wlCacheSize.setLayoutData(fdlCacheSize);
    wCacheSize = new Text(mainComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCacheSize);
    FormData fdCacheSize = new FormData();
    fdCacheSize.left = new FormAttachment(middle, 0);
    fdCacheSize.top = new FormAttachment(wPreloadCache, margin);
    fdCacheSize.right = new FormAttachment(100, 0);
    wCacheSize.setLayoutData(fdCacheSize);

    // THE BOTTOM BUTTONS
    wOk = new Button(mainComposite, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    Button wGet = new Button(mainComposite, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.GetFields.Button"));
    wGet.addListener(SWT.Selection, e -> get());
    wCreate = new Button(mainComposite, SWT.PUSH);
    wCreate.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.SQL.Button"));
    wCreate.addListener(SWT.Selection, e -> create());
    wCancel = new Button(mainComposite, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    wCancel.addListener(SWT.Selection, e -> cancel());
    setButtonPositions(new Button[] {wOk, wGet, wCreate, wCancel}, margin, null);

    wTabFolder = new CTabFolder(mainComposite, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addKeyTab(margin);
    addFieldsTab(margin);
    addTechnicalKeyTab(margin, middle);
    addVersioningTab(margin, middle);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.top = new FormAttachment(wCacheSize, margin);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    mainComposite.setLayoutData(fdComp);

    mainComposite.pack();

    setTableMax();
    setSequence();
    setAutoIncrementUse();

    wTabFolder.setSelection(0);

    getData();
    setTableFieldCombo();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  public void addKeyTab(int margin) {
    // ////////////////////////
    // START OF KEY TAB ///
    // /
    CTabItem wKeyTab = new CTabItem(wTabFolder, SWT.NONE);
    wKeyTab.setFont(GuiResource.getInstance().getFontDefault());
    wKeyTab.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.KeyTab.CTabItem"));

    Composite wKeyComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wKeyComp);
    wKeyComp.setLayout(props.createFormLayout());

    //
    // The Lookup fields: usually the key
    //
    Label wlKey = new Label(wKeyComp, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.KeyFields.Label"));
    PropsUi.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(0, margin);
    fdlKey.right = new FormAttachment(100, 0);
    wlKey.setLayoutData(fdlKey);

    int nrKeyRows = input.getFields().getKeys().size();

    keyColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.DimensionField"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.FieldInStream"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {""},
              false)
        };
    wKey =
        new TableView(
            variables,
            wKeyComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            keyColumns,
            nrKeyRows,
            null,
            props);
    tableFieldColumns.add(keyColumns[0]);

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(100, 0);
    wKey.setLayoutData(fdKey);

    FormData fdKeyComp = new FormData();
    fdKeyComp.left = new FormAttachment(0, 0);
    fdKeyComp.top = new FormAttachment(0, 0);
    fdKeyComp.right = new FormAttachment(100, 0);
    fdKeyComp.bottom = new FormAttachment(100, 0);
    wKeyComp.setLayoutData(fdKeyComp);

    wKeyComp.layout();
    wKeyTab.setControl(wKeyComp);
  }

  public void addTechnicalKeyTab(int margin, int middle) {
    CTabItem wTechnicalKeyTab = new CTabItem(wTabFolder, SWT.NONE);
    wTechnicalKeyTab.setFont(GuiResource.getInstance().getFontDefault());
    wTechnicalKeyTab.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.TechnicalKeyTab.CTabItem"));

    Composite wTechnicalKeyComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wTechnicalKeyComp);
    wTechnicalKeyComp.setLayout(props.createFormLayout());

    // Technical key field:
    Label wlTk = new Label(wTechnicalKeyComp, SWT.RIGHT);
    wlTk.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.TechnicalKeyField.Label"));
    PropsUi.setLook(wlTk);
    FormData fdlTk = new FormData();
    fdlTk.left = new FormAttachment(0, margin);
    fdlTk.top = new FormAttachment(0, 3 * margin);
    wlTk.setLayoutData(fdlTk);

    wTk = new Combo(wTechnicalKeyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTk);
    FormData fdTk = new FormData();
    fdTk.left = new FormAttachment(wlTk, margin);
    fdTk.top = new FormAttachment(wlTk, 0, SWT.CENTER);
    fdTk.right = new FormAttachment(30 + middle / 2, 0);
    wTk.setLayoutData(fdTk);
    wTk.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFieldsFromTable();
          shell.setCursor(null);
          busy.dispose();
        });

    wlTkRename = new Label(wTechnicalKeyComp, SWT.RIGHT);
    wlTkRename.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.NewName.Label"));
    PropsUi.setLook(wlTkRename);
    FormData fdlTkRename = new FormData();
    fdlTkRename.left = new FormAttachment(wTk, margin);
    fdlTkRename.top = new FormAttachment(wlTk, 0, SWT.CENTER);
    wlTkRename.setLayoutData(fdlTkRename);

    wTkRename = new Text(wTechnicalKeyComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTkRename);
    FormData fdTkRename = new FormData();
    fdTkRename.left = new FormAttachment(wlTkRename, margin);
    fdTkRename.top = new FormAttachment(wlTk, 0, SWT.CENTER);
    fdTkRename.right = new FormAttachment(100, -margin);
    wTkRename.setLayoutData(fdTkRename);

    Group gTechGroup = new Group(wTechnicalKeyComp, SWT.SHADOW_ETCHED_IN);
    gTechGroup.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.TechGroup.Label"));

    gTechGroup.setLayout(props.createFormLayout());
    PropsUi.setLook(gTechGroup);
    FormData fdTechGroup = new FormData();
    fdTechGroup.top = new FormAttachment(wTkRename, margin);
    fdTechGroup.left = new FormAttachment(0, margin);
    fdTechGroup.right = new FormAttachment(100, -margin);
    gTechGroup.setBackground(shell.getBackground()); // the default looks ugly
    gTechGroup.setLayoutData(fdTechGroup);

    // Use maximum of table + 1
    wTableMax = new Button(gTechGroup, SWT.RADIO);
    PropsUi.setLook(wTableMax);
    wTableMax.setSelection(false);
    FormData fdTableMax = new FormData();
    fdTableMax.left = new FormAttachment(0, 0);
    fdTableMax.top = new FormAttachment(wTkRename, margin);
    wTableMax.setLayoutData(fdTableMax);
    wTableMax.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.TableMaximum.Label"));
    wTableMax.setToolTipText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.TableMaximum.Tooltip", Const.CR));

    // Sequence Check Button
    wSeqButton = new Button(gTechGroup, SWT.RADIO);
    PropsUi.setLook(wSeqButton);
    wSeqButton.setSelection(false);
    FormData fdSeqButton = new FormData();
    fdSeqButton.left = new FormAttachment(0, 0);
    fdSeqButton.top = new FormAttachment(wTableMax, margin);
    wSeqButton.setLayoutData(fdSeqButton);
    wSeqButton.setToolTipText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.Sequence.Tooltip", Const.CR));
    wSeqButton.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Sequence.Label"));

    wSeq = new Text(gTechGroup, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSeq);
    FormData fdSeq = new FormData();
    fdSeq.left = new FormAttachment(wSeqButton, margin);
    fdSeq.top = new FormAttachment(wSeqButton, 0, SWT.CENTER);
    fdSeq.right = new FormAttachment(100, 0);
    wSeq.setLayoutData(fdSeq);
    wSeq.addListener(
        SWT.FocusIn,
        e -> {
          input.getFields().getReturns().setCreationMethod(SEQUENCE);
          wSeqButton.setSelection(true);
          wAutoIncrement.setSelection(false);
          wTableMax.setSelection(false);
        });

    // Use an autoincrement field?
    wAutoIncrement = new Button(gTechGroup, SWT.RADIO);
    PropsUi.setLook(wAutoIncrement);
    wAutoIncrement.setSelection(false);
    FormData fdAutoIncrement = new FormData();
    fdAutoIncrement.left = new FormAttachment(0, 0);
    fdAutoIncrement.top = new FormAttachment(wSeq, margin);
    wAutoIncrement.setLayoutData(fdAutoIncrement);
    wAutoIncrement.setToolTipText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.AutoIncrementButton.Tooltip", Const.CR));
    wAutoIncrement.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.AutoIncrement.Label"));

    FormData fdTechnicalKeyComp = new FormData();
    fdTechnicalKeyComp.left = new FormAttachment(0, 0);
    fdTechnicalKeyComp.top = new FormAttachment(0, 0);
    fdTechnicalKeyComp.right = new FormAttachment(100, 0);
    fdTechnicalKeyComp.bottom = new FormAttachment(100, 0);
    wTechnicalKeyComp.setLayoutData(fdTechnicalKeyComp);

    wTechnicalKeyComp.layout();
    wTechnicalKeyTab.setControl(wTechnicalKeyComp);
  }

  public void addFieldsTab(int margin) {

    wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.FieldsTab.CTabItem.Title"));

    Composite wFieldsComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wFieldsComp);

    wFieldsComp.setLayout(props.createFormLayout());

    // THE UPDATE/INSERT TABLE
    Label wlUpIns = new Label(wFieldsComp, SWT.NONE);
    wlUpIns.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.UpdateOrInsertFields.Label"));
    PropsUi.setLook(wlUpIns);
    FormData fdlUpIns = new FormData();
    fdlUpIns.left = new FormAttachment(0, 0);
    fdlUpIns.top = new FormAttachment(0, margin);
    wlUpIns.setLayoutData(fdlUpIns);

    int upInsCols = 3;
    int upInsRows = input.getFields().getFields().size();

    fieldColumns = new ColumnInfo[upInsCols];
    fieldColumns[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.DimensionField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    fieldColumns[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    fieldColumns[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.TypeOfDimensionUpdate"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            input.isUpdate()
                ? DimensionUpdateType.getDescriptions()
                : ValueMetaFactory.getValueMetaNames());
    tableFieldColumns.add(fieldColumns[0]);
    wUpIns =
        new TableView(
            variables,
            wFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            fieldColumns,
            upInsRows,
            null,
            props);

    FormData fdUpIns = new FormData();
    fdUpIns.left = new FormAttachment(0, 0);
    fdUpIns.top = new FormAttachment(wlUpIns, margin);
    fdUpIns.right = new FormAttachment(100, 0);
    fdUpIns.bottom = new FormAttachment(100, 0);
    wUpIns.setLayoutData(fdUpIns);

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

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wFieldsComp.setLayoutData(fdFieldsComp);

    wFieldsComp.layout();
    wFieldsTab.setControl(wFieldsComp);
  }

  public void addVersioningTab(int margin, int middle) {

    CTabItem wVersioningTab = new CTabItem(wTabFolder, SWT.NONE);
    wVersioningTab.setFont(GuiResource.getInstance().getFontDefault());
    wVersioningTab.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.VersioningTab.CTabItem"));

    Composite wVersioningComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wVersioningComp);

    wVersioningComp.setLayout(props.createFormLayout());

    // Version key field:
    wlVersion = new Label(wVersioningComp, SWT.RIGHT);
    wlVersion.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.Version.Label"));
    PropsUi.setLook(wlVersion);
    FormData fdlVersion = new FormData();
    fdlVersion.left = new FormAttachment(0, 0);
    fdlVersion.right = new FormAttachment(middle, 0);
    fdlVersion.top = new FormAttachment(0, 2 * margin);
    wlVersion.setLayoutData(fdlVersion);
    wVersion = new Combo(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wVersion);
    FormData fdVersion = new FormData();
    fdVersion.left = new FormAttachment(middle, 0);
    fdVersion.top = new FormAttachment(wlVersion, 0, SWT.CENTER);
    fdVersion.right = new FormAttachment(100, 0);
    wVersion.setLayoutData(fdVersion);
    wVersion.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFieldsFromTable();
          shell.setCursor(null);
          busy.dispose();
        });
    Control lastControl = wVersion;

    // DateField line
    Label wlDateField = new Label(wVersioningComp, SWT.RIGHT);
    wlDateField.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.DateField.Label"));
    PropsUi.setLook(wlDateField);
    FormData fdlDateField = new FormData();
    fdlDateField.left = new FormAttachment(0, 0);
    fdlDateField.right = new FormAttachment(middle, 0);
    fdlDateField.top = new FormAttachment(lastControl, margin);
    wlDateField.setLayoutData(fdlDateField);
    wDateField = new Combo(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wDateField);
    FormData fdDateField = new FormData();
    fdDateField.left = new FormAttachment(middle, 0);
    fdDateField.top = new FormAttachment(wlDateField, 0, SWT.CENTER);
    fdDateField.right = new FormAttachment(100, 0);
    wDateField.setLayoutData(fdDateField);
    wDateField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFields();
          shell.setCursor(null);
          busy.dispose();
        });
    lastControl = wDateField;

    // FromDate line
    //
    // 0 [wlFromDate] middle [wFromDate] (100-middle)/3 [wlMinYear]
    // 2*(100-middle)/3 [wMinYear] 100%
    //
    Label wlFromDate = new Label(wVersioningComp, SWT.RIGHT);
    wlFromDate.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.FromDate.Label"));
    PropsUi.setLook(wlFromDate);
    FormData fdlFromDate = new FormData();
    fdlFromDate.left = new FormAttachment(0, 0);
    fdlFromDate.right = new FormAttachment(middle, 0);
    fdlFromDate.top = new FormAttachment(lastControl, margin);
    wlFromDate.setLayoutData(fdlFromDate);
    wFromDate = new Combo(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wFromDate);
    FormData fdFromDate = new FormData();
    fdFromDate.left = new FormAttachment(middle, 0);
    fdFromDate.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
    fdFromDate.top = new FormAttachment(wlFromDate, 0, SWT.CENTER);
    wFromDate.setLayoutData(fdFromDate);
    wFromDate.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFieldsFromTable();
          shell.setCursor(null);
          busy.dispose();
        });

    // MinYear line
    wlMinYear = new Label(wVersioningComp, SWT.RIGHT);
    wlMinYear.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.MinYear.Label"));
    PropsUi.setLook(wlMinYear);
    FormData fdlMinYear = new FormData();
    fdlMinYear.left = new FormAttachment(wFromDate, margin);
    fdlMinYear.right = new FormAttachment(middle + 2 * (100 - middle) / 3, -margin);
    fdlMinYear.top = new FormAttachment(lastControl, margin);
    wlMinYear.setLayoutData(fdlMinYear);
    wMinYear = new Text(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMinYear);
    FormData fdMinYear = new FormData();
    fdMinYear.left = new FormAttachment(wlMinYear, margin);
    fdMinYear.right = new FormAttachment(100, 0);
    fdMinYear.top = new FormAttachment(wlMinYear, 0, SWT.CENTER);
    wMinYear.setLayoutData(fdMinYear);
    wMinYear.setToolTipText(BaseMessages.getString(PKG, "DimensionLookupDialog.MinYear.ToolTip"));
    lastControl = wFromDate;

    // Add a line with an option to specify an alternative start date...
    //
    Label wlUseAltStartDate = new Label(wVersioningComp, SWT.RIGHT);
    wlUseAltStartDate.setText(
        BaseMessages.getString(PKG, "DimensionLookupDialog.UseAlternativeStartDate.Label"));
    PropsUi.setLook(wlUseAltStartDate);
    FormData fdlUseAltStartDate = new FormData();
    fdlUseAltStartDate.left = new FormAttachment(0, 0);
    fdlUseAltStartDate.right = new FormAttachment(middle, -margin);
    fdlUseAltStartDate.top = new FormAttachment(lastControl, margin);
    wlUseAltStartDate.setLayoutData(fdlUseAltStartDate);

    wUseAltStartDate = new Button(wVersioningComp, SWT.CHECK);
    wUseAltStartDate.setText(" ");
    PropsUi.setLook(wUseAltStartDate);
    wUseAltStartDate.setToolTipText(
        BaseMessages.getString(
            PKG, "DimensionLookupDialog.UseAlternativeStartDate.Tooltip", Const.CR));
    FormData fdUseAltStartDate = new FormData();
    fdUseAltStartDate.left = new FormAttachment(middle, 0);
    fdUseAltStartDate.top = new FormAttachment(wlUseAltStartDate, 0, SWT.CENTER);
    wUseAltStartDate.setLayoutData(fdUseAltStartDate);
    wUseAltStartDate.addListener(SWT.Selection, e -> setFlags());

    // The alternative start date choices
    //
    wAltStartDate = new Combo(wVersioningComp, SWT.BORDER);
    PropsUi.setLook(wAltStartDate);
    // All options except for "No alternative"...
    wAltStartDate.setItems(StartDateAlternative.getDescriptions());
    wAltStartDate.setText(
        BaseMessages.getString(
            PKG, "DimensionLookupDialog.AlternativeStartDate.SelectItemDefault"));
    wAltStartDate.setToolTipText(
        BaseMessages.getString(
            PKG, "DimensionLookupDialog.AlternativeStartDate.Tooltip", Const.CR));
    FormData fdAltStartDate = new FormData();
    fdAltStartDate.left = new FormAttachment(wUseAltStartDate, margin);
    fdAltStartDate.right =
        new FormAttachment(wUseAltStartDate, (int) (200 * props.getZoomFactor()));
    fdAltStartDate.top = new FormAttachment(wlUseAltStartDate, 0, SWT.CENTER);
    wAltStartDate.setLayoutData(fdAltStartDate);
    wAltStartDate.addModifyListener(e -> setFlags());
    // Alternative start date field/argument
    //
    wAltStartDateField = new Combo(wVersioningComp, SWT.SINGLE | SWT.BORDER);
    PropsUi.setLook(wAltStartDateField);
    wAltStartDateField.setToolTipText(
        BaseMessages.getString(
            PKG, "DimensionLookupDialog.AlternativeStartDateField.Tooltip", Const.CR));
    FormData fdAltStartDateField = new FormData();
    fdAltStartDateField.left = new FormAttachment(wAltStartDate, 2 * margin);
    fdAltStartDateField.right = new FormAttachment(100, 0);
    fdAltStartDateField.top = new FormAttachment(wlUseAltStartDate, 0, SWT.CENTER);
    wAltStartDateField.setLayoutData(fdAltStartDateField);
    wAltStartDateField.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFieldsFromTable();
          shell.setCursor(null);
          busy.dispose();
        });
    lastControl = wAltStartDate;

    // ToDate line
    Label wlToDate = new Label(wVersioningComp, SWT.RIGHT);
    wlToDate.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.ToDate.Label"));
    PropsUi.setLook(wlToDate);
    FormData fdlToDate = new FormData();
    fdlToDate.left = new FormAttachment(0, 0);
    fdlToDate.right = new FormAttachment(middle, 0);
    fdlToDate.top = new FormAttachment(lastControl, margin);
    wlToDate.setLayoutData(fdlToDate);
    wToDate = new Combo(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wToDate);
    FormData fdToDate = new FormData();
    fdToDate.left = new FormAttachment(middle, 0);
    fdToDate.right = new FormAttachment(middle + (100 - middle) / 3, -margin);
    fdToDate.top = new FormAttachment(wlToDate, 0, SWT.CENTER);
    wToDate.setLayoutData(fdToDate);
    wToDate.addListener(
        SWT.FocusIn,
        e -> {
          Cursor busy = new Cursor(shell.getDisplay(), SWT.CURSOR_WAIT);
          shell.setCursor(busy);
          getFieldsFromTable();
          shell.setCursor(null);
          busy.dispose();
        });

    // MaxYear line
    wlMaxYear = new Label(wVersioningComp, SWT.RIGHT);
    wlMaxYear.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.MaxYear.Label"));
    PropsUi.setLook(wlMaxYear);
    FormData fdlMaxYear = new FormData();
    fdlMaxYear.left = new FormAttachment(wToDate, margin);
    fdlMaxYear.right = new FormAttachment(middle + 2 * (100 - middle) / 3, -margin);
    fdlMaxYear.top = new FormAttachment(wToDate, 0, SWT.CENTER);
    wlMaxYear.setLayoutData(fdlMaxYear);
    wMaxYear = new Text(wVersioningComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wMaxYear);
    FormData fdMaxYear = new FormData();
    fdMaxYear.left = new FormAttachment(wlMaxYear, margin);
    fdMaxYear.right = new FormAttachment(100, 0);
    fdMaxYear.top = new FormAttachment(wlMaxYear, 0, SWT.CENTER);
    wMaxYear.setLayoutData(fdMaxYear);
    wMaxYear.setToolTipText(BaseMessages.getString(PKG, "DimensionLookupDialog.MaxYear.ToolTip"));

    FormData fdFieldsComp = new FormData();
    fdFieldsComp.left = new FormAttachment(0, 0);
    fdFieldsComp.top = new FormAttachment(0, 0);
    fdFieldsComp.right = new FormAttachment(100, 0);
    fdFieldsComp.bottom = new FormAttachment(100, 0);
    wVersioningComp.setLayoutData(fdFieldsComp);

    wVersioningComp.layout();
    wVersioningTab.setControl(wVersioningComp);
  }

  public void setFlags() {
    ColumnInfo colinf =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DimensionLookupDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            input.isUpdate()
                ? DimensionUpdateType.getDescriptions()
                : ValueMetaFactory.getValueMetaNames());
    wUpIns.setColumnInfo(2, colinf);

    if (input.isUpdate()) {
      wUpIns.setColumnText(
          2,
          BaseMessages.getString(
              PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnText.SteamFieldToCompare"));
      wUpIns.setColumnText(
          3,
          BaseMessages.getString(
              PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnTextTypeOfDimensionUpdate"));
      wUpIns.setColumnToolTip(
          2,
          BaseMessages.getString(PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnToolTip")
              + Const.CR
              + "Punch Through: Kimball Type I"
              + Const.CR
              + "Update: Correct error in last version");
    } else {
      wUpIns.setColumnText(
          2,
          BaseMessages.getString(
              PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnText.NewNameOfOutputField"));
      wUpIns.setColumnText(
          3,
          BaseMessages.getString(
              PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnText.TypeOfReturnField"));
      wUpIns.setColumnToolTip(
          2,
          BaseMessages.getString(PKG, "DimensionLookupDialog.UpdateOrInsertFields.ColumnToolTip2"));
    }
    wUpIns.optWidth(true);

    // In case of lookup: disable commitsize, etc.
    boolean update = wUpdate.getSelection();
    wlCommit.setEnabled(update);
    wCommit.setEnabled(update);
    wlMinYear.setEnabled(update);
    wMinYear.setEnabled(update);
    wlMaxYear.setEnabled(update);
    wMaxYear.setEnabled(update);
    wlMinYear.setEnabled(update);
    wMinYear.setEnabled(update);
    wlVersion.setEnabled(update);
    wVersion.setEnabled(update);
    wlTkRename.setEnabled(!update);
    wTkRename.setEnabled(!update);

    wCreate.setEnabled(update);

    // Set the technical creation key fields correct... then disable
    // depending on update or not. Then reset if we're updating. It makes
    // sure that the disabled options because of database restrictions
    // will always be properly grayed out.
    setAutoIncrementUse();
    setSequence();
    setTableMax();

    // Surprisingly, we can't disable these fields as they influence the
    // calculation of the "Unknown" key
    // If we have a MySQL database with Auto-increment for example, the
    // "unknown" is 1.
    // If we have a MySQL database with Table-max the "unknown" is 0.
    //

    if (update) {
      setAutoIncrementUse();
      setSequence();
      setTableMax();
    }

    // The alternative start date
    //
    wAltStartDate.setEnabled(wUseAltStartDate.getSelection());
    StartDateAlternative alternative =
        StartDateAlternative.lookupWithDescription(wAltStartDate.getText());
    wAltStartDateField.setEnabled(alternative == StartDateAlternative.COLUMN_VALUE);

    // Caching...
    //
    wlPreloadCache.setEnabled(wUseCache.getSelection() && !wUpdate.getSelection());
    wPreloadCache.setEnabled(wUseCache.getSelection() && !wUpdate.getSelection());

    wlCacheSize.setEnabled(wUseCache.getSelection() && !wPreloadCache.getSelection());
    wCacheSize.setEnabled(wUseCache.getSelection() && !wPreloadCache.getSelection());
  }

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    keyColumns[1].setComboValues(fieldNames);
    fieldColumns[1].setComboValues(fieldNames);
  }

  public void setAutoIncrementUse() {
    boolean enable =
        (databaseMeta == null)
            || (databaseMeta.supportsAutoinc() && databaseMeta.supportsAutoGeneratedKeys());

    wAutoIncrement.setEnabled(enable);
    if (!enable && wAutoIncrement.getSelection()) {
      wAutoIncrement.setSelection(false);
      wSeqButton.setSelection(false);
      wTableMax.setSelection(true);
    }
  }

  public void setTableMax() {
    wTableMax.setEnabled(true);
  }

  public void setSequence() {
    boolean seq = (databaseMeta == null) || databaseMeta.supportsSequences();
    wSeq.setEnabled(seq);
    wSeqButton.setEnabled(seq);
    if (!seq && wSeqButton.getSelection()) {
      wAutoIncrement.setSelection(false);
      wSeqButton.setSelection(false);
      wTableMax.setSelection(true);
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    DLFields f = input.getFields();

    for (int i = 0; i < input.getFields().getKeys().size(); i++) {
      DLKey key = input.getFields().getKeys().get(i);
      TableItem item = wKey.table.getItem(i);
      item.setText(1, Const.NVL(key.getLookup(), ""));
      item.setText(2, Const.NVL(key.getName(), ""));
    }

    for (int i = 0; i < input.getFields().getFields().size(); i++) {
      DLField field = input.getFields().getFields().get(i);
      TableItem item = wUpIns.table.getItem(i);
      item.setText(1, Const.NVL(field.getLookup(), ""));
      item.setText(2, Const.NVL(field.getName(), ""));
      if (input.isUpdate()) {
        // String -> Type -> String
        DimensionUpdateType updateType = field.getUpdateType();
        item.setText(3, updateType == null ? "" : updateType.getDescription());
      } else {
        item.setText(3, Const.NVL(field.getReturnType(), ""));
      }
    }

    wUpdate.setSelection(input.isUpdate());

    wSchema.setText(Const.NVL(input.getSchemaName(), ""));
    wTable.setText(Const.NVL(input.getTableName(), ""));
    wTk.setText(Const.NVL(f.getReturns().getKeyField(), ""));
    wTkRename.setText(Const.NVL(f.getReturns().getKeyRename(), ""));

    wVersion.setText(Const.NVL(f.getReturns().getVersionField(), ""));

    wSeq.setText(Const.NVL(input.getSequenceName(), ""));
    if (input.getDatabaseMeta() != null) {
      wConnection.setText(input.getDatabaseMeta().getName());
    }
    wDateField.setText(Const.NVL(f.getDate().getName(), ""));
    wFromDate.setText(Const.NVL(f.getDate().getFrom(), ""));
    wToDate.setText(Const.NVL(f.getDate().getTo(), ""));

    TechnicalKeyCreationMethod creationMethod = f.getReturns().getCreationMethod();
    if (creationMethod == AUTO_INCREMENT) {
      wAutoIncrement.setSelection(true);
      wSeqButton.setSelection(false);
      wTableMax.setSelection(false);
    } else if (creationMethod == SEQUENCE) {
      wSeqButton.setSelection(true);
      wAutoIncrement.setSelection(false);
      wTableMax.setSelection(false);
    } else { // the rest
      wTableMax.setSelection(true);
      wAutoIncrement.setSelection(false);
      wSeqButton.setSelection(false);
      f.getReturns().setCreationMethod(TABLE_MAXIMUM);
    }
    wSeq.setText(Const.NVL(input.getSequenceName(), ""));

    wCommit.setText("" + input.getCommitSize());

    wUseCache.setSelection(input.getCacheSize() >= 0);
    wPreloadCache.setSelection(input.isPreloadingCache());
    wCacheSize.setText("" + input.getCacheSize());

    wMinYear.setText("" + input.getMinYear());
    wMaxYear.setText("" + input.getMaxYear());

    wUpIns.optimizeTableView();
    wKey.optimizeTableView();

    databaseMeta = wConnection.loadSelectedElement();

    // The alternative start date...
    //
    wUseAltStartDate.setSelection(input.isUsingStartDateAlternative());
    if (input.isUsingStartDateAlternative()) {
      StartDateAlternative alternative = input.getStartDateAlternative();
      String description = alternative == null ? "" : alternative.getDescription();
      wAltStartDate.setText(description);
    }
    wAltStartDateField.setText(Const.NVL(input.getStartDateFieldName(), ""));

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

    transformName = wTransformName.getText(); // return value

    if (input.getDatabaseMeta() == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DimensionLookupDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "DimensionLookupDialog.InvalidConnection.DialogTitle"));
      mb.open();
      return;
    }

    input.setChanged(true);

    dispose();
  }

  private void getInfo(DimensionLookupMeta in) {
    in.setUpdate(wUpdate.getSelection());

    DLFields f = in.getFields();

    f.getKeys().clear();
    for (TableItem item : wKey.getNonEmptyItems()) {
      DLKey key = new DLKey();
      key.setLookup(item.getText(1));
      key.setName(item.getText(2));
      f.getKeys().add(key);
    }
    logDebug(
        BaseMessages.getString(
            PKG, "DimensionLookupDialog.Log.FoundKeys", String.valueOf(f.getKeys().size())));

    f.getFields().clear();
    for (TableItem item : wUpIns.getNonEmptyItems()) {
      DLField field = new DLField();
      field.setLookup(item.getText(1));
      field.setName(item.getText(2));
      if (in.isUpdate()) {
        DimensionUpdateType updateType = DimensionUpdateType.lookupDescription(item.getText(3));
        if (updateType != null) {
          field.setUpdate(updateType.getCode());
        }
      } else {
        field.setReturnType(item.getText(3));
      }

      f.getFields().add(field);
    }
    if (log.isDebug()) {
      logDebug(
          BaseMessages.getString(PKG, "DimensionLookupDialog.Log.FoundFields", f.getKeys().size()));
    }

    in.setSchemaName(wSchema.getText());
    in.setTableName(wTable.getText());
    in.setSequenceName(wSeq.getText());
    f.getReturns().setKeyField(wTk.getText());
    f.getReturns().setKeyRename(wTkRename.getText());
    if (wAutoIncrement.getSelection()) {
      f.getReturns().setCreationMethod(AUTO_INCREMENT);
    } else if (wSeqButton.getSelection()) {
      f.getReturns().setCreationMethod(SEQUENCE);
    } else { // all the rest
      f.getReturns().setCreationMethod(TABLE_MAXIMUM);
    }

    f.getReturns().setVersionField(wVersion.getText());
    in.setDatabaseMeta(wConnection.loadSelectedElement());
    f.getDate().setName(wDateField.getText());
    f.getDate().setFrom(wFromDate.getText());
    f.getDate().setTo(wToDate.getText());

    in.setCommitSize(Const.toInt(wCommit.getText(), 0));

    if (wUseCache.getSelection()) {
      in.setCacheSize(Const.toInt(wCacheSize.getText(), -1));
    } else {
      in.setCacheSize(-1);
    }
    in.setPreloadingCache(wPreloadCache.getSelection());
    if (wPreloadCache.getSelection()) {
      in.setCacheSize(0);
    }

    in.setMinYear(Const.toInt(wMinYear.getText(), Const.MIN_YEAR));
    in.setMaxYear(Const.toInt(wMaxYear.getText(), Const.MAX_YEAR));

    in.setUsingStartDateAlternative(wUseAltStartDate.getSelection());
    in.setStartDateAlternative(StartDateAlternative.lookupWithDescription(wAltStartDate.getText()));
    in.setStartDateFieldName(wAltStartDateField.getText());
  }

  private void getTableName() {
    final DatabaseMeta dbMeta = wConnection.loadSelectedElement();
    if (dbMeta == null) {
      return;
    }
    logDebug(
        BaseMessages.getString(PKG, "DimensionLookupDialog.Log.LookingAtConnection")
            + dbMeta.getName());

    DatabaseExplorerDialog std =
        new DatabaseExplorerDialog(shell, SWT.NONE, variables, dbMeta, pipelineMeta.getDatabases());
    std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
    if (std.open()) {
      wSchema.setText(Const.NVL(std.getSchemaName(), ""));
      wTable.setText(Const.NVL(std.getTableName(), ""));
      setTableFieldCombo();
    }
  }

  private void get() {
    if (wTabFolder.getSelection() == wFieldsTab) {
      if (input.isUpdate()) {
        getUpdate();
      } else {
        getLookup();
      }
    } else {
      getKeys();
    }
  }

  /**
   * Get the fields from the previous transform and use them as "update fields". Only get the the
   * fields which are not yet in use as key, or in the field table. Also ignore technical key,
   * version, fromdate, todate.
   */
  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r,
            wUpIns,
            2,
            new int[] {1, 2},
            new int[] {},
            -1,
            -1,
            (tableItem, v) -> {
              tableItem.setText(
                  3, BaseMessages.getString(PKG, "DimensionLookupDialog.TableItem.Insert.Label"));

              int idx = wKey.indexOfString(v.getName(), 2);
              return idx < 0
                  && !v.getName().equalsIgnoreCase(wTk.getText())
                  && !v.getName().equalsIgnoreCase(wVersion.getText())
                  && !v.getName().equalsIgnoreCase(wFromDate.getText())
                  && !v.getName().equalsIgnoreCase(wToDate.getText());
            });
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DimensionLookupDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "DimensionLookupDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Set table "key field", "dimension field" and "technical key" drop downs
  private void setTableFieldCombo() {

    Runnable fieldLoader =
        () -> {
          final String tableName = variables.resolve(wTable.getText());
          final String schemaName = variables.resolve(wSchema.getText());
          final DatabaseMeta dbMeta = wConnection.loadSelectedElement();

          // Without a database or a table name we can't do very much
          //
          if (dbMeta == null || StringUtils.isEmpty(tableName)) {
            return;
          }

          // clear the column combo values.
          //
          for (ColumnInfo colInfo : tableFieldColumns) {
            colInfo.setComboValues(new String[] {});
          }

          // Ensure other table field dropdowns are refreshed fields when they
          // next get focus
          //
          gotTableFields = false;

          try (Database db = new Database(loggingObject, variables, dbMeta)) {
            db.connect();

            IRowMeta rowMeta = db.getTableFieldsMeta(schemaName, tableName);
            if (rowMeta == null) {
              return;
            }
            String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(fieldNames);
            }

            String tk = wTk.getText();
            wTk.setItems(fieldNames);
            wTk.setText(Const.NVL(tk, ""));
          } catch (Exception e) {
            for (ColumnInfo colInfo : tableFieldColumns) {
              colInfo.setComboValues(new String[] {});
            }
            // ignore any errors here. Combo items will not be
            // filled, but this is no problem for the user.
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  /**
   * Get the fields from the table in the database and use them as lookup keys. Only get the the
   * fields which are not yet in use as key, or in the field table. Also ignore technical key,
   * version, fromdate, todate.
   */
  private void getLookup() {
    final String tableName = variables.resolve(wTable.getText());
    final String schemaName = variables.resolve(wSchema.getText());
    DatabaseMeta dbMeta = wConnection.loadSelectedElement();
    if (dbMeta != null && StringUtils.isNotEmpty(tableName)) {
      try (Database db = new Database(loggingObject, variables, dbMeta)) {
        db.connect();

        IRowMeta rowMeta = db.getTableFieldsMeta(schemaName, tableName);
        if (rowMeta != null && !rowMeta.isEmpty()) {
          BaseTransformDialog.getFieldsFromPrevious(
              rowMeta,
              wUpIns,
              2,
              new int[] {1, 2},
              new int[] {3},
              -1,
              -1,
              (tableItem, v) -> {
                int idx = wKey.indexOfString(v.getName(), 2);
                return idx < 0
                    && !v.getName().equalsIgnoreCase(wTk.getText())
                    && !v.getName().equalsIgnoreCase(wVersion.getText())
                    && !v.getName().equalsIgnoreCase(wFromDate.getText())
                    && !v.getName().equalsIgnoreCase(wToDate.getText());
              });
        }
      } catch (HopException e) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.ErrorOccurred.DialogTitle"));
        mb.setMessage(
            BaseMessages.getString(PKG, "DimensionLookupDialog.ErrorOccurred.DialogMessage")
                + Const.CR
                + e.getMessage());
        mb.open();
      }
    }
  }

  private void getFields() {
    if (!gotPreviousFields) {
      try {
        String field = wDateField.getText();
        IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (rowMeta != null) {
          String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());
          wDateField.setItems(fieldNames);
        }
        if (field != null) {
          wDateField.setText(field);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "DimensionLookupDialog.ErrorGettingFields.Title"),
            BaseMessages.getString(PKG, "DimensionLookupDialog.ErrorGettingFields.Message"),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  private void getFieldsFromTable() {
    if (gotTableFields) {
      return;
    }
    final String schemaName = variables.resolve(wSchema.getText());
    final String tableName = variables.resolve(wTable.getText());
    final DatabaseMeta dbMeta = wConnection.loadSelectedElement();

    // Without a database or a table name we can't do very much
    if (dbMeta == null || StringUtils.isEmpty(tableName)) {
      return;
    }

    try (Database db = new Database(loggingObject, variables, dbMeta)) {
      db.connect();
      IRowMeta rowMeta = db.getTableFieldsMeta(schemaName, tableName);
      if (null != rowMeta) {
        String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());

        // Version
        String version = wVersion.getText();
        wVersion.setItems(fieldNames);
        wVersion.setText(Const.NVL(version, ""));

        // from date
        String fromDate = wFromDate.getText();
        wFromDate.setItems(fieldNames);
        wFromDate.setText(Const.NVL(fromDate, ""));

        // to date
        String toDate = wToDate.getText();
        wToDate.setItems(fieldNames);
        wToDate.setText(Const.NVL(toDate, ""));

        // tk
        String tk = wTk.getText();
        wTk.setItems(fieldNames);
        wTk.setText(Const.NVL(tk, ""));

        // AltStartDateField
        String sd = wAltStartDateField.getText();
        wAltStartDateField.setItems(fieldNames);
        wAltStartDateField.setText(Const.NVL(sd, ""));
      }
      gotTableFields = true;
    } catch (Exception e) {
      // ignore any errors here. Combo widgets will not be
      // filled, but this is no problem for the user.
    }
  }

  /**
   * Get the fields from the previous transform and use them as "keys". Only get the the fields
   * which are not yet in use as key, or in the field table. Also ignore technical key, version,
   * fromdate, todate.
   */
  private void getKeys() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r,
            wKey,
            2,
            new int[] {1, 2},
            new int[] {3},
            -1,
            -1,
            (tableItem, v) -> {
              int idx = wKey.indexOfString(v.getName(), 2);
              return idx < 0
                  && !v.getName().equalsIgnoreCase(wTk.getText())
                  && !v.getName().equalsIgnoreCase(wVersion.getText())
                  && !v.getName().equalsIgnoreCase(wFromDate.getText())
                  && !v.getName().equalsIgnoreCase(wToDate.getText());
            });

        Table table = wKey.table;
        for (int i = 0; i < r.size(); i++) {
          IValueMeta v = r.getValueMeta(i);
          int idx = wKey.indexOfString(v.getName(), 2);
          int idy = wUpIns.indexOfString(v.getName(), 2);
          if (idx < 0
              && idy < 0
              && !v.getName().equalsIgnoreCase(wTk.getText())
              && !v.getName().equalsIgnoreCase(wVersion.getText())
              && !v.getName().equalsIgnoreCase(wFromDate.getText())
              && !v.getName().equalsIgnoreCase(wToDate.getText())) {
            TableItem ti = new TableItem(table, SWT.NONE);
            ti.setText(1, v.getName());
            ti.setText(2, v.getName());
            ti.setText(3, v.getTypeDesc());
          }
        }
        wKey.removeEmptyRows();
        wKey.setRowNums();
        wKey.optWidth(true);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DimensionLookupDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(PKG, "DimensionLookupDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  // For Sybase ASE: don't keep everything in lowercase!
  private void create() {
    try {
      DimensionLookupMeta info = new DimensionLookupMeta();
      getInfo(info);

      String name = transformName; // new name might not yet be linked to other
      // transforms!
      TransformMeta transforminfo =
          new TransformMeta(
              BaseMessages.getString(PKG, "DimensionLookupDialog.Transform.Title"), name, info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

      String message = null;
      if (StringUtils.isEmpty(info.getFields().getReturns().getKeyField())) {
        message =
            BaseMessages.getString(PKG, "DimensionLookupDialog.Error.NoTechnicalKeySpecified");
      }
      if (Utils.isEmpty(info.getTableName())) {
        message = BaseMessages.getString(PKG, "DimensionLookupDialog.Error.NoTableNameSpecified");
      }

      if (message == null) {
        SqlStatement sql =
            info.getSqlStatements(variables, pipelineMeta, transforminfo, prev, metadataProvider);
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
                BaseMessages.getString(PKG, "DimensionLookupDialog.NoSQLNeeds.DialogMessage"));
            mb.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.NoSQLNeeds.DialogTitle"));
            mb.open();
          }
        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(sql.getError());
          mb.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.SQLError.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(message);
        mb.setText(BaseMessages.getString(PKG, "System.Dialog.Error.Title"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DimensionLookupDialog.UnableToBuildSQLError.DialogMessage"),
          BaseMessages.getString(PKG, "DimensionLookupDialog.UnableToBuildSQLError.DialogTitle"),
          ke);
    }
  }

  private void getSchemaNames() {
    DatabaseMeta dbMeta = wConnection.loadSelectedElement();
    if (dbMeta != null) {
      try (Database database = new Database(loggingObject, variables, dbMeta)) {
        database.connect();
        String[] schemas = Const.sortStrings(database.getSchemas());

        if (schemas != null && schemas.length > 0) {
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "DimensionLookupDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG,
                      "DimensionLookupDialog.AvailableSchemas.Message",
                      wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "DimensionLookupDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "DimensionLookupDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "DimensionLookupDialog.ErrorGettingSchemas"),
            e);
      }
    }
  }
}
