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

package org.apache.hop.pipeline.transforms.databaselookup;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaBase;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ColumnsResizer;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.pipeline.transform.ITableItemInsertListener;
import org.eclipse.swt.SWT;
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
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

public class DatabaseLookupDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DatabaseLookupMeta.class;

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private Button wCache;

  private Label wlCacheLoadAll;
  private Button wCacheLoadAll;

  private Label wlCacheSize;
  private Text wCacheSize;

  private TableView wKey;

  private TextVar wSchema;

  private TextVar wTable;

  private TableView wReturn;

  private Label wlOrderBy;
  private Text wOrderBy;

  private Label wlFailMultiple;
  private Button wFailMultiple;

  private Button wEatRows;

  private final DatabaseLookupMeta input;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  /** List of ColumnInfo that should have the previous fields combo box */
  private final List<ColumnInfo> fieldColumns = new ArrayList<>();

  /** all fields from the previous transforms */
  private IRowMeta prevFields = null;

  public DatabaseLookupDialog(
      Shell parent,
      IVariables variables,
      DatabaseLookupMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  private void addKeysTab(CTabFolder wTabFolder, int margin, ModifyListener lsMod) {

    CTabItem wKeysTab = new CTabItem(wTabFolder, SWT.NONE);
    wKeysTab.setFont(GuiResource.getInstance().getFontDefault());
    wKeysTab.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.KeysTab.TabTitle"));

    ScrolledComposite wKeySComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wKeySComp.setLayout(new FillLayout());

    Composite keysComp = new Composite(wKeySComp, SWT.NONE);
    PropsUi.setLook(keysComp);

    FormLayout keysLayout = new FormLayout();
    keysLayout.marginWidth = PropsUi.getFormMargin();
    keysLayout.marginHeight = PropsUi.getFormMargin();
    keysComp.setLayout(keysLayout);

    Button wGet = new Button(keysComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetLookupFields.Button"));
    setButtonPositions(new Button[] {wGet}, margin, null);
    PropsUi.setLook(wGet);

    Label wlKey = new Label(keysComp, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Keys.Label"));
    wlKey.setLayoutData(new FormDataBuilder().top().left().build());
    PropsUi.setLook(wlKey);

    int nrKeyCols = 4;
    int nrKeyRows =
        input.getLookup().getKeyFields().isEmpty() ? 1 : input.getLookup().getKeyFields().size();

    ColumnInfo[] ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Tablefield"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            DatabaseLookupMeta.conditionStrings);
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field2"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    tableFieldColumns.add(ciKey[0]);
    fieldColumns.add(ciKey[2]);
    fieldColumns.add(ciKey[3]);

    wKey =
        new TableView(
            variables,
            keysComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            nrKeyRows,
            lsMod,
            props);
    wKey.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 30, 10, 30, 30));
    wKey.setLayoutData(
        new FormDataBuilder().top(wlKey, margin).bottom(wGet, -margin).fullWidth().build());

    // After packing, center button vertically to TableView
    keysComp.pack();
    keysComp.setLayoutData(new FormDataBuilder().fullWidth().build());

    wKeySComp.setContent(keysComp);
    wKeySComp.setExpandHorizontal(true);
    wKeySComp.setExpandVertical(true);
    wKeySComp.setMinWidth(keysComp.getBounds().width);
    wKeySComp.setMinHeight(keysComp.getBounds().height);

    wGet.addListener(SWT.Selection, e -> get());
    wKeysTab.setControl(wKeySComp);
  }

  private void addGeneralTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {

    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTableFieldCombo();
          }
        };

    ModifyListener lsTableMod =
        arg0 -> {
          input.setChanged();
          setTableFieldCombo();
        };

    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GeneralTab.TabTitle"));

    ScrolledComposite wGeneralSComp =
        new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wGeneralSComp.setLayout(new FillLayout());

    Composite fieldGeneralComp = new Composite(wGeneralSComp, SWT.NONE);
    PropsUi.setLook(fieldGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = PropsUi.getFormMargin();
    generalLayout.marginHeight = PropsUi.getFormMargin();
    fieldGeneralComp.setLayout(generalLayout);

    // Connection line
    wConnection = addConnectionLine(fieldGeneralComp, null, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(fieldGeneralComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin * 2);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(fieldGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, 2 * margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);

    wSchema = new TextVar(variables, fieldGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsTableMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin * 2);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    Label wlTable = new Label(fieldGeneralComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Lookuptable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(fieldGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);

    wTable = new TextVar(variables, fieldGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsTableMod);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Enabled cache
    Label wlCache = new Label(fieldGeneralComp, SWT.RIGHT);
    wlCache.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Cache.Label"));
    PropsUi.setLook(wlCache);
    FormData fdlCache = new FormData();
    fdlCache.left = new FormAttachment(0, 0);
    fdlCache.right = new FormAttachment(middle, -margin);
    fdlCache.top = new FormAttachment(wTable, margin);
    wlCache.setLayoutData(fdlCache);
    wCache = new Button(fieldGeneralComp, SWT.CHECK);
    PropsUi.setLook(wCache);
    FormData fdCache = new FormData();
    fdCache.left = new FormAttachment(middle, 0);
    fdCache.top = new FormAttachment(wlCache, 0, SWT.CENTER);
    wCache.setLayoutData(fdCache);
    wCache.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // Cache size line
    wlCacheSize = new Label(fieldGeneralComp, SWT.RIGHT);
    wlCacheSize.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Cachesize.Label"));
    PropsUi.setLook(wlCacheSize);
    wlCacheSize.setEnabled(input.isCached());
    FormData fdlCachesize = new FormData();
    fdlCachesize.left = new FormAttachment(0, 0);
    fdlCachesize.right = new FormAttachment(middle, -margin);
    fdlCachesize.top = new FormAttachment(wCache, margin);
    wlCacheSize.setLayoutData(fdlCachesize);
    wCacheSize = new Text(fieldGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCacheSize);
    wCacheSize.setEnabled(input.isCached());
    wCacheSize.addModifyListener(lsMod);
    FormData fdCachesize = new FormData();
    fdCachesize.left = new FormAttachment(middle, 0);
    fdCachesize.right = new FormAttachment(100, 0);
    fdCachesize.top = new FormAttachment(wCache, margin);
    wCacheSize.setLayoutData(fdCachesize);

    // Cache load all
    wlCacheLoadAll = new Label(fieldGeneralComp, SWT.RIGHT);
    wlCacheLoadAll.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.CacheLoadAll.Label"));
    PropsUi.setLook(wlCacheLoadAll);
    FormData fdlCacheLoadAll = new FormData();
    fdlCacheLoadAll.left = new FormAttachment(0, 0);
    fdlCacheLoadAll.right = new FormAttachment(middle, -margin);
    fdlCacheLoadAll.top = new FormAttachment(wCacheSize, margin);
    wlCacheLoadAll.setLayoutData(fdlCacheLoadAll);
    wCacheLoadAll = new Button(fieldGeneralComp, SWT.CHECK);
    PropsUi.setLook(wCacheLoadAll);
    FormData fdCacheLoadAll = new FormData();
    fdCacheLoadAll.left = new FormAttachment(middle, 0);
    fdCacheLoadAll.top = new FormAttachment(wlCacheLoadAll, 0, SWT.CENTER);
    wCacheLoadAll.setLayoutData(fdCacheLoadAll);
    wCacheLoadAll.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // EatRows?
    Label wlEatRows = new Label(fieldGeneralComp, SWT.RIGHT);
    wlEatRows.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.EatRows.Label"));
    PropsUi.setLook(wlEatRows);
    FormData fdlEatRows = new FormData();
    fdlEatRows.left = new FormAttachment(0, 0);
    fdlEatRows.top = new FormAttachment(wCacheLoadAll, margin);
    fdlEatRows.right = new FormAttachment(middle, -margin);
    wlEatRows.setLayoutData(fdlEatRows);
    wEatRows = new Button(fieldGeneralComp, SWT.CHECK);
    PropsUi.setLook(wEatRows);
    FormData fdEatRows = new FormData();
    fdEatRows.left = new FormAttachment(middle, 0);
    fdEatRows.top = new FormAttachment(wlEatRows, 0, SWT.CENTER);
    wEatRows.setLayoutData(fdEatRows);
    wEatRows.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    // FailMultiple?
    wlFailMultiple = new Label(fieldGeneralComp, SWT.RIGHT);
    wlFailMultiple.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.FailMultiple.Label"));
    PropsUi.setLook(wlFailMultiple);
    FormData fdlFailMultiple = new FormData();
    fdlFailMultiple.left = new FormAttachment(0, 0);
    fdlFailMultiple.top = new FormAttachment(wEatRows, margin);
    fdlFailMultiple.right = new FormAttachment(middle, -margin);
    wlFailMultiple.setLayoutData(fdlFailMultiple);
    wFailMultiple = new Button(fieldGeneralComp, SWT.CHECK);
    PropsUi.setLook(wFailMultiple);
    FormData fdFailMultiple = new FormData();
    fdFailMultiple.left = new FormAttachment(middle, 0);
    fdFailMultiple.top = new FormAttachment(wlFailMultiple, 0, SWT.CENTER);
    wFailMultiple.setLayoutData(fdFailMultiple);
    wFailMultiple.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            enableFields();
          }
        });

    fieldGeneralComp.setLayoutData(new FormDataBuilder().fullWidth().build());

    fieldGeneralComp.pack();
    Rectangle bounds = fieldGeneralComp.getBounds();

    wGeneralSComp.setContent(fieldGeneralComp);
    wGeneralSComp.setExpandHorizontal(true);
    wGeneralSComp.setExpandVertical(true);
    wGeneralSComp.setMinWidth(bounds.width);
    wGeneralSComp.setMinHeight(bounds.height);

    wbSchema.addListener(SWT.Selection, e -> getSchemaName());
    wbTable.addListener(SWT.Selection, e -> getTableName());

    wGeneralTab.setControl(wGeneralSComp);
  }

  private void addFieldsTab(CTabFolder wTabFolder, int middle, int margin, ModifyListener lsMod) {

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.FieldsTab.TabTitle"));

    ScrolledComposite wFieldsSComp = new ScrolledComposite(wTabFolder, SWT.V_SCROLL | SWT.H_SCROLL);
    wFieldsSComp.setLayout(new FillLayout());

    Composite fieldFieldsComp = new Composite(wFieldsSComp, SWT.NONE);
    PropsUi.setLook(fieldFieldsComp);

    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = PropsUi.getFormMargin();
    fieldsLayout.marginHeight = PropsUi.getFormMargin();
    fieldFieldsComp.setLayout(fieldsLayout);

    // Return fields
    Label wlReturn = new Label(fieldFieldsComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Return.Label"));
    wlReturn.setLayoutData(new FormDataBuilder().top().left().build());
    PropsUi.setLook(wlReturn);

    Button wGetLU = new Button(fieldFieldsComp, SWT.PUSH);
    wGetLU.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFields.Button"));
    wGetLU.addListener(SWT.Selection, e -> getlookup());
    PropsUi.setLook(wGetLU);
    setButtonPositions(new Button[] {wGetLU}, margin, null);

    // OrderBy line
    wOrderBy = new Text(fieldFieldsComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOrderBy.setLayoutData(new FormDataBuilder().bottom(wGetLU, -margin).fullWidth().build());
    wOrderBy.addModifyListener(lsMod);
    PropsUi.setLook(wOrderBy);

    wlOrderBy = new Label(fieldFieldsComp, SWT.LEFT);
    wlOrderBy.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.Orderby.Label"));
    wlOrderBy.setLayoutData(new FormDataBuilder().bottom(wOrderBy, -margin).fullWidth().build());
    PropsUi.setLook(wlOrderBy);

    int upInsCols = 5;
    int upInsRows =
        input.getLookup().getReturnValues().isEmpty()
            ? 1
            : input.getLookup().getReturnValues().size();

    ColumnInfo[] ciReturn = new ColumnInfo[upInsCols];

    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Field"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {},
            false);

    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Newname"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Default"),
            ColumnInfo.COLUMN_TYPE_TEXT,
            false);

    ciReturn[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ColumnInfo.Type"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaFactory.getValueMetaNames());

    ciReturn[4] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.TrimTypeColumn.Column"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            ValueMetaBase.trimTypeDesc,
            true);

    tableFieldColumns.add(ciReturn[0]);

    wReturn =
        new TableView(
            variables,
            fieldFieldsComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            upInsRows,
            lsMod,
            props);
    wReturn.getTable().addListener(SWT.Resize, new ColumnsResizer(0, 30, 30, 15, 15, 10));
    wReturn.setLayoutData(
        new FormDataBuilder()
            .top(wlReturn, margin)
            .bottom(wlOrderBy, -margin * 2)
            .fullWidth()
            .build());

    fieldFieldsComp.setLayoutData(new FormDataBuilder().fullWidth().build());

    fieldFieldsComp.pack();
    Rectangle bounds = fieldFieldsComp.getBounds();

    wFieldsSComp.setContent(fieldFieldsComp);
    wFieldsSComp.setExpandHorizontal(true);
    wFieldsSComp.setExpandVertical(true);
    wFieldsSComp.setMinWidth(bounds.width);
    wFieldsSComp.setMinHeight(bounds.height);

    wFieldsTab.setControl(wFieldsSComp);
  }

  @Override
  public String open() {
    Shell parent = getParent();

    shell = new Shell(parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    setShellImage(shell, input);

    ModifyListener lsMod = e -> input.setChanged();

    backupChanged = input.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();

    shell.setLayout(formLayout);
    shell.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.shell.Title"));

    int middle = props.getMiddlePct();
    int margin = PropsUi.getMargin();

    // Buttons at the very bottom
    wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    PropsUi.setLook(wOk);

    wCancel = new Button(shell, SWT.PUSH);
    wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
    PropsUi.setLook(wCancel);

    setButtonPositions(new Button[] {wOk, wCancel}, margin, null);

    // TransformName line
    wlTransformName = new Label(shell, SWT.RIGHT);
    wlTransformName.setText(BaseMessages.getString(PKG, "System.TransformName.Label"));
    wlTransformName.setToolTipText(BaseMessages.getString(PKG, "System.TransformName.Tooltip"));
    PropsUi.setLook(wlTransformName);
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment(0, 0);
    fdlTransformName.right = new FormAttachment(middle, -margin);
    fdlTransformName.top = new FormAttachment(0, margin);
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

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, middle, margin, lsMod);
    addKeysTab(wTabFolder, margin, lsMod);
    addFieldsTab(wTabFolder, middle, margin, lsMod);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wTransformName, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -2 * margin);
    wTabFolder.setLayoutData(fdTabFolder);

    // Add listeners
    wOk.addListener(SWT.Selection, e -> ok());
    wCancel.addListener(SWT.Selection, e -> cancel());

    getData();

    setInputFieldCombo();
    setTableFieldCombo();

    wTabFolder.setSelection(0);

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void setInputFieldCombo() {
    shell
        .getDisplay()
        .asyncExec(
            () -> {
              try {
                prevFields = pipelineMeta.getPrevTransformFields(variables, transformName);
              } catch (HopException e) {
                prevFields = null;
              }
              if (prevFields == null) {
                new RowMeta();
                String msg =
                    BaseMessages.getString(PKG, "DatabaseLookupDialog.DoMapping.UnableToFindInput");
                logError(msg);
              } else {
                String[] fieldNames = Const.sortStrings(prevFields.getFieldNames());
                for (ColumnInfo colInfo : fieldColumns) {
                  colInfo.setComboValues(fieldNames);
                }
              }
            });
  }

  private void setTableFieldCombo() {
    shell
        .getDisplay()
        .asyncExec(
            () -> {
              if (!wTable.isDisposed() && !wConnection.isDisposed() && !wSchema.isDisposed()) {
                final String tableName = wTable.getText();
                final String connectionName = wConnection.getText();
                final String schemaName = wSchema.getText();
                if (!Utils.isEmpty(tableName)) {
                  DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
                  if (databaseMeta != null) {
                    try (Database database = new Database(loggingObject, variables, databaseMeta)) {
                      database.connect();

                      String schemaTable =
                          databaseMeta.getQuotedSchemaTableCombination(
                              variables, schemaName, tableName);
                      IRowMeta rowMeta = database.getTableFields(schemaTable);

                      if (null != rowMeta) {
                        String[] fieldNames = Const.sortStrings(rowMeta.getFieldNames());
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
            });
  }

  private void enableFields() {
    wlOrderBy.setEnabled(!wFailMultiple.getSelection());
    wOrderBy.setEnabled(!wFailMultiple.getSelection());

    wCacheSize.setEnabled(wCache.getSelection() && !wCacheLoadAll.getSelection());
    wlCacheSize.setEnabled(wCache.getSelection() && !wCacheLoadAll.getSelection());
    wCacheLoadAll.setEnabled(wCache.getSelection());
    wlCacheLoadAll.setEnabled(wCache.getSelection());
    wFailMultiple.setEnabled(!wCache.getSelection());
    wlFailMultiple.setEnabled(!wCache.getSelection());
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    logDebug(BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.GettingKeyInfo"));

    wCache.setSelection(input.isCached());
    wCacheSize.setText("" + input.getCacheSize());
    wCacheLoadAll.setSelection(input.isLoadingAllDataInCache());

    Lookup lookup = input.getLookup();

    for (int i = 0; i < lookup.getKeyFields().size(); i++) {
      KeyField keyField = lookup.getKeyFields().get(i);

      TableItem item = wKey.table.getItem(i);
      item.setText(1, Const.NVL(keyField.getTableField(), ""));
      item.setText(2, Const.NVL(keyField.getCondition(), ""));
      item.setText(3, Const.NVL(keyField.getStreamField1(), ""));
      item.setText(4, Const.NVL(keyField.getStreamField2(), ""));
    }
    for (int i = 0; i < lookup.getReturnValues().size(); i++) {
      ReturnValue returnValue = lookup.getReturnValues().get(i);

      TableItem item = wReturn.table.getItem(i);
      item.setText(1, Const.NVL(returnValue.getTableField(), ""));
      item.setText(2, Const.NVL(returnValue.getNewName(), ""));
      item.setText(3, Const.NVL(returnValue.getDefaultValue(), ""));
      item.setText(4, Const.NVL(returnValue.getDefaultType(), ""));
      item.setText(
          5,
          Const.NVL(
              ValueMetaBase.getTrimTypeDesc(
                  ValueMetaBase.getTrimTypeByCode(returnValue.getTrimType())),
              ValueMetaBase.trimTypeCode[0]));
    }
    wSchema.setText(Const.NVL(input.getSchemaName(), ""));
    wTable.setText(Const.NVL(input.getTableName(), ""));
    if (!Utils.isEmpty(input.getConnection())) {
      wConnection.setText(input.getConnection());
    }
    wOrderBy.setText(Const.NVL(lookup.getOrderByClause(), ""));
    wFailMultiple.setSelection(lookup.isFailingOnMultipleResults());
    wEatRows.setSelection(lookup.isEatingRowOnLookupFailure());

    wKey.optimizeTableView();
    wReturn.optimizeTableView();

    enableFields();

    wTransformName.selectAll();
    wTransformName.setFocus();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(backupChanged);
    dispose();
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    Lookup lookup = input.getLookup();
    lookup.getKeyFields().clear();
    lookup.getReturnValues().clear();

    input.setCached(wCache.getSelection());
    input.setCacheSize(Const.toInt(wCacheSize.getText(), 0));
    input.setLoadingAllDataInCache(wCacheLoadAll.getSelection());

    for (TableItem item : wKey.getNonEmptyItems()) {
      KeyField keyField = new KeyField();
      keyField.setTableField(item.getText(1));
      keyField.setCondition(item.getText(2));
      keyField.setStreamField1(item.getText(3));
      keyField.setStreamField2(item.getText(4));
      lookup.getKeyFields().add(keyField);
    }

    for (TableItem item : wReturn.getNonEmptyItems()) {
      ReturnValue returnValue = new ReturnValue();
      returnValue.setTableField(item.getText(1));
      returnValue.setNewName(item.getText(2));
      returnValue.setDefaultValue(item.getText(3));
      returnValue.setDefaultType(item.getText(4));
      returnValue.setTrimType(
          ValueMetaBase.getTrimTypeCode(ValueMetaBase.getTrimTypeByDesc(item.getText(5))));
      lookup.getReturnValues().add(returnValue);
    }

    lookup.setSchemaName(wSchema.getText());
    lookup.setTableName(wTable.getText());
    input.setConnection(wConnection.getText());
    lookup.setOrderByClause(wOrderBy.getText());
    lookup.setFailingOnMultipleResults(wFailMultiple.getSelection());
    lookup.setEatingRowOnLookupFailure(wEatRows.getSelection());

    transformName = wTransformName.getText(); // return value

    if (pipelineMeta.findDatabase(wConnection.getText(), variables) == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }

    dispose();
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
            BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.LookingAtConnection")
                + databaseMeta);
      }

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogMessage"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(2, "=");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wKey, 1, new int[] {1, 3}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFieldsFailed.DialogTitle"),
          BaseMessages.getString(PKG, "DatabaseLookupDialog.GetFieldsFailed.DialogMessage"),
          ke);
    }
  }

  private void getlookup() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();

        if (!Utils.isEmpty(wTable.getText())) {
          String schemaTable =
              databaseMeta.getQuotedSchemaTableCombination(
                  variables, wSchema.getText(), wTable.getText());
          IRowMeta r = database.getTableFields(schemaTable);

          if (r != null && !r.isEmpty()) {
            logDebug(
                BaseMessages.getString(PKG, "DatabaseLookupDialog.Log.FoundTableFields")
                    + schemaTable
                    + " --> "
                    + r.toStringMeta());
            BaseTransformDialog.getFieldsFromPrevious(
                r, wReturn, 1, new int[] {1, 2}, new int[] {4}, -1, -1, null);
          } else {
            MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
            mb.setMessage(
                BaseMessages.getString(
                    PKG, "DatabaseLookupDialog.CouldNotReadTableInfo.DialogMessage"));
            mb.setText(
                BaseMessages.getString(
                    PKG, "DatabaseLookupDialog.CouldNotReadTableInfo.DialogTitle"));
            mb.open();
          }
        }
      } catch (HopException e) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorOccurred.DialogMessage")
                + Const.CR
                + e.getMessage());
        mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorOccurred.DialogTitle"));
        mb.open();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnectionName.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "DatabaseLookupDialog.InvalidConnectionName.DialogTitle"));
      mb.open();
    }
  }

  private void getSchemaName() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          schemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  schemas,
                  BaseMessages.getString(
                      PKG, "DatabaseLookupDialog.AvailableSchemas.Title", wConnection.getText()),
                  BaseMessages.getString(
                      PKG, "DatabaseLookupDialog.AvailableSchemas.Message", wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "DatabaseLookupDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "DatabaseLookupDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "DatabaseLookupDialog.ErrorGettingSchemas"),
            e);
      }
    }
  }
}
