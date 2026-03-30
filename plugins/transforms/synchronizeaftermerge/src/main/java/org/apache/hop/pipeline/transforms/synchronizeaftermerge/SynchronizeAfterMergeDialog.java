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

package org.apache.hop.pipeline.transforms.synchronizeaftermerge;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
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
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
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
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.jetbrains.annotations.Nullable;

public class SynchronizeAfterMergeDialog extends BaseTransformDialog {
  private static final Class<?> PKG = SynchronizeAfterMergeMeta.class;
  public static final String
      CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_TITLE =
          "SynchronizeAfterMergeDialog.FailedToGetFields.DialogTitle";
  public static final String
      CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_MESSAGE =
          "SynchronizeAfterMergeDialog.FailedToGetFields.DialogMessage";

  private MetaSelectionLine<DatabaseMeta> wConnection;

  private TableView wKey;

  private TextVar wSchema;

  private Label wlTable;
  private Button wbTable;
  private TextVar wTable;

  private TableView wReturn;

  private TextVar wCommit;

  private Label wlTableField;
  private CCombo wTableField;

  private Button wTableNameInField;

  private Button wbBatch;

  private Button wbPerformLookup;

  private CCombo wOperationField;

  private TextVar wOrderInsert;

  private TextVar wOrderDelete;

  private TextVar wOrderUpdate;

  private final SynchronizeAfterMergeMeta input;

  private final List<String> inputFields = new ArrayList<>();

  private ColumnInfo[] ciKey;

  private ColumnInfo[] ciReturn;

  private boolean gotPreviousFields = false;

  /** List of ColumnInfo that should have the field names of the selected database table */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public SynchronizeAfterMergeDialog(
      Shell parent,
      IVariables variables,
      SynchronizeAfterMergeMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).sql(e -> create()).cancel(e -> cancel()).build();

    ModifyListener lsMod = e -> input.setChanged();
    ModifyListener lsTableMod =
        arg0 -> {
          input.setChanged();
          setTableFieldCombo();
        };
    SelectionListener lsSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
            setTableFieldCombo();
          }
        };
    SelectionListener lsSimpleSelection =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            input.setChanged();
          }
        };
    changed = input.hasChanged();

    CTabFolder wTabFolder = new CTabFolder(shell, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    addGeneralTab(wTabFolder, lsMod, lsSelection, lsTableMod, lsSimpleSelection);
    addKeysTab(wTabFolder, lsMod);
    addUpdatesTab(wTabFolder, lsMod);
    addAdvancedTab(wTabFolder, lsMod, lsSimpleSelection);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wSpacer, margin);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(wOk, -margin);
    wTabFolder.setLayoutData(fdTabFolder);

    wTabFolder.setSelection(0);

    getData();
    setTableFieldCombo();
    activeTableNameField();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private void addAdvancedTab(
      CTabFolder wTabFolder, ModifyListener lsMod, SelectionListener lsSimpleSelection) {
    CTabItem wAdvancedTab = new CTabItem(wTabFolder, SWT.NONE);
    wAdvancedTab.setFont(GuiResource.getInstance().getFontDefault());
    wAdvancedTab.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.AdvancedTab.TabTitle"));

    Composite wAdvancedComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wAdvancedComp);

    FormLayout advancedLayout = new FormLayout();
    advancedLayout.marginWidth = 3;
    advancedLayout.marginHeight = 3;
    wAdvancedComp.setLayout(advancedLayout);

    // ///////////////////////////////
    // START OF OPERATION ORDER GROUP //
    // ///////////////////////////////

    Group wOperationOrder = new Group(wAdvancedComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOperationOrder);
    wOperationOrder.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OperationOrder.Label"));

    FormLayout originFilesgroupLayout = new FormLayout();
    originFilesgroupLayout.marginWidth = 10;
    originFilesgroupLayout.marginHeight = 10;
    wOperationOrder.setLayout(originFilesgroupLayout);

    Label wlOperationField = new Label(wOperationOrder, SWT.RIGHT);
    wlOperationField.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OperationField.Label"));
    PropsUi.setLook(wlOperationField);
    FormData fdlOperationField = new FormData();
    fdlOperationField.left = new FormAttachment(0, 0);
    fdlOperationField.top = new FormAttachment(wTableField, margin);
    fdlOperationField.right = new FormAttachment(middle, -margin);
    wlOperationField.setLayoutData(fdlOperationField);
    wOperationField = new CCombo(wOperationOrder, SWT.BORDER | SWT.READ_ONLY);
    wOperationField.setEditable(true);
    PropsUi.setLook(wOperationField);
    wOperationField.addModifyListener(lsMod);
    FormData fdOperationField = new FormData();
    fdOperationField.left = new FormAttachment(middle, 0);
    fdOperationField.top = new FormAttachment(wTableField, margin);
    fdOperationField.right = new FormAttachment(100, 0);
    wOperationField.setLayoutData(fdOperationField);
    wOperationField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do Nothing
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

    // OrderInsert line...
    Label wlOrderInsert = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderInsert.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderInsert.Label"));
    PropsUi.setLook(wlOrderInsert);
    FormData fdlOrderInsert = new FormData();
    fdlOrderInsert.left = new FormAttachment(0, 0);
    fdlOrderInsert.right = new FormAttachment(middle, -margin);
    fdlOrderInsert.top = new FormAttachment(wOperationField, margin);
    wlOrderInsert.setLayoutData(fdlOrderInsert);

    wOrderInsert = new TextVar(variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOrderInsert.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderInsert.ToolTip"));
    PropsUi.setLook(wOrderInsert);
    wOrderInsert.addModifyListener(lsMod);
    FormData fdOrderInsert = new FormData();
    fdOrderInsert.left = new FormAttachment(middle, 0);
    fdOrderInsert.top = new FormAttachment(wOperationField, margin);
    fdOrderInsert.right = new FormAttachment(100, 0);
    wOrderInsert.setLayoutData(fdOrderInsert);

    // OrderUpdate line...
    Label wlOrderUpdate = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderUpdate.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderUpdate.Label"));
    PropsUi.setLook(wlOrderUpdate);
    FormData fdlOrderUpdate = new FormData();
    fdlOrderUpdate.left = new FormAttachment(0, 0);
    fdlOrderUpdate.right = new FormAttachment(middle, -margin);
    fdlOrderUpdate.top = new FormAttachment(wOrderInsert, margin);
    wlOrderUpdate.setLayoutData(fdlOrderUpdate);

    wOrderUpdate = new TextVar(variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOrderUpdate.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderUpdate.ToolTip"));
    PropsUi.setLook(wOrderUpdate);
    wOrderUpdate.addModifyListener(lsMod);
    FormData fdOrderUpdate = new FormData();
    fdOrderUpdate.left = new FormAttachment(middle, 0);
    fdOrderUpdate.top = new FormAttachment(wOrderInsert, margin);
    fdOrderUpdate.right = new FormAttachment(100, 0);
    wOrderUpdate.setLayoutData(fdOrderUpdate);

    // OrderDelete line...
    Label wlOrderDelete = new Label(wOperationOrder, SWT.RIGHT);
    wlOrderDelete.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderDelete.Label"));
    PropsUi.setLook(wlOrderDelete);
    FormData fdlOrderDelete = new FormData();
    fdlOrderDelete.left = new FormAttachment(0, 0);
    fdlOrderDelete.right = new FormAttachment(middle, -margin);
    fdlOrderDelete.top = new FormAttachment(wOrderUpdate, margin);
    wlOrderDelete.setLayoutData(fdlOrderDelete);

    wOrderDelete = new TextVar(variables, wOperationOrder, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wOrderDelete.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.OrderDelete.ToolTip"));
    PropsUi.setLook(wOrderDelete);
    wOrderDelete.addModifyListener(lsMod);
    FormData fdOrderDelete = new FormData();
    fdOrderDelete.left = new FormAttachment(middle, 0);
    fdOrderDelete.top = new FormAttachment(wOrderUpdate, margin);
    fdOrderDelete.right = new FormAttachment(100, 0);
    wOrderDelete.setLayoutData(fdOrderDelete);

    // Perform a lookup?
    Label wlPerformLookup = new Label(wOperationOrder, SWT.RIGHT);
    wlPerformLookup.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.PerformLookup.Label"));
    PropsUi.setLook(wlPerformLookup);
    FormData fdlPerformLookup = new FormData();
    fdlPerformLookup.left = new FormAttachment(0, 0);
    fdlPerformLookup.top = new FormAttachment(wOrderDelete, margin);
    fdlPerformLookup.right = new FormAttachment(middle, -margin);
    wlPerformLookup.setLayoutData(fdlPerformLookup);
    wbPerformLookup = new Button(wOperationOrder, SWT.CHECK);
    wbPerformLookup.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.PerformLookup.Tooltip"));
    wbPerformLookup.addSelectionListener(lsSimpleSelection);
    PropsUi.setLook(wbPerformLookup);
    FormData fdPerformLookup = new FormData();
    fdPerformLookup.left = new FormAttachment(middle, 0);
    fdPerformLookup.top = new FormAttachment(wlPerformLookup, 0, SWT.CENTER);
    fdPerformLookup.right = new FormAttachment(100, 0);
    wbPerformLookup.setLayoutData(fdPerformLookup);

    FormData fdOperationOrder = new FormData();
    fdOperationOrder.left = new FormAttachment(0, margin);
    fdOperationOrder.top = new FormAttachment(wSpacer, margin);
    fdOperationOrder.right = new FormAttachment(100, -margin);
    wOperationOrder.setLayoutData(fdOperationOrder);

    // ///////////////////////////////////////////////////////////
    // / END OF Operation order GROUP
    // ///////////////////////////////////////////////////////////

    FormData fdAdvancedComp = new FormData();
    fdAdvancedComp.left = new FormAttachment(0, 0);
    fdAdvancedComp.top = new FormAttachment(0, 0);
    fdAdvancedComp.right = new FormAttachment(100, 0);
    fdAdvancedComp.bottom = new FormAttachment(100, 0);
    wAdvancedComp.setLayoutData(fdAdvancedComp);

    wAdvancedComp.layout();
    wAdvancedTab.setControl(wAdvancedComp);
    PropsUi.setLook(wAdvancedComp);
  }

  private void addUpdatesTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wUpdatesTab = new CTabItem(wTabFolder, SWT.NONE);
    wUpdatesTab.setFont(GuiResource.getInstance().getFontDefault());
    wUpdatesTab.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.UpdatesTab.TabTitle"));

    Composite wUpdatesComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wUpdatesComp);

    FormLayout updatesLayout = new FormLayout();
    updatesLayout.marginWidth = 3;
    updatesLayout.marginHeight = 3;
    wUpdatesComp.setLayout(updatesLayout);

    // THE UPDATE/INSERT TABLE
    Label wlReturn = new Label(wUpdatesComp, SWT.NONE);
    wlReturn.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.UpdateFields.Label"));
    PropsUi.setLook(wlReturn);
    FormData fdlReturn = new FormData();
    fdlReturn.left = new FormAttachment(0, 0);
    fdlReturn.top = new FormAttachment(wKey, margin);
    wlReturn.setLayoutData(fdlReturn);

    int upInsCols = 3;

    ciReturn = new ColumnInfo[upInsCols];
    ciReturn[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciReturn[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.Update"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            "Y",
            "N");
    tableFieldColumns.add(ciReturn[0]);
    wReturn =
        new TableView(
            variables,
            wUpdatesComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciReturn,
            1,
            lsMod,
            props);

    Button wbGetLU = new Button(wUpdatesComp, SWT.PUSH);
    wbGetLU.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.GetAndUpdateFields.Label"));
    FormData fdGetLU = new FormData();
    fdGetLU.bottom = new FormAttachment(100, -2 * margin);
    fdGetLU.left = new FormAttachment(0, 0);
    wbGetLU.setLayoutData(fdGetLU);
    wbGetLU.addListener(SWT.Selection, e -> getUpdate());

    FormData fdReturn = new FormData();
    fdReturn.left = new FormAttachment(0, 0);
    fdReturn.top = new FormAttachment(wlReturn, margin);
    fdReturn.right = new FormAttachment(100, 0);
    fdReturn.bottom = new FormAttachment(wbGetLU, -2 * margin);
    wReturn.setLayoutData(fdReturn);

    Button wbDoMapping = new Button(wUpdatesComp, SWT.PUSH);
    wbDoMapping.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.EditMapping.Label"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wbGetLU, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wbDoMapping.setLayoutData(fdDoMapping);
    wbDoMapping.addListener(SWT.Selection, arg0 -> generateMappings());

    //
    // Search the fields in the background
    //

    final Runnable runnable =
        () -> {
          // This is running in a new process: copy some HopVariables info
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

    FormData fdUpdatesComp = new FormData();
    fdUpdatesComp.left = new FormAttachment(0, 0);
    fdUpdatesComp.top = new FormAttachment(0, 0);
    fdUpdatesComp.right = new FormAttachment(100, 0);
    fdUpdatesComp.bottom = new FormAttachment(100, 0);
    wUpdatesComp.setLayoutData(fdUpdatesComp);

    wUpdatesComp.layout();
    wUpdatesTab.setControl(wUpdatesComp);
    PropsUi.setLook(wUpdatesComp);
  }

  private void addKeysTab(CTabFolder wTabFolder, ModifyListener lsMod) {
    CTabItem wKeysTab = new CTabItem(wTabFolder, SWT.NONE);
    wKeysTab.setFont(GuiResource.getInstance().getFontDefault());
    wKeysTab.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.KeysTab.TabTitle"));

    Composite wKeysComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wKeysComp);

    FormLayout keysLayout = new FormLayout();
    keysLayout.marginWidth = 3;
    keysLayout.marginHeight = 3;
    wKeysComp.setLayout(keysLayout);

    Label wlKey = new Label(wKeysComp, SWT.NONE);
    wlKey.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Keys.Label"));
    PropsUi.setLook(wlKey);
    FormData fdlKey = new FormData();
    fdlKey.left = new FormAttachment(0, 0);
    fdlKey.top = new FormAttachment(wTableField, margin);
    wlKey.setLayoutData(fdlKey);

    int nrKeyCols = 4;

    ciKey = new ColumnInfo[nrKeyCols];
    ciKey[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.Comparator"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            "=",
            "<>",
            "<",
            "<=",
            ">",
            ">=",
            "LIKE",
            "BETWEEN",
            "IS NULL",
            "IS NOT NULL");
    ciKey[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField1"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciKey[3] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ColumnInfo.StreamField2"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    tableFieldColumns.add(ciKey[0]);
    wKey =
        new TableView(
            variables,
            wKeysComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciKey,
            1,
            lsMod,
            props);

    wGet = new Button(wKeysComp, SWT.PUSH);
    wGet.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.GetFields.Button"));
    fdGet = new FormData();
    fdGet.left = new FormAttachment(0, 0);
    fdGet.bottom = new FormAttachment(100, -2 * margin);
    wGet.setLayoutData(fdGet);
    wGet.addListener(SWT.Selection, e -> get());

    FormData fdKey = new FormData();
    fdKey.left = new FormAttachment(0, 0);
    fdKey.top = new FormAttachment(wlKey, margin);
    fdKey.right = new FormAttachment(100, 0);
    fdKey.bottom = new FormAttachment(wGet, -2 * margin);
    wKey.setLayoutData(fdKey);

    FormData fdKeysComp = new FormData();
    fdKeysComp.left = new FormAttachment(0, 0);
    fdKeysComp.top = new FormAttachment(0, 0);
    fdKeysComp.right = new FormAttachment(100, 0);
    fdKeysComp.bottom = new FormAttachment(100, 0);
    wKeysComp.setLayoutData(fdKeysComp);

    wKeysComp.layout();
    wKeysTab.setControl(wKeysComp);
    PropsUi.setLook(wKeysComp);
  }

  private void addGeneralTab(
      CTabFolder wTabFolder,
      ModifyListener lsMod,
      SelectionListener lsSelection,
      ModifyListener lsTableMod,
      SelectionListener lsSimpleSelection) {
    CTabItem wGeneralTab = new CTabItem(wTabFolder, SWT.NONE);
    wGeneralTab.setFont(GuiResource.getInstance().getFontDefault());
    wGeneralTab.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.GeneralTab.TabTitle"));

    Composite wGeneralComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wGeneralComp);

    FormLayout generalLayout = new FormLayout();
    generalLayout.marginWidth = 3;
    generalLayout.marginHeight = 3;
    wGeneralComp.setLayout(generalLayout);

    // Connection line
    wConnection = addConnectionLine(wGeneralComp, wSpacer, input.getConnection(), lsMod);
    wConnection.addSelectionListener(lsSelection);

    // Schema line...
    Label wlSchema = new Label(wGeneralComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(wConnection, margin);
    wlSchema.setLayoutData(fdlSchema);

    Button wbSchema = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbSchema);
    wbSchema.setText(BaseMessages.getString(PKG, "System.Button.Browse"));
    FormData fdbSchema = new FormData();
    fdbSchema.top = new FormAttachment(wConnection, margin);
    fdbSchema.right = new FormAttachment(100, 0);
    wbSchema.setLayoutData(fdbSchema);
    wbSchema.addListener(SWT.Selection, e -> getSchemaNames());

    wSchema = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsTableMod);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wConnection, margin);
    fdSchema.right = new FormAttachment(wbSchema, -margin);
    wSchema.setLayoutData(fdSchema);

    // Table line...
    wlTable = new Label(wGeneralComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wbSchema, margin);
    wlTable.setLayoutData(fdlTable);

    wbTable = new Button(wGeneralComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Browse.Button"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wbSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addListener(SWT.Selection, e -> getTableName());

    wTable = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsTableMod);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wbSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Commit line
    Label wlCommit = new Label(wGeneralComp, SWT.RIGHT);
    wlCommit.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.CommitSize.Label"));
    PropsUi.setLook(wlCommit);
    FormData fdlCommit = new FormData();
    fdlCommit.left = new FormAttachment(0, 0);
    fdlCommit.top = new FormAttachment(wTable, margin);
    fdlCommit.right = new FormAttachment(middle, -margin);
    wlCommit.setLayoutData(fdlCommit);

    wCommit = new TextVar(variables, wGeneralComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wCommit);
    wCommit.addModifyListener(lsMod);
    FormData fdCommit = new FormData();
    fdCommit.left = new FormAttachment(middle, 0);
    fdCommit.top = new FormAttachment(wTable, margin);
    fdCommit.right = new FormAttachment(100, 0);
    wCommit.setLayoutData(fdCommit);

    // UsePart update
    Label wlBatch = new Label(wGeneralComp, SWT.RIGHT);
    wlBatch.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Batch.Label"));
    PropsUi.setLook(wlBatch);
    FormData fdlBatch = new FormData();
    fdlBatch.left = new FormAttachment(0, 0);
    fdlBatch.top = new FormAttachment(wCommit, margin);
    fdlBatch.right = new FormAttachment(middle, -margin);
    wlBatch.setLayoutData(fdlBatch);
    wbBatch = new Button(wGeneralComp, SWT.CHECK);
    wbBatch.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Batch.Tooltip"));
    wbBatch.addSelectionListener(lsSimpleSelection);
    PropsUi.setLook(wbBatch);
    FormData fdBatch = new FormData();
    fdBatch.left = new FormAttachment(middle, 0);
    fdBatch.top = new FormAttachment(wlBatch, 0, SWT.CENTER);
    fdBatch.right = new FormAttachment(100, 0);
    wbBatch.setLayoutData(fdBatch);

    // TablenameInField line
    Label wlTablenameInField = new Label(wGeneralComp, SWT.RIGHT);
    wlTablenameInField.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TablenameInField.Label"));
    PropsUi.setLook(wlTablenameInField);
    FormData fdlTablenameInField = new FormData();
    fdlTablenameInField.left = new FormAttachment(0, 0);
    fdlTablenameInField.top = new FormAttachment(wbBatch, margin);
    fdlTablenameInField.right = new FormAttachment(middle, -margin);
    wlTablenameInField.setLayoutData(fdlTablenameInField);
    wTableNameInField = new Button(wGeneralComp, SWT.CHECK);
    wTableNameInField.setToolTipText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TablenameInField.Tooltip"));
    PropsUi.setLook(wTableNameInField);
    FormData fdTablenameInField = new FormData();
    fdTablenameInField.left = new FormAttachment(middle, 0);
    fdTablenameInField.top = new FormAttachment(wlTablenameInField, 0, SWT.CENTER);
    fdTablenameInField.right = new FormAttachment(100, 0);
    wTableNameInField.setLayoutData(fdTablenameInField);
    wTableNameInField.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent e) {
            activeTableNameField();
            input.setChanged();
          }
        });

    wlTableField = new Label(wGeneralComp, SWT.RIGHT);
    wlTableField.setText(
        BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TableField.Label"));
    PropsUi.setLook(wlTableField);
    FormData fdlTableField = new FormData();
    fdlTableField.left = new FormAttachment(0, 0);
    fdlTableField.top = new FormAttachment(wTableNameInField, margin);
    fdlTableField.right = new FormAttachment(middle, -margin);
    wlTableField.setLayoutData(fdlTableField);
    wTableField = new CCombo(wGeneralComp, SWT.BORDER | SWT.READ_ONLY);
    wTableField.setEditable(true);
    PropsUi.setLook(wTableField);
    wTableField.addModifyListener(lsMod);
    FormData fdTableField = new FormData();
    fdTableField.left = new FormAttachment(middle, 0);
    fdTableField.top = new FormAttachment(wTableNameInField, margin);
    fdTableField.right = new FormAttachment(100, 0);
    wTableField.setLayoutData(fdTableField);
    wTableField.addFocusListener(
        new FocusListener() {
          @Override
          public void focusLost(FocusEvent e) {
            // Do Nothing
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

    FormData fdGeneralComp = new FormData();
    fdGeneralComp.left = new FormAttachment(0, 0);
    fdGeneralComp.top = new FormAttachment(0, 0);
    fdGeneralComp.right = new FormAttachment(100, 0);
    fdGeneralComp.bottom = new FormAttachment(100, 0);
    wGeneralComp.setLayoutData(fdGeneralComp);

    wGeneralComp.layout();
    wGeneralTab.setControl(wGeneralComp);
    PropsUi.setLook(wGeneralComp);
  }

  /**
   * Reads in the fields from the previous transforms and from the ONE next transform and opens an
   * EnterMappingDialog with this information. After the user did the mapping, those information is
   * put into the Select/Rename table.
   */
  private void generateMappings() {
    // Determine the source and target fields...
    //
    IRowMeta sourceFields = findSourceFields();
    if (sourceFields == null) return;

    // refresh data
    input.setConnection(wConnection.getText());
    input.getLookup().setTableName(variables.resolve(wTable.getText()));
    IRowMeta targetFields = findTargetFields();
    if (targetFields == null) {
      return;
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
                    "SynchronizeAfterMergeDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString())
                + Const.CR;
      }
      if (!missingTargetFields.isEmpty()) {
        message +=
            BaseMessages.getString(
                    PKG,
                    "SynchronizeAfterMergeDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingTargetFields.toString())
                + Const.CR;
      }
      message += Const.CR;
      message +=
          BaseMessages.getString(
                  PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeFieldsNotFoundContinue")
              + Const.CR;

      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(
                  PKG, "SynchronizeAfterMergeDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message,
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      boolean goOn = (answer & SWT.YES) != 0;

      if (!goOn) {
        return;
      }
    }
    EnterMappingDialog d =
        new EnterMappingDialog(
            SynchronizeAfterMergeDialog.this.shell,
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

  private @Nullable IRowMeta findTargetFields() {
    IRowMeta targetFields;
    ITransformMeta transformMetaInterface = transformMeta.getTransform();
    try {
      targetFields = transformMetaInterface.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return null;
    }
    return targetFields;
  }

  private @Nullable IRowMeta findSourceFields() {
    IRowMeta sourceFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return null;
    }
    return sourceFields;
  }

  private void setTableFieldCombo() {
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

                  IRowMeta r =
                      db.getTableFieldsMeta(
                          variables.resolve(schemaName), variables.resolve(tableName));
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

  protected void setComboBoxes() {
    // Something was changed in the row.
    //
    String[] fieldNames = ConstUi.sortFieldNames(inputFields);
    ciKey[2].setComboValues(fieldNames);
    ciKey[3].setComboValues(fieldNames);
    ciReturn[1].setComboValues(fieldNames);
  }

  private void activeTableNameField() {
    wlTableField.setEnabled(wTableNameInField.getSelection());
    wTableField.setEnabled(wTableNameInField.getSelection());
    wlTable.setEnabled(!wTableNameInField.getSelection());
    wTable.setEnabled(!wTableNameInField.getSelection());
    wbTable.setEnabled(!wTableNameInField.getSelection());
    wSql.setEnabled(!wTableNameInField.getSelection());
  }

  private void getFields() {
    if (!gotPreviousFields) {
      try {
        String field = wTableField.getText();
        String fieldoperation = wOperationField.getText();
        IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
        if (r != null) {
          wTableField.setItems(r.getFieldNames());
          wOperationField.setItems(r.getFieldNames());
        }
        if (field != null) {
          wTableField.setText(field);
        }
        if (fieldoperation != null) {
          wOperationField.setText(fieldoperation);
        }
      } catch (HopException ke) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(
                PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_TITLE),
            BaseMessages.getString(
                PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_MESSAGE),
            ke);
      }
      gotPreviousFields = true;
    }
  }

  /** Copy information from the meta-data input to the dialog fields. */
  public void getData() {
    wConnection.setText(Const.NVL(input.getConnection(), ""));
    wSchema.setText(Const.NVL(input.getLookup().getSchemaName(), ""));
    wTable.setText(Const.NVL(input.getLookup().getTableName(), ""));

    wCommit.setText(input.getCommitSize());
    wTableNameInField.setSelection(input.isTableNameInField());
    wTableField.setText(Const.NVL(input.getTableNameField(), ""));
    wbBatch.setSelection(input.isUsingBatchUpdates());
    wOperationField.setText(Const.NVL(input.getOperationOrderField(), ""));
    wOrderInsert.setText(Const.NVL(input.getOrderInsert(), ""));
    wOrderUpdate.setText(Const.NVL(input.getOrderUpdate(), ""));
    wOrderDelete.setText(Const.NVL(input.getOrderDelete(), ""));
    wbPerformLookup.setSelection(input.isPerformingLookup());

    for (SynchronizeAfterMergeMeta.KeyCondition keyLookup : input.getLookup().getKeyConditions()) {
      TableItem item = new TableItem(wKey.table, SWT.NONE);
      item.setText(1, Const.NVL(keyLookup.getColumnName(), ""));
      item.setText(2, Const.NVL(keyLookup.getCondition(), ""));
      item.setText(3, Const.NVL(keyLookup.getFieldName(), ""));
      item.setText(4, Const.NVL(keyLookup.getFieldName2(), ""));
    }
    wKey.optimizeTableView();

    for (SynchronizeAfterMergeMeta.ValueUpdate valueUpdate : input.getLookup().getValueUpdates()) {
      TableItem item = new TableItem(wReturn.table, SWT.NONE);
      item.setText(1, Const.NVL(valueUpdate.getColumnName(), ""));
      item.setText(2, Const.NVL(valueUpdate.getFieldName(), ""));
      item.setText(3, valueUpdate.isUpdate() ? "Y" : "N");
    }
    wReturn.optimizeTableView();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getInfo(SynchronizeAfterMergeMeta inf) {
    SynchronizeAfterMergeMeta.Lookup lookup = inf.getLookup();

    inf.setConnection(wConnection.getText());
    lookup.setSchemaName(wSchema.getText());
    lookup.setTableName(wTable.getText());

    inf.setCommitSize(wCommit.getText());
    inf.setTableNameInField(wTableNameInField.getSelection());
    inf.setTableNameField(wTableField.getText());
    inf.setUsingBatchUpdates(wbBatch.getSelection());
    inf.setPerformingLookup(wbPerformLookup.getSelection());

    inf.setOperationOrderField(wOperationField.getText());
    inf.setOrderInsert(wOrderInsert.getText());
    inf.setOrderUpdate(wOrderUpdate.getText());
    inf.setOrderDelete(wOrderDelete.getText());

    lookup.getKeyConditions().clear();
    for (TableItem item : wKey.getNonEmptyItems()) {
      SynchronizeAfterMergeMeta.KeyCondition keyCondition =
          new SynchronizeAfterMergeMeta.KeyCondition();
      keyCondition.setColumnName(item.getText(1));
      keyCondition.setCondition(item.getText(2));
      keyCondition.setFieldName(item.getText(3));
      keyCondition.setFieldName2(item.getText(4));
      lookup.getKeyConditions().add(keyCondition);
    }

    lookup.getValueUpdates().clear();
    for (TableItem item : wReturn.getNonEmptyItems()) {
      SynchronizeAfterMergeMeta.ValueUpdate valueUpdate =
          new SynchronizeAfterMergeMeta.ValueUpdate();
      valueUpdate.setColumnName(item.getText(1));
      valueUpdate.setFieldName(item.getText(2));
      valueUpdate.setUpdate("Y".equals(item.getText(3)));
      lookup.getValueUpdates().add(valueUpdate);
    }

    transformName = wTransformName.getText(); // return value
  }

  private void ok() {
    if (Utils.isEmpty(wTransformName.getText())) {
      return;
    }

    // Get the information for the dialog into the input structure.
    getInfo(input);

    if (Strings.isNullOrEmpty(input.getConnection())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogTitle"));
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
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.Log.LookingAtConnection")
                + databaseMeta.toString());
      }

      DatabaseExplorerDialog std =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
      if (std.open()) {
        wSchema.setText(Const.NVL(std.getSchemaName(), ""));
        wTable.setText(Const.NVL(std.getTableName(), ""));
        wTable.setFocus();
        setTableFieldCombo();
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogMessage"));
      mb.setText(
          BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.InvalidConnection.DialogTitle"));
      mb.open();
    }
  }

  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
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
          BaseMessages.getString(
              PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_TITLE),
          BaseMessages.getString(
              PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_MESSAGE),
          ke);
    }
  }

  private void getUpdate() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null) {
        ITableItemInsertListener listener =
            (tableItem, v) -> {
              tableItem.setText(3, "Y");
              return true;
            };
        BaseTransformDialog.getFieldsFromPrevious(
            r, wReturn, 1, new int[] {1, 2}, new int[] {}, -1, -1, listener);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_TITLE),
          BaseMessages.getString(
              PKG, CONST_SYNCHRONIZE_AFTER_MERGE_DIALOG_FAILED_TO_GET_FIELDS_DIALOG_MESSAGE),
          ke);
    }
  }

  // Generate code for create table...
  // Conversions done by Database
  private void create() {
    try {
      SynchronizeAfterMergeMeta info = new SynchronizeAfterMergeMeta();
      getInfo(info);

      String name = transformName; // new name might not yet be linked to other transforms!
      TransformMeta transformMeta =
          new TransformMeta(
              BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.TransformMeta.Title"),
              name,
              info);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);

      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);

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
              BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.NoSQLNeeds.DialogMessage"));
          mb.setText(
              BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.NoSQLNeeds.DialogTitle"));
          mb.open();
        }
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.SQLError.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.CouldNotBuildSQL.DialogTitle"),
          BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.CouldNotBuildSQL.DialogMessage"),
          ke);
    }
  }

  private void getSchemaNames() {
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
    if (databaseMeta != null) {
      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();
        String[] schemas = database.getSchemas();

        if (null != schemas && schemas.length > 0) {
          String[] sortedSchemas = Const.sortStrings(schemas);
          EnterSelectionDialog dialog =
              new EnterSelectionDialog(
                  shell,
                  sortedSchemas,
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMergeDialog.AvailableSchemas.Title",
                      wConnection.getText()),
                  BaseMessages.getString(
                      PKG,
                      "SynchronizeAfterMergeDialog.AvailableSchemas.Message",
                      wConnection.getText()));
          String d = dialog.open();
          if (d != null) {
            wSchema.setText(Const.NVL(d, ""));
            setTableFieldCombo();
          }

        } else {
          MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
          mb.setMessage(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.NoSchema.Error"));
          mb.setText(BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.GetSchemas.Error"));
          mb.open();
        }
      } catch (Exception e) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "System.Dialog.Error.Title"),
            BaseMessages.getString(PKG, "SynchronizeAfterMergeDialog.ErrorGettingSchemas"),
            e);
      }
    }
  }
}
