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

package org.apache.hop.pipeline.transforms.mssqlbulkloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.DbCache;
import org.apache.hop.core.Props;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.SqlStatement;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.ITransformMeta;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.database.dialog.SqlEditor;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class MsSqlServerBulkLoaderDialog extends BaseTransformDialog {

  private static final Class<?> PKG = MsSqlServerBulkLoaderMeta.class;
  private static final String CONST_SYSTEM_DIALOG_ERROR_TITLE = "System.Dialog.Error.Title";

  private final MsSqlServerBulkLoaderMeta input;
  private final Map<String, Integer> inputFields;

  private MetaSelectionLine<DatabaseMeta> wConnection;
  private TextVar wSchema;
  private TextVar wTable;
  private TextVar wBatchSize;
  private TextVar wBulkCopyTimeout;
  private Button wTruncate;
  private Button wOnlyWhenHaveRows;
  private Button wTableLock;
  private Button wKeepIdentity;
  private Button wKeepNulls;
  private Button wCheckConstraints;
  private Button wFireTriggers;
  private Button wAllowEncryptedValueModifications;
  private Button wSpecifyFields;
  private TableView wFields;
  private Button wGetFields;
  private Button wDoMapping;

  private ColumnInfo[] ciFields;

  /** The columns whose drop-down has to be filled with the target table's field names. */
  private final List<ColumnInfo> tableFieldColumns = new ArrayList<>();

  public MsSqlServerBulkLoaderDialog(
      Shell parent,
      IVariables variables,
      MsSqlServerBulkLoaderMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
    inputFields = new HashMap<>();
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).sql(e -> sql()).cancel(e -> cancel()).build();

    ScrolledComposite scrolledComposite = new ScrolledComposite(shell, SWT.V_SCROLL | SWT.H_SCROLL);
    PropsUi.setLook(scrolledComposite);
    FormData fdScrolledComposite = new FormData();
    fdScrolledComposite.left = new FormAttachment(0, 0);
    fdScrolledComposite.top = new FormAttachment(wSpacer, 0);
    fdScrolledComposite.right = new FormAttachment(100, 0);
    fdScrolledComposite.bottom = new FormAttachment(wOk, -margin);
    scrolledComposite.setLayoutData(fdScrolledComposite);
    scrolledComposite.setLayout(new FillLayout());

    Composite wContent = new Composite(scrolledComposite, SWT.NONE);
    PropsUi.setLook(wContent);
    FormLayout contentLayout = new FormLayout();
    contentLayout.marginWidth = PropsUi.getFormMargin();
    contentLayout.marginHeight = PropsUi.getFormMargin();
    wContent.setLayout(contentLayout);

    ModifyListener lsMod = e -> input.setChanged();
    SelectionAdapter lsSelMod =
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            input.setChanged();
          }
        };
    FocusListener lsFocusLost =
        new FocusAdapter() {
          @Override
          public void focusLost(FocusEvent event) {
            setTableFieldCombo();
          }
        };
    changed = input.hasChanged();

    wConnection = addConnectionLine(wContent, null, input.getConnection(), lsMod);
    if (input.getConnection() == null) {
      wConnection.select(0);
    }
    wConnection.addModifyListener(lsMod);

    CTabFolder wTabFolder = new CTabFolder(wContent, SWT.BORDER);
    PropsUi.setLook(wTabFolder, Props.WIDGET_STYLE_TAB);

    Composite wSettingsComp = addSettingsTab(wTabFolder, lsMod, lsSelMod, lsFocusLost);
    addFieldsTab(wTabFolder, lsMod, lsSelMod);

    FormData fdTabFolder = new FormData();
    fdTabFolder.left = new FormAttachment(0, 0);
    fdTabFolder.top = new FormAttachment(wConnection, margin * 2);
    fdTabFolder.right = new FormAttachment(100, 0);
    fdTabFolder.bottom = new FormAttachment(100, 0);
    wTabFolder.setLayoutData(fdTabFolder);
    wTabFolder.setSelection(0);
    wSettingsComp.layout();

    // Look up the incoming fields in the background, they feed the stream field drop-down.
    BackgroundThreadFacade.start(
        () -> {
          TransformMeta stepMeta = pipelineMeta.findTransform(transformName);
          if (stepMeta == null) {
            return;
          }
          try {
            IRowMeta row = pipelineMeta.getPrevTransformFields(variables, stepMeta);
            for (int i = 0; i < row.size(); i++) {
              inputFields.put(row.getValueMeta(i).getName(), i);
            }
            setComboBoxes();
          } catch (HopException e) {
            logError(BaseMessages.getString(PKG, "System.Dialog.GetFieldsFailed.Message"));
          }
        });

    wContent.pack();
    Rectangle bounds = wContent.getBounds();
    scrolledComposite.setContent(wContent);
    scrolledComposite.setExpandHorizontal(true);
    scrolledComposite.setExpandVertical(true);
    scrolledComposite.setMinWidth(bounds.width);
    scrolledComposite.setMinHeight(bounds.height);

    setSize();

    getData();
    setTableFieldCombo();
    input.setChanged(changed);
    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return transformName;
  }

  private Composite addSettingsTab(
      CTabFolder wTabFolder,
      ModifyListener lsMod,
      SelectionAdapter lsSelMod,
      FocusListener lsFocusLost) {

    CTabItem wSettingsTab = new CTabItem(wTabFolder, SWT.NONE);
    wSettingsTab.setFont(GuiResource.getInstance().getFontDefault());
    wSettingsTab.setText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.Tab.Settings.Label"));

    Composite wComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wComp);
    FormLayout tabLayout = new FormLayout();
    tabLayout.marginWidth = PropsUi.getFormMargin();
    tabLayout.marginHeight = PropsUi.getFormMargin();
    wComp.setLayout(tabLayout);

    // Target schema
    Label wlSchema = new Label(wComp, SWT.RIGHT);
    wlSchema.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.TargetSchema.Label"));
    PropsUi.setLook(wlSchema);
    FormData fdlSchema = new FormData();
    fdlSchema.left = new FormAttachment(0, 0);
    fdlSchema.right = new FormAttachment(middle, -margin);
    fdlSchema.top = new FormAttachment(0, margin);
    wlSchema.setLayoutData(fdlSchema);

    wSchema = new TextVar(variables, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wSchema);
    wSchema.addModifyListener(lsMod);
    wSchema.addFocusListener(lsFocusLost);
    FormData fdSchema = new FormData();
    fdSchema.left = new FormAttachment(middle, 0);
    fdSchema.top = new FormAttachment(wlSchema, 0, SWT.CENTER);
    fdSchema.right = new FormAttachment(100, 0);
    wSchema.setLayoutData(fdSchema);

    // Target table, with a browse button onto the database explorer
    Label wlTable = new Label(wComp, SWT.RIGHT);
    wlTable.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.TargetTable.Label"));
    PropsUi.setLook(wlTable);
    FormData fdlTable = new FormData();
    fdlTable.left = new FormAttachment(0, 0);
    fdlTable.right = new FormAttachment(middle, -margin);
    fdlTable.top = new FormAttachment(wSchema, margin);
    wlTable.setLayoutData(fdlTable);

    Button wbTable = new Button(wComp, SWT.PUSH | SWT.CENTER);
    PropsUi.setLook(wbTable);
    wbTable.setText(BaseMessages.getString("System.Button.Browse"));
    FormData fdbTable = new FormData();
    fdbTable.right = new FormAttachment(100, 0);
    fdbTable.top = new FormAttachment(wSchema, margin);
    wbTable.setLayoutData(fdbTable);
    wbTable.addListener(SWT.Selection, e -> getTableName());

    wTable = new TextVar(variables, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(wTable);
    wTable.addModifyListener(lsMod);
    wTable.addFocusListener(lsFocusLost);
    FormData fdTable = new FormData();
    fdTable.left = new FormAttachment(middle, 0);
    fdTable.top = new FormAttachment(wSchema, margin);
    fdTable.right = new FormAttachment(wbTable, -margin);
    wTable.setLayoutData(fdTable);

    // Batch size
    Label wlBatchSize = new Label(wComp, SWT.RIGHT);
    wlBatchSize.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BatchSize.Label"));
    wlBatchSize.setToolTipText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BatchSize.Tooltip"));
    PropsUi.setLook(wlBatchSize);
    FormData fdlBatchSize = new FormData();
    fdlBatchSize.left = new FormAttachment(0, 0);
    fdlBatchSize.right = new FormAttachment(middle, -margin);
    fdlBatchSize.top = new FormAttachment(wTable, margin);
    wlBatchSize.setLayoutData(fdlBatchSize);

    wBatchSize = new TextVar(variables, wComp, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBatchSize.setToolTipText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BatchSize.Tooltip"));
    PropsUi.setLook(wBatchSize);
    wBatchSize.addModifyListener(lsMod);
    FormData fdBatchSize = new FormData();
    fdBatchSize.left = new FormAttachment(middle, 0);
    fdBatchSize.top = new FormAttachment(wlBatchSize, 0, SWT.CENTER);
    fdBatchSize.right = new FormAttachment(100, 0);
    wBatchSize.setLayoutData(fdBatchSize);

    wTruncate =
        addCheckBox(
            wComp,
            wBatchSize,
            "MsSqlServerBulkLoaderDialog.TruncateTable.Label",
            "MsSqlServerBulkLoaderDialog.TruncateTable.Tooltip",
            lsSelMod);
    wOnlyWhenHaveRows =
        addCheckBox(
            wComp,
            wTruncate,
            "MsSqlServerBulkLoaderDialog.OnlyWhenHaveRows.Label",
            "MsSqlServerBulkLoaderDialog.OnlyWhenHaveRows.Tooltip",
            lsSelMod);

    // Everything the driver's SQLServerBulkCopyOptions can be told, in one place.
    Group wOptions = new Group(wComp, SWT.SHADOW_NONE);
    PropsUi.setLook(wOptions);
    wOptions.setText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BulkCopyOptions.Label"));
    FormLayout optionsLayout = new FormLayout();
    optionsLayout.marginWidth = PropsUi.getFormMargin();
    optionsLayout.marginHeight = PropsUi.getFormMargin();
    wOptions.setLayout(optionsLayout);
    FormData fdOptions = new FormData();
    fdOptions.left = new FormAttachment(0, 0);
    fdOptions.right = new FormAttachment(100, 0);
    fdOptions.top = new FormAttachment(wOnlyWhenHaveRows, margin * 2);
    wOptions.setLayoutData(fdOptions);

    wTableLock =
        addCheckBox(
            wOptions,
            null,
            "MsSqlServerBulkLoaderDialog.TableLock.Label",
            "MsSqlServerBulkLoaderDialog.TableLock.Tooltip",
            lsSelMod);
    wKeepIdentity =
        addCheckBox(
            wOptions,
            wTableLock,
            "MsSqlServerBulkLoaderDialog.KeepIdentity.Label",
            "MsSqlServerBulkLoaderDialog.KeepIdentity.Tooltip",
            lsSelMod);
    wKeepNulls =
        addCheckBox(
            wOptions,
            wKeepIdentity,
            "MsSqlServerBulkLoaderDialog.KeepNulls.Label",
            "MsSqlServerBulkLoaderDialog.KeepNulls.Tooltip",
            lsSelMod);
    wCheckConstraints =
        addCheckBox(
            wOptions,
            wKeepNulls,
            "MsSqlServerBulkLoaderDialog.CheckConstraints.Label",
            "MsSqlServerBulkLoaderDialog.CheckConstraints.Tooltip",
            lsSelMod);
    wFireTriggers =
        addCheckBox(
            wOptions,
            wCheckConstraints,
            "MsSqlServerBulkLoaderDialog.FireTriggers.Label",
            "MsSqlServerBulkLoaderDialog.FireTriggers.Tooltip",
            lsSelMod);
    wAllowEncryptedValueModifications =
        addCheckBox(
            wOptions,
            wFireTriggers,
            "MsSqlServerBulkLoaderDialog.AllowEncryptedValueModifications.Label",
            "MsSqlServerBulkLoaderDialog.AllowEncryptedValueModifications.Tooltip",
            lsSelMod);

    Label wlBulkCopyTimeout = new Label(wOptions, SWT.RIGHT);
    wlBulkCopyTimeout.setText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BulkCopyTimeout.Label"));
    wlBulkCopyTimeout.setToolTipText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BulkCopyTimeout.Tooltip"));
    PropsUi.setLook(wlBulkCopyTimeout);
    FormData fdlBulkCopyTimeout = new FormData();
    fdlBulkCopyTimeout.left = new FormAttachment(0, 0);
    fdlBulkCopyTimeout.right = new FormAttachment(middle, -margin);
    fdlBulkCopyTimeout.top = new FormAttachment(wAllowEncryptedValueModifications, margin);
    wlBulkCopyTimeout.setLayoutData(fdlBulkCopyTimeout);

    wBulkCopyTimeout = new TextVar(variables, wOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    wBulkCopyTimeout.setToolTipText(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BulkCopyTimeout.Tooltip"));
    PropsUi.setLook(wBulkCopyTimeout);
    wBulkCopyTimeout.addModifyListener(lsMod);
    FormData fdBulkCopyTimeout = new FormData();
    fdBulkCopyTimeout.left = new FormAttachment(middle, 0);
    fdBulkCopyTimeout.top = new FormAttachment(wlBulkCopyTimeout, 0, SWT.CENTER);
    fdBulkCopyTimeout.right = new FormAttachment(100, 0);
    wBulkCopyTimeout.setLayoutData(fdBulkCopyTimeout);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    wComp.setLayoutData(fdComp);

    wSettingsTab.setControl(wComp);
    return wComp;
  }

  /** Every checkbox on this dialog is a label on the left and a box at the middle. */
  private Button addCheckBox(
      Composite parent,
      Control previous,
      String labelKey,
      String tooltipKey,
      SelectionAdapter lsSelMod) {

    Label label = new Label(parent, SWT.RIGHT);
    label.setText(BaseMessages.getString(PKG, labelKey));
    label.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    PropsUi.setLook(label);
    FormData fdLabel = new FormData();
    fdLabel.left = new FormAttachment(0, 0);
    fdLabel.right = new FormAttachment(middle, -margin);
    fdLabel.top =
        previous == null ? new FormAttachment(0, margin) : new FormAttachment(previous, margin);
    label.setLayoutData(fdLabel);

    Button button = new Button(parent, SWT.CHECK);
    button.setToolTipText(BaseMessages.getString(PKG, tooltipKey));
    PropsUi.setLook(button);
    FormData fdButton = new FormData();
    fdButton.left = new FormAttachment(middle, 0);
    fdButton.top = new FormAttachment(label, 0, SWT.CENTER);
    fdButton.right = new FormAttachment(100, 0);
    button.setLayoutData(fdButton);
    button.addSelectionListener(lsSelMod);
    return button;
  }

  private void addFieldsTab(
      CTabFolder wTabFolder, ModifyListener lsMod, SelectionAdapter lsSelMod) {

    CTabItem wFieldsTab = new CTabItem(wTabFolder, SWT.NONE);
    wFieldsTab.setFont(GuiResource.getInstance().getFontDefault());
    wFieldsTab.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.Tab.Fields.Label"));

    Composite wComp = new Composite(wTabFolder, SWT.NONE);
    PropsUi.setLook(wComp);
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = Const.FORM_MARGIN;
    fieldsLayout.marginHeight = Const.FORM_MARGIN;
    wComp.setLayout(fieldsLayout);

    wSpecifyFields =
        addCheckBox(
            wComp,
            null,
            "MsSqlServerBulkLoaderDialog.SpecifyFields.Label",
            "MsSqlServerBulkLoaderDialog.SpecifyFields.Tooltip",
            lsSelMod);
    wSpecifyFields.addSelectionListener(
        new SelectionAdapter() {
          @Override
          public void widgetSelected(SelectionEvent event) {
            setFlags();
          }
        });

    ciFields = new ColumnInfo[3];
    ciFields[0] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciFields[1] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    ciFields[2] =
        new ColumnInfo(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.ColumnInfo.OrderHint"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            MsSqlServerBulkLoaderMeta.getOrderHintDescriptions(),
            false);
    ciFields[2].setToolTip(
        BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.ColumnInfo.OrderHint.Tooltip"));
    tableFieldColumns.add(ciFields[0]);

    int rows =
        input.getFields() == null || input.getFields().isEmpty() ? 1 : input.getFields().size();
    wFields =
        new TableView(
            variables,
            wComp,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            ciFields,
            rows,
            lsMod,
            props);

    wGetFields = new Button(wComp, SWT.PUSH);
    wGetFields.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.GetFields.Button"));
    FormData fdGetFields = new FormData();
    fdGetFields.top = new FormAttachment(wSpecifyFields, margin * 2);
    fdGetFields.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGetFields);
    wGetFields.addListener(SWT.Selection, e -> get());

    wDoMapping = new Button(wComp, SWT.PUSH);
    wDoMapping.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.DoMapping.Button"));
    FormData fdDoMapping = new FormData();
    fdDoMapping.top = new FormAttachment(wGetFields, margin);
    fdDoMapping.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdDoMapping);
    wDoMapping.addListener(SWT.Selection, e -> generateMappings());

    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wSpecifyFields, margin * 2);
    fdFields.right = new FormAttachment(wDoMapping, -margin);
    fdFields.bottom = new FormAttachment(100, -margin);
    wFields.setLayoutData(fdFields);

    FormData fdComp = new FormData();
    fdComp.left = new FormAttachment(0, 0);
    fdComp.top = new FormAttachment(0, 0);
    fdComp.right = new FormAttachment(100, 0);
    fdComp.bottom = new FormAttachment(100, 0);
    wComp.setLayoutData(fdComp);

    wComp.layout();
    wFieldsTab.setControl(wComp);
  }

  /**
   * Reads the fields from the previous transform and from the target table and lets the user draw
   * the mapping between them.
   */
  private void generateMappings() {
    IRowMeta sourceFields;
    IRowMeta targetFields;

    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Title"),
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.DoMapping.UnableToFindSourceFields.Message"),
          e);
      return;
    }

    // getRequiredFields() reads the meta, so it has to see what the dialog currently shows.
    input.setConnection(wConnection.getText());
    input.setSchemaName(wSchema.getText());
    input.setTableName(wTable.getText());
    ITransformMeta iTransformMeta = transformMeta.getTransform();
    try {
      targetFields = iTransformMeta.getRequiredFields(variables);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Title"),
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.DoMapping.UnableToFindTargetFields.Message"),
          e);
      return;
    }

    List<SourceToTargetMapping> mappings = new ArrayList<>();
    StringBuilder missingSourceFields = new StringBuilder();
    StringBuilder missingTargetFields = new StringBuilder();

    int nrFields = wFields.nrNonEmpty();
    for (int i = 0; i < nrFields; i++) {
      TableItem item = wFields.getNonEmpty(i);
      String target = item.getText(1);
      String source = item.getText(2);

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
      mappings.add(new SourceToTargetMapping(sourceIndex, targetIndex));
    }

    if (missingSourceFields.length() > 0 || missingTargetFields.length() > 0) {
      StringBuilder message = new StringBuilder();
      if (missingSourceFields.length() > 0) {
        message
            .append(
                BaseMessages.getString(
                    PKG,
                    "MsSqlServerBulkLoaderDialog.DoMapping.SomeSourceFieldsNotFound",
                    missingSourceFields.toString()))
            .append(Const.CR);
      }
      if (missingTargetFields.length() > 0) {
        message
            .append(
                BaseMessages.getString(
                    PKG,
                    "MsSqlServerBulkLoaderDialog.DoMapping.SomeTargetFieldsNotFound",
                    missingTargetFields.toString()))
            .append(Const.CR);
      }
      message
          .append(Const.CR)
          .append(
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderDialog.DoMapping.SomeFieldsNotFoundContinue"))
          .append(Const.CR);

      int answer =
          BaseDialog.openMessageBox(
              shell,
              BaseMessages.getString(
                  PKG, "MsSqlServerBulkLoaderDialog.DoMapping.SomeFieldsNotFoundTitle"),
              message.toString(),
              SWT.ICON_QUESTION | SWT.YES | SWT.NO);
      if ((answer & SWT.YES) == 0) {
        return;
      }
    }

    EnterMappingDialog dialog =
        new EnterMappingDialog(
            shell, sourceFields.getFieldNames(), targetFields.getFieldNames(), mappings);
    mappings = dialog.open();

    if (mappings == null) {
      // Cancelled.
      return;
    }

    wFields.table.removeAll();
    wFields.table.setItemCount(mappings.size());
    for (int i = 0; i < mappings.size(); i++) {
      SourceToTargetMapping mapping = mappings.get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, targetFields.getValueMeta(mapping.getTargetPosition()).getName());
      item.setText(2, sourceFields.getValueMeta(mapping.getSourcePosition()).getName());
      item.setText(3, MsSqlServerBulkLoaderMeta.OrderHint.NONE.getDescription());
    }
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void setTableFieldCombo() {
    Runnable fieldLoader =
        () -> {
          for (ColumnInfo fieldColumn : tableFieldColumns) {
            fieldColumn.setComboValues(new String[] {});
          }
          if (StringUtil.isEmpty(wTable.getText())) {
            return;
          }
          DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
          if (databaseMeta == null) {
            return;
          }
          try (Database db = new Database(loggingObject, variables, databaseMeta)) {
            db.connect();
            String schemaTable =
                databaseMeta.getQuotedSchemaTableCombination(
                    variables,
                    variables.resolve(wSchema.getText()),
                    variables.resolve(wTable.getText()));
            IRowMeta r = db.getTableFields(schemaTable);
            if (r != null && r.getFieldNames() != null) {
              for (ColumnInfo fieldColumn : tableFieldColumns) {
                fieldColumn.setComboValues(r.getFieldNames());
              }
            }
          } catch (Exception e) {
            // The drop-downs stay empty, which is not worth interrupting the user over.
            for (ColumnInfo fieldColumn : tableFieldColumns) {
              fieldColumn.setComboValues(new String[] {});
            }
          }
        };
    shell.getDisplay().asyncExec(fieldLoader);
  }

  protected void setComboBoxes() {
    List<String> entries = new ArrayList<>(inputFields.keySet());
    String[] fieldNames = entries.toArray(new String[0]);
    if (PropsUi.getInstance().isSortFieldByName()) {
      Const.sortStrings(fieldNames);
    }
    ciFields[1].setComboValues(fieldNames);
  }

  private void setFlags() {
    boolean specifyFields = wSpecifyFields.getSelection();
    wFields.setEnabled(specifyFields);
    wGetFields.setEnabled(specifyFields);
    wDoMapping.setEnabled(specifyFields);
  }

  /** Copy information from the meta-data input to the dialog fields. */
  private void getData() {
    wConnection.setText(Const.NVL(input.getConnection(), ""));
    wSchema.setText(Const.NVL(input.getSchemaName(), ""));
    wTable.setText(Const.NVL(input.getTableName(), ""));
    wBatchSize.setText(
        Const.NVL(input.getBatchSize(), MsSqlServerBulkLoaderMeta.DEFAULT_BATCH_SIZE));
    wBulkCopyTimeout.setText(Const.NVL(input.getBulkCopyTimeout(), "0"));

    wTruncate.setSelection(input.isTruncateTable());
    wOnlyWhenHaveRows.setSelection(input.isOnlyWhenHaveRows());
    wTableLock.setSelection(input.isTableLock());
    wKeepIdentity.setSelection(input.isKeepIdentity());
    wKeepNulls.setSelection(input.isKeepNulls());
    wCheckConstraints.setSelection(input.isCheckConstraints());
    wFireTriggers.setSelection(input.isFireTriggers());
    wAllowEncryptedValueModifications.setSelection(input.isAllowEncryptedValueModifications());
    wSpecifyFields.setSelection(input.isSpecifyFields());

    for (int i = 0; i < input.getFields().size(); i++) {
      MsSqlServerBulkLoaderMeta.Field field = input.getFields().get(i);
      TableItem item = wFields.table.getItem(i);
      item.setText(1, Const.NVL(field.getFieldTable(), ""));
      item.setText(2, Const.NVL(field.getFieldStream(), ""));
      item.setText(3, field.getOrderHint().getDescription());
    }
    wFields.setRowNums();
    wFields.optWidth(true);

    setFlags();
  }

  private void getInfo(MsSqlServerBulkLoaderMeta info) {
    info.setConnection(wConnection.getText());
    info.setSchemaName(wSchema.getText());
    info.setTableName(wTable.getText());
    info.setBatchSize(wBatchSize.getText());
    info.setBulkCopyTimeout(wBulkCopyTimeout.getText());

    info.setTruncateTable(wTruncate.getSelection());
    info.setOnlyWhenHaveRows(wOnlyWhenHaveRows.getSelection());
    info.setTableLock(wTableLock.getSelection());
    info.setKeepIdentity(wKeepIdentity.getSelection());
    info.setKeepNulls(wKeepNulls.getSelection());
    info.setCheckConstraints(wCheckConstraints.getSelection());
    info.setFireTriggers(wFireTriggers.getSelection());
    info.setAllowEncryptedValueModifications(wAllowEncryptedValueModifications.getSelection());
    info.setSpecifyFields(wSpecifyFields.getSelection());

    info.getFields().clear();
    int nrRows = wFields.nrNonEmpty();
    for (int i = 0; i < nrRows; i++) {
      TableItem item = wFields.getNonEmpty(i);
      MsSqlServerBulkLoaderMeta.Field field =
          new MsSqlServerBulkLoaderMeta.Field(
              Const.NVL(item.getText(1), ""),
              Const.NVL(item.getText(2), ""),
              MsSqlServerBulkLoaderMeta.lookupOrderHint(item.getText(3)));
      info.getFields().add(field);
    }
  }

  private void ok() {
    if (StringUtil.isEmpty(wTransformName.getText())) {
      return;
    }
    transformName = wTransformName.getText();
    getInfo(input);

    if (Utils.isEmpty(input.getConnection())) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.ConnectionError.DialogMessage"));
      mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
      return;
    }
    dispose();
  }

  private void cancel() {
    transformName = null;
    input.setChanged(changed);
    dispose();
  }

  private void getTableName() {
    String connectionName = wConnection.getText();
    if (StringUtil.isEmpty(connectionName)) {
      return;
    }
    DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
    if (databaseMeta == null) {
      MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      mb.setMessage(
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.ConnectionError2.DialogMessage"));
      mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
      mb.open();
      return;
    }

    DatabaseExplorerDialog std =
        new DatabaseExplorerDialog(
            shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
    std.setSelectedSchemaAndTable(wSchema.getText(), wTable.getText());
    if (std.open()) {
      wSchema.setText(Const.NVL(std.getSchemaName(), ""));
      wTable.setText(Const.NVL(std.getTableName(), ""));
    }
  }

  /** Fill the fields grid with the incoming fields, mapped one to one onto same-named columns. */
  private void get() {
    try {
      IRowMeta r = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (r != null && !r.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            r, wFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
        // A blank order hint column would read back as null rather than "no hint".
        String none = MsSqlServerBulkLoaderMeta.OrderHint.NONE.getDescription();
        for (TableItem item : wFields.table.getItems()) {
          if (StringUtil.isEmpty(item.getText(3))) {
            item.setText(3, none);
          }
        }
        wFields.optWidth(true);
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.FailedToGetFields.DialogTitle"),
          BaseMessages.getString(
              PKG, "MsSqlServerBulkLoaderDialog.FailedToGetFields.DialogMessage"),
          ke);
    }
  }

  /** Generate the DDL that would make the target table able to take what this transform writes. */
  private void sql() {
    try {
      MsSqlServerBulkLoaderMeta info = new MsSqlServerBulkLoaderMeta();
      getInfo(info);

      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(wConnection.getText(), variables);
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      TransformMeta stepMeta = pipelineMeta.findTransform(transformName);

      if (info.isSpecifyFields()) {
        IRowMeta prevNew = new RowMeta();
        for (MsSqlServerBulkLoaderMeta.Field field : info.getFields()) {
          IValueMeta insValue = prev.searchValueMeta(field.getFieldStream());
          if (insValue == null) {
            throw new HopTransformException(
                BaseMessages.getString(
                    PKG,
                    "MsSqlServerBulkLoaderDialog.FailedToFindField.Message",
                    field.getFieldStream()));
          }
          IValueMeta insertValue = insValue.clone();
          insertValue.setName(field.getFieldTable());
          prevNew.addValueMeta(insertValue);
        }
        prev = prevNew;
      }

      SqlStatement sql =
          info.getSqlStatements(variables, pipelineMeta, stepMeta, prev, metadataProvider);
      if (sql.hasError()) {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
        mb.setMessage(sql.getError());
        mb.setText(BaseMessages.getString(CONST_SYSTEM_DIALOG_ERROR_TITLE));
        mb.open();
      } else if (sql.hasSql()) {
        SqlEditor sqlEditor =
            new SqlEditor(
                shell, SWT.NONE, variables, databaseMeta, DbCache.getInstance(), sql.getSql());
        sqlEditor.open();
      } else {
        MessageBox mb = new MessageBox(shell, SWT.OK | SWT.ICON_INFORMATION);
        mb.setMessage(
            BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.NoSQL.DialogMessage"));
        mb.setText(BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.NoSQL.DialogTitle"));
        mb.open();
      }
    } catch (HopException ke) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BuildSQLError.DialogTitle"),
          BaseMessages.getString(PKG, "MsSqlServerBulkLoaderDialog.BuildSQLError.DialogMessage"),
          ke);
    }
  }
}
