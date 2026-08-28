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
package org.apache.hop.pipeline.transforms.databasevaluevalidation;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.SourceToTargetMapping;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterMappingDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeWidgetsListener;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class DatabaseValueValidationDialog extends BaseTransformDialog {
  private static final Class<?> PKG = DatabaseValueValidationMeta.class;

  private final DatabaseValueValidationMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFields;
  private ColumnInfo tableColumnInfo;
  private ColumnInfo streamColumnInfo;

  public DatabaseValueValidationDialog(
      Shell parent,
      IVariables variables,
      DatabaseValueValidationMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "DatabaseValueValidationDialog.Shell.Title"));

    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            DatabaseValueValidationMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w ->
                w.registerExtraGroup(
                    BaseMessages.getString(PKG, "DatabaseValueValidationDialog.Group.Fields"),
                    "0200",
                    null,
                    this::addFieldsTable));
    widgets.setWidgetsListener(
        new IGuiPluginCompositeWidgetsListener() {
          @Override
          public void widgetsCreated(GuiCompositeWidgets compositeWidgets) {
            // Mapping table is created through the extra group.
          }

          @Override
          public void widgetsPopulated(GuiCompositeWidgets compositeWidgets) {
            populateFieldsTable();
            refreshCombos();
          }

          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
            if ("connectionName".equals(widgetId)
                || "schemaName".equals(widgetId)
                || "tableName".equals(widgetId)
                || "browseTable".equals(widgetId)) {
              refreshCombos();
            }
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            persistFieldsTable();
          }
        });
    widgets.setCompositeButtonsListener(
        sourceObject -> {
          widgets.getWidgetsContents(
              input, DatabaseValueValidationMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
          persistFieldsTable();
        });

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFieldsTable(Composite parent) {
    PropsUi props = PropsUi.getInstance();
    int margin = PropsUi.getMargin();
    ModifyListener lsMod = e -> input.setChanged();

    Button wGetFields = new Button(parent, SWT.PUSH);
    wGetFields.setText(
        BaseMessages.getString(PKG, "DatabaseValueValidationDialog.GetFields.Button"));
    FormData fdGet = new FormData();
    fdGet.top = new FormAttachment(0, 0);
    fdGet.right = new FormAttachment(100, 0);
    wGetFields.setLayoutData(fdGet);
    wGetFields.addListener(SWT.Selection, e -> getFields());

    Button wDoMapping = new Button(parent, SWT.PUSH);
    wDoMapping.setText(
        BaseMessages.getString(PKG, "DatabaseValueValidationDialog.DoMapping.Button"));
    FormData fdMap = new FormData();
    fdMap.top = new FormAttachment(wGetFields, margin);
    fdMap.right = new FormAttachment(100, 0);
    wDoMapping.setLayoutData(fdMap);
    wDoMapping.addListener(SWT.Selection, e -> generateMappings());

    tableColumnInfo =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ColumnInfo.TableField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);
    streamColumnInfo =
        new ColumnInfo(
            BaseMessages.getString(PKG, "DatabaseValueValidationDialog.ColumnInfo.StreamField"),
            ColumnInfo.COLUMN_TYPE_CCOMBO,
            new String[] {""},
            false);

    int rows =
        input.getFields() != null && !input.getFields().isEmpty() ? input.getFields().size() : 1;
    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.V_SCROLL | SWT.H_SCROLL,
            new ColumnInfo[] {tableColumnInfo, streamColumnInfo},
            rows,
            lsMod,
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(wGetFields, -margin);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  private void populateFieldsTable() {
    if (wFields == null || wFields.isDisposed()) {
      return;
    }
    wFields.clearAll();
    if (input.getFields() != null) {
      for (DatabaseValueValidationField field : input.getFields()) {
        TableItem item = new TableItem(wFields.table, SWT.NONE);
        item.setText(1, Const.NVL(field.getFieldDatabase(), ""));
        item.setText(2, Const.NVL(field.getFieldStream(), ""));
      }
    }
    if (wFields.table.getItemCount() == 0) {
      new TableItem(wFields.table, SWT.NONE);
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private void persistFieldsTable() {
    if (wFields == null || wFields.isDisposed()) {
      return;
    }
    List<DatabaseValueValidationField> fields = new ArrayList<>();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      String tableField = item.getText(1);
      String streamField = item.getText(2);
      if (Utils.isEmpty(tableField) && Utils.isEmpty(streamField)) {
        continue;
      }
      fields.add(new DatabaseValueValidationField(tableField, streamField));
    }
    input.setFields(fields);
  }

  private void refreshCombos() {
    String[] streamNames = new String[0];
    try {
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (prev != null) {
        streamNames = prev.getFieldNames();
      }
    } catch (HopException ignored) {
      // Combos stay empty until the previous transform has fields.
    }
    if (streamColumnInfo != null) {
      streamColumnInfo.setComboValues(streamNames);
    }

    String[] tableNames = new String[0];
    try {
      widgets.getWidgetsContents(input, DatabaseValueValidationMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      IRowMeta tableFields = input.loadTableFields(variables, metadataProvider);
      if (tableFields != null) {
        tableNames = tableFields.getFieldNames();
      }
    } catch (Exception ignored) {
      // Table may not exist yet while the user is still filling in the dialog.
    }
    if (tableColumnInfo != null) {
      tableColumnInfo.setComboValues(tableNames);
    }
  }

  private void getFields() {
    try {
      IRowMeta prev = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (prev != null && !prev.isEmpty()) {
        BaseTransformDialog.getFieldsFromPrevious(
            prev, wFields, 1, new int[] {1, 2}, new int[] {}, -1, -1, null);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.FailedToGetFields.Title"),
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.FailedToGetFields.Message"),
          e);
    }
  }

  private void generateMappings() {
    IRowMeta sourceFields;
    IRowMeta targetFields;
    try {
      sourceFields = pipelineMeta.getPrevTransformFields(variables, transformMeta);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.DoMapping.Source.Title"),
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.DoMapping.Source.Message"),
          e);
      return;
    }
    try {
      widgets.getWidgetsContents(input, DatabaseValueValidationMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      persistFieldsTable();
      targetFields = input.loadTableFields(variables, metadataProvider);
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.DoMapping.Target.Title"),
          BaseMessages.getString(PKG, "DatabaseValueValidationDialog.DoMapping.Target.Message"),
          e);
      return;
    }

    List<SourceToTargetMapping> mappings = new ArrayList<>();
    for (int i = 0; i < wFields.nrNonEmpty(); i++) {
      TableItem item = wFields.getNonEmpty(i);
      int sourceIndex = sourceFields.indexOfValue(item.getText(2));
      int targetIndex = targetFields.indexOfValue(item.getText(1));
      if (sourceIndex >= 0 && targetIndex >= 0) {
        mappings.add(new SourceToTargetMapping(sourceIndex, targetIndex));
      }
    }
    EnterMappingDialog dialog =
        new EnterMappingDialog(
            shell, sourceFields.getFieldNames(), targetFields.getFieldNames(), mappings);
    mappings = dialog.open();
    if (mappings == null) {
      return;
    }
    wFields.table.removeAll();
    wFields.table.setItemCount(mappings.size());
    for (int i = 0; i < mappings.size(); i++) {
      SourceToTargetMapping mapping = mappings.get(i);
      TableItem item = wFields.table.getItem(i);
      IValueMeta target = targetFields.getValueMeta(mapping.getTargetPosition());
      IValueMeta source = sourceFields.getValueMeta(mapping.getSourcePosition());
      item.setText(1, target.getName());
      item.setText(2, source.getName());
    }
    wFields.setRowNums();
    wFields.optWidth(true);
    input.setChanged();
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
    widgets.getWidgetsContents(input, DatabaseValueValidationMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    persistFieldsTable();
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
