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

package org.apache.hop.parquet.transforms.output;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.NamingSchemeTypes;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class ParquetOutputDialog extends BaseTransformDialog {

  public static final Class<?> PKG = ParquetOutputMeta.class;

  private final ParquetOutputMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFields;
  private TableView wPartitionFields;

  public ParquetOutputDialog(
      Shell parent,
      IVariables variables,
      ParquetOutputMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "ParquetOutput.Name"));

    changed = input.hasChanged();

    buildButtonBar().ok(e -> ok()).get(e -> getFields()).cancel(e -> cancel()).build();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            ParquetOutputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run during createCompositeWidgets, before
              // addScrolledComposite returns. Keep the field assigned so they can
              // look up widgets already placed on the same tab.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "ParquetOutputMeta.Group.Partitioning"),
                  "0300",
                  null,
                  this::addPartitionFieldsTable);
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "ParquetOutputMeta.Group.Fields"),
                  "0400",
                  null,
                  this::addFieldsTable);
            });
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            if (!loading) {
              input.setChanged();
            }
            if (ParquetOutputMeta.WIDGET_FILENAME_INCLUDE_DATETIME.equals(widgetId)
                || ParquetOutputMeta.WIDGET_FILENAME_INCLUDE_SPLIT_NR.equals(widgetId)) {
              enableFields();
            }
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            persistTables();
            persistDescribedEnums();
          }
        });

    applyDescribedEnumCombos();
    enableExpandedIntegers();
    populateTables();
    setFieldComboValues();
    enableFields();
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addPartitionFieldsTable(Composite parent) {
    Control last = widgets.getWidgetsMap().get(ParquetOutputMeta.WIDGET_MAX_OPEN_PARTITIONS);

    Label wlPartitionFields = new Label(parent, SWT.LEFT);
    wlPartitionFields.setText(
        BaseMessages.getString(PKG, "ParquetOutputDialog.PartitionFields.Label"));
    PropsUi.setLook(wlPartitionFields);
    FormData fdlPartitionFields = new FormData();
    fdlPartitionFields.left = new FormAttachment(0, 0);
    fdlPartitionFields.right = new FormAttachment(100, 0);
    fdlPartitionFields.top =
        last == null ? new FormAttachment(0, 0) : new FormAttachment(last, margin);
    wlPartitionFields.setLayoutData(fdlPartitionFields);

    ColumnInfo[] partitionColumns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetOutputDialog.PartitionFieldsColumn.Field.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0]),
        };
    wPartitionFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            partitionColumns,
            input.getPartitionFields() == null ? 0 : input.getPartitionFields().size(),
            false,
            e -> {
              if (!loading) {
                input.setChanged();
              }
              enableFields();
            },
            props);
    FormData fdPartitionFields = new FormData();
    fdPartitionFields.left = new FormAttachment(0, 0);
    fdPartitionFields.top = new FormAttachment(wlPartitionFields, margin);
    fdPartitionFields.right = new FormAttachment(100, 0);
    fdPartitionFields.bottom = new FormAttachment(100, 0);
    wPartitionFields.setLayoutData(fdPartitionFields);
  }

  private void addFieldsTable(Composite parent) {
    Label wlFields = new Label(parent, SWT.LEFT);
    wlFields.setText(BaseMessages.getString(PKG, "ParquetOutputDialog.Fields.Label"));
    PropsUi.setLook(wlFields);
    FormData fdlFields = new FormData();
    fdlFields.left = new FormAttachment(0, 0);
    fdlFields.right = new FormAttachment(100, 0);
    fdlFields.top = new FormAttachment(0, 0);
    wlFields.setLayoutData(fdlFields);

    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsColumn.SourceField.Label"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[0]),
          new ColumnInfo(
              BaseMessages.getString(PKG, "ParquetOutputDialog.FieldsColumn.TargetField.Label"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false,
              false),
        };
    columns[1].setNamingSchemeType(NamingSchemeTypes.HOP_FIELD);
    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            input.getFields() == null ? 0 : input.getFields().size(),
            false,
            e -> {
              if (!loading) {
                input.setChanged();
              }
            },
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.top = new FormAttachment(wlFields, margin);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  private void applyDescribedEnumCombos() {
    widgets.setComboValues(ParquetOutputMeta.WIDGET_VERSION, ParquetVersion.getDescriptions());
    setComboText(ParquetOutputMeta.WIDGET_VERSION, input.getVersionDescription());
    widgets.setComboValues(ParquetOutputMeta.WIDGET_WRITE_MODE, ParquetWriteMode.getDescriptions());
    setComboText(ParquetOutputMeta.WIDGET_WRITE_MODE, input.getWriteModeDescription());
  }

  private void enableExpandedIntegers() {
    enableExpandedInteger(ParquetOutputMeta.WIDGET_FILE_SPLIT_SIZE);
    enableExpandedInteger(ParquetOutputMeta.WIDGET_ROW_GROUP_SIZE);
    enableExpandedInteger(ParquetOutputMeta.WIDGET_DATA_PAGE_SIZE);
    enableExpandedInteger(ParquetOutputMeta.WIDGET_DICTIONARY_PAGE_SIZE);
    enableExpandedInteger(ParquetOutputMeta.WIDGET_MAX_OPEN_PARTITIONS);
  }

  private void enableExpandedInteger(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof TextVar textVar) {
      textVar.enableExpandedInteger();
    }
  }

  private void populateTables() {
    if (wFields != null && !wFields.isDisposed()) {
      wFields.clearAll();
      if (input.getFields() != null) {
        for (ParquetField field : input.getFields()) {
          TableItem item = new TableItem(wFields.table, SWT.NONE);
          item.setText(1, Const.NVL(field.getSourceFieldName(), ""));
          item.setText(2, Const.NVL(field.getTargetFieldName(), ""));
        }
      }
      wFields.optimizeTableView();
    }

    if (wPartitionFields != null && !wPartitionFields.isDisposed()) {
      wPartitionFields.clearAll();
      if (input.getPartitionFields() != null) {
        for (ParquetPartitionField field : input.getPartitionFields()) {
          TableItem item = new TableItem(wPartitionFields.table, SWT.NONE);
          item.setText(1, Const.NVL(field.getName(), ""));
        }
      }
      wPartitionFields.optimizeTableView();
    }
  }

  private void persistTables() {
    if (wFields != null && !wFields.isDisposed()) {
      List<ParquetField> fields = new ArrayList<>();
      for (TableItem item : wFields.getNonEmptyItems()) {
        fields.add(new ParquetField(item.getText(1), item.getText(2)));
      }
      input.setFields(fields);
    }
    if (wPartitionFields != null && !wPartitionFields.isDisposed()) {
      List<ParquetPartitionField> partitionFields = new ArrayList<>();
      for (TableItem item : wPartitionFields.getNonEmptyItems()) {
        partitionFields.add(new ParquetPartitionField(item.getText(1)));
      }
      input.setPartitionFields(partitionFields);
    }
  }

  private void persistDescribedEnums() {
    input.setVersion(
        ParquetVersion.getVersionFromDescription(getComboText(ParquetOutputMeta.WIDGET_VERSION)));
    input.setWriteMode(
        ParquetWriteMode.getModeFromDescription(getComboText(ParquetOutputMeta.WIDGET_WRITE_MODE)));
  }

  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      if (wFields != null && !wFields.isDisposed()) {
        wFields.getColumns()[0].setComboValues(names);
      }
      if (wPartitionFields != null && !wPartitionFields.isDisposed()) {
        wPartitionFields.getColumns()[0].setComboValues(names);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
  }

  private void enableFields() {
    boolean includeDateTime = isChecked(ParquetOutputMeta.WIDGET_FILENAME_INCLUDE_DATETIME);
    setEnabled(ParquetOutputMeta.WIDGET_FILENAME_DATETIME_FORMAT, includeDateTime);

    boolean includeSplit = isChecked(ParquetOutputMeta.WIDGET_FILENAME_INCLUDE_SPLIT_NR);
    setEnabled(ParquetOutputMeta.WIDGET_FILE_SPLIT_SIZE, includeSplit);

    boolean partitioning =
        wPartitionFields != null
            && !wPartitionFields.isDisposed()
            && !wPartitionFields.getNonEmptyItems().isEmpty();
    setEnabled(ParquetOutputMeta.WIDGET_WRITE_MODE, partitioning);
    setEnabled(ParquetOutputMeta.WIDGET_MAX_OPEN_PARTITIONS, partitioning);
  }

  private boolean isChecked(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    return control instanceof Button button && button.getSelection();
  }

  private void setEnabled(String widgetId, boolean enabled) {
    Control label = widgets.getLabelsMap().get(widgetId);
    if (label != null && !label.isDisposed()) {
      label.setEnabled(enabled);
    }
    Control widget = widgets.getWidgetsMap().get(widgetId);
    if (widget != null && !widget.isDisposed()) {
      widget.setEnabled(enabled);
    }
  }

  private void setComboText(String widgetId, String text) {
    String value = Const.NVL(text, "");
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar) {
      comboVar.setText(value);
    } else if (control instanceof Combo combo) {
      combo.setText(value);
    }
  }

  private String getComboText(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar) {
      return Const.NVL(comboVar.getText(), "");
    }
    if (control instanceof Combo combo) {
      return Const.NVL(combo.getText(), "");
    }
    return "";
  }

  private void getFields() {
    try {
      IRowMeta rowMeta = pipelineMeta.getPrevTransformFields(variables, transformName);
      BaseTransformDialog.getFieldsFromPrevious(
          rowMeta, wFields, 2, new int[] {1, 2}, new int[0], -1, -1, true, null);
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "ParquetOutputDialog.FailedToGetFields.Title"),
          BaseMessages.getString(PKG, "ParquetOutputDialog.FailedToGetFields.Message"),
          e);
    }
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

    widgets.getWidgetsContents(input, ParquetOutputMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    persistTables();
    persistDescribedEnums();
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
