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
package org.apache.hop.ai.transforms.structuredextract;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;

public class StructuredExtractDialog extends BaseTransformDialog {

  private static final Class<?> PKG = StructuredExtractMeta.class;

  /** Types a language model can actually return, so the grid cannot offer one that will fail. */
  private static final String[] SUPPORTED_TYPES = {
    "String", "Integer", "Number", "BigNumber", "Boolean", "Date", "Timestamp"
  };

  private final StructuredExtractMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFields;
  private boolean loading;

  public StructuredExtractDialog(
      Shell parent,
      IVariables variables,
      StructuredExtractMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "StructuredExtractDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            StructuredExtractMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run before addScrolledComposite returns, so keep the field
              // assigned for anything they look up.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Label"),
                  "0200",
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
          }

          @Override
          public void persistContents(GuiCompositeWidgets compositeWidgets) {
            input.setFields(readFields());
          }
        });

    setFieldComboValues();
    populateFields();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFieldsTable(Composite parent) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Column.Name"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Column.Type"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              SUPPORTED_TYPES,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Column.Description"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Column.Required"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {"Y", "N"},
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "StructuredExtractDialog.Fields.Column.AllowedValues"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false)
        };

    int rows = input.getFields() != null ? input.getFields().size() : 0;
    wFields =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            e -> input.setChanged(),
            props);
    FormData fdFields = new FormData();
    fdFields.left = new FormAttachment(0, 0);
    fdFields.right = new FormAttachment(100, 0);
    fdFields.top = new FormAttachment(0, 0);
    fdFields.bottom = new FormAttachment(100, 0);
    wFields.setLayoutData(fdFields);
  }

  /**
   * Stream field names cannot come from {@code comboValuesMethod}, which is handed only a log
   * channel and a metadata provider, so the input field combo is filled once the widgets exist.
   */
  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      setComboItems(StructuredExtractMeta.WIDGET_INPUT_FIELD, names);
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
  }

  /** Fills a combo without losing the selection the transform was saved with. */
  private void setComboItems(String widgetId, String[] names) {
    // Setting items clears the widget's text, so put the saved selection back afterwards.
    String selected = comboText(widgetId);
    widgets.setComboValues(widgetId, names);
    if (!Utils.isEmpty(selected)) {
      setComboText(widgetId, selected);
    }
  }

  /**
   * {@code GuiCompositeWidgets} builds a plain SWT {@link Combo} when the element has no variable
   * support and a {@link ComboVar} when it does, so both have to be handled.
   */
  private String comboText(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof ComboVar comboVar && !comboVar.isDisposed()) {
      return comboVar.getText();
    }
    if (control instanceof Combo combo && !combo.isDisposed()) {
      return combo.getText();
    }
    return "";
  }

  private void setComboText(String widgetId, String text) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control == null || control.isDisposed()) {
      return;
    }
    if (control instanceof ComboVar comboVar) {
      comboVar.setText(text);
    } else if (control instanceof Combo combo) {
      combo.setText(text);
    }
  }

  private void populateFields() {
    if (wFields == null || input.getFields() == null) {
      return;
    }
    for (StructuredExtractField field : input.getFields()) {
      TableItem item = new TableItem(wFields.table, SWT.NONE);
      item.setText(1, Const.NVL(field.getName(), ""));
      item.setText(2, Const.NVL(field.getType(), "String"));
      item.setText(3, Const.NVL(field.getDescription(), ""));
      item.setText(4, field.isRequired() ? "Y" : "N");
      item.setText(5, Const.NVL(field.getAllowedValues(), ""));
    }
    wFields.removeEmptyRows();
    wFields.setRowNums();
    wFields.optWidth(true);
  }

  private List<StructuredExtractField> readFields() {
    List<StructuredExtractField> fields = new ArrayList<>();
    if (wFields == null || wFields.isDisposed()) {
      return fields;
    }
    for (TableItem item : wFields.getNonEmptyItems()) {
      String name = item.getText(1);
      if (Utils.isEmpty(name)) {
        continue;
      }
      StructuredExtractField field = new StructuredExtractField();
      field.setName(name);
      field.setType(defaultedType(item.getText(2)));
      field.setDescription(item.getText(3));
      field.setRequired(!"N".equalsIgnoreCase(item.getText(4)));
      field.setAllowedValues(item.getText(5));
      fields.add(field);
    }
    return fields;
  }

  /** A blank type column means String, which is what an unfilled row most likely wants. */
  private static String defaultedType(String type) {
    if (Utils.isEmpty(type)) {
      return "String";
    }
    return ValueMetaFactory.getValueMetaName(ValueMetaFactory.getIdForValueMeta(type.trim()));
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
    widgets.getWidgetsContents(input, StructuredExtractMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setFields(readFields());
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
