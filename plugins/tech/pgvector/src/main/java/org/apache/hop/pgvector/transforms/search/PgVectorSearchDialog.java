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
package org.apache.hop.pgvector.transforms.search;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.PgVectorSearchFilter;
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

public class PgVectorSearchDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PgVectorSearchMeta.class;
  private static final String CONST_COMBO_YES = "System.Combo.Yes";
  private static final String CONST_COMBO_NO = "System.Combo.No";

  private final PgVectorSearchMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wFilters;
  private boolean loading;

  public PgVectorSearchDialog(
      Shell parent,
      IVariables variables,
      PgVectorSearchMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PgVectorSearchDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            PgVectorSearchMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run before addScrolledComposite returns, so keep the field
              // assigned for anything they look up.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Label"),
                  "0200",
                  null,
                  this::addFiltersTable);
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
            input.setFilters(readFilters());
          }
        });

    setFieldComboValues();
    populateFilters();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addFiltersTable(Composite parent) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.Table"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.Stream"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              true),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.SkipIfEmpty"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {
                BaseMessages.getString(PKG, CONST_COMBO_YES),
                BaseMessages.getString(PKG, CONST_COMBO_NO)
              },
              true)
        };
    columns[2].setToolTip(
        BaseMessages.getString(PKG, "PgVectorSearchDialog.Filters.Column.SkipIfEmpty.Tooltip"));

    int rows = input.getFilters() != null ? input.getFilters().size() : 0;
    wFilters =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            e -> input.setChanged(),
            props);
    FormData fdFilters = new FormData();
    fdFilters.left = new FormAttachment(0, 0);
    fdFilters.right = new FormAttachment(100, 0);
    fdFilters.top = new FormAttachment(0, 0);
    fdFilters.bottom = new FormAttachment(100, 0);
    wFilters.setLayoutData(fdFilters);
  }

  /**
   * Stream field names cannot come from {@code comboValuesMethod}, which is handed only a log
   * channel and a metadata provider, so they are filled once the widgets exist.
   */
  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      setComboItems(PgVectorSearchMeta.WIDGET_EMBEDDING_FIELD, names);
      if (wFilters != null && !wFilters.isDisposed()) {
        wFilters.setColumnInfo(1, new ColumnInfo("", ColumnInfo.COLUMN_TYPE_CCOMBO, names, true));
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error getting source fields", e);
    }
  }

  private void setComboItems(String widgetId, String[] names) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control == null || control.isDisposed()) {
      return;
    }
    if (control instanceof ComboVar comboVar) {
      String selected = comboVar.getText();
      comboVar.setItems(names);
      if (!Utils.isEmpty(selected)) {
        comboVar.setText(selected);
      }
    } else if (control instanceof Combo combo) {
      String selected = combo.getText();
      combo.setItems(names);
      if (!Utils.isEmpty(selected)) {
        combo.setText(selected);
      }
    }
  }

  private void populateFilters() {
    if (wFilters == null || input.getFilters() == null) {
      return;
    }
    for (PgVectorSearchFilter filter : input.getFilters()) {
      TableItem item = new TableItem(wFilters.table, SWT.NONE);
      item.setText(1, Const.NVL(filter.getColumnName(), ""));
      item.setText(2, Const.NVL(filter.getStreamField(), ""));
      item.setText(
          3,
          BaseMessages.getString(PKG, filter.isSkipIfEmpty() ? CONST_COMBO_YES : CONST_COMBO_NO));
    }
    wFilters.removeEmptyRows();
    wFilters.setRowNums();
  }

  private List<PgVectorSearchFilter> readFilters() {
    List<PgVectorSearchFilter> filters = new ArrayList<>();
    if (wFilters == null || wFilters.isDisposed()) {
      return filters;
    }
    for (TableItem item : wFilters.getNonEmptyItems()) {
      String columnName = item.getText(1);
      String streamField = item.getText(2);
      boolean skipIfEmpty =
          BaseMessages.getString(PKG, CONST_COMBO_YES).equalsIgnoreCase(item.getText(3));
      if (!Utils.isEmpty(columnName) && !Utils.isEmpty(streamField)) {
        filters.add(new PgVectorSearchFilter(columnName, streamField, skipIfEmpty));
      }
    }
    return filters;
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
    widgets.getWidgetsContents(input, PgVectorSearchMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setFilters(readFilters());
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
