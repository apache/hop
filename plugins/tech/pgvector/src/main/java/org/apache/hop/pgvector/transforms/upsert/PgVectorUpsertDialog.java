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
package org.apache.hop.pgvector.transforms.upsert;

import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pgvector.util.PgVectorColumnMapping;
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

public class PgVectorUpsertDialog extends BaseTransformDialog {

  private static final Class<?> PKG = PgVectorUpsertMeta.class;

  private final PgVectorUpsertMeta input;
  private GuiCompositeWidgets widgets;
  private TableView wMappings;
  private boolean loading;

  public PgVectorUpsertDialog(
      Shell parent,
      IVariables variables,
      PgVectorUpsertMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "PgVectorUpsertDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).cancel(e -> cancel()).build();

    changed = input.hasChanged();
    loading = true;

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            PgVectorUpsertMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            input,
            w -> {
              // Extra-group builders run before addScrolledComposite returns, so keep the field
              // assigned for anything they look up.
              widgets = w;
              w.registerExtraGroup(
                  BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Label"),
                  "0200",
                  null,
                  this::addMappingsTable);
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
            input.setColumnMappings(readMappings());
          }
        });

    setFieldComboValues();
    populateMappings();
    loading = false;
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  private void addMappingsTable(Composite parent) {
    ColumnInfo[] columns =
        new ColumnInfo[] {
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Column.Table"),
              ColumnInfo.COLUMN_TYPE_TEXT,
              false),
          new ColumnInfo(
              BaseMessages.getString(PKG, "PgVectorUpsertDialog.Mappings.Column.Stream"),
              ColumnInfo.COLUMN_TYPE_CCOMBO,
              new String[] {},
              true)
        };

    int rows = input.getColumnMappings() != null ? input.getColumnMappings().size() : 0;
    wMappings =
        new TableView(
            variables,
            parent,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columns,
            rows,
            e -> input.setChanged(),
            props);
    FormData fdMappings = new FormData();
    fdMappings.left = new FormAttachment(0, 0);
    fdMappings.right = new FormAttachment(100, 0);
    fdMappings.top = new FormAttachment(0, 0);
    fdMappings.bottom = new FormAttachment(100, 0);
    wMappings.setLayoutData(fdMappings);
  }

  /**
   * Stream field names cannot come from {@code comboValuesMethod}, which is handed only a log
   * channel and a metadata provider, so they are filled once the widgets exist.
   */
  private void setFieldComboValues() {
    try {
      IRowMeta fields = pipelineMeta.getPrevTransformFields(variables, transformName);
      String[] names = fields == null ? new String[0] : fields.getFieldNames();
      setComboItems(PgVectorUpsertMeta.WIDGET_ID_FIELD, names);
      setComboItems(PgVectorUpsertMeta.WIDGET_DOCUMENT_ID_FIELD, names);
      setComboItems(PgVectorUpsertMeta.WIDGET_CHUNK_INDEX_FIELD, names);
      setComboItems(PgVectorUpsertMeta.WIDGET_CONTENT_FIELD, names);
      setComboItems(PgVectorUpsertMeta.WIDGET_EMBEDDING_FIELD, names);
      if (wMappings != null && !wMappings.isDisposed()) {
        wMappings.setColumnInfo(1, new ColumnInfo("", ColumnInfo.COLUMN_TYPE_CCOMBO, names, true));
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

  private void populateMappings() {
    if (wMappings == null || input.getColumnMappings() == null) {
      return;
    }
    for (PgVectorColumnMapping mapping : input.getColumnMappings()) {
      TableItem item = new TableItem(wMappings.table, SWT.NONE);
      item.setText(1, Const.NVL(mapping.getColumnName(), ""));
      item.setText(2, Const.NVL(mapping.getStreamField(), ""));
    }
    wMappings.removeEmptyRows();
    wMappings.setRowNums();
  }

  private List<PgVectorColumnMapping> readMappings() {
    List<PgVectorColumnMapping> mappings = new ArrayList<>();
    if (wMappings == null || wMappings.isDisposed()) {
      return mappings;
    }
    for (TableItem item : wMappings.getNonEmptyItems()) {
      String columnName = item.getText(1);
      String streamField = item.getText(2);
      if (!Utils.isEmpty(columnName) && !Utils.isEmpty(streamField)) {
        mappings.add(new PgVectorColumnMapping(columnName, streamField));
      }
    }
    return mappings;
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
    widgets.getWidgetsContents(input, PgVectorUpsertMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    input.setColumnMappings(readMappings());
    transformName = wTransformName.getText();
    input.setChanged();
    dispose();
  }
}
