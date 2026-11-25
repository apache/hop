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

package org.apache.hop.pipeline.transforms.mergeintotable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.dml.DmlSupport;
import org.apache.hop.core.database.dml.UpsertForm;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.database.dialog.DatabaseExplorerDialog;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiCompositeWidgets;
import org.apache.hop.ui.core.gui.GuiCompositeWidgetsAdapter;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.IGuiPluginCompositeButtonsListener;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.MetaSelectionLine;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Link;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

/**
 * The Merge into table dialog. Every option is an annotated widget on {@link MergeIntoTableMeta};
 * this class only adds the field-name lists of the two grids, the database explorer behind Browse,
 * Get fields, and the line that says why a connection can't run the selected upsert.
 */
public class MergeIntoTableDialog extends BaseTransformDialog {
  private static final Class<?> PKG = MergeIntoTableMeta.class;

  private final MergeIntoTableMeta input;

  /**
   * What the widgets show and edit until OK. Browse and Get fields go through the metadata, since
   * the widgets are re-read from it after a button: on this copy, so that Cancel leaves the
   * transform as it was. OK writes the widgets to {@link #input}.
   */
  private MergeIntoTableMeta editing;

  private GuiCompositeWidgets widgets;

  private String[] streamFieldNames = new String[0];

  /** The connection, schema and table the cached column names were read from. */
  private String tableColumnsKey;

  private String[] tableColumnNames = new String[0];

  public MergeIntoTableDialog(
      Shell parent,
      IVariables variables,
      MergeIntoTableMeta transformMeta,
      PipelineMeta pipelineMeta) {
    super(parent, variables, transformMeta, pipelineMeta);
    input = transformMeta;
  }

  @Override
  public String open() {
    createShell(BaseMessages.getString(PKG, "MergeIntoTableDialog.Shell.Title"));
    buildButtonBar().ok(e -> ok()).get(e -> getFields()).cancel(e -> cancel()).build();
    changed = input.hasChanged();
    editing = input.clone();

    widgets =
        GuiCompositeWidgets.addScrolledComposite(
            shell,
            variables,
            wTransformName,
            wOk,
            MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID,
            editing);
    widgets.setWidgetsListener(
        new GuiCompositeWidgetsAdapter() {
          @Override
          public void widgetModified(
              GuiCompositeWidgets compositeWidgets, Control changedWidget, String widgetId) {
            input.setChanged();
            if (MergeIntoTableMeta.WIDGET_CONNECTION.equals(widgetId)
                || MergeIntoTableMeta.WIDGET_UPSERT_FORM.equals(widgetId)) {
              showMergeSupport();
            }
            if (MergeIntoTableMeta.WIDGET_UPDATE_EXISTING.equals(widgetId)) {
              enableFields();
            }
          }
        });
    widgets.setCompositeButtonsListener(
        new IGuiPluginCompositeButtonsListener() {
          @Override
          public void buttonPressed(Object sourceObject) {
            // The button re-reads every widget from the metadata afterwards: save them first.
            widgets.getWidgetsContents(editing, MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
            browseTable();
          }
        });

    installComboSuppliers();
    readStreamFields();
    showMergeSupport();
    enableFields();
    input.setChanged(changed);

    focusTransformName();
    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());
    return transformName;
  }

  /**
   * Shows why the selected connection can't run the selected upsert, under the connection, or
   * nothing when it can. A connection name that is a variable, or one that isn't selected yet,
   * shows nothing: the transform checks it again when it starts.
   */
  private void showMergeSupport() {
    if (!(widgets.getWidgetsMap().get(MergeIntoTableMeta.WIDGET_MERGE_SUPPORT) instanceof Link link)
        || link.isDisposed()) {
      return;
    }
    String reason = null;
    String connectionName = readWidgetText(MergeIntoTableMeta.WIDGET_CONNECTION);
    if (!Utils.isEmpty(connectionName) && !StringUtil.containsVariableToken(connectionName)) {
      DatabaseMeta databaseMeta = findDatabase(connectionName);
      if (databaseMeta != null) {
        reason = DmlSupport.unsupportedReason(databaseMeta, selectedForm());
      }
    }
    String text = reason == null ? "" : reason;
    if (!Objects.equals(text, link.getText())) {
      link.setText(text);
      link.setForeground(GuiResource.getInstance().getColorRed());
      link.getParent().layout(true, true);
    }
  }

  /**
   * @return the upsert form selected in the combo, or null for Automatic
   */
  private UpsertForm selectedForm() {
    Control control = widgets.getWidgetsMap().get(MergeIntoTableMeta.WIDGET_UPSERT_FORM);
    String text = "";
    if (control instanceof Combo combo) {
      text = combo.getText();
    } else if (control instanceof ComboVar comboVar) {
      text = comboVar.getText();
    }
    MergeUpsertForm form =
        IEnumHasCodeAndDescription.lookupDescription(MergeUpsertForm.class, text, null);
    if (form == null) {
      form = IEnumHasCode.lookupCode(MergeUpsertForm.class, text, editing.getUpsertForm());
    }
    return MergeUpsertForm.upsertFormOf(form);
  }

  /** Update only changed rows means nothing when existing rows are never updated. */
  private void enableFields() {
    Control updateExisting = widgets.getWidgetsMap().get(MergeIntoTableMeta.WIDGET_UPDATE_EXISTING);
    Control onlyChanged =
        widgets.getWidgetsMap().get(MergeIntoTableMeta.WIDGET_UPDATE_ONLY_CHANGED);
    if (updateExisting instanceof Button check
        && onlyChanged != null
        && !onlyChanged.isDisposed()) {
      onlyChanged.setEnabled(check.getSelection());
    }
  }

  private void installComboSuppliers() {
    for (String tableId :
        new String[] {
          MergeIntoTableMeta.WIDGET_KEY_FIELDS, MergeIntoTableMeta.WIDGET_VALUE_FIELDS
        }) {
      TableView table = tableView(tableId);
      if (table == null || table.getColumns().length < MergeIntoTableMeta.COLUMN_TABLE_COLUMN) {
        continue;
      }
      table.getColumns()[MergeIntoTableMeta.COLUMN_STREAM_FIELD - 1].setComboValueSupplier(
          () -> streamFieldNames);
      table.getColumns()[MergeIntoTableMeta.COLUMN_TABLE_COLUMN - 1].setComboValueSupplier(
          this::tableColumnNames);
    }
  }

  private void readStreamFields() {
    try {
      IRowMeta previous = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (previous != null) {
        streamFieldNames = ConstUi.sortFieldNames(previous.getFieldNames());
      }
    } catch (HopException e) {
      logError(BaseMessages.getString(PKG, "MergeIntoTableDialog.GetFieldsFailed.Message"));
    }
  }

  /**
   * The columns of the target table, read when a grid first asks for them and again only when the
   * connection, schema or table changed. An unreachable database leaves the list empty, so the
   * column can still be typed.
   */
  private String[] tableColumnNames() {
    String connectionName = readWidgetText(MergeIntoTableMeta.WIDGET_CONNECTION);
    String schema = variables.resolve(readWidgetText(MergeIntoTableMeta.WIDGET_SCHEMA));
    String table = variables.resolve(readWidgetText(MergeIntoTableMeta.WIDGET_TABLE));
    String key = connectionName + "|" + schema + "|" + table;
    if (key.equals(tableColumnsKey)) {
      return tableColumnNames;
    }
    tableColumnsKey = key;
    tableColumnNames = new String[0];
    DatabaseMeta databaseMeta = findDatabase(connectionName);
    if (databaseMeta == null || Utils.isEmpty(table)) {
      return tableColumnNames;
    }
    try (Database db =
        new Database(new LoggingObject("MergeIntoTableDialog"), variables, databaseMeta)) {
      db.connect();
      IRowMeta fields = db.getTableFieldsMeta(schema, table);
      if (fields != null) {
        tableColumnNames = fields.getFieldNames();
      }
    } catch (Exception e) {
      // The table may not exist yet while the dialog is being filled in.
    }
    return tableColumnNames;
  }

  private DatabaseMeta findDatabase(String connectionName) {
    if (Utils.isEmpty(connectionName)) {
      return null;
    }
    try {
      DatabaseMeta databaseMeta = pipelineMeta.findDatabase(connectionName, variables);
      if (databaseMeta == null && metadataProvider != null) {
        databaseMeta =
            metadataProvider
                .getSerializer(DatabaseMeta.class)
                .load(variables.resolve(connectionName));
      }
      return databaseMeta;
    } catch (Exception e) {
      return null;
    }
  }

  /** Opens the database explorer and writes the chosen schema and table into the metadata. */
  private void browseTable() {
    DatabaseMeta databaseMeta = findDatabase(readWidgetText(MergeIntoTableMeta.WIDGET_CONNECTION));
    if (databaseMeta == null) {
      MessageBox box = new MessageBox(shell, SWT.OK | SWT.ICON_ERROR);
      box.setText(BaseMessages.getString(PKG, "MergeIntoTableDialog.ConnectionMissing.Title"));
      box.setMessage(BaseMessages.getString(PKG, "MergeIntoTableDialog.ConnectionMissing.Message"));
      box.open();
      return;
    }
    try {
      DatabaseExplorerDialog explorer =
          new DatabaseExplorerDialog(
              shell, SWT.NONE, variables, databaseMeta, pipelineMeta.getDatabases());
      explorer.setSelectedSchemaAndTable(
          readWidgetText(MergeIntoTableMeta.WIDGET_SCHEMA),
          readWidgetText(MergeIntoTableMeta.WIDGET_TABLE));
      if (explorer.open()) {
        editing.setSchemaName(Const.NVL(explorer.getSchemaName(), ""));
        editing.setTableName(Const.NVL(explorer.getTableName(), ""));
        input.setChanged();
      }
    } catch (Exception e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MergeIntoTableDialog.BrowseError.Title"),
          BaseMessages.getString(PKG, "MergeIntoTableDialog.BrowseError.Message"),
          e);
    }
  }

  /** Adds every incoming field that isn't written yet as a value field, updated, to its column. */
  private void getFields() {
    try {
      widgets.getWidgetsContents(editing, MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      if (editing.getValueFields() == null) {
        editing.setValueFields(new ArrayList<>());
      }
      IRowMeta previous = pipelineMeta.getPrevTransformFields(variables, transformName);
      if (previous == null) {
        return;
      }
      Set<String> present = new HashSet<>();
      for (MergeValueField valueField : editing.getValueFields()) {
        present.add(valueField.getStreamField());
      }
      boolean added = false;
      for (IValueMeta valueMeta : previous.getValueMetaList()) {
        if (present.add(valueMeta.getName())) {
          editing
              .getValueFields()
              .add(new MergeValueField(valueMeta.getName(), valueMeta.getName(), true));
          added = true;
        }
      }
      if (added) {
        input.setChanged();
        widgets.setWidgetsContents(editing, shell, MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
      }
    } catch (HopException e) {
      new ErrorDialog(
          shell,
          BaseMessages.getString(PKG, "MergeIntoTableDialog.GetFieldsFailed.Title"),
          BaseMessages.getString(PKG, "MergeIntoTableDialog.GetFieldsFailed.Message"),
          e);
    }
  }

  private TableView tableView(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    return control instanceof TableView tableView ? tableView : null;
  }

  private String readWidgetText(String widgetId) {
    Control control = widgets.getWidgetsMap().get(widgetId);
    if (control instanceof TextVar textVar) {
      return Const.NVL(textVar.getText(), "");
    }
    if (control instanceof Text text) {
      return Const.NVL(text.getText(), "");
    }
    if (control instanceof MetaSelectionLine<?> line) {
      return Const.NVL(line.getText(), "");
    }
    return "";
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
    widgets.getWidgetsContents(input, MergeIntoTableMeta.GUI_PLUGIN_ELEMENT_PARENT_ID);
    transformName = wTransformName.getText();
    dispose();
  }
}
