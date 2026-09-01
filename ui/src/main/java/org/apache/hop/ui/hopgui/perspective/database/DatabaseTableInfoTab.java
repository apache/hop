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

package org.apache.hop.ui.hopgui.perspective.database;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/** Table information tab: identity, columns, indexes. */
public class DatabaseTableInfoTab implements IHopFileTypeHandler {

  public static final Class<?> PKG = DatabasePerspective.class;

  private static final DatabaseTableInfoFileType FILE_TYPE = new DatabaseTableInfoFileType();

  private final IDatabaseWorkbenchHost host;
  private final DatabaseWorkbench workbench;
  @Getter private final DatabaseMeta databaseMeta;
  @Getter private final String schemaName;
  @Getter private final String tableName;
  @Getter private final Composite control;
  @Getter private CTabItem tabItem;

  private TableView columnsView;
  private TableView indexesView;

  public DatabaseTableInfoTab(
      Composite parent,
      IDatabaseWorkbenchHost host,
      DatabaseWorkbench workbench,
      DatabaseMeta databaseMeta,
      String schemaName,
      String tableName) {
    this.host = host;
    this.workbench = workbench;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
    this.tableName = tableName;

    control = new Composite(parent, SWT.NONE);
    control.setLayout(new FormLayout());
    PropsUi.setLook(control);

    Label header = new Label(control, SWT.WRAP);
    PropsUi.setLook(header);
    header.setText(headerText());
    header.setLayoutData(new FormDataBuilder().top().fullWidth().result());

    CTabFolder folder = new CTabFolder(control, SWT.BORDER);
    PropsUi.setLook(folder, PropsUi.WIDGET_STYLE_TAB);
    folder.setLayoutData(
        new FormDataBuilder().top(header, PropsUi.getMargin()).bottom().fullWidth().result());

    CTabItem columnsTab = new CTabItem(folder, SWT.NONE);
    columnsTab.setText(BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Columns"));
    columnsView =
        new TableView(
            host.getVariables(),
            folder,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            columnInfos(),
            0,
            true,
            null,
            PropsUi.getInstance());
    columnsView.setReadonly(true);
    columnsTab.setControl(columnsView);

    CTabItem indexesTab = new CTabItem(folder, SWT.NONE);
    indexesTab.setText(BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Indexes"));
    indexesView =
        new TableView(
            host.getVariables(),
            folder,
            SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI,
            indexInfos(),
            0,
            true,
            null,
            PropsUi.getInstance());
    indexesView.setReadonly(true);
    indexesTab.setControl(indexesView);

    folder.setSelection(0);
  }

  public void setTabItem(CTabItem tabItem) {
    this.tabItem = tabItem;
    if (tabItem != null && !tabItem.isDisposed()) {
      tabItem.setText(getName());
      tabItem.setImage(GuiResource.getInstance().getImageTable());
    }
  }

  public void loadDetails() {
    String qualified =
        databaseMeta.getQuotedSchemaTableCombination(host.getVariables(), schemaName, tableName);
    String description =
        BaseMessages.getString(PKG, "DatabasePerspective.Operation.TableInfo", qualified);
    workbench.runOperation(
        description,
        databaseMeta.getName(),
        operation -> {
          IRowMeta fields;
          List<DatabaseIndexInfo> indexes;
          try (Database db =
              new Database(host.getLoggingObject(), host.getVariables(), databaseMeta)) {
            operation.attachDatabase(db);
            db.connect();
            if (operation.isCancelled()) {
              return;
            }
            fields = loadFields(db, qualified);
            indexes = loadIndexes(db, schemaName, tableName);
          }
          IRowMeta loadedFields = fields;
          List<DatabaseIndexInfo> loadedIndexes = indexes;
          host.asyncExec(() -> populate(loadedFields, loadedIndexes));
        });
  }

  private IRowMeta loadFields(Database db, String qualified) throws Exception {
    try {
      IRowMeta meta = db.getTableFieldsMeta(schemaName, tableName);
      if (meta != null && meta.size() > 0) {
        return meta;
      }
    } catch (Exception ignored) {
      // Fall back to the query-based layout.
    }
    return db.getTableFields(qualified);
  }

  static List<DatabaseIndexInfo> loadIndexes(Database db, String schema, String table)
      throws Exception {
    Map<String, DatabaseIndexInfo> byName = new LinkedHashMap<>();
    DatabaseMetaData metaData = db.getDatabaseMetaData();
    String catalog = null;
    try {
      if (metaData.supportsCatalogsInIndexDefinitions()) {
        catalog = db.getConnection().getCatalog();
      }
    } catch (Exception ignored) {
      // Driver-dependent.
    }
    try (ResultSet indexList = metaData.getIndexInfo(catalog, schema, table, false, true)) {
      if (indexList == null) {
        return List.of();
      }
      while (indexList.next()) {
        String indexName = indexList.getString("INDEX_NAME");
        if (Utils.isEmpty(indexName)) {
          continue;
        }
        DatabaseIndexInfo info = byName.computeIfAbsent(indexName, n -> new DatabaseIndexInfo());
        info.setName(indexName);
        info.setUnique(!indexList.getBoolean("NON_UNIQUE"));
        String column = indexList.getString("COLUMN_NAME");
        if (!Utils.isEmpty(column) && !info.getColumns().contains(column)) {
          info.getColumns().add(column);
        }
      }
    }
    return new ArrayList<>(byName.values());
  }

  private void populate(IRowMeta fields, List<DatabaseIndexInfo> indexes) {
    if (control.isDisposed()) {
      return;
    }
    fillColumns(fields);
    fillIndexes(indexes);
  }

  private void fillColumns(IRowMeta fields) {
    columnsView.clearAll(false);
    if (fields == null) {
      columnsView.removeEmptyRows();
      columnsView.setRowNums();
      return;
    }
    for (int i = 0; i < fields.size(); i++) {
      IValueMeta value = fields.getValueMeta(i);
      TableItem item =
          i == 0 ? columnsView.table.getItem(0) : new TableItem(columnsView.table, SWT.NONE);
      item.setText(1, Const.NVL(value.getName(), ""));
      item.setText(2, Const.NVL(value.getTypeDesc(), ""));
      item.setText(3, value.getLength() >= 0 ? Integer.toString(value.getLength()) : "");
      item.setText(4, value.getPrecision() >= 0 ? Integer.toString(value.getPrecision()) : "");
      item.setText(5, Const.NVL(value.getComments(), ""));
    }
    columnsView.removeEmptyRows();
    columnsView.setRowNums();
    columnsView.optWidth(true);
  }

  private void fillIndexes(List<DatabaseIndexInfo> indexes) {
    indexesView.clearAll(false);
    if (indexes == null) {
      indexesView.removeEmptyRows();
      indexesView.setRowNums();
      return;
    }
    for (int i = 0; i < indexes.size(); i++) {
      DatabaseIndexInfo info = indexes.get(i);
      TableItem item =
          i == 0 ? indexesView.table.getItem(0) : new TableItem(indexesView.table, SWT.NONE);
      item.setText(1, Const.NVL(info.getName(), ""));
      item.setText(
          2,
          info.isUnique()
              ? BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Yes")
              : BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.No"));
      item.setText(3, info.columnsAsString());
    }
    indexesView.removeEmptyRows();
    indexesView.setRowNums();
    indexesView.optWidth(true);
  }

  private ColumnInfo[] columnInfos() {
    return new ColumnInfo[] {
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Type"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Length"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Precision"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          true),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Comments"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false)
    };
  }

  private ColumnInfo[] indexInfos() {
    return new ColumnInfo[] {
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Index.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Index.Unique"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Index.Columns"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false)
    };
  }

  private String headerText() {
    return BaseMessages.getString(
        PKG,
        "DatabasePerspective.TableInfo.Header",
        Const.NVL(databaseMeta.getName(), ""),
        Const.NVL(schemaName, ""),
        Const.NVL(tableName, ""));
  }

  public boolean matches(String connectionName, String schema, String table) {
    return Objects.equals(databaseMeta.getName(), connectionName)
        && Objects.equals(Const.NVL(schemaName, ""), Const.NVL(schema, ""))
        && Objects.equals(tableName, table);
  }

  @Override
  public Object getSubject() {
    return this;
  }

  @Override
  public String getName() {
    if (Utils.isEmpty(schemaName)) {
      return tableName;
    }
    return schemaName + "." + tableName;
  }

  @Override
  public void setName(String name) {}

  @Override
  public IHopFileType getFileType() {
    return FILE_TYPE;
  }

  @Override
  public String getFilename() {
    return null;
  }

  @Override
  public void setFilename(String filename) {}

  @Override
  public void save() {}

  @Override
  public void saveAs(String filename) {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void pause() {}

  @Override
  public void resume() {}

  @Override
  public void preview() {}

  @Override
  public void debug() {}

  @Override
  public void redraw() {}

  @Override
  public void updateGui() {
    host.updateGui(this);
  }

  @Override
  public void selectAll() {}

  @Override
  public void unselectAll() {}

  @Override
  public void copySelectedToClipboard() {}

  @Override
  public void cutSelectedToClipboard() {}

  @Override
  public void deleteSelected() {}

  @Override
  public void pasteFromClipboard() {}

  @Override
  public boolean isCloseable() {
    return true;
  }

  @Override
  public void close() {
    workbench.remove(this);
  }

  @Override
  public boolean hasChanged() {
    return false;
  }

  @Override
  public void undo() {}

  @Override
  public void redo() {}

  @Override
  public Map<String, Object> getStateProperties() {
    return Collections.emptyMap();
  }

  @Override
  public void applyStateProperties(Map<String, Object> stateProperties) {}

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return List.of();
  }

  @Override
  public IVariables getVariables() {
    return host.getVariables();
  }
}
