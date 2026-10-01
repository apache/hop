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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseObjectDdl;
import org.apache.hop.core.database.types.DatabaseColumn;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.DatabaseTreeNode;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.editor.IContentEditorWidget;
import org.apache.hop.ui.hopgui.ContentEditorFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.TableItem;

/** Object information tab: identity, columns, indexes, DDL. */
public class DatabaseTableInfoTab implements IHopFileTypeHandler {

  public static final Class<?> PKG = DatabasePerspective.class;

  private static final DatabaseTableInfoFileType FILE_TYPE = new DatabaseTableInfoFileType();

  private final IDatabaseWorkbenchHost host;
  private final DatabaseWorkbench workbench;
  @Getter @Setter private DatabaseMeta databaseMeta;
  @Getter private final String schemaName;
  @Getter private final String tableName;
  @Getter private final DatabaseTreeNode.Kind kind;
  @Getter private final Composite control;
  @Getter private CTabItem tabItem;

  private TableView columnsView;
  private TableView indexesView;
  private IContentEditorWidget ddlEditor;

  public DatabaseTableInfoTab(
      Composite parent,
      IDatabaseWorkbenchHost host,
      DatabaseWorkbench workbench,
      DatabaseMeta databaseMeta,
      String schemaName,
      String tableName) {
    this(parent, host, workbench, databaseMeta, schemaName, tableName, DatabaseTreeNode.Kind.TABLE);
  }

  public DatabaseTableInfoTab(
      Composite parent,
      IDatabaseWorkbenchHost host,
      DatabaseWorkbench workbench,
      DatabaseMeta databaseMeta,
      String schemaName,
      String tableName,
      DatabaseTreeNode.Kind kind) {
    this.host = host;
    this.workbench = workbench;
    this.databaseMeta = databaseMeta;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.kind = kind == null ? DatabaseTreeNode.Kind.TABLE : kind;

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

    CTabItem ddlTab = new CTabItem(folder, SWT.NONE);
    ddlTab.setText(BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Ddl"));
    Composite ddlParent = new Composite(folder, SWT.NONE);
    ddlParent.setLayout(new FormLayout());
    PropsUi.setLook(ddlParent);
    ddlEditor = ContentEditorFacade.createContentEditor(ddlParent, "sql");
    ddlEditor.getControl().setLayoutData(new FormDataBuilder().fullSize().result());
    ddlEditor.setReadOnly(true);
    ddlTab.setControl(ddlParent);

    folder.setSelection(0);
  }

  public void setTabItem(CTabItem tabItem) {
    this.tabItem = tabItem;
    if (tabItem != null && !tabItem.isDisposed()) {
      tabItem.setText(getName());
      tabItem.setImage(tabImage());
    }
  }

  private Image tabImage() {
    GuiResource resources = GuiResource.getInstance();
    return switch (kind) {
      case VIEW -> resources.getImageView();
      case SYNONYM -> resources.getImageSynonym();
      default -> resources.getImageTable();
    };
  }

  public void loadDetails() {
    if (databaseMeta != null) {
      DatabaseMeta freshMeta = workbench.reloadConnectionMeta(databaseMeta.getName());
      if (freshMeta == null) {
        new ErrorDialog(
            host.getShell(),
            BaseMessages.getString(PKG, "DatabasePerspective.Error.Title"),
            BaseMessages.getString(
                PKG, "DatabasePerspective.Error.ConnectionNotFound", databaseMeta.getName()),
            new HopException(
                BaseMessages.getString(
                    PKG, "DatabasePerspective.Error.ConnectionNotFound", databaseMeta.getName())));
        return;
      }
      this.databaseMeta = freshMeta;
    }
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
          String ddl;
          Map<String, String> definitions;
          boolean view = kind == DatabaseTreeNode.Kind.VIEW;
          try (Database db =
              new Database(host.getLoggingObject(), host.getVariables(), databaseMeta)) {
            operation.attachDatabase(db);
            db.connect();
            if (operation.isCancelled()) {
              return;
            }
            fields = loadFields(db, qualified);
            indexes = loadIndexes(db, schemaName, tableName);
            definitions = loadColumnDefinitions(db, schemaName, tableName, fields);
            try {
              ddl = db.getObjectDdl(schemaName, tableName, view, fields);
            } catch (Exception e) {
              ddl = "-- " + Const.NVL(e.getMessage(), e.getClass().getSimpleName());
            }
          }
          IRowMeta loadedFields = fields;
          List<DatabaseIndexInfo> loadedIndexes = indexes;
          Map<String, String> loadedDefinitions = definitions;
          String loadedDdl =
              view
                  ? ddl
                  : withIndexStatements(
                      databaseMeta, host.getVariables(), schemaName, tableName, ddl, indexes);
          host.asyncExec(() -> populate(loadedFields, loadedIndexes, loadedDdl, loadedDefinitions));
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

  static Map<String, String> loadColumnDefinitions(
      Database db, String schemaName, String tableName, IRowMeta fields) {
    Map<String, String> definitions = new HashMap<>();
    if (db != null) {
      try {
        DatabaseMetaData metaData = db.getDatabaseMetaData();
        if (metaData != null) {
          String escape = null;
          try {
            escape = metaData.getSearchStringEscape();
          } catch (Exception ignored) {
            // Driver-dependent.
          }
          boolean supportsCatalogs = false;
          boolean supportsSchemas = false;
          try {
            supportsCatalogs = metaData.supportsCatalogsInTableDefinitions();
            supportsSchemas = metaData.supportsSchemasInTableDefinitions();
          } catch (Exception ignored) {
            // Driver-dependent.
          }

          String catalog = null;
          String schemaPattern = Utils.isEmpty(schemaName) ? null : schemaName;
          if (supportsCatalogs && !supportsSchemas) {
            // Catalogs are used instead of schemas (e.g. MySQL / MariaDB)
            catalog = schemaName;
            schemaPattern = null;
          } else {
            try {
              if (supportsCatalogs && db.getConnection() != null) {
                catalog = db.getConnection().getCatalog();
              }
            } catch (Exception ignored) {
              // Driver-dependent.
            }
          }

          String escapedSchema = escapePattern(schemaPattern, escape);
          String escapedTable = escapePattern(tableName, escape);

          readColumnDefinitions(
              metaData,
              catalog,
              escapedSchema,
              escapedTable,
              catalog,
              schemaPattern,
              tableName,
              definitions);

          // If empty and schemaName was provided, try using schemaName as catalog
          if (definitions.isEmpty() && !Utils.isEmpty(schemaName)) {
            if (catalog == null || !schemaName.equals(catalog)) {
              readColumnDefinitions(
                  metaData,
                  schemaName,
                  null,
                  escapedTable,
                  schemaName,
                  null,
                  tableName,
                  definitions);
            }
          }

          if (definitions.isEmpty() && !Utils.isEmpty(tableName)) {
            String upperTable = escapePattern(tableName.toUpperCase(Locale.ROOT), escape);
            readColumnDefinitions(
                metaData,
                catalog,
                escapedSchema,
                upperTable,
                catalog,
                schemaPattern,
                tableName,
                definitions);
            if (definitions.isEmpty()) {
              String lowerTable = escapePattern(tableName.toLowerCase(Locale.ROOT), escape);
              readColumnDefinitions(
                  metaData,
                  catalog,
                  escapedSchema,
                  lowerTable,
                  catalog,
                  schemaPattern,
                  tableName,
                  definitions);
            }
          }
        }
      } catch (Exception ignored) {
        // Fall back to fields.
      }
    }
    if (fields != null) {
      for (int i = 0; i < fields.size(); i++) {
        IValueMeta value = fields.getValueMeta(i);
        if (value != null && !Utils.isEmpty(value.getName())) {
          String key = value.getName().toLowerCase(Locale.ROOT);
          if (!definitions.containsKey(key)) {
            definitions.put(key, DatabaseColumn.calculateDefinition(value));
          }
        }
      }
    }
    return definitions;
  }

  static String escapePattern(String value, String escape) {
    if (Utils.isEmpty(value) || Utils.isEmpty(escape)) {
      return value;
    }
    char escapeChar = escape.charAt(0);
    StringBuilder escaped = new StringBuilder(value.length() + 4);
    for (char c : value.toCharArray()) {
      if (c == '_' || c == '%' || c == escapeChar) {
        escaped.append(escapeChar);
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  private static void readColumnDefinitions(
      DatabaseMetaData metaData,
      String catalog,
      String schemaPattern,
      String tableNamePattern,
      String expectedCatalog,
      String expectedSchema,
      String expectedTableName,
      Map<String, String> definitions) {
    try (ResultSet columns = metaData.getColumns(catalog, schemaPattern, tableNamePattern, null)) {
      if (columns != null) {
        String chosenSchema = null;
        while (columns.next()) {
          try {
            if (!Utils.isEmpty(expectedTableName)) {
              String actualTable = columns.getString("TABLE_NAME");
              if (!Utils.isEmpty(actualTable) && !expectedTableName.equalsIgnoreCase(actualTable)) {
                continue;
              }
            }
            String actualSchema = columns.getString("TABLE_SCHEM");
            if (!Utils.isEmpty(expectedSchema)) {
              if (!Utils.isEmpty(actualSchema) && !expectedSchema.equalsIgnoreCase(actualSchema)) {
                continue;
              }
            } else if (!Utils.isEmpty(actualSchema)) {
              if (chosenSchema == null) {
                chosenSchema = actualSchema;
              } else if (!chosenSchema.equalsIgnoreCase(actualSchema)) {
                continue;
              }
            }
            if (!Utils.isEmpty(expectedCatalog)) {
              String actualCatalog = columns.getString("TABLE_CAT");
              if (!Utils.isEmpty(actualCatalog)
                  && !expectedCatalog.equalsIgnoreCase(actualCatalog)) {
                continue;
              }
            }
            DatabaseColumn column = DatabaseColumn.ofColumnsRow(columns);
            if (column != null && !Utils.isEmpty(column.getName())) {
              definitions.putIfAbsent(
                  column.getName().toLowerCase(Locale.ROOT), column.getDefinition());
            }
          } catch (Exception ignored) {
            // Ignore single column read errors
          }
        }
      }
    } catch (Exception ignored) {
      // Driver error on getColumns
    }
  }

  private void populate(
      IRowMeta fields,
      List<DatabaseIndexInfo> indexes,
      String ddl,
      Map<String, String> definitions) {
    if (control.isDisposed()) {
      return;
    }
    fillColumns(fields, definitions);
    fillIndexes(indexes);
    fillDdl(ddl);
  }

  private void fillDdl(String ddl) {
    if (ddlEditor == null || ddlEditor.isDisposed()) {
      return;
    }
    ddlEditor.setTextSuppressModify(Const.NVL(ddl, ""));
  }

  /**
   * Append {@code CREATE INDEX} statements when the table DDL was synthesized and does not already
   * describe keys.
   */
  static String withIndexStatements(
      DatabaseMeta meta,
      IVariables variables,
      String schema,
      String table,
      String ddl,
      List<DatabaseIndexInfo> indexes) {
    if (Utils.isEmpty(ddl)
        || indexes == null
        || indexes.isEmpty()
        || catalogDdlIncludesIndexes(ddl)) {
      return ddl;
    }
    String qualified = meta.getQuotedSchemaTableCombination(variables, schema, table);
    StringBuilder buffer = new StringBuilder(ddl.trim());
    if (!buffer.toString().endsWith(";")) {
      buffer.append(';');
    }
    for (DatabaseIndexInfo index : indexes) {
      if (Utils.isEmpty(index.getName()) || index.getColumns().isEmpty()) {
        continue;
      }
      buffer.append(Const.CR).append(Const.CR);
      buffer.append(index.isUnique() ? "CREATE UNIQUE INDEX " : "CREATE INDEX ");
      buffer.append(Const.NVL(meta.quoteField(index.getName()), index.getName()));
      buffer.append(" ON ").append(qualified).append(" (");
      for (int i = 0; i < index.getColumns().size(); i++) {
        if (i > 0) {
          buffer.append(", ");
        }
        String column = index.getColumns().get(i);
        buffer.append(Const.NVL(meta.quoteField(column), column));
      }
      buffer.append(");");
    }
    return buffer.toString();
  }

  static boolean catalogDdlIncludesIndexes(String ddl) {
    if (Utils.isEmpty(ddl)) {
      return false;
    }
    String upper = ddl.toUpperCase(Locale.ROOT);
    return upper.contains("PRIMARY KEY")
        || upper.contains("CREATE INDEX")
        || upper.contains(" UNIQUE KEY")
        || DatabaseObjectDdl.startsWithCreate(ddl) && upper.contains(" KEY ");
  }

  private void fillColumns(IRowMeta fields, Map<String, String> definitions) {
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
      String definition =
          definitions != null && !Utils.isEmpty(value.getName())
              ? Const.NVL(definitions.get(value.getName().toLowerCase(Locale.ROOT)), "")
              : DatabaseColumn.calculateDefinition(value);
      item.setText(1, Const.NVL(value.getName(), ""));
      item.setText(2, Const.NVL(definition, ""));
      item.setText(3, Const.NVL(value.getTypeDesc(), ""));
      item.setText(4, value.getLength() >= 0 ? Integer.toString(value.getLength()) : "");
      item.setText(5, value.getPrecision() >= 0 ? Integer.toString(value.getPrecision()) : "");
      item.setText(6, Const.NVL(value.getComments(), ""));
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

  static ColumnInfo[] columnInfos() {
    return new ColumnInfo[] {
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Name"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.Definition"),
          ColumnInfo.COLUMN_TYPE_TEXT,
          false),
      new ColumnInfo(
          BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Column.HopType"),
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
    String kindLabel =
        switch (kind) {
          case VIEW -> BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Kind.View");
          case SYNONYM -> BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Kind.Synonym");
          default -> BaseMessages.getString(PKG, "DatabasePerspective.TableInfo.Kind.Table");
        };
    return BaseMessages.getString(
        PKG,
        "DatabasePerspective.TableInfo.Header",
        Const.NVL(databaseMeta.getName(), ""),
        Const.NVL(schemaName, ""),
        kindLabel,
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
  public void selectAll() {
    if (ddlEditor != null && !ddlEditor.isDisposed()) {
      ddlEditor.selectAll();
    }
  }

  @Override
  public void unselectAll() {
    if (ddlEditor != null && !ddlEditor.isDisposed()) {
      ddlEditor.unselectAll();
    }
  }

  @Override
  public void copySelectedToClipboard() {
    if (ddlEditor != null && !ddlEditor.isDisposed()) {
      ddlEditor.copy();
    }
  }

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
    return new HashMap<>();
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
