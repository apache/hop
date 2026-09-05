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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hop.core.Const;
import org.apache.hop.core.database.Catalog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.database.Schema;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageDialogWithToggle;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.database.config.DatabasePerspectiveConfig;
import org.apache.hop.ui.hopgui.perspective.database.config.DatabasePerspectiveConfigSingleton;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

/**
 * The database workbench UI (connection tree + editor tabs + operations). A plain Composite so a
 * later host can put it in a perspective, a floating dialog, or a dock tab.
 */
@GuiPlugin
public class DatabaseWorkbench extends Composite implements TabClosable {

  public static final Class<?> PKG = DatabasePerspective.class;

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "DatabaseWorkbench-Toolbar";
  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "DatabaseWorkbench-ContextMenu";

  public static final String TOOLBAR_ITEM_CONNECT = "DatabaseWorkbench-Toolbar-10000-Connect";
  public static final String TOOLBAR_ITEM_DISCONNECT = "DatabaseWorkbench-Toolbar-10010-Disconnect";
  public static final String TOOLBAR_ITEM_REFRESH = "DatabaseWorkbench-Toolbar-10020-Refresh";
  public static final String TOOLBAR_ITEM_SQL = "DatabaseWorkbench-Toolbar-10030-SqlEditor";
  public static final String TOOLBAR_ITEM_DDL = "DatabaseWorkbench-Toolbar-10040-GenerateDdl";
  public static final String TOOLBAR_ITEM_PREVIEW = "DatabaseWorkbench-Toolbar-10050-Preview";
  public static final String TOOLBAR_ITEM_SHOW = "DatabaseWorkbench-Toolbar-10060-Show";
  public static final String TOOLBAR_ITEM_FLOAT = "DatabaseWorkbench-Toolbar-20000-Float";
  public static final String TOOLBAR_ITEM_DOCK = "DatabaseWorkbench-Toolbar-20010-Dock";

  public static final String CONTEXT_MENU_CONNECT = "DatabaseWorkbench-ContextMenu-10000-Connect";
  public static final String CONTEXT_MENU_DISCONNECT =
      "DatabaseWorkbench-ContextMenu-10010-Disconnect";
  public static final String CONTEXT_MENU_SQL = "DatabaseWorkbench-ContextMenu-10030-SqlEditor";
  public static final String CONTEXT_MENU_DDL = "DatabaseWorkbench-ContextMenu-10040-GenerateDdl";
  public static final String CONTEXT_MENU_PREVIEW = "DatabaseWorkbench-ContextMenu-10050-Preview";
  public static final String CONTEXT_MENU_SHOW = "DatabaseWorkbench-ContextMenu-10060-Show";

  /** {@link TreeMemory} key for schema/catalog expand-collapse across filter and refresh. */
  public static final String TREE_MEMORY_KEY = "DatabaseWorkbench-Tree";

  private static final int FILTER_DEBOUNCE_MS = 250;
  private static final String RIGHT_SASH_KEY = "database-workbench-right-sash";

  private final IDatabaseWorkbenchHost host;
  private final Map<String, DatabaseConnectionState> connections = new LinkedHashMap<>();
  private final List<TabItemHandler> items = new ArrayList<>();

  private final SashForm horizontalSash;
  private final Composite rightComposite;
  private final SashForm rightSash;
  private final Tree tree;
  private final Text searchText;
  private final GuiToolbarWidgets toolBarWidgets;
  private final GuiMenuWidgets menuWidgets;
  private final CTabFolder tabFolder;
  private final DatabaseOperationsPanel operationsPanel;
  private final DatabaseSqlFileType sqlFileType = new DatabaseSqlFileType();

  private String filterText = "";
  private SearchMatcher filterMatcher = new SearchMatcher("", false, false, false);
  private final Runnable applyFilterRunnable = this::rebuildTree;

  /** True while rebuild applies expand state so SWT Expand/Collapse does not rewrite TreeMemory. */
  private boolean applyingTreeState;

  final Runnable persistSqlTabsRunnable = () -> DatabaseSqlTabMemory.save(this);
  private final String eventListenerId;
  volatile boolean restoringSqlTabs;

  public DatabaseWorkbench(Composite parent, IDatabaseWorkbenchHost host) {
    super(parent, SWT.NONE);
    this.host = host;
    this.eventListenerId = getClass().getName() + "-" + System.identityHashCode(this);

    PropsUi.setLook(this);
    setLayout(new FormLayout());

    horizontalSash = new SashForm(this, SWT.HORIZONTAL);
    horizontalSash.setLayoutData(new FormDataBuilder().fullSize().result());

    Composite treeComposite = new Composite(horizontalSash, SWT.NONE);
    treeComposite.setLayout(new FormLayout());
    PropsUi.setLook(treeComposite);

    searchText = new Text(treeComposite, SWT.SEARCH | SWT.ICON_CANCEL | SWT.ICON_SEARCH);
    searchText.setMessage(BaseMessages.getString(PKG, "DatabasePerspective.Search.Placeholder"));
    PropsUi.setLook(searchText);
    searchText.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    searchText.addListener(SWT.Modify, e -> scheduleFilterApply());
    searchText.addListener(
        SWT.DefaultSelection,
        e -> {
          cancelScheduledFilterApply();
          rebuildTree();
        });

    Composite treeBorder = new Composite(treeComposite, SWT.BORDER);
    treeBorder.setLayout(new FormLayout());
    treeBorder.setLayoutData(
        new FormDataBuilder().top(searchText, PropsUi.getMargin()).bottom().fullWidth().result());

    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(treeBorder, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolBar = toolBarContainer.getControl();
    toolBar.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(toolBar, PropsUi.WIDGET_STYLE_TOOLBAR);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    toolBar.pack();

    tree = new Tree(treeBorder, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(tree);
    tree.setLayoutData(
        new FormDataBuilder().top(toolBar, PropsUi.getMargin()).bottom().fullWidth().result());
    tree.addListener(SWT.Selection, e -> updateToolbar());
    tree.addListener(SWT.DefaultSelection, e -> onTreeDefaultSelection());
    tree.addListener(SWT.Expand, e -> rememberTreeExpand((TreeItem) e.item, true));
    tree.addListener(SWT.Collapse, e -> rememberTreeExpand((TreeItem) e.item, false));

    Menu menu = new Menu(tree);
    menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, host.getShell(), menu);
    tree.setMenu(menu);
    tree.addListener(SWT.MenuDetect, e -> updateToolbar());

    rightComposite = new Composite(horizontalSash, SWT.NONE);
    rightComposite.setLayout(new FormLayout());
    PropsUi.setLook(rightComposite);

    rightSash = new SashForm(rightComposite, SWT.VERTICAL);
    tabFolder = new CTabFolder(rightSash, SWT.MULTI | SWT.BORDER);
    PropsUi.setLook(tabFolder, PropsUi.WIDGET_STYLE_TAB);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            closeTab(event, (CTabItem) event.item);
          }
        });
    tabFolder.addListener(
        SWT.Selection,
        e -> {
          IHopFileTypeHandler handler = getActiveFileTypeHandler();
          host.updateGui(handler);
          schedulePersistSqlTabs();
        });
    new TabCloseHandler(this, tabFolder);

    operationsPanel = new DatabaseOperationsPanel(rightSash, rightComposite);
    operationsPanel.setExpandedListener(this::layoutOperationsPane);
    rightSash.setWeights(80, 20);
    SashFormMemory.persist(rightSash, RIGHT_SASH_KEY, 80, 20);
    layoutOperationsPane(false);

    horizontalSash.setWeights(22, 78);
    SashFormMemory.persist(horizontalSash, "database-workbench-tree-width", 22, 78);

    host.getHopGui()
        .getEventsHandler()
        .addEventListener(
            eventListenerId,
            e -> host.asyncExec(this::reloadConnections),
            HopGuiEvents.MetadataChanged.name(),
            HopGuiEvents.MetadataCreated.name(),
            HopGuiEvents.MetadataDeleted.name());
    host.getHopGui()
        .getEventsHandler()
        .addEventListener(
            eventListenerId + "-project",
            e ->
                host.asyncExec(
                    () -> {
                      closeSqlEditorTabs();
                      reloadConnections();
                      DatabaseSqlTabMemory.restore(this);
                    }),
            HopGuiEvents.ProjectActivated.name());

    addDisposeListener(
        e -> {
          DatabaseSqlTabMemory.saveNow(this);
          operationsPanel.cancelAll();
          host.getHopGui().getEventsHandler().removeEventListeners(eventListenerId);
          host.getHopGui().getEventsHandler().removeEventListeners(eventListenerId + "-project");
        });

    reloadConnections();
    DatabaseSqlTabMemory.restore(this);
  }

  public DatabaseSqlFileType getSqlFileType() {
    return sqlFileType;
  }

  public void reloadConnections() {
    if (isDisposed()) {
      return;
    }
    Map<String, DatabaseConnectionState> previous = new LinkedHashMap<>(connections);
    connections.clear();
    try {
      List<DatabaseMeta> metas =
          host.getMetadataProvider().getSerializer(DatabaseMeta.class).loadAll();
      metas.sort(DatabaseMeta.comparator);
      for (DatabaseMeta meta : metas) {
        DatabaseConnectionState state = previous.get(meta.getName());
        if (state == null) {
          state = new DatabaseConnectionState(meta);
        } else {
          state.setDatabaseMeta(meta);
        }
        connections.put(meta.getName(), state);
      }
    } catch (Exception e) {
      new ErrorDialog(
          host.getShell(),
          BaseMessages.getString(PKG, "DatabasePerspective.Error.Title"),
          BaseMessages.getString(PKG, "DatabasePerspective.Error.LoadConnections"),
          e);
    }
    rebuildTree();
  }

  public void clearSearchFilter() {
    if (searchText != null && !searchText.isDisposed()) {
      searchText.setText("");
    }
    filterText = "";
    filterMatcher = new SearchMatcher("", false, false, false);
    rebuildTree();
  }

  private void scheduleFilterApply() {
    Display display = getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.timerExec(-1, applyFilterRunnable);
    display.timerExec(FILTER_DEBOUNCE_MS, applyFilterRunnable);
  }

  private void cancelScheduledFilterApply() {
    Display display = getDisplay();
    if (display == null || display.isDisposed()) {
      return;
    }
    display.timerExec(-1, applyFilterRunnable);
  }

  private void rebuildTree() {
    if (tree.isDisposed()) {
      return;
    }
    filterText = Const.NVL(searchText.getText(), "");
    filterMatcher = new SearchMatcher(filterText, false, false, false);
    tree.setRedraw(false);
    applyingTreeState = true;
    try {
      tree.removeAll();
      for (DatabaseConnectionState state : connections.values()) {
        if (!connectionMatches(state)) {
          continue;
        }
        TreeItem connectionItem = new TreeItem(tree, SWT.NONE);
        connectionItem.setText(state.getDatabaseMeta().getName());
        connectionItem.setImage(GuiResource.getInstance().getImageDatabase());
        connectionItem.setData(
            DatabaseTreeNode.connection(state.getDatabaseMeta().getName(), state.isConnected()));
        if (state.isConnected() && state.getInformation() != null) {
          fillConnectionChildren(connectionItem, state);
          connectionItem.setExpanded(true);
        }
      }
      restoreSchemaExpandState();
      if (!Utils.isEmpty(filterText)) {
        expandMatchingSchemaItems();
      }
    } finally {
      applyingTreeState = false;
      tree.setRedraw(true);
    }
    updateToolbar();
  }

  private void rememberTreeExpand(TreeItem item, boolean expanded) {
    if (applyingTreeState || item == null || item.isDisposed()) {
      return;
    }
    Object data = item.getData();
    if (data instanceof DatabaseTreeNode node && remembersExpandState(node.getKind())) {
      TreeMemory.getInstance().storeExpanded(TREE_MEMORY_KEY, item, expanded);
    }
  }

  static boolean remembersExpandState(DatabaseTreeNode.Kind kind) {
    return kind == DatabaseTreeNode.Kind.SCHEMA || kind == DatabaseTreeNode.Kind.CATALOG;
  }

  private void restoreSchemaExpandState() {
    for (TreeItem connection : tree.getItems()) {
      restoreSchemaExpandState(connection);
    }
  }

  private void restoreSchemaExpandState(TreeItem parent) {
    for (TreeItem child : parent.getItems()) {
      Object data = child.getData();
      if (data instanceof DatabaseTreeNode node && remembersExpandState(node.getKind())) {
        child.setExpanded(
            TreeMemory.getInstance().isExpanded(TREE_MEMORY_KEY, ConstUi.getTreeStrings(child)));
      }
      restoreSchemaExpandState(child);
    }
  }

  /**
   * While a filter is active, open schema/catalog folders that still have visible children so
   * matches are not hidden. Those expands are not stored (see {@link #applyingTreeState}).
   */
  private void expandMatchingSchemaItems() {
    for (TreeItem connection : tree.getItems()) {
      boolean any = false;
      for (TreeItem child : connection.getItems()) {
        Object data = child.getData();
        if (data instanceof DatabaseTreeNode node
            && remembersExpandState(node.getKind())
            && child.getItemCount() > 0) {
          child.setExpanded(true);
          any = true;
        }
      }
      if (any) {
        connection.setExpanded(true);
      }
    }
  }

  private boolean connectionMatches(DatabaseConnectionState state) {
    if (Utils.isEmpty(filterText)) {
      return true;
    }
    DatabaseMeta meta = state.getDatabaseMeta();
    if (filterMatcher.matches(meta.getName())) {
      return true;
    }
    DatabaseMetaInformation info = state.getInformation();
    if (info == null) {
      return false;
    }
    if (info.getSchemas() != null) {
      for (Schema schema : info.getSchemas()) {
        if (filterMatcher.matches(schema.getSchemaName())) {
          return true;
        }
        if (schema.getItems() != null) {
          for (String table : schema.getItems()) {
            if (filterMatcher.matches(table)) {
              return true;
            }
          }
        }
      }
    }
    if (info.getTables() != null) {
      for (String table : info.getTables()) {
        if (filterMatcher.matches(table)) {
          return true;
        }
      }
    }
    if (mapHasMatch(info.getViewMap()) || mapHasMatch(info.getSynonymMap())) {
      return true;
    }
    return false;
  }

  private boolean mapHasMatch(Map<String, Collection<String>> map) {
    if (map == null) {
      return false;
    }
    for (Collection<String> names : map.values()) {
      if (names == null) {
        continue;
      }
      for (String name : names) {
        if (filterMatcher.matches(name)) {
          return true;
        }
      }
    }
    return false;
  }

  private void fillConnectionChildren(TreeItem connectionItem, DatabaseConnectionState state) {
    DatabaseMetaInformation info = state.getInformation();
    String connectionName = state.getDatabaseMeta().getName();
    Schema[] schemas = info.getSchemas();
    if (schemas != null && schemas.length > 0) {
      for (Schema schema : schemas) {
        if (!schemaOrChildMatches(schema, info)) {
          continue;
        }
        TreeItem schemaItem = new TreeItem(connectionItem, SWT.NONE);
        schemaItem.setText(Const.NVL(schema.getSchemaName(), ""));
        schemaItem.setImage(GuiResource.getInstance().getImageSchema());
        schemaItem.setData(DatabaseTreeNode.schema(connectionName, schema.getSchemaName()));
        addSchemaObjects(
            schemaItem, connectionName, schema.getSchemaName(), schema.getItems(), info);
      }
      return;
    }
    Catalog[] catalogs = info.getCatalogs();
    if (catalogs != null && catalogs.length > 0) {
      for (Catalog catalog : catalogs) {
        TreeItem catalogItem = new TreeItem(connectionItem, SWT.NONE);
        catalogItem.setText(Const.NVL(catalog.getCatalogName(), ""));
        catalogItem.setImage(GuiResource.getInstance().getImageFolder());
        catalogItem.setData(DatabaseTreeNode.catalog(connectionName, catalog.getCatalogName()));
        addSchemaObjects(
            catalogItem, connectionName, catalog.getCatalogName(), catalog.getItems(), info);
      }
      return;
    }
    addFolder(
        connectionItem,
        connectionName,
        BaseMessages.getString(PKG, "DatabasePerspective.Tree.Tables"),
        info.getTables(),
        DatabaseTreeNode.Kind.TABLE);
    addFolder(
        connectionItem,
        connectionName,
        BaseMessages.getString(PKG, "DatabasePerspective.Tree.Views"),
        info.getViews(),
        DatabaseTreeNode.Kind.VIEW);
    addFolder(
        connectionItem,
        connectionName,
        BaseMessages.getString(PKG, "DatabasePerspective.Tree.Synonyms"),
        info.getSynonyms(),
        DatabaseTreeNode.Kind.SYNONYM);
  }

  private boolean schemaOrChildMatches(Schema schema, DatabaseMetaInformation info) {
    if (Utils.isEmpty(filterText)) {
      return true;
    }
    if (filterMatcher.matches(schema.getSchemaName())) {
      return true;
    }
    if (schema.getItems() != null) {
      for (String table : schema.getItems()) {
        if (filterMatcher.matches(table)) {
          return true;
        }
      }
    }
    for (String view : namesForSchema(info.getViewMap(), schema.getSchemaName())) {
      if (filterMatcher.matches(view)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Tables, views and synonyms under a schema (or catalog). Views get {@code view.svg} via {@link
   * DatabaseTreeNode#kindOf}.
   */
  private void addSchemaObjects(
      TreeItem parent,
      String connectionName,
      String schemaName,
      String[] items,
      DatabaseMetaInformation info) {
    Collection<String> views = namesForSchema(info.getViewMap(), schemaName);
    Collection<String> synonyms = namesForSchema(info.getSynonymMap(), schemaName);
    List<String> names = new ArrayList<>();
    if (items != null) {
      names.addAll(Arrays.asList(items));
    }
    for (String view : views) {
      if (!DatabaseTreeNode.containsIgnoreCase(names, view)) {
        names.add(view);
      }
    }
    for (String synonym : synonyms) {
      if (!DatabaseTreeNode.containsIgnoreCase(names, synonym)) {
        names.add(synonym);
      }
    }
    names.sort(String.CASE_INSENSITIVE_ORDER);
    for (String name : names) {
      if (!matchesFilter(name, schemaName, connectionName)) {
        continue;
      }
      DatabaseTreeNode.Kind kind = DatabaseTreeNode.kindOf(name, views, synonyms);
      TreeItem item = new TreeItem(parent, SWT.NONE);
      item.setText(name);
      item.setImage(imageFor(kind));
      item.setData(DatabaseTreeNode.table(kind, connectionName, schemaName, name));
    }
  }

  static Collection<String> namesForSchema(Map<String, Collection<String>> map, String schemaName) {
    if (map == null || map.isEmpty()) {
      return List.of();
    }
    if (schemaName != null) {
      Collection<String> exact = map.get(schemaName);
      if (exact != null) {
        return exact;
      }
      for (Map.Entry<String, Collection<String>> entry : map.entrySet()) {
        if (schemaName.equalsIgnoreCase(entry.getKey()) && entry.getValue() != null) {
          return entry.getValue();
        }
      }
    }
    Collection<String> empty = map.get("");
    if (empty != null) {
      return empty;
    }
    Collection<String> missing = map.get(null);
    return missing != null ? missing : List.of();
  }

  private boolean matchesFilter(String name, String schemaName, String connectionName) {
    if (Utils.isEmpty(filterText)) {
      return true;
    }
    return filterMatcher.matches(name)
        || filterMatcher.matches(schemaName)
        || filterMatcher.matches(connectionName);
  }

  private void addFolder(
      TreeItem parent,
      String connectionName,
      String folderName,
      String[] names,
      DatabaseTreeNode.Kind kind) {
    if (names == null || names.length == 0) {
      return;
    }
    TreeItem folder = new TreeItem(parent, SWT.NONE);
    folder.setText(folderName);
    folder.setImage(GuiResource.getInstance().getImageFolder());
    folder.setData(DatabaseTreeNode.folder(connectionName, folderName));
    addTables(folder, connectionName, null, names, kind);
    folder.setExpanded(true);
  }

  private void addTables(
      TreeItem parent,
      String connectionName,
      String schemaName,
      String[] names,
      DatabaseTreeNode.Kind kind) {
    if (names == null) {
      return;
    }
    for (String name : names) {
      if (!Utils.isEmpty(filterText)
          && !filterMatcher.matches(name)
          && !filterMatcher.matches(schemaName)
          && !filterMatcher.matches(connectionName)) {
        continue;
      }
      TreeItem item = new TreeItem(parent, SWT.NONE);
      item.setText(name);
      item.setImage(imageFor(kind));
      item.setData(DatabaseTreeNode.table(kind, connectionName, schemaName, name));
    }
  }

  private org.eclipse.swt.graphics.Image imageFor(DatabaseTreeNode.Kind kind) {
    GuiResource resources = GuiResource.getInstance();
    return switch (kind) {
      case VIEW -> resources.getImageView();
      case SYNONYM -> resources.getImageSynonym();
      default -> resources.getImageTable();
    };
  }

  private DatabaseTreeNode selectedNode() {
    TreeItem[] selection = tree.getSelection();
    if (selection.length != 1) {
      return null;
    }
    Object data = selection[0].getData();
    return data instanceof DatabaseTreeNode node ? node : null;
  }

  private DatabaseConnectionState selectedState() {
    DatabaseTreeNode node = selectedNode();
    if (node == null) {
      return null;
    }
    return connections.get(node.getConnectionName());
  }

  private void updateToolbar() {
    DatabaseTreeNode node = selectedNode();
    DatabaseConnectionState state = selectedState();
    boolean hasConnection = state != null;
    boolean connected = state != null && state.isConnected();
    boolean table = node != null && node.isTableLike();

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_CONNECT, hasConnection && !connected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DISCONNECT, hasConnection && connected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_REFRESH, true);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_SQL, hasConnection);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DDL, table);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PREVIEW, table);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_SHOW, table);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_FLOAT, true);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DOCK, true);

    enableMenu(CONTEXT_MENU_CONNECT, hasConnection && !connected);
    enableMenu(CONTEXT_MENU_DISCONNECT, hasConnection && connected);
    enableMenu(CONTEXT_MENU_SQL, hasConnection);
    enableMenu(CONTEXT_MENU_DDL, table);
    enableMenu(CONTEXT_MENU_PREVIEW, table);
    enableMenu(CONTEXT_MENU_SHOW, table);
  }

  private void enableMenu(String id, boolean enabled) {
    MenuItem item = menuWidgets.findMenuItem(id);
    if (item != null) {
      item.setEnabled(enabled);
    }
  }

  private void onTreeDefaultSelection() {
    TreeItem[] selection = tree.getSelection();
    TreeItem item = selection.length == 1 ? selection[0] : null;
    DatabaseTreeNode node = selectedNode();
    DatabaseConnectionState state = selectedState();
    if (node != null
        && node.getKind() == DatabaseTreeNode.Kind.CONNECTION
        && state != null
        && !state.isConnected()) {
      connect(state);
      return;
    }
    if (item != null && item.getItemCount() > 0 && (node == null || !node.isTableLike())) {
      item.setExpanded(!item.getExpanded());
      return;
    }
    previewTable();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_CONNECT,
      toolTip = "i18n::DatabasePerspective.Toolbar.Connect.Tooltip",
      image = "ui/images/connection.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_CONNECT,
      label = "i18n::DatabasePerspective.Menu.Connect",
      image = "ui/images/connection.svg")
  public void connectSelected() {
    DatabaseConnectionState state = selectedState();
    if (state == null) {
      return;
    }
    connect(state);
  }

  public void connect(DatabaseConnectionState state) {
    connect(state, null);
  }

  /**
   * Load schemas/tables for {@code state}. When {@code afterConnected} is set, it runs on the UI
   * thread after a successful connect.
   */
  public void connect(DatabaseConnectionState state, Runnable afterConnected) {
    DatabaseMeta meta = state.getDatabaseMeta();
    String description =
        BaseMessages.getString(PKG, "DatabasePerspective.Operation.Connect", meta.getName());
    runOperation(
        description,
        meta.getName(),
        operation -> {
          DatabaseMetaInformation info = new DatabaseMetaInformation(host.getVariables(), meta);
          info.getData(host.getLoggingObject(), operation.newMonitor());
          if (operation.isCancelled()) {
            return;
          }
          host.asyncExec(
              () -> {
                state.setInformation(info);
                state.setConnected(true);
                rebuildTree();
                selectConnection(meta.getName());
                if (afterConnected != null) {
                  afterConnected.run();
                }
              });
        });
  }

  /**
   * If {@code meta} is already connected in this workbench, run {@code afterConnected} immediately.
   * Otherwise ask (or auto-connect from config), connect, then run {@code afterConnected}.
   *
   * @return {@code false} when the user declined to connect
   */
  public boolean ensureConnectedForExecute(DatabaseMeta meta, Runnable afterConnected) {
    if (meta == null || afterConnected == null) {
      return false;
    }
    DatabaseConnectionState state = ensureConnection(meta);
    if (state.isConnected()) {
      afterConnected.run();
      return true;
    }
    if (!confirmConnectForExecute(meta)) {
      return false;
    }
    connect(state, afterConnected);
    return true;
  }

  /**
   * @return {@code true} when SQL execution should connect (auto-connect, or the user chose Yes)
   */
  boolean confirmConnectForExecute(DatabaseMeta meta) {
    DatabasePerspectiveConfig config = DatabasePerspectiveConfigSingleton.getConfig();
    if (config.isAutoConnectWhenExecutingSql()) {
      return true;
    }
    MessageDialogWithToggle dialog =
        new MessageDialogWithToggle(
            host.getShell(),
            BaseMessages.getString(PKG, "DatabasePerspective.ConnectToExecute.Title"),
            BaseMessages.getString(
                PKG, "DatabasePerspective.ConnectToExecute.Message", Const.NVL(meta.getName(), "")),
            SWT.ICON_QUESTION,
            new String[] {
              BaseMessages.getString(PKG, "System.Button.Yes"),
              BaseMessages.getString(PKG, "System.Button.No")
            },
            BaseMessages.getString(PKG, "DatabasePerspective.ConnectToExecute.Toggle"),
            false);
    int answer = dialog.open();
    if (dialog.getToggleState()) {
      config.setAutoConnectWhenExecutingSql(true);
      try {
        DatabasePerspectiveConfigSingleton.saveConfig();
      } catch (Exception e) {
        new ErrorDialog(
            host.getShell(),
            BaseMessages.getString(PKG, "DatabasePerspective.Error.Title"),
            e.getMessage(),
            e);
      }
    }
    return (answer & 0xFF) == 0;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DISCONNECT,
      toolTip = "i18n::DatabasePerspective.Toolbar.Disconnect.Tooltip",
      image = "ui/images/shutdown.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DISCONNECT,
      label = "i18n::DatabasePerspective.Menu.Disconnect",
      image = "ui/images/shutdown.svg")
  public void disconnectSelected() {
    DatabaseConnectionState state = selectedState();
    if (state == null) {
      return;
    }
    state.setConnected(false);
    state.setInformation(null);
    rebuildTree();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::DatabasePerspective.Toolbar.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  public void refreshSelected() {
    DatabaseConnectionState state = selectedState();
    if (state != null && state.isConnected()) {
      connect(state);
      return;
    }
    reloadConnections();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SQL,
      toolTip = "i18n::DatabasePerspective.Toolbar.SqlEditor.Tooltip",
      image = "ui/images/script.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SQL,
      label = "i18n::DatabasePerspective.Menu.SqlEditor",
      image = "ui/images/script.svg")
  public void openSqlEditor() {
    DatabaseConnectionState state = selectedState();
    if (state == null) {
      return;
    }
    DatabaseTreeNode node = selectedNode();
    String sql = "";
    if (node != null && node.isTableLike()) {
      String qualified =
          state
              .getDatabaseMeta()
              .getQuotedSchemaTableCombination(
                  host.getVariables(), node.getSchemaName(), node.getObjectName());
      sql = "SELECT * FROM " + qualified;
    }
    openSqlTab(state.getDatabaseMeta(), sql, null, sql, false);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DDL,
      toolTip = "i18n::DatabasePerspective.Toolbar.GenerateDdl.Tooltip",
      image = "ui/images/script.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DDL,
      label = "i18n::DatabasePerspective.Menu.GenerateDdl",
      image = "ui/images/script.svg")
  public void generateDdl() {
    DatabaseTreeNode node = selectedNode();
    DatabaseConnectionState state = selectedState();
    if (node == null || state == null || !node.isTableLike()) {
      return;
    }
    DatabaseMeta meta = state.getDatabaseMeta();
    String qualified =
        meta.getQuotedSchemaTableCombination(
            host.getVariables(), node.getSchemaName(), node.getObjectName());
    String description =
        BaseMessages.getString(PKG, "DatabasePerspective.Operation.GenerateDdl", qualified);
    runOperation(
        description,
        meta.getName(),
        operation -> {
          String ddl;
          try (Database db = new Database(host.getLoggingObject(), host.getVariables(), meta)) {
            operation.attachDatabase(db);
            db.connect();
            IRowMeta fields = db.getTableFields(qualified);
            ddl = db.getCreateTableStatement(node.getObjectName(), fields, null, false, null, true);
          }
          String sql = ddl;
          host.asyncExec(() -> openSqlTab(meta, sql, null, sql, true));
        });
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PREVIEW,
      toolTip = "i18n::DatabasePerspective.Toolbar.Preview.Tooltip",
      image = "ui/images/preview.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_PREVIEW,
      label = "i18n::DatabasePerspective.Menu.Preview",
      image = "ui/images/preview.svg")
  public void previewTable() {
    DatabaseTreeNode node = selectedNode();
    DatabaseConnectionState state = selectedState();
    if (node == null || state == null || !node.isTableLike()) {
      return;
    }
    DatabaseMeta meta = state.getDatabaseMeta();
    String sql =
        previewSelectSql(
            meta,
            host.getVariables(),
            node.getSchemaName(),
            node.getObjectName(),
            DatabaseSqlEditorTab.queryRowLimit());
    DatabaseSqlEditorTab tab = openSqlTab(meta, sql, null, sql, false);
    tab.executeSql();
  }

  /**
   * {@code SELECT * FROM schema.table} plus the dialect's limit clause ({@link
   * DatabaseMeta#getLimitClause(int)}).
   */
  static String previewSelectSql(
      DatabaseMeta meta, IVariables variables, String schemaName, String tableName, int rowLimit) {
    String qualified = meta.getQuotedSchemaTableCombination(variables, schemaName, tableName);
    String limit = Const.NVL(meta.getLimitClause(rowLimit), "");
    return "SELECT * FROM " + qualified + limit;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW,
      toolTip = "i18n::DatabasePerspective.Toolbar.Show.Tooltip",
      image = "ui/images/show.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SHOW,
      label = "i18n::DatabasePerspective.Menu.Show",
      image = "ui/images/show.svg")
  public void showTableInfo() {
    DatabaseTreeNode node = selectedNode();
    DatabaseConnectionState state = selectedState();
    if (node == null || state == null || !node.isTableLike()) {
      return;
    }
    openTableInfo(
        state.getDatabaseMeta(), node.getSchemaName(), node.getObjectName(), node.getKind());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_FLOAT,
      toolTip = "i18n::DatabasePerspective.Toolbar.Float.Tooltip",
      image = "ui/images/detach-panel.svg",
      separator = true)
  public void openFloatingWindow() {
    DatabaseSqlTabMemory.saveNow(this);
    DatabaseWorkbenchViews.openDialog(host.getHopGui());
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DOCK,
      toolTip = "i18n::DatabasePerspective.Toolbar.Dock.Tooltip",
      image = "ui/images/dock-panel.svg")
  public void openInBottomDock() {
    DatabaseSqlTabMemory.saveNow(this);
    DatabaseWorkbenchViews.openDock(host.getHopGui());
  }

  public DatabaseSqlEditorTab openSqlTab(
      DatabaseMeta meta, String sql, String filename, String buffer, boolean dirty) {
    if (!Utils.isEmpty(filename)) {
      for (TabItemHandler item : items) {
        if (item.getTypeHandler() instanceof DatabaseSqlEditorTab tab
            && tab.isSameFile(filename)
            && meta.getName().equals(tab.getDatabaseMeta().getName())) {
          tabFolder.setSelection(item.getTabItem());
          return tab;
        }
      }
    }
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    DatabaseSqlEditorTab tab = new DatabaseSqlEditorTab(tabFolder, host, this, meta);
    if (!Utils.isEmpty(filename)) {
      tab.setFilename(filename);
      if (buffer != null) {
        tab.applyBuffer(buffer, dirty);
      } else {
        try {
          tab.loadFromVfs();
        } catch (Exception e) {
          new ErrorDialog(
              host.getShell(),
              BaseMessages.getString(PKG, "DatabasePerspective.Error.Title"),
              BaseMessages.getString(PKG, "DatabasePerspective.Error.OpenFile", filename),
              e);
        }
      }
    } else {
      tab.setInitialText(Const.NVL(sql, ""));
    }
    tab.setTabItem(tabItem);
    tabItem.setControl(tab.getControl());
    tabItem.setData(tab);
    items.add(new TabItemHandler(tabItem, tab));
    if (!Utils.isEmpty(filename)) {
      host.getHopGui().fileRefreshDelegate.register(filename, tab);
    }
    tabFolder.setSelection(tabItem);
    host.updateGui(tab);
    schedulePersistSqlTabs();
    return tab;
  }

  public void openTableInfo(DatabaseMeta meta, String schemaName, String tableName) {
    openTableInfo(meta, schemaName, tableName, DatabaseTreeNode.Kind.TABLE);
  }

  public void openTableInfo(
      DatabaseMeta meta, String schemaName, String tableName, DatabaseTreeNode.Kind kind) {
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof DatabaseTableInfoTab tab
          && tab.matches(meta.getName(), schemaName, tableName)) {
        tabFolder.setSelection(item.getTabItem());
        return;
      }
    }
    CTabItem tabItem = new CTabItem(tabFolder, SWT.CLOSE);
    DatabaseTableInfoTab tab =
        new DatabaseTableInfoTab(tabFolder, host, this, meta, schemaName, tableName, kind);
    tab.setTabItem(tabItem);
    tabItem.setControl(tab.getControl());
    tabItem.setData(tab);
    items.add(new TabItemHandler(tabItem, tab));
    tabFolder.setSelection(tabItem);
    host.updateGui(tab);
    tab.loadDetails();
  }

  public void openSqlFile(String filename, DatabaseMeta meta, String buffer, boolean dirty) {
    openSqlTab(meta, null, filename, buffer, dirty);
  }

  /**
   * Select {@code meta} in the tree, start a background connect/refresh if needed, and open a SQL
   * tab with {@code sql} immediately so the user can edit and run while schemas load.
   */
  public void openSuggestedSql(DatabaseMeta meta, String sql) {
    if (meta == null || isDisposed()) {
      return;
    }
    DatabaseConnectionState state = ensureConnection(meta);
    selectConnection(meta.getName());
    openSqlTab(meta, Const.NVL(sql, ""), null, Const.NVL(sql, ""), true);
    if (!state.isConnected()) {
      connect(state);
    }
  }

  DatabaseConnectionState ensureConnection(DatabaseMeta meta) {
    DatabaseConnectionState state = connections.get(meta.getName());
    if (state == null) {
      state = new DatabaseConnectionState(meta);
      connections.put(meta.getName(), state);
      rebuildTree();
    } else {
      state.setDatabaseMeta(meta);
    }
    return state;
  }

  void schedulePersistSqlTabs() {
    DatabaseSqlTabMemory.scheduleSave(this);
  }

  boolean hasSqlEditorTabs() {
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof DatabaseSqlEditorTab) {
        return true;
      }
    }
    return false;
  }

  boolean restoreSqlTab(DatabaseSqlTabMemory.Snapshot snapshot) {
    DatabaseMeta meta = findConnection(snapshot.connection);
    if (meta == null) {
      return false;
    }
    if (!Utils.isEmpty(snapshot.filename) && !snapshot.dirty) {
      openSqlFile(snapshot.filename, meta, null, false);
      return true;
    }
    if (!Utils.isEmpty(snapshot.filename)) {
      openSqlFile(snapshot.filename, meta, snapshot.sql, true);
      return true;
    }
    DatabaseSqlEditorTab tab =
        openSqlTab(
            meta, Const.NVL(snapshot.sql, ""), null, Const.NVL(snapshot.sql, ""), snapshot.dirty);
    if (!Utils.isEmpty(snapshot.name)) {
      tab.setName(snapshot.name);
    }
    return true;
  }

  void selectSqlTabIndex(int index) {
    int sqlIndex = 0;
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof DatabaseSqlEditorTab) {
        if (sqlIndex == index && item.getTabItem() != null && !item.getTabItem().isDisposed()) {
          tabFolder.setSelection(item.getTabItem());
          return;
        }
        sqlIndex++;
      }
    }
  }

  List<DatabaseSqlTabMemory.Snapshot> snapshotSqlTabs() {
    List<DatabaseSqlTabMemory.Snapshot> snapshots = new ArrayList<>();
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof DatabaseSqlEditorTab tab) {
        snapshots.add(DatabaseSqlTabMemory.snapshotOf(tab));
      }
    }
    return snapshots;
  }

  int selectedSqlTabIndex() {
    CTabItem selected = tabFolder.getSelection();
    int index = 0;
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() instanceof DatabaseSqlEditorTab) {
        if (item.getTabItem() == selected) {
          return index;
        }
        index++;
      }
    }
    return 0;
  }

  void closeSqlEditorTabs() {
    restoringSqlTabs = true;
    try {
      List<TabItemHandler> copy = new ArrayList<>(items);
      for (TabItemHandler item : copy) {
        if (item.getTypeHandler() instanceof DatabaseSqlEditorTab) {
          disposeTab(item);
        }
      }
    } finally {
      restoringSqlTabs = false;
    }
  }

  void selectConnection(String name) {
    if (tree == null || tree.isDisposed() || Utils.isEmpty(name)) {
      return;
    }
    for (TreeItem item : tree.getItems()) {
      Object data = item.getData();
      if (data instanceof DatabaseTreeNode node
          && node.getKind() == DatabaseTreeNode.Kind.CONNECTION
          && name.equals(node.getConnectionName())) {
        tree.setSelection(item);
        tree.showItem(item);
        updateToolbar();
        return;
      }
    }
  }

  public List<String> connectionNames() {
    return new ArrayList<>(connections.keySet());
  }

  public DatabaseMeta findConnection(String name) {
    DatabaseConnectionState state = connections.get(name);
    return state == null ? null : state.getDatabaseMeta();
  }

  private void layoutOperationsPane(boolean expanded) {
    if (rightSash.isDisposed() || rightComposite.isDisposed()) {
      return;
    }
    Composite statusBar = operationsPanel.getStatusBar();
    if (expanded) {
      statusBar.setVisible(false);
      statusBar.setLayoutData(new FormDataBuilder().left().bottom().height(0).width(0).result());
      rightSash.setLayoutData(new FormDataBuilder().fullSize().result());
      rightSash.setMaximizedControl(null);
      SashFormMemory.restore(rightSash, RIGHT_SASH_KEY, 80, 20);
    } else {
      statusBar.setVisible(true);
      statusBar.setLayoutData(new FormDataBuilder().bottom().fullWidth().result());
      rightSash.setLayoutData(
          new FormDataBuilder().top().fullWidth().bottom(statusBar, 0).result());
      rightSash.setMaximizedControl(tabFolder);
    }
    rightComposite.layout(true, true);
  }

  public void runOperation(String description, String connectionName, DatabaseOperation.Work work) {
    DatabaseOperation operation = new DatabaseOperation(description, connectionName);
    operationsPanel.addOperation(operation);
    BackgroundThreadFacade.start(
        () -> {
          try {
            work.run(operation);
            operation.complete();
          } catch (Exception e) {
            operation.fail(Const.NVL(e.getMessage(), e.toString()));
          }
          host.asyncExec(operationsPanel::refresh);
        },
        "hop-database-op");
  }

  public void refreshTab(DatabaseSqlEditorTab tab) {
    tab.setTabItem(tab.getTabItem());
    host.updateGui(tab);
  }

  public IHopFileTypeHandler getActiveFileTypeHandler() {
    CTabItem selected = tabFolder.getSelection();
    if (selected == null || selected.isDisposed()) {
      return new EmptyHopFileTypeHandler();
    }
    Object data = selected.getData();
    if (data instanceof IHopFileTypeHandler handler) {
      return handler;
    }
    return new EmptyHopFileTypeHandler();
  }

  public void setActiveFileTypeHandler(IHopFileTypeHandler handler) {
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() == handler) {
        tabFolder.setSelection(item.getTabItem());
        return;
      }
    }
  }

  public List<TabItemHandler> getItems() {
    return items;
  }

  public boolean remove(IHopFileTypeHandler handler) {
    if (handler == null || !handler.isCloseable()) {
      return false;
    }
    TabItemHandler found = null;
    for (TabItemHandler item : items) {
      if (item.getTypeHandler() == handler) {
        found = item;
        break;
      }
    }
    if (found == null) {
      return true;
    }
    disposeTab(found);
    return true;
  }

  private void disposeTab(TabItemHandler item) {
    items.remove(item);
    IHopFileTypeHandler handler = item.getTypeHandler();
    if (handler != null && handler.getFilename() != null) {
      host.getHopGui().fileRefreshDelegate.remove(handler.getFilename());
    }
    if (item.getTabItem() != null && !item.getTabItem().isDisposed()) {
      item.getTabItem().dispose();
    }
    host.updateGui(getActiveFileTypeHandler());
    schedulePersistSqlTabs();
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    if (tabItem == null || tabItem.isDisposed()) {
      return;
    }
    Object data = tabItem.getData();
    boolean removed = true;
    if (data instanceof IHopFileTypeHandler handler) {
      removed = remove(handler);
    }
    if (removed && !tabItem.isDisposed()) {
      tabItem.dispose();
    }
    if (!removed && event != null) {
      event.doit = false;
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return tabFolder;
  }
}
