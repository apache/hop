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

package org.apache.hop.ui.core.database.dialog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.database.Catalog;
import org.apache.hop.core.database.Database;
import org.apache.hop.core.database.DatabaseMeta;
import org.apache.hop.core.database.DatabaseMetaInformation;
import org.apache.hop.core.database.Schema;
import org.apache.hop.core.exception.HopDatabaseException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.menu.GuiMenuElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElementType;
import org.apache.hop.core.logging.ILoggingObject;
import org.apache.hop.core.logging.LoggingObject;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.search.SearchMatcher;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.database.DatabaseTreeNode;
import org.apache.hop.ui.core.database.DatabaseTreeUtil;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.EnterSelectionDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ShowRowsDialog;
import org.apache.hop.ui.core.dialog.TransformFieldsDialog;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.gui.WindowProperty;
import org.apache.hop.ui.core.widget.FolderTreeIcons;
import org.apache.hop.ui.core.widget.HopTree;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.perspective.database.DatabaseWorkbenchDialog;
import org.apache.hop.ui.hopgui.perspective.database.DatabaseWorkbenchViews;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.jspecify.annotations.Nullable;

/**
 * This dialog represents an explorer type of interface on a given database connection. It shows the
 * tables defined in the visible schemas or catalogs on that connection. The interface also allows
 * you to get all kinds of information on those tables.
 */
@GuiPlugin
public class DatabaseExplorerDialog extends Dialog {
  private static final Class<?> PKG = DatabaseExplorerDialog.class;
  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID =
      "DatabaseExplorerDialog-ContextMenu";
  public static final String GUI_PLUGIN_SEARCHBAR_PARENT_ID = "DatabaseExplorerDialog-SearchBar";
  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "DatabaseExplorerDialog-Toolbar";

  public static final String TOOLBAR_ITEM_REGEX = "10000-DatabaseExplorerDialog-ToolBar-Regex";
  public static final String TOOLBAR_ITEM_REFRESH = "DatabaseExplorerDialog-ToolBar-10000-Refresh";
  public static final String TOOLBAR_ITEM_EXPAND_ALL =
      "DatabaseExplorerDialog-ToolBar-10010-ExpandAll";
  public static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "DatabaseExplorerDialog-ToolBar-10020-CollapseAll";
  public static final String TOOLBAR_ITEM_PREVIEW = "DatabaseExplorerDialog-Toolbar-10050-Preview";
  public static final String TOOLBAR_ITEM_SHOW_LAYOUT =
      "DatabaseExplorerDialog-Toolbar-10060-ShowLayout";
  public static final String TOOLBAR_ITEM_SQL_SELECT =
      "DatabaseExplorerDialog-Toolbar-10070-SqlSelect";
  public static final String TOOLBAR_ITEM_OPEN_PERSPECTIVE =
      "DatabaseExplorerDialog-Toolbar-20000-OpenPerspective";

  public static final String CONTEXT_MENU_PREVIEW_100 =
      "DatabaseExplorerDialog-ContextMenu-10010-Preview100";
  public static final String CONTEXT_MENU_PREVIEW_N =
      "DatabaseExplorerDialog-ContextMenu-10020-PreviewN";
  public static final String CONTEXT_MENU_SHOW_SIZE =
      "DatabaseExplorerDialog-ContextMenu-10030-ShowSize";
  public static final String CONTEXT_MENU_SHOW_LAYOUT =
      "DatabaseExplorerDialog-ContextMenu-10040-ShowLayout";
  public static final String CONTEXT_MENU_DDL =
      "DatabaseExplorerDialog-ContextMenu-10050-GenerateDdl";
  public static final String CONTEXT_MENU_DDL_OTHER =
      "DatabaseExplorerDialog-ContextMenu-10060-GenerateDdlOther";
  public static final String CONTEXT_MENU_SQL_SELECT =
      "DatabaseExplorerDialog-ContextMenu-10070-SqlSelect";
  public static final String CONTEXT_MENU_SQL_TRUNCATE =
      "DatabaseExplorerDialog-ContextMenu-10080-SqlTruncate";

  private static final String STRING_CATALOG =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Catalogs.Label");
  private static final String STRING_SCHEMAS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Schemas.Label");
  private static final String STRING_TABLES =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Tables.Label");
  private static final String STRING_VIEWS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Views.Label");
  private static final String STRING_SYNONYMS =
      BaseMessages.getString(PKG, "DatabaseExplorerDialog.Synonyms.Label");

  private static final int FILTER_DEBOUNCE_MS = 250;

  /** Debounced search action so we don't rebuild the tree on every keystroke. */
  private final Runnable filterRunnable = this::updateTree;

  private final DatabaseMeta databaseMeta;
  private final IVariables variables;
  private final ILoggingObject loggingObject;
  private final List<DatabaseMeta> databases;
  private final boolean justLook;

  private Shell shell;
  private Tree wTree;
  private TreeItem connectionItem;
  private Text wSearch;
  private GuiToolbarWidgets searchBarWidgets;
  private GuiToolbarWidgets toolBarWidgets;
  private GuiMenuWidgets menuWidgets;
  private SearchMatcher filter;
  private boolean filterUseRegEx;
  private String selectedSchemaName;
  private String selectedTableName;
  @Setter @Getter private boolean splitSchemaAndTable;
  private DatabaseMetaInformation databaseMetaInformation;

  public DatabaseExplorerDialog(
      Shell parentShell,
      int style,
      IVariables variables,
      DatabaseMeta databaseMeta,
      List<DatabaseMeta> databases) {
    this(parentShell, style, variables, databaseMeta, databases, false, true);
  }

  public DatabaseExplorerDialog(
      Shell parent,
      int style,
      IVariables variables,
      DatabaseMeta databaseMeta,
      List<DatabaseMeta> databases,
      boolean look,
      boolean splitSchemaAndTable) {
    super(parent, style);
    this.databaseMeta = databaseMeta;
    this.variables = variables;
    this.databases = databases;
    this.justLook = look;
    this.splitSchemaAndTable = splitSchemaAndTable;
    this.loggingObject = new LoggingObject("Database Explorer");

    filterUseRegEx = false;
    selectedSchemaName = null;
    selectedTableName = null;
  }

  public boolean open() {
    // Modeless so the floating Database window (SQL / DDL) can stay usable on Linux.
    shell = new Shell(getParent(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN);
    PropsUi.setLook(shell);
    shell.setImage(GuiResource.getInstance().getImageDatabase());
    // Do not include the connection name in the title (to save/restore the dialog box dimensions
    // once)
    shell.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Title"));

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = PropsUi.getFormMargin();
    formLayout.marginHeight = PropsUi.getFormMargin();
    shell.setLayout(formLayout);
    shell.setMinimumSize(200, 400);

    int margin = PropsUi.getMargin();

    // Main buttons at the bottom
    //
    List<Button> buttons = new ArrayList<>();
    Button wOk = new Button(shell, SWT.PUSH);
    wOk.setText(BaseMessages.getString(PKG, "System.Button.OK"));
    wOk.addListener(SWT.Selection, e -> ok());
    buttons.add(wOk);
    shell.setDefaultButton(wOk);

    if (!justLook) {
      Button wCancel = new Button(shell, SWT.PUSH);
      wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel"));
      wCancel.addListener(SWT.Selection, e -> cancel());
      buttons.add(wCancel);
    }
    BaseTransformDialog.positionBottomButtons(shell, buttons.toArray(new Button[0]), margin, null);

    // Create toolbar for search options at the top right
    IToolbarContainer searchBarContainer =
        ToolbarFacade.createToolbarContainer(shell, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    searchBarWidgets = new GuiToolbarWidgets();
    searchBarWidgets.registerGuiPluginObject(this);
    searchBarWidgets.createToolbarWidgets(searchBarContainer, GUI_PLUGIN_SEARCHBAR_PARENT_ID);
    Control searchBar = searchBarContainer.getControl();
    searchBar.setLayoutData(FormDataBuilder.builder().top().right().build());
    PropsUi.setLook(searchBar, Props.WIDGET_STYLE_TOOLBAR);

    // Create search/filter text box at the top left
    wSearch = new Text(shell, SWT.SEARCH | SWT.ICON_SEARCH | SWT.ICON_CANCEL | SWT.BORDER);
    wSearch.setMessage(BaseMessages.getString(PKG, "DatabaseExplorerDialog.Search.Placeholder"));
    wSearch.setLayoutData(FormDataBuilder.builder().top().left().right(searchBar, -margin).build());
    wSearch.setData(BaseDialog.NO_DEFAULT_HANDLER, "Nop");
    wSearch.addListener(SWT.Modify, e -> filterTree());
    wSearch.addListener(SWT.DefaultSelection, e -> updateTree());
    PropsUi.setLook(wSearch, Props.WIDGET_STYLE_TOOLBAR);

    // Create composite
    Composite composite = new Composite(shell, SWT.BORDER);
    composite.setLayout(new FormLayout());
    composite.setLayoutData(
        FormDataBuilder.builder()
            .top(searchBar, margin)
            .bottom(wOk, -2 * margin)
            .fullWidth()
            .build());
    PropsUi.setLook(composite);

    // Create toolbar for tree actions
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    Control toolBar = toolBarContainer.getControl();
    toolBar.setLayoutData(FormDataBuilder.builder().top().fullWidth().build());
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    // Create tree
    wTree = new HopTree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    FolderTreeIcons.install(wTree);
    wTree.setLayoutData(
        FormDataBuilder.builder().top(toolBar, margin).bottom().fullWidth().build());
    wTree.addListener(SWT.DefaultSelection, this::openTreeItem);
    wTree.addListener(SWT.Selection, e -> updateGui());
    PropsUi.setLook(wTree);

    // Create context menu
    Menu menu = new Menu(wTree);
    menuWidgets = new GuiMenuWidgets();
    menuWidgets.registerGuiPluginObject(this);
    menuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, shell, menu);
    wTree.setMenu(menu);
    wTree.addListener(SWT.MenuDetect, e -> updateGui());

    // So shortcut work and we're tried before the active perspective
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this, shell);
    HopGui.getInstance().replaceKeyboardShortcutListeners(shell, keyHandler);

    refresh();

    updateGui();

    wSearch.setFocus();

    BaseDialog.defaultShellHandling(shell, c -> ok(), c -> cancel());

    return selectedTableName != null;
  }

  public void cancel() {
    selectedSchemaName = null;
    selectedTableName = null;
    dispose();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_SEARCHBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REGEX,
      toolTip = "i18n::DatabaseExplorerDialog.RegEx.Tooltip",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/regex.svg")
  public void filterUseRegEx() {
    this.filterUseRegEx = !this.filterUseRegEx;
    // Update the button icon
    updateGui();
    // Apply the filter
    updateTree();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EXPAND_ALL,
      toolTip = "i18n::System.Tooltip.ExpandAll",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/expand-all.svg",
      separator = true)
  @GuiKeyboardShortcut(control = true, key = '+')
  @GuiOsxKeyboardShortcut(command = true, key = '+')
  public void expandAll() {
    expandAllItems(connectionItem.getItems(), true);
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COLLAPSE_ALL,
      toolTip = "i18n::System.Tooltip.CollapseALl",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/collapse-all.svg")
  @GuiKeyboardShortcut(control = true, key = '-')
  @GuiOsxKeyboardShortcut(command = true, key = '-')
  public void collapseAll() {
    expandAllItems(connectionItem.getItems(), false);
  }

  private void expandAllItems(TreeItem[] items, boolean expand) {
    for (TreeItem item : items) {
      FolderTreeIcons.setExpanded(item, expand);
      if (item.getItemCount() > 0) {
        expandAllItems(item.getItems(), expand);
      }
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::System.Button.Refresh",
      type = GuiToolbarElementType.BUTTON,
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    GetDatabaseInfoProgressDialog dialog =
        new GetDatabaseInfoProgressDialog(shell, variables, databaseMeta);
    this.databaseMetaInformation = dialog.open();
    updateTree();
  }

  public void updateGui() {
    // Update the regex filter icons in the search bar
    ToolItem item = searchBarWidgets.findToolItem(TOOLBAR_ITEM_REGEX);
    if (item != null && !item.isDisposed()) {
      if (filterUseRegEx) {
        item.setImage(GuiResource.getInstance().getImageRegex());
      } else {
        item.setImage(GuiResource.getInstance().getImageRegexDisabled());
      }
    }

    DatabaseTreeNode node = getSelectedNode();
    boolean isTable = node != null && node.isTableLike();
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_PREVIEW, isTable);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_SHOW_LAYOUT, isTable);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_SQL_SELECT, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_PREVIEW_100, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_PREVIEW_N, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_SHOW_SIZE, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_SHOW_LAYOUT, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_DDL, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_DDL_OTHER, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_SQL_SELECT, isTable);
    menuWidgets.enableMenuItem(CONTEXT_MENU_SQL_TRUNCATE, isTable);
  }

  private SearchMatcher createSearchMatcher() {
    if (wSearch != null && !wSearch.isDisposed()) {
      String search = wSearch.getText();
      if (!Utils.isEmpty(search)) {
        return new SearchMatcher(search, false, filterUseRegEx, false);
      }
    }
    return null; // new SearchMatcher("", false, false, false);
  }

  /** Filter the tree based on search text, debounced so we don't rebuild on every keystroke. */
  protected void filterTree() {
    if (shell == null || shell.isDisposed()) {
      return;
    }
    shell.getDisplay().timerExec(FILTER_DEBOUNCE_MS, filterRunnable);
  }

  /** Update tree when refresh or filter changed */
  private void updateTree() {
    if (wTree.isDisposed()) {
      return;
    }

    shell.setCursor(shell.getDisplay().getSystemCursor(SWT.CURSOR_WAIT));
    wTree.setRedraw(false);

    // Remove all previous items
    wTree.removeAll();

    // Create connection tree item
    String connectionName = "";
    Image image = GuiResource.getInstance().getImageDatabase();
    if (databaseMeta != null) {
      connectionName = databaseMeta.getName();
      image = GuiResource.getInstance().getImage(databaseMeta.getIDatabase());
    }
    connectionItem = new TreeItem(wTree, SWT.NONE);
    connectionItem.setImage(image);
    connectionItem.setText(connectionName);
    connectionItem.setData(DatabaseTreeNode.connection(connectionName, true));

    if (databaseMetaInformation != null) {

      // Create search filter
      this.filter = createSearchMatcher();

      if (!updateTreeSchemas()) {
        if (!updateTreeCatalogs()) {
          addFolder(
              connectionItem,
              connectionName,
              STRING_TABLES,
              databaseMetaInformation.getTables(),
              DatabaseTreeNode.Kind.TABLE);
          addFolder(
              connectionItem,
              connectionName,
              STRING_VIEWS,
              databaseMetaInformation.getViews(),
              DatabaseTreeNode.Kind.VIEW);
          addFolder(
              connectionItem,
              connectionName,
              STRING_SYNONYMS,
              databaseMetaInformation.getSynonyms(),
              DatabaseTreeNode.Kind.SYNONYM);
        }
      }
    }

    // Always expand the root item
    FolderTreeIcons.setExpanded(connectionItem, true);

    wTree.setRedraw(true);
    shell.setCursor(null);
  }

  // Database support catalogs
  private boolean updateTreeCatalogs() {
    Catalog[] catalogs = databaseMetaInformation.getCatalogs();
    if (catalogs != null && catalogs.length > 0) {
      String connectionName = databaseMeta.getName();
      for (Catalog catalog : catalogs) {
        TreeItem catalogItem = new TreeItem(connectionItem, SWT.NONE);
        catalogItem.setText(Const.NVL(catalog.getCatalogName(), ""));
        catalogItem.setImage(GuiResource.getInstance().getImageFolder());
        catalogItem.setData(DatabaseTreeNode.catalog(connectionName, catalog.getCatalogName()));
        addSchemaObjects(
            catalogItem,
            connectionName,
            catalog.getCatalogName(),
            catalog.getItems(),
            databaseMetaInformation);
      }
      return true;
    }
    return false;
  }

  // Database support schemas
  private boolean updateTreeSchemas() {
    Schema[] schemas = databaseMetaInformation.getSchemas();
    if (schemas != null && schemas.length > 0) {
      String connectionName = databaseMeta.getName();
      for (Schema schema : schemas) {
        if (schemaOrChildMatches(schema, databaseMetaInformation)) {
          TreeItem schemaItem = new TreeItem(connectionItem, SWT.NONE);
          schemaItem.setText(Const.NVL(schema.getSchemaName(), ""));
          schemaItem.setImage(GuiResource.getInstance().getImageSchema());
          schemaItem.setData(DatabaseTreeNode.schema(connectionName, schema.getSchemaName()));
          addSchemaObjects(
              schemaItem,
              connectionName,
              schema.getSchemaName(),
              schema.getItems(),
              databaseMetaInformation);
        }
      }
      return true;
    }
    return false;
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
    TreeItem item = new TreeItem(parent, SWT.NONE);
    item.setText(folderName);
    item.setImage(GuiResource.getInstance().getImageFolder());
    item.setData(DatabaseTreeNode.folder(connectionName, folderName));
    addTables(item, connectionName, names, kind);
  }

  private void addTables(
      TreeItem parent, String connectionName, String[] names, DatabaseTreeNode.Kind kind) {
    if (names == null) {
      return;
    }
    boolean expanded = false;
    for (String name : names) {
      if (matchesFilter(name, null)) {
        TreeItem item = new TreeItem(parent, SWT.NONE);
        item.setText(name);
        item.setImage(DatabaseTreeUtil.imageFor(kind));
        item.setData(DatabaseTreeNode.table(kind, connectionName, null, name));

        // Highlight selected item
        if (name.equalsIgnoreCase(selectedTableName)) {
          item.setFont(GuiResource.getInstance().getFontBold());
          wTree.setSelection(item);
          wTree.showItem(item);
          expanded = true;
        }
      }
    }
    FolderTreeIcons.setExpanded(parent, expanded);
  }

  /**
   * Tables, views and synonyms under a schema (or catalog). Views get {@code view.svg} via {@link
   * DatabaseTreeUtil#kindOf}.
   */
  private void addSchemaObjects(
      TreeItem parent,
      String connectionName,
      String schemaName,
      String[] items,
      DatabaseMetaInformation info) {
    Collection<String> views = DatabaseTreeUtil.namesForSchema(info.getViewMap(), schemaName);
    Collection<String> synonyms = DatabaseTreeUtil.namesForSchema(info.getSynonymMap(), schemaName);
    List<String> names = new ArrayList<>();
    if (items != null) {
      names.addAll(Arrays.asList(items));
    }
    for (String view : views) {
      if (!DatabaseTreeUtil.containsIgnoreCase(names, view)) {
        names.add(view);
      }
    }
    for (String synonym : synonyms) {
      if (!DatabaseTreeUtil.containsIgnoreCase(names, synonym)) {
        names.add(synonym);
      }
    }
    names.sort(String.CASE_INSENSITIVE_ORDER);
    boolean expanded = false;
    for (String name : names) {
      if (matchesFilter(name, schemaName)) {
        DatabaseTreeNode.Kind kind = DatabaseTreeUtil.kindOf(name, views, synonyms);
        TreeItem item = new TreeItem(parent, SWT.NONE);
        item.setText(name);
        item.setImage(DatabaseTreeUtil.imageFor(kind));
        item.setData(DatabaseTreeNode.table(kind, connectionName, schemaName, name));

        // Highlight selected item
        if (schemaName.equalsIgnoreCase(selectedSchemaName)
            && name.equalsIgnoreCase(selectedTableName)) {
          parent.setFont(GuiResource.getInstance().getFontBold());
          item.setFont(GuiResource.getInstance().getFontBold());
          wTree.setSelection(item);
          wTree.showItem(item);
          expanded = true;
        }

        // If is filtered and an item match then expand parent
        if (filter != null) {
          expanded = true;
        }
      }
    }
    FolderTreeIcons.setExpanded(parent, expanded);
  }

  private boolean matchesFilter(String name, String schemaName) {
    if (filter == null) {
      return true;
    }
    return filter.matches(name) || filter.matches(schemaName);
  }

  private boolean schemaOrChildMatches(Schema schema, DatabaseMetaInformation info) {
    if (filter == null) {
      return true;
    }
    if (filter.matches(schema.getSchemaName())) {
      return true;
    }
    if (schema.getItems() != null) {
      for (String table : schema.getItems()) {
        if (filter.matches(table)) {
          return true;
        }
      }
    }
    for (String view : DatabaseTreeUtil.namesForSchema(info.getViewMap(), schema.getSchemaName())) {
      if (filter.matches(view)) {
        return true;
      }
    }
    return false;
  }

  private @Nullable DatabaseTreeNode getSelectedNode() {
    TreeItem[] selection = wTree.getSelection();
    if (selection.length != 1) {
      return null;
    }
    Object data = selection[0].getData();
    return data instanceof DatabaseTreeNode node ? node : null;
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_PREVIEW,
      type = GuiToolbarElementType.BUTTON,
      toolTip = "i18n::DatabaseExplorerDialog.Toolbar.Preview.Tooltip",
      image = "ui/images/preview.svg",
      separator = true)
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_PREVIEW_100,
      label = "i18n::DatabaseExplorerDialog.Menu.Preview100",
      image = "ui/images/preview.svg")
  @GuiKeyboardShortcut(key = SWT.F4)
  @GuiOsxKeyboardShortcut(key = SWT.F4)
  public void showTablePreview100() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      showTablePreview(node, false);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_PREVIEW_N,
      label = "i18n::DatabaseExplorerDialog.Menu.PreviewN",
      image = "ui/images/preview.svg")
  public void showTablePreviewN() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      showTablePreview(node, true);
    }
  }

  public void showTablePreview(DatabaseTreeNode node, boolean askSetting) {
    int limit = 100;
    int queryTimeoutSeconds = 0;

    if (askSetting) {
      PreviewTableSettingsDialog settingsDialog =
          new PreviewTableSettingsDialog(shell, 100, variables, false);
      PreviewTableSettingsDialog.Settings settings = settingsDialog.open();
      if (settings == null) {
        return;
      }
      limit = settings.rowLimit;
      queryTimeoutSeconds = settings.queryTimeoutSeconds;
    }

    GetPreviewTableProgressDialog dialog =
        new GetPreviewTableProgressDialog(
            shell,
            variables,
            databaseMeta,
            node.getSchemaName(),
            node.getObjectName(),
            limit,
            queryTimeoutSeconds);
    List<Object[]> rows = dialog.open();
    if (dialog.isPreviewSucceeded()) {
      if (!rows.isEmpty()) {
        new ShowRowsDialog(
                shell,
                variables,
                BaseMessages.getString(PKG, "DatabaseExplorerDialog.ShowRows.Title"),
                BaseMessages.getString(
                    PKG, "DatabaseExplorerDialog.ShowRows.Message", node.getObjectName()),
                dialog.getRowMeta(),
                rows)
            .open();
      } else {
        MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        mb.setMessage(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoRows.Message"));
        mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoRows.Title"));
        mb.open();
      }
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_LAYOUT,
      type = GuiToolbarElementType.BUTTON,
      toolTip = "i18n::DatabaseExplorerDialog.Toolbar.ShowLayout.Tooltip",
      image = "ui/images/layout.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SHOW_LAYOUT,
      label = "i18n::DatabaseExplorerDialog.Menu.ShowLayout",
      image = "ui/images/layout.svg",
      separator = true)
  public void showTableLayout() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      String table =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());
      String sql = databaseMeta.getSqlQueryFields(table);
      IRowMeta result = null;
      try (Database db =
          new Database(HopGui.getInstance().getLoggingObject(), variables, databaseMeta)) {
        db.connect();
        result = db.getQueryFields(sql, false);
      } catch (Exception e) {
        // Do Nothing
      }
      if (result != null) {
        TransformFieldsDialog sfd =
            new TransformFieldsDialog(shell, variables, SWT.NONE, table, result);
        sfd.open();
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SHOW_SIZE,
      label = "i18n::DatabaseExplorerDialog.Menu.ShowSize")
  public void showTableSize() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      GetTableSizeProgressDialog dialog =
          new GetTableSizeProgressDialog(
              shell, variables, databaseMeta, node.getObjectName(), node.getSchemaName());
      Long size = dialog.open();
      if (size != null) {
        String tableName =
            databaseMeta.getQuotedSchemaTableCombination(
                variables, node.getSchemaName(), node.getObjectName());

        MessageBox mb = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.OK);
        mb.setMessage(
            BaseMessages.getString(
                PKG, "DatabaseExplorerDialog.TableSize.Message", tableName, size.toString()));
        mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.TableSize.Title"));
        mb.open();
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DDL,
      label = "i18n::DatabaseExplorerDialog.Menu.GenDDL")
  public void showDDL() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      String table =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());

      try (Database db = new Database(loggingObject, variables, databaseMeta)) {
        db.connect();
        IRowMeta rowMeta = db.getTableFields(table);
        String sql = db.getCreateTableStatement(table, rowMeta, null, false, null, true);
        openSqlEditor(sql);
      } catch (HopDatabaseException dbe) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "Dialog.Error.Header"),
            BaseMessages.getString(PKG, "DatabaseExplorerDialog.Error.RetrieveLayout"),
            dbe);
      }
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_DDL_OTHER,
      label = "i18n::DatabaseExplorerDialog.Menu.GenDDLOtherConn")
  public void showDDLForOther() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null && databases != null) {
      String table =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());

      try (Database database = new Database(loggingObject, variables, databaseMeta)) {
        database.connect();

        IRowMeta rowMeta = database.getTableFields(table);

        // Now select the other connection...
        String[] connectionNames = new String[databases.size()];
        for (int i = 0; i < connectionNames.length; i++) {
          connectionNames[i] = (databases.get(i)).getName();
        }

        EnterSelectionDialog dialog =
            new EnterSelectionDialog(
                shell,
                connectionNames,
                BaseMessages.getString(PKG, "DatabaseExplorerDialog.TargetDatabase.Title"),
                BaseMessages.getString(PKG, "DatabaseExplorerDialog.TargetDatabase.Message"));
        String target = dialog.open();
        if (target != null) {
          DatabaseMeta targetDatabaseMeta = DatabaseMeta.findDatabase(databases, target);
          try (Database targetDatabase =
              new Database(loggingObject, variables, targetDatabaseMeta)) {
            String sql =
                targetDatabase.getCreateTableStatement(table, rowMeta, null, false, null, true);
            openSqlEditor(targetDatabaseMeta, sql);
          }
        }
      } catch (HopDatabaseException dbe) {
        new ErrorDialog(
            shell,
            BaseMessages.getString(PKG, "Dialog.Error.Header"),
            BaseMessages.getString(PKG, "DatabaseExplorerDialog.Error.GenDDL"),
            dbe);
      }
    } else {
      MessageBox mb = new MessageBox(shell, SWT.NONE | SWT.ICON_INFORMATION);
      mb.setMessage(
          BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoConnectionsKnown.Message"));
      mb.setText(BaseMessages.getString(PKG, "DatabaseExplorerDialog.NoConnectionsKnown.Title"));
      mb.open();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SQL_SELECT,
      type = GuiToolbarElementType.BUTTON,
      toolTip = "i18n::DatabaseExplorerDialog.Toolbar.SQLSelect.Tooltip",
      image = "ui/images/script.svg")
  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SQL_SELECT,
      label = "i18n::DatabaseExplorerDialog.Menu.SQLSelect",
      image = "ui/images/script.svg",
      separator = true)
  public void showSqlSelect() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      String table =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());
      openSqlEditor("SELECT * FROM " + table);
    }
  }

  @GuiMenuElement(
      root = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      parentId = GUI_PLUGIN_CONTEXT_MENU_PARENT_ID,
      id = CONTEXT_MENU_SQL_TRUNCATE,
      label = "i18n::DatabaseExplorerDialog.Menu.Truncate")
  public void showSqlTruncate() {
    DatabaseTreeNode node = getSelectedNode();
    if (node != null) {
      String table =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());
      openSqlEditor("-- TRUNCATE TABLE " + table);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_OPEN_PERSPECTIVE,
      type = GuiToolbarElementType.BUTTON,
      toolTip = "i18n::DatabaseExplorerDialog.Toolbar.OpenPerspective.Tooltip",
      image = "ui/images/database-perspective.svg",
      separator = true)
  public void openInDatabasePerspective() {
    cancel();
    DatabaseWorkbenchViews.openInDatabase(databaseMeta, "");
  }

  protected void openSqlEditor(String sql) {
    openSqlEditor(databaseMeta, sql);
  }

  protected void openSqlEditor(DatabaseMeta meta, String sql) {
    DatabaseWorkbenchDialog.openSql(meta, sql);
  }

  public void dispose() {
    PropsUi.getInstance().setScreen(new WindowProperty(shell));
    shell.dispose();
  }

  public void ok() {
    if (justLook) {
      dispose();
      return;
    }

    DatabaseTreeNode node = getSelectedNode();
    if (node == null || !node.isTableLike()) {
      return;
    }

    if (splitSchemaAndTable) {
      selectedSchemaName = node.getSchemaName();
      selectedTableName = node.getObjectName();
    } else {
      selectedSchemaName = null;
      selectedTableName =
          databaseMeta.getQuotedSchemaTableCombination(
              variables, node.getSchemaName(), node.getObjectName());
    }

    dispose();
  }

  public void openTreeItem(Event e) {
    TreeItem[] selection = wTree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];

    DatabaseTreeNode node = (DatabaseTreeNode) item.getData();
    if (!node.isTableLike()) {
      // Expand/Collapse hierarchy
      FolderTreeIcons.setExpanded(item, !item.getExpanded());
      return;
    }

    if (justLook) {
      showTablePreview(node, false);
    } else {
      ok();
    }
  }

  public void setSelectedSchemaAndTable(String schema, String table) {
    this.selectedSchemaName = schema;
    this.selectedTableName = table;
  }

  public String getSchemaName() {
    return selectedSchemaName;
  }

  public String getTableName() {
    return selectedTableName;
  }
}
