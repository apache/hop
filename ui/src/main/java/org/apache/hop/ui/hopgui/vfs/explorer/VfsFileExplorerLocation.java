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

package org.apache.hop.ui.hopgui.vfs.explorer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.Selectors;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.core.widget.FolderTreeIcons;
import org.apache.hop.ui.core.widget.HopTree;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.TreeItem;

/**
 * One location tab: lazy folder tree, file details, filter and bookmarks. Listings run off the UI
 * thread.
 */
@GuiPlugin
public class VfsFileExplorerLocation extends Composite {

  public static final Class<?> PKG = VfsFileExplorer.class;

  public static final String NAVIGATE_TOOLBAR_PARENT_ID = "VfsFileExplorer-NavigateToolbar";
  public static final String TREE_TOOLBAR_PARENT_ID = "VfsFileExplorer-TreeToolbar";
  public static final String DETAILS_TOOLBAR_PARENT_ID = "VfsFileExplorer-DetailsToolbar";
  public static final String BOOKMARKS_TOOLBAR_PARENT_ID = "VfsFileExplorer-BookmarksToolbar";

  private static final String NAVIGATE_HOME = "VfsFileExplorer-Navigate-0000-Home";
  private static final String NAVIGATE_UP = "VfsFileExplorer-Navigate-0010-Up";
  private static final String NAVIGATE_BACK = "VfsFileExplorer-Navigate-0100-Back";
  private static final String NAVIGATE_FORWARD = "VfsFileExplorer-Navigate-0110-Forward";
  private static final String NAVIGATE_REFRESH = "VfsFileExplorer-Navigate-9999-Refresh";

  private static final String TREE_CREATE = "VfsFileExplorer-Tree-0020-CreateFolder";
  private static final String TREE_DELETE = "VfsFileExplorer-Tree-0100-Delete";
  private static final String TREE_RENAME = "VfsFileExplorer-Tree-0110-Rename";
  private static final String TREE_HIDDEN = "VfsFileExplorer-Tree-0200-Hidden";

  private static final String DETAILS_OPEN = "VfsFileExplorer-Details-0010-Open";
  private static final String DETAILS_DRILL = "VfsFileExplorer-Details-0030-Drill";
  private static final String DETAILS_COLUMNS = "VfsFileExplorer-Details-0200-Columns";

  private static final String BOOKMARK_ADD = "VfsFileExplorer-Bookmarks-0010-Add";
  private static final String BOOKMARK_REMOVE = "VfsFileExplorer-Bookmarks-0030-Remove";

  private final VfsFileExplorer explorer;
  private final IVariables variables;
  private final VfsListingGeneration generation = new VfsListingGeneration();
  private final List<String> history = new ArrayList<>();
  private final TextVar locationText;
  private final Text filterText;
  private final HopTree tree;
  private final Table table;
  private final org.eclipse.swt.widgets.List bookmarksList;
  private final GuiToolbarWidgets navigateToolbar;
  private final GuiToolbarWidgets treeToolbar;
  private final GuiToolbarWidgets detailsToolbar;
  private final GuiToolbarWidgets bookmarksToolbar;
  private final Image folderImage;
  private final Image fileImage;

  private CTabItem tabItem;
  private int historyIndex = -1;
  private boolean adjustingTree;
  private boolean columnsBuilt;
  private boolean enteringLocation;

  public VfsFileExplorerLocation(Composite parent, VfsFileExplorer explorer) {
    super(parent, SWT.NONE);
    this.explorer = explorer;
    this.variables = explorer.getVariables();
    this.folderImage = GuiResource.getInstance().getImageFolder();
    this.fileImage = GuiResource.getInstance().getImageFile();
    PropsUi.setLook(this);
    setLayout(new FormLayout());

    Composite navigateRow = new Composite(this, SWT.NONE);
    PropsUi.setLook(navigateRow);
    navigateRow.setLayout(new FormLayout());
    navigateRow.setLayoutData(new FormDataBuilder().top().fullWidth().result());

    IToolbarContainer navigateBar =
        ToolbarFacade.createToolbarContainer(navigateRow, SWT.LEFT | SWT.HORIZONTAL);
    Control navigateControl = navigateBar.getControl();
    navigateControl.setLayoutData(new FormDataBuilder().left().top().bottom().result());
    PropsUi.setLook(navigateControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    navigateToolbar = new GuiToolbarWidgets();
    navigateToolbar.registerGuiPluginObject(this);
    navigateToolbar.createToolbarWidgets(navigateBar, NAVIGATE_TOOLBAR_PARENT_ID);
    navigateControl.pack();

    Button goButton = new Button(navigateRow, SWT.PUSH);
    goButton.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Location.Go"));
    goButton.setToolTipText(BaseMessages.getString(PKG, "VfsFileExplorer.Location.Go.Tooltip"));
    PropsUi.setLook(goButton);
    FormData fdGo = new FormData();
    fdGo.top = new FormAttachment(0, 0);
    fdGo.right = new FormAttachment(100, 0);
    fdGo.bottom = new FormAttachment(100, 0);
    goButton.setLayoutData(fdGo);
    goButton.addListener(SWT.Selection, e -> goToEnteredLocation());

    locationText = new TextVar(variables, navigateRow, SWT.SINGLE | SWT.LEFT | SWT.BORDER);
    PropsUi.setLook(locationText);
    locationText.setLayoutData(
        new FormDataBuilder()
            .left(navigateControl, PropsUi.getMargin())
            .top()
            .right(goButton, -PropsUi.getMargin())
            .bottom()
            .result());
    locationText.addListener(
        SWT.KeyDown,
        e -> {
          if (e.keyCode == SWT.CR || e.keyCode == SWT.KEYPAD_CR) {
            e.doit = false;
            goToEnteredLocation();
          }
        });
    locationText.addTraverseListener(
        e -> {
          if (e.detail == SWT.TRAVERSE_RETURN) {
            e.doit = false;
            goToEnteredLocation();
          }
        });

    SashForm horizontal = new SashForm(this, SWT.HORIZONTAL);
    horizontal.setLayoutData(
        new FormDataBuilder().top(navigateRow, PropsUi.getMargin()).bottom().fullWidth().result());

    SashForm left = new SashForm(horizontal, SWT.VERTICAL);
    Composite treeComposite = new Composite(left, SWT.NONE);
    treeComposite.setLayout(new FormLayout());
    PropsUi.setLook(treeComposite);
    IToolbarContainer treeBar =
        ToolbarFacade.createToolbarContainer(treeComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control treeBarControl = treeBar.getControl();
    treeBarControl.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(treeBarControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    treeToolbar = new GuiToolbarWidgets();
    treeToolbar.registerGuiPluginObject(this);
    treeToolbar.createToolbarWidgets(treeBar, TREE_TOOLBAR_PARENT_ID);
    treeBarControl.pack();

    tree = new HopTree(treeComposite, SWT.BORDER | SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    FolderTreeIcons.install(tree);
    PropsUi.setLook(tree);
    tree.setLayoutData(new FormDataBuilder().top(treeBarControl, 0).bottom().fullWidth().result());
    tree.addListener(SWT.Selection, e -> onTreeSelected());
    tree.addListener(SWT.Expand, e -> onTreeExpand((TreeItem) e.item));
    tree.addListener(SWT.FocusIn, e -> explorer.activate());

    Composite bookmarksComposite = new Composite(left, SWT.NONE);
    bookmarksComposite.setLayout(new FormLayout());
    PropsUi.setLook(bookmarksComposite);
    IToolbarContainer bookmarkBar =
        ToolbarFacade.createToolbarContainer(
            bookmarksComposite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control bookmarkBarControl = bookmarkBar.getControl();
    bookmarkBarControl.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    PropsUi.setLook(bookmarkBarControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    bookmarksToolbar = new GuiToolbarWidgets();
    bookmarksToolbar.registerGuiPluginObject(this);
    bookmarksToolbar.createToolbarWidgets(bookmarkBar, BOOKMARKS_TOOLBAR_PARENT_ID);
    bookmarkBarControl.pack();
    bookmarksList =
        new org.eclipse.swt.widgets.List(
            bookmarksComposite, SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
    PropsUi.setLook(bookmarksList);
    bookmarksList.setLayoutData(
        new FormDataBuilder().top(bookmarkBarControl, 0).bottom().fullWidth().result());
    bookmarksList.addListener(SWT.DefaultSelection, e -> openSelectedBookmark());
    bookmarksList.addListener(SWT.Selection, e -> updateToolbar());
    left.setWeights(75, 25);
    SashFormMemory.persist(left, "vfs-explorer-bookmarks-height", 75, 25);

    Composite details = new Composite(horizontal, SWT.NONE);
    details.setLayout(new FormLayout());
    PropsUi.setLook(details);
    Composite detailsRow = new Composite(details, SWT.NONE);
    detailsRow.setLayout(new FormLayout());
    PropsUi.setLook(detailsRow);
    detailsRow.setLayoutData(new FormDataBuilder().top().fullWidth().result());
    filterText = new Text(detailsRow, SWT.SEARCH | SWT.ICON_CANCEL | SWT.ICON_SEARCH);
    filterText.setMessage(BaseMessages.getString(PKG, "VfsFileExplorer.Filter.Placeholder"));
    PropsUi.setLook(filterText, PropsUi.WIDGET_STYLE_TOOLBAR);
    filterText.setLayoutData(new FormDataBuilder().right().top().width(220).result());
    filterText.addListener(SWT.Modify, e -> refillTable());
    IToolbarContainer detailsBar =
        ToolbarFacade.createToolbarContainer(detailsRow, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control detailsBarControl = detailsBar.getControl();
    detailsBarControl.setLayoutData(
        new FormDataBuilder().left().top().right(filterText, -PropsUi.getMargin()).result());
    PropsUi.setLook(detailsBarControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    detailsToolbar = new GuiToolbarWidgets();
    detailsToolbar.registerGuiPluginObject(this);
    detailsToolbar.createToolbarWidgets(detailsBar, DETAILS_TOOLBAR_PARENT_ID);
    detailsBarControl.pack();

    table =
        new Table(
            details, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    PropsUi.setLook(table);
    table.setHeaderVisible(true);
    table.setLinesVisible(false);
    table.setLayoutData(new FormDataBuilder().top(detailsRow, 0).bottom().fullWidth().result());
    table.addListener(SWT.Selection, e -> updateToolbar());
    table.addListener(SWT.DefaultSelection, e -> onTableDefaultSelection());
    table.addListener(SWT.FocusIn, e -> explorer.activate());
    rebuildColumns();

    horizontal.setWeights(28, 72);
    SashFormMemory.persist(horizontal, "vfs-explorer-tree-width", 28, 72);

    addListener(SWT.FocusIn, e -> explorer.activate());
    addDisposeListener(
        e -> {
          invalidate();
          navigateToolbar.dispose();
          treeToolbar.dispose();
          detailsToolbar.dispose();
          bookmarksToolbar.dispose();
        });
    refreshBookmarks();
    updateToolbar();
  }

  void setTabItem(CTabItem tabItem) {
    this.tabItem = tabItem;
  }

  public String getLocationText() {
    return locationText.isDisposed() ? "" : locationText.getText();
  }

  /**
   * Open the path typed in the location field. Enter in the field and the Go button both call this.
   * A second event from the same key press is ignored. Variables in the path are resolved by {@link
   * #navigateTo(String, boolean)}.
   */
  private void goToEnteredLocation() {
    if (enteringLocation || locationText.isDisposed()) {
      return;
    }
    enteringLocation = true;
    getDisplay()
        .asyncExec(
            () -> {
              if (!isDisposed()) {
                enteringLocation = false;
              }
            });
    navigateTo(locationText.getText(), true);
  }

  /** Drop a result that is still running. Does not close the file system. */
  public void invalidate() {
    generation.next();
  }

  public void navigateTo(String location, boolean recordHistory) {
    String resolved = variables == null ? location : variables.resolve(Const.NVL(location, ""));
    if (StringUtils.isBlank(resolved)) {
      return;
    }
    int token = generation.next();
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Opening", resolved), resolved);
    Display display = getDisplay();
    Thread thread =
        BackgroundThreadFacade.start(
            () -> {
              try {
                if (stopped(operation, token)) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                FileObject file = HopVfs.getFileObject(resolved, variables);
                file.refresh();
                String selectName = null;
                FileObject folder = file;
                if (!file.isFolder()) {
                  selectName = file.getName().getBaseName();
                  if (file.getParent() != null) {
                    folder = file.getParent();
                    folder.refresh();
                  }
                }
                String folderUri = HopVfs.getFilename(folder);
                List<VfsFileRow> rows = VfsFileListing.childrenOf(folder);
                if (stopped(operation, token)) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                operation.complete();
                String selected = selectName;
                display.asyncExec(
                    () -> {
                      if (!canApply(token)) {
                        explorer.refreshOperations();
                        return;
                      }
                      List<VfsFileRow> published =
                          VfsFileListing.publish(generation, token, List.of(), rows);
                      installRoot(folderUri, published);
                      if (recordHistory) {
                        recordHistory(folderUri);
                      }
                      if (selected != null) {
                        selectTableName(selected);
                      }
                      explorer.refreshOperations();
                    });
              } catch (Exception e) {
                fail(operation, e);
                refreshPanel(display);
              }
            },
            "hop-vfs-explorer");
    operation.attachThread(thread);
    explorer.refreshOperations();
  }

  public void refreshFolder() {
    TreeItem item = selectedTreeItem();
    FolderNode node = nodeOf(item);
    if (item == null || node == null) {
      navigateTo(getLocationText(), false);
      return;
    }
    startList(item, node, true);
  }

  public void renameSelected() {
    VfsFileRow row = singleTableRow();
    if (row == null) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            row.getName(),
            BaseMessages.getString(PKG, "VfsFileExplorer.Rename.Header"),
            BaseMessages.getString(PKG, "VfsFileExplorer.Rename.Message"));
    String newName = dialog.open();
    if (StringUtils.isBlank(newName) || newName.equals(row.getName()) || newName.contains("/")) {
      return;
    }
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Renaming", row.getName()),
        row.getUri(),
        () -> {
          FileObject file = HopVfs.getFileObject(row.getUri(), variables);
          FileObject parent = file.getParent();
          if (parent == null) {
            throw new IllegalStateException(row.getUri());
          }
          FileObject dest = parent.resolveFile(newName);
          file.moveTo(dest);
        });
  }

  public void refreshBookmarks() {
    if (bookmarksList.isDisposed()) {
      return;
    }
    List<String> names = new ArrayList<>(explorer.bookmarks().keySet());
    Collections.sort(names);
    bookmarksList.setItems(names.toArray(new String[0]));
    updateToolbar();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_HOME,
      toolTip = "i18n::VfsFileExplorer.Navigate.Home.Tooltip",
      image = "ui/images/home.svg")
  public void navigateHome() {
    navigateTo(System.getProperty("user.home"), true);
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_UP,
      toolTip = "i18n::VfsFileExplorer.Navigate.Up.Tooltip",
      image = "ui/images/navigate-up.svg")
  public void navigateUp() {
    TreeItem item = selectedTreeItem();
    if (item != null && item.getParentItem() != null) {
      adjustingTree = true;
      try {
        tree.setSelection(item.getParentItem());
      } finally {
        adjustingTree = false;
      }
      onTreeSelected();
      return;
    }
    FolderNode node = nodeOf(item);
    if (node == null) {
      return;
    }
    String uri = node.uri;
    int token = generation.next();
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Opening", uri), uri);
    Display display = getDisplay();
    Thread thread =
        BackgroundThreadFacade.start(
            () -> {
              try {
                if (stopped(operation, token)) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                FileObject folder = HopVfs.getFileObject(uri, variables);
                FileObject parent = folder.getParent();
                if (parent == null) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                String parentUri = HopVfs.getFilename(parent);
                operation.complete();
                display.asyncExec(
                    () -> {
                      if (canApply(token)) {
                        navigateTo(parentUri, true);
                      }
                      explorer.refreshOperations();
                    });
              } catch (Exception e) {
                fail(operation, e);
                refreshPanel(display);
              }
            },
            "hop-vfs-explorer");
    operation.attachThread(thread);
    explorer.refreshOperations();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_BACK,
      toolTip = "i18n::VfsFileExplorer.Navigate.Back.Tooltip",
      image = "ui/images/navigate-back.svg")
  public void navigateBack() {
    if (historyIndex <= 0) {
      return;
    }
    historyIndex--;
    navigateTo(history.get(historyIndex), false);
    updateToolbar();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_FORWARD,
      toolTip = "i18n::VfsFileExplorer.Navigate.Forward.Tooltip",
      image = "ui/images/navigate-forward.svg")
  public void navigateForward() {
    if (historyIndex + 1 >= history.size()) {
      return;
    }
    historyIndex++;
    navigateTo(history.get(historyIndex), false);
    updateToolbar();
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_REFRESH,
      toolTip = "i18n::VfsFileExplorer.Navigate.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  public void refreshNavigation() {
    refreshFolder();
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_CREATE,
      toolTip = "i18n::VfsFileExplorer.Tree.CreateFolder.Tooltip",
      image = "ui/images/folder-add.svg")
  public void createFolder() {
    FolderNode node = selectedNode();
    if (node == null) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "VfsFileExplorer.CreateFolder.Header"),
            BaseMessages.getString(PKG, "VfsFileExplorer.CreateFolder.Message"));
    String name = dialog.open();
    if (StringUtils.isBlank(name) || name.contains("/") || name.contains("\\")) {
      return;
    }
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Creating", name),
        node.uri,
        () -> {
          FileObject folder = HopVfs.getFileObject(node.uri, variables);
          folder.resolveFile(name).createFolder();
        });
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_DELETE,
      toolTip = "i18n::VfsFileExplorer.Tree.Delete.Tooltip",
      image = "ui/images/delete.svg")
  public void deleteSelected() {
    List<VfsFileRow> rows = selectedTableRows();
    if (rows.isEmpty()) {
      return;
    }
    MessageBox box = new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Title"));
    box.setMessage(
        rows.size() == 1
            ? BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Message", rows.get(0).getName())
            : BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Message.Many", rows.size()));
    if (box.open() != SWT.YES) {
      return;
    }
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Deleting", rows.get(0).getName()),
        rows.get(0).getUri(),
        () -> {
          for (VfsFileRow row : rows) {
            FileObject file = HopVfs.getFileObject(row.getUri(), variables);
            if (file.isFolder()) {
              file.delete(Selectors.SELECT_ALL);
            } else {
              file.delete();
            }
          }
        });
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_RENAME,
      toolTip = "i18n::VfsFileExplorer.Tree.Rename.Tooltip",
      image = "ui/images/rename.svg")
  public void renameFromToolbar() {
    renameSelected();
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_HIDDEN,
      toolTip = "i18n::VfsFileExplorer.Tree.Hidden.Tooltip",
      image = "ui/images/hide.svg",
      separator = true)
  public void toggleHidden() {
    VfsExplorerViewState state = explorer.getViewState();
    state.setShowHidden(!state.isShowHidden());
    state.save();
    ToolItem toolItem = treeToolbar.findToolItem(TREE_HIDDEN);
    if (toolItem != null) {
      toolItem.setImage(
          state.isShowHidden()
              ? GuiResource.getInstance().getImageShow()
              : GuiResource.getInstance().getImageHide());
    }
    refillTable();
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_OPEN,
      toolTip = "i18n::VfsFileExplorer.Details.Open.Tooltip",
      image = "ui/images/open.svg")
  public void openSelected() {
    VfsFileRow row = singleTableRow();
    if (row == null || row.isFolder() || !canOpen(row)) {
      return;
    }
    try {
      IHopFileType type = VfsHopFileTypes.find(row.getUri(), false);
      type.openFile(explorer.getHopGui(), row.getUri(), variables);
    } catch (Exception e) {
      failOnUi(e);
    }
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_DRILL,
      toolTip = "i18n::VfsFileExplorer.Details.Drill.Tooltip",
      image = "ui/images/zipfile.svg")
  public void drillInto() {
    VfsFileRow row = singleTableRow();
    if (row == null || row.isFolder()) {
      return;
    }
    try {
      FileObject file = HopVfs.getFileObject(row.getUri(), variables);
      String archiveUri = HopVfsFileDialog.buildArchiveBrowseUri(file);
      if (archiveUri != null) {
        navigateTo(archiveUri, true);
      }
    } catch (Exception e) {
      failOnUi(e);
    }
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_COLUMNS,
      toolTip = "i18n::VfsFileExplorer.Details.Columns.Tooltip",
      image = "ui/images/show-grid.svg")
  public void chooseColumns() {
    // Parent on the shell, not the table. Rebuilding the table from a menu parented on it
    // disposes the popup before a mouse click can change the check.
    Menu menu = new Menu(getShell(), SWT.POP_UP);
    for (VfsFileColumn column : VfsFileColumn.values()) {
      if (column == VfsFileColumn.NAME) {
        continue;
      }
      MenuItem item = new MenuItem(menu, SWT.CHECK);
      item.setText(columnHeader(column));
      item.setSelection(explorer.getViewState().isVisible(column));
      item.addListener(
          SWT.Selection,
          e -> {
            if (item.isDisposed()) {
              return;
            }
            boolean shown = !explorer.getViewState().isVisible(column);
            item.setSelection(shown);
            explorer.getViewState().setVisible(column, shown);
            explorer.getViewState().save();
            getDisplay()
                .asyncExec(
                    () -> {
                      if (table.isDisposed()) {
                        return;
                      }
                      rebuildColumns();
                      refillTable();
                    });
          });
    }
    menu.addListener(
        SWT.Hide,
        e ->
            menu.getDisplay()
                .asyncExec(
                    () -> {
                      if (!menu.isDisposed()) {
                        menu.dispose();
                      }
                    }));
    menu.setLocation(columnsMenuLocation());
    menu.setVisible(true);
  }

  private Point columnsMenuLocation() {
    Control anchor = detailsToolbar.getControlForMenu(DETAILS_COLUMNS);
    if (anchor != null && !anchor.isDisposed() && anchor.getParent() != null) {
      Rectangle rect = anchor.getBounds();
      return anchor.getParent().toDisplay(rect.x, rect.y + rect.height);
    }
    ToolItem toolItem = detailsToolbar.findToolItem(DETAILS_COLUMNS);
    if (toolItem != null && !toolItem.isDisposed() && toolItem.getParent() != null) {
      Rectangle rect = toolItem.getBounds();
      return toolItem.getParent().toDisplay(rect.x, rect.y + rect.height);
    }
    return getDisplay().getCursorLocation();
  }

  @GuiToolbarElement(
      root = BOOKMARKS_TOOLBAR_PARENT_ID,
      id = BOOKMARK_ADD,
      toolTip = "i18n::VfsFileExplorer.Bookmark.Add.Tooltip",
      image = "ui/images/bookmark-add.svg")
  public void addBookmark() {
    VfsFileRow row = singleTableRow();
    String uri = row != null ? row.getUri() : currentFolderUri();
    String suggested = row != null ? row.getName() : titleOf(uri);
    if (StringUtils.isBlank(uri)) {
      return;
    }
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            suggested,
            BaseMessages.getString(PKG, "VfsFileExplorer.Bookmark.Name.Header"),
            BaseMessages.getString(PKG, "VfsFileExplorer.Bookmark.Name.Message"));
    String name = dialog.open();
    if (StringUtils.isBlank(name)) {
      return;
    }
    explorer.putBookmark(name, uri);
  }

  @GuiToolbarElement(
      root = BOOKMARKS_TOOLBAR_PARENT_ID,
      id = BOOKMARK_REMOVE,
      toolTip = "i18n::VfsFileExplorer.Bookmark.Remove.Tooltip",
      image = "ui/images/delete.svg")
  public void removeBookmark() {
    String[] selection = bookmarksList.getSelection();
    if (selection.length != 1) {
      return;
    }
    explorer.removeBookmark(selection[0]);
  }

  private void onTreeSelected() {
    if (adjustingTree) {
      return;
    }
    TreeItem item = selectedTreeItem();
    FolderNode node = nodeOf(item);
    if (item == null || node == null) {
      return;
    }
    setLocationText(node.uri);
    setTabTitle(titleOf(node.uri));
    if (node.loaded) {
      fillTable(node);
    } else {
      startList(item, node, true);
    }
  }

  private void onTreeExpand(TreeItem item) {
    FolderNode node = nodeOf(item);
    if (node != null && !node.loaded) {
      startList(item, node, false);
    }
  }

  private void onTableDefaultSelection() {
    VfsFileRow row = singleTableRow();
    if (row == null) {
      return;
    }
    if (row.isFolder()) {
      openFolderRow(row);
      return;
    }
    if (HopVfsFileDialog.getArchiveScheme(row.getName()) != null) {
      drillInto();
      return;
    }
    openSelected();
  }

  private void openFolderRow(VfsFileRow row) {
    TreeItem current = selectedTreeItem();
    if (current != null) {
      for (TreeItem child : current.getItems()) {
        FolderNode node = nodeOf(child);
        if (node != null && row.getUri().equals(node.uri)) {
          adjustingTree = true;
          try {
            tree.setSelection(child);
            FolderTreeIcons.setExpanded(current, true);
          } finally {
            adjustingTree = false;
          }
          recordHistory(row.getUri());
          onTreeSelected();
          return;
        }
      }
    }
    navigateTo(row.getUri(), true);
  }

  private void startList(TreeItem item, FolderNode node, boolean showTable) {
    int token = generation.next();
    String uri = node.uri;
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Listing", uri), uri);
    Display display = getDisplay();
    Thread thread =
        BackgroundThreadFacade.start(
            () -> {
              try {
                if (stopped(operation, token)) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                FileObject folder = HopVfs.getFileObject(uri, variables);
                folder.refresh();
                List<VfsFileRow> rows = VfsFileListing.childrenOf(folder);
                if (stopped(operation, token)) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                operation.complete();
                display.asyncExec(
                    () -> {
                      if (!canApply(token) || item.isDisposed()) {
                        explorer.refreshOperations();
                        return;
                      }
                      node.children =
                          VfsFileListing.publish(generation, token, node.children, rows);
                      node.loaded = true;
                      replaceChildren(item, node.children);
                      if (showTable || isSelected(item)) {
                        setLocationText(uri);
                        fillTable(node);
                      }
                      explorer.refreshOperations();
                    });
              } catch (Exception e) {
                fail(operation, e);
                refreshPanel(display);
              }
            },
            "hop-vfs-explorer");
    operation.attachThread(thread);
    explorer.refreshOperations();
  }

  private void runChange(String description, String location, Change change) {
    TreeItem item = selectedTreeItem();
    VfsExplorerOperation operation = explorer.beginOperation(description, location);
    Display display = getDisplay();
    Thread thread =
        BackgroundThreadFacade.start(
            () -> {
              try {
                if (operation.isCancelled()) {
                  operation.complete();
                  refreshPanel(display);
                  return;
                }
                change.run();
                operation.complete();
              } catch (Exception e) {
                fail(operation, e);
              }
              display.asyncExec(
                  () -> {
                    explorer.refreshOperations();
                    if (!isDisposed() && item != null && !item.isDisposed()) {
                      FolderNode node = nodeOf(item);
                      if (node != null) {
                        startList(item, node, true);
                      }
                    }
                  });
            },
            "hop-vfs-explorer");
    operation.attachThread(thread);
    explorer.refreshOperations();
  }

  private void installRoot(String folderUri, List<VfsFileRow> rows) {
    tree.removeAll();
    FolderNode node = new FolderNode(folderUri, titleOf(folderUri));
    node.loaded = true;
    node.children = rows;
    TreeItem item = new TreeItem(tree, SWT.NONE);
    item.setText(node.name);
    item.setImage(folderImage);
    item.setData(node);
    replaceChildren(item, rows);
    adjustingTree = true;
    try {
      tree.setSelection(item);
      FolderTreeIcons.setExpanded(item, true);
    } finally {
      adjustingTree = false;
    }
    setLocationText(folderUri);
    setTabTitle(node.name);
    fillTable(node);
  }

  private void replaceChildren(TreeItem item, List<VfsFileRow> rows) {
    item.removeAll();
    if (rows == null) {
      return;
    }
    for (VfsFileRow row : rows) {
      if (!row.isFolder()) {
        continue;
      }
      TreeItem child = new TreeItem(item, SWT.NONE);
      child.setText(row.getName());
      child.setImage(folderImage);
      child.setData(new FolderNode(row.getUri(), row.getName()));
      new TreeItem(child, SWT.NONE);
    }
  }

  private void refillTable() {
    FolderNode node = selectedNode();
    if (node != null && node.loaded) {
      fillTable(node);
    }
  }

  private void fillTable(FolderNode node) {
    if (!columnsBuilt) {
      rebuildColumns();
    }
    VfsExplorerViewState state = explorer.getViewState();
    List<VfsFileRow> rows =
        VfsFileListing.visible(
            node.children,
            state.isShowHidden(),
            filterText.getText(),
            state.getSortColumn(),
            state.isAscending());
    table.removeAll();
    List<VfsFileColumn> columns = shownColumns();
    for (VfsFileRow row : rows) {
      TableItem item = new TableItem(table, SWT.NONE);
      item.setData(row);
      item.setImage(imageFor(row));
      for (int i = 0; i < columns.size(); i++) {
        item.setText(i, textFor(row, columns.get(i)));
      }
    }
    updateToolbar();
  }

  private void rebuildColumns() {
    for (TableColumn column : table.getColumns()) {
      column.dispose();
    }
    PropsUi props = PropsUi.getInstance();
    for (VfsFileColumn column : shownColumns()) {
      TableColumn tableColumn =
          new TableColumn(table, column == VfsFileColumn.SIZE ? SWT.RIGHT : SWT.LEFT);
      tableColumn.setText(columnHeader(column));
      tableColumn.setWidth((int) (columnWidth(column) * props.getZoomFactor()));
      tableColumn.addListener(SWT.Selection, e -> sortBy(column));
    }
    columnsBuilt = true;
    applySortIndicator();
  }

  private void sortBy(VfsFileColumn column) {
    VfsExplorerViewState state = explorer.getViewState();
    if (state.getSortColumn() == column) {
      state.setAscending(!state.isAscending());
    } else {
      state.setSortColumn(column);
      state.setAscending(true);
    }
    state.save();
    applySortIndicator();
    refillTable();
  }

  private void applySortIndicator() {
    VfsExplorerViewState state = explorer.getViewState();
    List<VfsFileColumn> columns = shownColumns();
    TableColumn[] tableColumns = table.getColumns();
    for (int i = 0; i < columns.size() && i < tableColumns.length; i++) {
      if (columns.get(i) == state.getSortColumn()) {
        table.setSortColumn(tableColumns[i]);
        table.setSortDirection(state.isAscending() ? SWT.UP : SWT.DOWN);
        return;
      }
    }
  }

  private List<VfsFileColumn> shownColumns() {
    List<VfsFileColumn> columns = new ArrayList<>();
    for (VfsFileColumn column : VfsFileColumn.values()) {
      if (explorer.getViewState().isVisible(column)) {
        columns.add(column);
      }
    }
    if (columns.isEmpty()) {
      columns.add(VfsFileColumn.NAME);
    }
    return columns;
  }

  private void selectTableName(String name) {
    for (TableItem item : table.getItems()) {
      if (item.getData() instanceof VfsFileRow row && name.equals(row.getName())) {
        table.setSelection(item);
        table.showSelection();
        updateToolbar();
        return;
      }
    }
  }

  private void recordHistory(String location) {
    if (historyIndex >= 0 && historyIndex < history.size() - 1) {
      history.subList(historyIndex + 1, history.size()).clear();
    }
    if (historyIndex >= 0 && location.equals(history.get(historyIndex))) {
      updateToolbar();
      return;
    }
    history.add(location);
    historyIndex = history.size() - 1;
    updateToolbar();
  }

  private void setLocationText(String uri) {
    if (!locationText.getText().equals(uri)) {
      locationText.setText(Const.NVL(uri, ""));
    }
  }

  private void setTabTitle(String title) {
    if (tabItem != null && !tabItem.isDisposed()) {
      tabItem.setText(StringUtils.isBlank(title) ? getLocationText() : title);
    }
  }

  private void updateToolbar() {
    navigateToolbar.enableToolbarItem(NAVIGATE_BACK, historyIndex > 0);
    navigateToolbar.enableToolbarItem(NAVIGATE_FORWARD, historyIndex + 1 < history.size());
    VfsFileRow row = singleTableRow();
    boolean one = row != null;
    treeToolbar.enableToolbarItem(TREE_DELETE, !selectedTableRows().isEmpty());
    treeToolbar.enableToolbarItem(TREE_RENAME, one);
    detailsToolbar.enableToolbarItem(DETAILS_OPEN, one && canOpen(row));
    detailsToolbar.enableToolbarItem(
        DETAILS_DRILL,
        one && !row.isFolder() && HopVfsFileDialog.getArchiveScheme(row.getName()) != null);
    bookmarksToolbar.enableToolbarItem(BOOKMARK_REMOVE, bookmarksList.getSelectionIndex() >= 0);
  }

  private boolean canOpen(VfsFileRow row) {
    if (row == null || row.isFolder()) {
      return false;
    }
    try {
      IHopFileType type = VfsHopFileTypes.find(row.getUri(), false);
      return type != null
          && !(type instanceof GenericFileType)
          && !(type instanceof FolderFileType)
          && type.supportsOpening();
    } catch (Exception e) {
      return false;
    }
  }

  private Image imageFor(VfsFileRow row) {
    if (row.isFolder()) {
      return folderImage;
    }
    try {
      IHopFileType type = VfsHopFileTypes.find(row.getUri(), false);
      if (type != null) {
        HopGui hopGui = explorer.getHopGui();
        if (hopGui != null && hopGui.getPerspectiveManager() != null) {
          ExplorerPerspective perspective =
              hopGui.getPerspectiveManager().findPerspective(ExplorerPerspective.class);
          if (perspective != null) {
            Image image = perspective.getFileTypeImage(type);
            if (image != null) {
              return image;
            }
          }
        }
      }
    } catch (Exception ignored) {
      // Generic file icon.
    }
    return fileImage;
  }

  private void openSelectedBookmark() {
    String[] selection = bookmarksList.getSelection();
    if (selection.length != 1) {
      return;
    }
    String path = explorer.bookmarks().get(selection[0]);
    if (StringUtils.isNotEmpty(path)) {
      navigateTo(path, true);
    }
  }

  private String currentFolderUri() {
    FolderNode node = selectedNode();
    return node == null ? getLocationText() : node.uri;
  }

  private TreeItem selectedTreeItem() {
    TreeItem[] selection = tree.getSelection();
    return selection.length == 0 ? null : selection[0];
  }

  private FolderNode selectedNode() {
    return nodeOf(selectedTreeItem());
  }

  private static FolderNode nodeOf(TreeItem item) {
    if (item == null || item.isDisposed()) {
      return null;
    }
    Object data = item.getData();
    return data instanceof FolderNode node ? node : null;
  }

  private boolean isSelected(TreeItem item) {
    return item == selectedTreeItem();
  }

  private List<VfsFileRow> selectedTableRows() {
    List<VfsFileRow> rows = new ArrayList<>();
    for (TableItem item : table.getSelection()) {
      if (item.getData() instanceof VfsFileRow row) {
        rows.add(row);
      }
    }
    return rows;
  }

  private VfsFileRow singleTableRow() {
    List<VfsFileRow> rows = selectedTableRows();
    return rows.size() == 1 ? rows.get(0) : null;
  }

  private boolean stopped(VfsExplorerOperation operation, int token) {
    return operation.isCancelled() || !generation.isCurrent(token);
  }

  private boolean canApply(int token) {
    return !isDisposed() && generation.isCurrent(token);
  }

  private void fail(VfsExplorerOperation operation, Exception exception) {
    if (operation.isCancelled() || Thread.currentThread().isInterrupted()) {
      operation.complete();
      return;
    }
    operation.fail(
        Const.NVL(exception.getMessage(), exception.toString()),
        Const.getClassicStackTrace(exception));
  }

  private void failOnUi(Exception exception) {
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Error.Title"), getLocationText());
    fail(operation, exception);
    explorer.refreshOperations();
  }

  private void refreshPanel(Display display) {
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (!isDisposed()) {
            explorer.refreshOperations();
          }
        });
  }

  private static String titleOf(String uri) {
    if (StringUtils.isBlank(uri)) {
      return "";
    }
    String base = org.apache.hop.ui.hopgui.file.HopFileTypeBase.extractBaseName(uri);
    if (StringUtils.isNotEmpty(base)) {
      return base;
    }
    int scheme = uri.indexOf(':');
    return scheme > 0 ? uri.substring(0, scheme) : uri;
  }

  private static String columnHeader(VfsFileColumn column) {
    return BaseMessages.getString(PKG, "VfsFileExplorer.Column." + column.name());
  }

  private static int columnWidth(VfsFileColumn column) {
    return switch (column) {
      case NAME -> 220;
      case EXTENSION -> 80;
      case SIZE -> 90;
      case MODIFIED -> 150;
      case OWNER -> 100;
      case PERMISSIONS -> 110;
    };
  }

  private static String textFor(VfsFileRow row, VfsFileColumn column) {
    return switch (column) {
      case NAME -> row.getName();
      case EXTENSION -> row.getExtension();
      case SIZE -> row.getSizeText();
      case MODIFIED -> row.getLastModifiedText();
      case OWNER -> row.getOwner();
      case PERMISSIONS -> row.getPermissions();
    };
  }

  @FunctionalInterface
  private interface Change {
    void run() throws Exception;
  }

  private static final class FolderNode {
    private final String uri;
    private final String name;
    private boolean loaded;
    private List<VfsFileRow> children = List.of();

    private FolderNode(String uri, String name) {
      this.uri = uri;
      this.name = StringUtils.isBlank(name) ? uri : name;
    }
  }
}
