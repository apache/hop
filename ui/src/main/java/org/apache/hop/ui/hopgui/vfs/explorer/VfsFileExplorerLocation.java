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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.Const;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.security.HopSecurity;
import org.apache.hop.core.security.Permission;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.DialogBoxWithButtons;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.security.HopSecurityUi;
import org.apache.hop.ui.core.vfs.HopVfsFileDialog;
import org.apache.hop.ui.core.widget.FolderTreeIcons;
import org.apache.hop.ui.core.widget.HopTree;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.hopgui.BackgroundThreadFacade;
import org.apache.hop.ui.hopgui.BrowserFileTransferFacade;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.FolderFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.GenericFileType;
import org.apache.hop.ui.hopgui.perspective.explorer.file.types.raw.RawExplorerFileType;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.dnd.FileTransfer;
import org.eclipse.swt.dnd.Transfer;
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
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

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
  private static final String TREE_COPY = "VfsFileExplorer-Tree-0030-Copy";
  private static final String TREE_CUT = "VfsFileExplorer-Tree-0040-Cut";
  private static final String TREE_PASTE = "VfsFileExplorer-Tree-0050-Paste";
  private static final String TREE_DELETE = "VfsFileExplorer-Tree-0100-Delete";
  private static final String TREE_RENAME = "VfsFileExplorer-Tree-0110-Rename";
  private static final String TREE_HIDDEN = "VfsFileExplorer-Tree-0200-Hidden";

  private static final String DETAILS_OPEN = "VfsFileExplorer-Details-0010-Open";
  private static final String DETAILS_OPEN_TEXT = "VfsFileExplorer-Details-0015-OpenText";
  private static final String DETAILS_DOWNLOAD = "VfsFileExplorer-Details-0020-Download";
  private static final String DETAILS_UPLOAD = "VfsFileExplorer-Details-0025-Upload";
  private static final String DETAILS_DRILL = "VfsFileExplorer-Details-0030-Drill";
  private static final String DETAILS_COPY = "VfsFileExplorer-Details-0040-Copy";
  private static final String DETAILS_CUT = "VfsFileExplorer-Details-0050-Cut";
  private static final String DETAILS_PASTE = "VfsFileExplorer-Details-0060-Paste";
  private static final String DETAILS_RENAME = "VfsFileExplorer-Details-0070-Rename";
  private static final String DETAILS_DELETE = "VfsFileExplorer-Details-0100-Delete";
  private static final String DETAILS_MORE = "VfsFileExplorer-Details-0150-More";
  private static final String DETAILS_HIDDEN = "VfsFileExplorer-Details-0160-Hidden";
  private static final String DETAILS_COLUMNS = "VfsFileExplorer-Details-0200-Columns";

  private static final String BOOKMARK_ADD = "VfsFileExplorer-Bookmarks-0010-Add";
  private static final String BOOKMARK_REMOVE = "VfsFileExplorer-Bookmarks-0030-Remove";
  private static final String PLACES_REFRESH = "VfsFileExplorer-Bookmarks-0100-Refresh";

  private static final String PROJECT_HOME = "PROJECT_HOME";

  /** The largest file a Hop Web user can upload into a folder. */
  private static final long BROWSER_UPLOAD_LIMIT = 512L * 1024 * 1024;

  /** Typing in the filter waits this long before the table is filled again. */
  private static final int FILTER_DELAY_MS = 250;

  private final VfsFileExplorer explorer;
  private final IVariables variables;
  private final Runnable delayedRefill = this::refillTableIfAlive;
  private final VfsListingGeneration generation = new VfsListingGeneration();
  private final List<String> history = new ArrayList<>();
  private final TextVar locationText;
  private final Text filterText;
  private final HopTree tree;
  private final Table table;
  private final Tree placesTree;
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
  private FolderNode shown;
  private List<VfsFileRow> shownRows = List.of();

  /** The paths of the dragged selection. Set when the drag starts, read when it is dropped here. */
  private List<String> draggedUris = List.of();

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

    tree = new HopTree(treeComposite, SWT.BORDER | SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL);
    FolderTreeIcons.install(tree);
    PropsUi.setLook(tree);
    tree.setLayoutData(new FormDataBuilder().top(treeBarControl, 0).bottom().fullWidth().result());
    tree.addListener(
        SWT.Selection, e -> onTreeSelected(e.item instanceof TreeItem treeItem ? treeItem : null));
    tree.addListener(SWT.Expand, e -> onTreeExpand((TreeItem) e.item));
    tree.addListener(SWT.MenuDetect, e -> showFolderMenu());

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
    placesTree =
        new Tree(bookmarksComposite, SWT.SINGLE | SWT.V_SCROLL | SWT.H_SCROLL | SWT.BORDER);
    PropsUi.setLook(placesTree);
    placesTree.setLayoutData(
        new FormDataBuilder().top(bookmarkBarControl, 0).bottom().fullWidth().result());
    placesTree.addListener(
        SWT.Selection,
        e -> {
          updateToolbar();
          openPlace(e.item);
        });
    left.setWeights(65, 35);
    SashFormMemory.persist(left, "vfs-explorer-bookmarks-height", 65, 35);

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
    filterText.addListener(
        SWT.Modify,
        e -> {
          getDisplay().timerExec(-1, delayedRefill);
          getDisplay().timerExec(FILTER_DELAY_MS, delayedRefill);
        });
    IToolbarContainer detailsBar =
        ToolbarFacade.createToolbarContainer(detailsRow, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control detailsBarControl = detailsBar.getControl();
    detailsBarControl.setLayoutData(
        new FormDataBuilder().left().top().right(filterText, -PropsUi.getMargin()).result());
    PropsUi.setLook(detailsBarControl, PropsUi.WIDGET_STYLE_TOOLBAR);
    detailsToolbar = new GuiToolbarWidgets();
    detailsToolbar.registerGuiPluginObject(this);
    detailsToolbar.createToolbarWidgets(detailsBar, DETAILS_TOOLBAR_PARENT_ID);
    if (BrowserFileTransferFacade.isSupported()) {
      detailsToolbar.setToolbarItemToolTip(
          DETAILS_DOWNLOAD,
          BaseMessages.getString(PKG, "VfsFileExplorer.Details.Download.Web.Tooltip"));
      detailsToolbar.setToolbarItemToolTip(
          DETAILS_UPLOAD,
          BaseMessages.getString(PKG, "VfsFileExplorer.Details.Upload.Web.Tooltip"));
    }
    detailsBarControl.pack();

    table =
        new Table(
            details,
            SWT.BORDER
                | SWT.FULL_SELECTION
                | SWT.MULTI
                | SWT.H_SCROLL
                | SWT.V_SCROLL
                | SWT.VIRTUAL);
    PropsUi.setLook(table);
    table.setHeaderVisible(true);
    table.setLinesVisible(false);
    table.setLayoutData(new FormDataBuilder().top(detailsRow, 0).bottom().fullWidth().result());
    table.addListener(SWT.SetData, e -> fillItem((TableItem) e.item));
    table.addListener(SWT.Selection, e -> updateToolbar());
    table.addListener(SWT.DefaultSelection, e -> onTableDefaultSelection());
    table.addListener(SWT.MenuDetect, e -> showFileMenu());
    rebuildColumns();

    installDragAndDrop();

    horizontal.setWeights(28, 72);
    SashFormMemory.persist(horizontal, "vfs-explorer-tree-width", 28, 72);

    addDisposeListener(
        e -> {
          invalidate();
          navigateToolbar.dispose();
          treeToolbar.dispose();
          detailsToolbar.dispose();
          bookmarksToolbar.dispose();
        });
    refreshPlaces();
    updateToolbar();
  }

  /**
   * Drag folders from the tree and rows from the file list, and drop on a folder. The paths travel
   * as a {@link FileTransfer}, as in the File Explorer perspective, so files dragged between the
   * two or from the operating system are accepted too. A drop copies: the source and target are
   * often different stores. Only the move modifier of the desktop platform moves, see {@link
   * #dropOperation}.
   */
  private void installDragAndDrop() {
    FileTransfer fileTransfer = FileTransfer.getInstance();
    Transfer[] transfers = {fileTransfer};

    DragSource treeSource = new DragSource(tree, DND.DROP_COPY | DND.DROP_MOVE);
    treeSource.setTransfer(transfers);
    treeSource.addDragListener(dragListener(this::selectedFolderEntries));
    DragSource tableSource = new DragSource(table, DND.DROP_COPY | DND.DROP_MOVE);
    tableSource.setTransfer(transfers);
    tableSource.addDragListener(dragListener(this::selectedFileEntries));

    // DROP_DEFAULT: without it GTK and Cocoa report a drag without a modifier as a move.
    int dropStyle = DND.DROP_COPY | DND.DROP_MOVE | DND.DROP_DEFAULT;
    DropTarget treeTarget = new DropTarget(tree, dropStyle);
    treeTarget.setTransfer(transfers);
    treeTarget.addDropListener(
        dropListener(
            event -> {
              FolderNode node = event.item instanceof TreeItem item ? nodeOf(item) : null;
              return node == null ? null : node.uri;
            }));
    DropTarget tableTarget = new DropTarget(table, dropStyle);
    tableTarget.setTransfer(transfers);
    tableTarget.addDropListener(
        dropListener(
            event -> {
              // A folder row is the target. Anywhere else in the list is the folder being viewed.
              if (event.item instanceof TableItem item && !item.isDisposed()) {
                int index = table.indexOf(item);
                if (index >= 0 && index < shownRows.size() && shownRows.get(index).isFolder()) {
                  return shownRows.get(index).getUri();
                }
              }
              return currentFolderUri();
            }));
  }

  private DragSourceAdapter dragListener(
      java.util.function.Supplier<List<VfsFileTransfer.Entry>> selection) {
    return new DragSourceAdapter() {
      @Override
      public void dragStart(DragSourceEvent event) {
        // A drop elsewhere, for example on the File Explorer perspective, writes or moves files.
        List<String> uris = canWrite() ? urisOf(selection.get()) : List.of();
        event.doit = !uris.isEmpty();
        draggedUris = uris;
        if (EnvironmentUtils.getInstance().isWeb()) {
          // RAP has no native drag image to fall back on.
          event.image = GuiResource.getInstance().getImageFolder();
        }
      }

      @Override
      public void dragSetData(DragSourceEvent event) {
        // Read the selection again rather than the paths kept at drag start: Hop Web can finish
        // the drag before it asks for the data, and dragFinished has cleared them by then.
        if (FileTransfer.getInstance().isSupportedType(event.dataType)) {
          List<String> uris = urisOf(selection.get());
          event.doit = !uris.isEmpty();
          event.data = uris.toArray(new String[0]);
        }
      }

      @Override
      public void dragFinished(DragSourceEvent event) {
        draggedUris = List.of();
      }
    };
  }

  private DropTargetAdapter dropListener(
      java.util.function.Function<DropTargetEvent, String> targetOf) {
    return new DropTargetAdapter() {
      @Override
      public void dragEnter(DropTargetEvent event) {
        event.detail =
            dropOperation(event.detail, event.operations, EnvironmentUtils.getInstance().isWeb());
      }

      @Override
      public void dragOperationChanged(DropTargetEvent event) {
        dragEnter(event);
      }

      @Override
      public void dragOver(DropTargetEvent event) {
        event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL | DND.FEEDBACK_EXPAND;
        String target = targetOf.apply(event);
        if (!canWrite() || StringUtils.isBlank(target) || !canDropInto(draggedUris, target)) {
          event.detail = DND.DROP_NONE;
        } else {
          event.detail =
              dropOperation(event.detail, event.operations, EnvironmentUtils.getInstance().isWeb());
        }
      }

      @Override
      public void drop(DropTargetEvent event) {
        String target = targetOf.apply(event);
        if (!(event.data instanceof String[] paths)
            || paths.length == 0
            || StringUtils.isBlank(target)
            || !checkWrite()) {
          event.detail = DND.DROP_NONE;
          return;
        }
        // A folder and a path inside it are one transfer: the folder copy includes the rest.
        List<String> uris = VfsFileTransfer.withoutNestedPaths(List.of(paths));
        if (uris.isEmpty() || !canDropInto(uris, target)) {
          event.detail = DND.DROP_NONE;
          return;
        }
        List<VfsFileTransfer.Entry> entries = new ArrayList<>();
        for (String uri : uris) {
          entries.add(new VfsFileTransfer.Entry(uri, titleOf(uri), false));
        }
        int operation =
            dropOperation(event.detail, event.operations, EnvironmentUtils.getInstance().isWeb());
        VfsFileTransfer.Mode mode =
            operation == DND.DROP_MOVE ? VfsFileTransfer.Mode.MOVE : VfsFileTransfer.Mode.COPY;
        String key =
            mode == VfsFileTransfer.Mode.MOVE
                ? "VfsFileExplorer.Operation.Moving"
                : "VfsFileExplorer.Operation.Copying";
        copyIntoFolder(
            target, entries, mode, BaseMessages.getString(PKG, key, entries.get(0).getName()));
      }
    };
  }

  /**
   * The operation a drop performs. Only an explicit move, the platform's move modifier on the
   * desktop, moves files; everything else copies. Hop Web reports a drag without a modifier as a
   * move, so a drop always copies there: Cut and Paste move files.
   */
  static int dropOperation(int detail, int operations, boolean web) {
    boolean canCopy = (operations & DND.DROP_COPY) != 0;
    if (!web && detail == DND.DROP_MOVE && (operations & DND.DROP_MOVE) != 0) {
      return DND.DROP_MOVE;
    }
    return canCopy ? DND.DROP_COPY : DND.DROP_NONE;
  }

  /**
   * A drop into a folder is refused onto one of the dragged items itself, into its own subfolder,
   * or into the folder the items already sit in. Paths from outside this tab are not known while
   * dragging, so they are accepted here and checked by the copy itself.
   */
  static boolean canDropInto(List<String> uris, String target) {
    if (uris.isEmpty()) {
      return true;
    }
    boolean allInTarget = true;
    for (String uri : uris) {
      if (VfsLocations.sameLocation(uri, target) || VfsLocations.isUnder(uri, target)) {
        return false;
      }
      if (!VfsLocations.sameLocation(VfsLocations.parentOf(uri), target)) {
        allInTarget = false;
      }
    }
    return !allInTarget;
  }

  /** For UI tests. */
  Table fileTable() {
    return table;
  }

  /** For UI tests. */
  Tree folderTree() {
    return tree;
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
    runInBackground(
        operation,
        () -> {
          try {
            if (stopped(operation, token)) {
              operation.cancelFromWorker();
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
            FileObject[] children = VfsFileListing.listChildren(folder);
            int read = Math.min(children.length, VfsFileListing.PAGE_SIZE);
            List<VfsFileRow> rows = VfsFileListing.rowsOf(children, 0, read);
            if (stopped(operation, token)) {
              operation.cancelFromWorker();
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
                  installRoot(folderUri, published, children, read);
                  if (recordHistory) {
                    recordHistory(folderUri);
                  }
                  if (selected != null) {
                    selectTableName(selected);
                  }
                  explorer.refreshOperations();
                });
          } catch (Throwable e) {
            fail(operation, e);
            refreshPanel(display);
          }
        });
  }

  public void refreshFolder() {
    TreeItem item = findTreeItem(currentFolderUri());
    FolderNode node = nodeOf(item);
    if (item == null || node == null) {
      navigateTo(getLocationText(), false);
      return;
    }
    startList(item, node, true);
  }

  /**
   * Folder whose contents are showing. A half-typed location is used only before the first listing.
   */
  public String shownFolder() {
    return currentFolderUri();
  }

  /** F2 renames the folder under the tree, or the single file under the file list. */
  public void renameFromShortcut() {
    Control focus = getDisplay().getFocusControl();
    if (isUnder(table, focus)) {
      renameSelectedFile();
    } else if (isUnder(tree, focus)) {
      renameSelectedFolder();
    }
  }

  /** Delete removes the selection of the tree or the file list, whichever has focus. */
  public void deleteFromShortcut() {
    Control focus = getDisplay().getFocusControl();
    if (isUnder(table, focus)) {
      deleteSelectedFiles();
    } else if (isUnder(tree, focus)) {
      deleteSelectedFolders();
    }
  }

  public void copyFromShortcut() {
    Control focus = getDisplay().getFocusControl();
    if (isUnder(table, focus)) {
      copySelectedFiles();
    } else if (isUnder(tree, focus)) {
      copySelectedFolders();
    }
  }

  public void cutFromShortcut() {
    Control focus = getDisplay().getFocusControl();
    if (isUnder(table, focus)) {
      cutSelectedFiles();
    } else if (isUnder(tree, focus)) {
      cutSelectedFolders();
    }
  }

  public void focusLocation() {
    if (!locationText.isDisposed()) {
      locationText.setFocus();
      locationText.selectAll();
    }
  }

  public void publishCounts() {
    if (isDisposed()) {
      return;
    }
    int unread = shown == null ? 0 : shown.unread();
    explorer.setListingCounts(this, VfsListingCounts.of(shownRows, selectedTableRows(), unread));
  }

  /**
   * Fill the places: the project, the named connections, this computer (the server on Hop Web) and
   * the bookmarks. Connections are read again, so a connection saved since shows up.
   */
  public void refreshPlaces() {
    if (placesTree.isDisposed()) {
      return;
    }
    placesTree.removeAll();
    HopGui hopGui = explorer.getHopGui();
    addPlaces(
        "VfsFileExplorer.Places.Project",
        VfsPlaces.project(variables, BaseMessages.getString(PKG, "VfsFileExplorer.Places.Home")));
    addPlaces(
        "VfsFileExplorer.Places.Connections",
        VfsPlaces.connections(hopGui == null ? null : hopGui.getMetadataProvider(), variables));
    addPlaces(
        EnvironmentUtils.getInstance().isWeb()
            ? "VfsFileExplorer.Places.Server"
            : "VfsFileExplorer.Places.Local",
        VfsPlaces.local(BaseMessages.getString(PKG, "VfsFileExplorer.Places.UserHome")));
    addPlaces("VfsFileExplorer.Places.Bookmarks", VfsPlaces.bookmarks(explorer.bookmarks()));
    updateToolbar();
  }

  /** A section with its places, expanded. An empty section is left out. */
  private void addPlaces(String titleKey, List<VfsPlaces.Place> places) {
    if (places.isEmpty()) {
      return;
    }
    TreeItem section = new TreeItem(placesTree, SWT.NONE);
    section.setText(BaseMessages.getString(PKG, titleKey));
    for (VfsPlaces.Place place : places) {
      TreeItem item = new TreeItem(section, SWT.NONE);
      item.setText(place.getName());
      item.setImage(placeImage(place));
      item.setData(place);
    }
    section.setExpanded(true);
  }

  private Image placeImage(VfsPlaces.Place place) {
    GuiResource resource = GuiResource.getInstance();
    try {
      return switch (place.getKind()) {
        case PROJECT -> resource.getImage("ui/images/home.svg");
        case BOOKMARK -> resource.getImage("ui/images/bookmark-add.svg");
        case CONNECTION ->
            place.getImage() == null
                ? folderImage
                : resource.getImage(
                    place.getImage(),
                    place.getClassLoader(),
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);
        case LOCAL -> folderImage;
      };
    } catch (Exception e) {
      return folderImage;
    }
  }

  private VfsPlaces.Place selectedPlace() {
    if (placesTree.isDisposed()) {
      return null;
    }
    TreeItem[] selection = placesTree.getSelection();
    return selection.length == 1 && selection[0].getData() instanceof VfsPlaces.Place place
        ? place
        : null;
  }

  private void openPlace(Widget item) {
    if (item instanceof TreeItem treeItem
        && !treeItem.isDisposed()
        && treeItem.getData() instanceof VfsPlaces.Place place
        && StringUtils.isNotEmpty(place.getUri())) {
      navigateTo(place.getUri(), true);
    }
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_HOME,
      toolTip = "i18n::VfsFileExplorer.Navigate.Home.Tooltip",
      image = "ui/images/home.svg")
  public void navigateHome() {
    navigateTo(VfsFileExplorerViews.defaultLocation(variables), true);
  }

  @GuiToolbarElement(
      root = NAVIGATE_TOOLBAR_PARENT_ID,
      id = NAVIGATE_UP,
      toolTip = "i18n::VfsFileExplorer.Navigate.Up.Tooltip",
      image = "ui/images/navigate-up.svg")
  public void navigateUp() {
    TreeItem item = findTreeItem(currentFolderUri());
    if (item != null && item.getParentItem() != null) {
      adjustingTree = true;
      try {
        tree.setSelection(item.getParentItem());
      } finally {
        adjustingTree = false;
      }
      onTreeSelected(item.getParentItem());
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
    runInBackground(
        operation,
        () -> {
          try {
            if (stopped(operation, token)) {
              operation.cancelFromWorker();
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
          } catch (Throwable e) {
            fail(operation, e);
            refreshPanel(display);
          }
        });
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
    if (!checkWrite()) {
      return;
    }
    FolderNode node = shown != null ? shown : selectedNode();
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
        operation -> {
          FileObject folder = HopVfs.getFileObject(node.uri, variables);
          folder.resolveFile(name).createFolder();
          return null;
        });
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_COPY,
      toolTip = "i18n::VfsFileExplorer.Tree.Copy.Tooltip",
      image = "ui/images/copy.svg",
      separator = true)
  public void copySelectedFolders() {
    copyEntries(selectedFolderEntries(), VfsFileTransfer.Mode.COPY);
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_CUT,
      toolTip = "i18n::VfsFileExplorer.Tree.Cut.Tooltip",
      image = "ui/images/cut.svg")
  public void cutSelectedFolders() {
    if (!checkWrite()) {
      return;
    }
    copyEntries(selectedFolderEntries(), VfsFileTransfer.Mode.MOVE);
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_PASTE,
      toolTip = "i18n::VfsFileExplorer.Tree.Paste.Tooltip",
      image = "ui/images/paste.svg")
  public void pasteIntoFolder() {
    pasteClipboard();
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_DELETE,
      toolTip = "i18n::VfsFileExplorer.Tree.Delete.Tooltip",
      image = "ui/images/delete.svg",
      separator = true)
  public void deleteSelectedFolders() {
    if (!checkWrite()) {
      return;
    }
    List<VfsFileTransfer.Entry> folders = selectedFolderEntries();
    if (folders.isEmpty() || !confirmDelete(namesOf(folders))) {
      return;
    }
    String shownUri = currentFolderUri();
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Deleting", folders.get(0).getName()),
        folders.get(0).getUri(),
        operation -> deleteEntries(folders, shownUri));
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_RENAME,
      toolTip = "i18n::VfsFileExplorer.Tree.Rename.Tooltip",
      image = "ui/images/rename.svg")
  public void renameSelectedFolder() {
    if (!checkWrite()) {
      return;
    }
    List<VfsFileTransfer.Entry> folders = selectedFolderEntries();
    if (folders.size() != 1) {
      return;
    }
    VfsFileTransfer.Entry folder = folders.get(0);
    String newName = askName(folder.getName());
    if (newName == null) {
      return;
    }
    String uri = folder.getUri();
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Renaming", folder.getName()),
        uri,
        operation -> renameEntry(uri, newName));
  }

  @GuiToolbarElement(
      root = TREE_TOOLBAR_PARENT_ID,
      id = TREE_HIDDEN,
      toolTip = "i18n::VfsFileExplorer.Tree.Hidden.Tooltip",
      image = "ui/images/hide.svg",
      separator = true)
  public void toggleHiddenFolders() {
    VfsExplorerViewState state = explorer.getViewState();
    state.setShowHiddenFolders(!state.isShowHiddenFolders());
    state.save();
    refilterFolders();
    updateToolbar();
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
    openWith(row, false);
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_OPEN_TEXT,
      toolTip = "i18n::VfsFileExplorer.Details.OpenText.Tooltip",
      image = "ui/images/file.svg")
  public void openSelectedAsText() {
    VfsFileRow row = singleFileRow();
    if (row == null) {
      return;
    }
    openWith(row, true);
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_DOWNLOAD,
      toolTip = "i18n::VfsFileExplorer.Details.Download.Tooltip",
      image = "ui/images/download.svg")
  public void downloadSelected() {
    if (BrowserFileTransferFacade.isSupported()) {
      downloadToBrowser();
      return;
    }
    if (!checkWrite()) {
      return;
    }
    VfsFileRow row = singleFileRow();
    if (row == null) {
      return;
    }
    HopVfsFileDialog dialog = new HopVfsFileDialog(getShell(), variables, null, false, true, false);
    dialog.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Download.Title"));
    dialog.setFilterExtensions(new String[] {"*.*"});
    dialog.setFilterNames(
        new String[] {BaseMessages.getString(PKG, "VfsFileExplorer.Download.AllFiles")});
    dialog.setSaveFilename(row.getName());
    String folder = currentFolderUri();
    if (StringUtils.isNotEmpty(folder)) {
      dialog.setFilterPath(folder);
    }
    String destination = dialog.open();
    if (StringUtils.isBlank(destination)) {
      return;
    }
    String sourceUri = row.getUri();
    String sourceName = row.getName();
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Copying", sourceName),
        destination,
        operation -> {
          FileObject source = HopVfs.getFileObject(sourceUri, variables);
          FileObject chosen = HopVfs.getFileObject(destination, variables);
          FileObject target = chosen;
          if (chosen.exists() && chosen.isFolder()) {
            target = chosen.resolveFile(source.getName().getBaseName());
          }
          if (VfsFileTransfer.same(source, target)) {
            throw new IllegalStateException(
                BaseMessages.getString(PKG, "VfsFileExplorer.Transfer.Same", sourceName));
          }
          if (!target.exists()) {
            VfsFileTransfer.copy(source, target, operation::isCancelled, operation.progress());
            return null;
          }
          if (target.isFolder()) {
            throw new IllegalStateException(
                transferMessage(
                    VfsFileTransfer.Refusal.OTHER_KIND, target.getName().getBaseName()));
          }
          VfsFileTransfer.Resolution resolution = conflictResolver().resolve(source, target);
          if (resolution == VfsFileTransfer.Resolution.CANCEL) {
            throw new CancellationException();
          }
          if (resolution == VfsFileTransfer.Resolution.REPLACE) {
            VfsFileTransfer.replaceFile(
                source,
                target,
                VfsFileTransfer.Mode.COPY,
                operation::isCancelled,
                operation.progress());
          }
          return null;
        });
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_UPLOAD,
      toolTip = "i18n::VfsFileExplorer.Details.Upload.Tooltip",
      image = "ui/images/upload.svg")
  public void uploadSelected() {
    if (BrowserFileTransferFacade.isSupported()) {
      uploadFromBrowser();
      return;
    }
    if (!checkWrite()) {
      return;
    }
    List<VfsFileTransfer.Entry> files = selectedUploadEntries();
    if (files.isEmpty()) {
      return;
    }
    String start = currentFolderUri();
    String destination =
        BaseDialog.presentDirectoryDialog(
            getShell(),
            StringUtils.isBlank(start) ? null : start,
            BaseMessages.getString(PKG, "VfsFileExplorer.Upload.Message"),
            variables);
    if (StringUtils.isBlank(destination)) {
      return;
    }
    if (variables != null) {
      destination = variables.resolve(destination);
    }
    String description =
        files.size() == 1
            ? BaseMessages.getString(
                PKG, "VfsFileExplorer.Operation.Copying", files.get(0).getName())
            : BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Copying.Many", files.size());
    copyIntoFolder(destination, files, VfsFileTransfer.Mode.COPY, description);
  }

  /**
   * Hop Web: send the selected file to the browser. It is read into a server-side temporary file
   * first, because a remote file cannot be streamed to the browser directly.
   */
  private void downloadToBrowser() {
    if (!HopSecurityUi.check(Permission.FILE_EXPORT)) {
      return;
    }
    VfsFileRow row = singleFileRow();
    if (row == null) {
      return;
    }
    String sourceUri = row.getUri();
    String name = row.getName();
    Display display = getDisplay();
    Path local;
    try {
      // In the session's temporary folder, which Hop Web removes when the session ends.
      local = BrowserFileTransferFacade.createTempFile("download-");
    } catch (Exception e) {
      failOnUi(e);
      return;
    }
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Downloading", name),
        sourceUri,
        false,
        operation -> {
          boolean handedOver = false;
          try {
            FileObject source = HopVfs.getFileObject(sourceUri, variables);
            FileObject target = HopVfs.getFileObject(local.toUri().toString());
            // Stops between chunks, and removes a partly written copy.
            VfsFileTransfer.copy(source, target, operation::isCancelled, operation.progress());
            if (operation.isCancelled()) {
              throw new CancellationException();
            }
            display.asyncExec(
                () -> {
                  try {
                    if (!operation.isCancelled() && !isDisposed()) {
                      BrowserFileTransferFacade.download(name, local);
                    }
                  } catch (Exception e) {
                    failOnUi(e);
                  } finally {
                    deleteLocalQuietly(local);
                  }
                });
            handedOver = true;
          } finally {
            if (!handedOver) {
              deleteLocalQuietly(local);
            }
          }
          return null;
        });
  }

  /** Hop Web: upload a file from the user's computer into the folder being viewed. */
  private void uploadFromBrowser() {
    if (!checkWrite()) {
      return;
    }
    String folderUri = currentFolderUri();
    if (StringUtils.isBlank(folderUri)) {
      return;
    }
    BrowserFileTransferFacade.upload(
        BROWSER_UPLOAD_LIMIT,
        new BrowserFileTransferFacade.IUploadListener() {
          @Override
          public void uploaded(String filename, Path uploadedFile) {
            storeUpload(folderUri, filename, uploadedFile);
          }

          @Override
          public void error(Exception exception) {
            failOnUi(exception);
          }
        });
  }

  private void storeUpload(String folderUri, String filename, Path uploadedFile) {
    if (StringUtils.isBlank(filename)
        || filename.contains("/")
        || filename.contains("\\")
        || filename.equals("..")) {
      deleteLocalQuietly(uploadedFile);
      failOnUi(new IllegalArgumentException(filename));
      return;
    }
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Uploading", filename),
        folderUri,
        operation -> {
          try {
            FileObject source = HopVfs.getFileObject(uploadedFile.toUri().toString());
            FileObject target = HopVfs.getFileObject(folderUri, variables).resolveFile(filename);
            if (!target.exists()) {
              VfsFileTransfer.copy(source, target, operation::isCancelled, operation.progress());
              return null;
            }
            if (target.isFolder()) {
              throw new IllegalStateException(
                  transferMessage(VfsFileTransfer.Refusal.OTHER_KIND, filename));
            }
            VfsFileTransfer.Resolution resolution = conflictResolver().resolve(source, target);
            if (resolution == VfsFileTransfer.Resolution.CANCEL) {
              throw new CancellationException();
            }
            if (resolution == VfsFileTransfer.Resolution.REPLACE) {
              VfsFileTransfer.replaceFile(
                  source,
                  target,
                  VfsFileTransfer.Mode.COPY,
                  operation::isCancelled,
                  operation.progress());
            }
            return null;
          } finally {
            deleteLocalQuietly(uploadedFile);
          }
        });
  }

  private static void deleteLocalQuietly(Path file) {
    try {
      Files.deleteIfExists(file);
    } catch (Exception ignored) {
      // A file in the session's temporary folder: Hop Web removes that folder with the session.
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
    } catch (Throwable e) {
      failOnUi(e);
    }
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_COPY,
      toolTip = "i18n::VfsFileExplorer.Details.Copy.Tooltip",
      image = "ui/images/copy.svg",
      separator = true)
  public void copySelectedFiles() {
    copyEntries(selectedFileEntries(), VfsFileTransfer.Mode.COPY);
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_CUT,
      toolTip = "i18n::VfsFileExplorer.Details.Cut.Tooltip",
      image = "ui/images/cut.svg")
  public void cutSelectedFiles() {
    if (!checkWrite()) {
      return;
    }
    copyEntries(selectedFileEntries(), VfsFileTransfer.Mode.MOVE);
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_PASTE,
      toolTip = "i18n::VfsFileExplorer.Details.Paste.Tooltip",
      image = "ui/images/paste.svg")
  public void pasteIntoFileList() {
    pasteClipboard();
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_RENAME,
      toolTip = "i18n::VfsFileExplorer.Details.Rename.Tooltip",
      image = "ui/images/rename.svg")
  public void renameSelectedFile() {
    if (!checkWrite()) {
      return;
    }
    VfsFileRow row = singleFileRow();
    if (row == null) {
      return;
    }
    String newName = askName(row.getName());
    if (newName == null) {
      return;
    }
    String uri = row.getUri();
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Renaming", row.getName()),
        uri,
        operation -> {
          renameEntry(uri, newName);
          return null;
        });
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_DELETE,
      toolTip = "i18n::VfsFileExplorer.Details.Delete.Tooltip",
      image = "ui/images/delete.svg",
      separator = true)
  public void deleteSelectedFiles() {
    if (!checkWrite()) {
      return;
    }
    List<VfsFileTransfer.Entry> entries = selectedFileEntries();
    if (entries.isEmpty() || !confirmDelete(namesOf(entries))) {
      return;
    }
    String shownUri = currentFolderUri();
    runChange(
        BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Deleting", entries.get(0).getName()),
        entries.get(0).getUri(),
        operation -> deleteEntries(entries, shownUri));
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_MORE,
      toolTip = "i18n::VfsFileExplorer.Details.More.Tooltip",
      image = "ui/images/arrow-down.svg",
      separator = true)
  public void loadMore() {
    FolderNode node = shown;
    if (node == null || node.unread() == 0) {
      return;
    }
    FileObject[] children = node.all;
    int from = node.read;
    int to = Math.min(children.length, from + VfsFileListing.PAGE_SIZE);
    int token = generation.next();
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Listing", node.uri), node.uri);
    Display display = getDisplay();
    runInBackground(
        operation,
        () -> {
          try {
            List<VfsFileRow> rows = VfsFileListing.rowsOf(children, from, to);
            if (stopped(operation, token)) {
              operation.cancelFromWorker();
              refreshPanel(display);
              return;
            }
            operation.complete();
            display.asyncExec(
                () -> {
                  // A refresh in the meantime replaced the children: drop this page.
                  if (!canApply(token) || node.all != children || node.read != from) {
                    explorer.refreshOperations();
                    return;
                  }
                  List<VfsFileRow> merged = new ArrayList<>(node.children);
                  merged.addAll(rows);
                  node.children = merged;
                  node.read = to;
                  TreeItem item = findTreeItem(node.uri);
                  if (item != null && nodeOf(item) == node) {
                    boolean expanded = item.getExpanded();
                    replaceChildren(item, node.children);
                    FolderTreeIcons.setExpanded(item, expanded);
                  }
                  fillTable(node);
                  explorer.refreshOperations();
                });
          } catch (Throwable e) {
            fail(operation, e);
            refreshPanel(display);
          }
        });
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_HIDDEN,
      toolTip = "i18n::VfsFileExplorer.Details.Hidden.Tooltip",
      image = "ui/images/hide.svg",
      separator = true)
  public void toggleHiddenFiles() {
    VfsExplorerViewState state = explorer.getViewState();
    state.setShowHiddenFiles(!state.isShowHiddenFiles());
    state.save();
    refillTable();
    updateToolbar();
  }

  @GuiToolbarElement(
      root = DETAILS_TOOLBAR_PARENT_ID,
      id = DETAILS_COLUMNS,
      toolTip = "i18n::VfsFileExplorer.Details.Columns.Tooltip",
      image = "ui/images/show-grid.svg",
      separator = true)
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
    VfsPlaces.Place place = selectedPlace();
    if (place == null || place.getKind() != VfsPlaces.Kind.BOOKMARK) {
      return;
    }
    explorer.removeBookmark(place.getName());
  }

  @GuiToolbarElement(
      root = BOOKMARKS_TOOLBAR_PARENT_ID,
      id = PLACES_REFRESH,
      toolTip = "i18n::VfsFileExplorer.Places.Refresh.Tooltip",
      image = "ui/images/refresh.svg",
      separator = true)
  public void refreshPlacesFromToolbar() {
    refreshPlaces();
  }

  private void onTreeSelected(TreeItem item) {
    if (adjustingTree) {
      return;
    }
    if (item == null) {
      item = selectedTreeItem();
    }
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
    TreeItem current = findTreeItem(currentFolderUri());
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
          onTreeSelected(child);
          return;
        }
      }
    }
    navigateTo(row.getUri(), true);
  }

  /**
   * List {@code node}. A listing for the table replaces whatever the table was waiting for. A
   * listing that only expands the tree has its own token per folder, so it does not drop the
   * table's listing or another folder's.
   */
  private void startList(TreeItem item, FolderNode node, boolean showTable) {
    VfsListingGeneration listing = showTable ? generation : node.listing;
    int token = listing.next();
    String uri = node.uri;
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Operation.Listing", uri), uri);
    Display display = getDisplay();
    runInBackground(
        operation,
        () -> {
          try {
            if (operation.isCancelled() || !listing.isCurrent(token)) {
              dropListing(operation, display, item, node);
              return;
            }
            FileObject folder = HopVfs.getFileObject(uri, variables);
            folder.refresh();
            FileObject[] children = VfsFileListing.listChildren(folder);
            int read = Math.min(children.length, VfsFileListing.PAGE_SIZE);
            List<VfsFileRow> rows = VfsFileListing.rowsOf(children, 0, read);
            if (operation.isCancelled() || !listing.isCurrent(token)) {
              dropListing(operation, display, item, node);
              return;
            }
            operation.complete();
            display.asyncExec(
                () -> {
                  if (isDisposed() || !listing.isCurrent(token) || item.isDisposed()) {
                    explorer.refreshOperations();
                    return;
                  }
                  node.children = VfsFileListing.publish(listing, token, node.children, rows);
                  node.all = children;
                  node.read = read;
                  node.loaded = true;
                  replaceChildren(item, node.children);
                  if (showTable || isSelected(item)) {
                    setLocationText(uri);
                    fillTable(node);
                  }
                  explorer.refreshOperations();
                });
          } catch (Throwable e) {
            fail(operation, e);
            collapseUnloaded(display, item, node);
          }
        });
  }

  /**
   * Run {@code work} on its own thread. A hung server keeps its thread, so past {@link
   * VfsFileExplorer#MAX_RUNNING_OPERATIONS} unfinished operations a new one fails instead of
   * starting yet another thread.
   */
  private void runInBackground(VfsExplorerOperation operation, Runnable work) {
    if (explorer.runningOperations() > VfsFileExplorer.MAX_RUNNING_OPERATIONS) {
      operation.fail(
          BaseMessages.getString(
              PKG, "VfsFileExplorer.Error.TooManyRunning", VfsFileExplorer.MAX_RUNNING_OPERATIONS),
          null);
      explorer.refreshOperations();
      return;
    }
    Thread thread = BackgroundThreadFacade.start(work, "hop-vfs-explorer");
    operation.attachThread(thread);
    explorer.refreshOperations();
  }

  /** A stopped or replaced listing shows as cancelled, not as done. */
  private void dropListing(
      VfsExplorerOperation operation, Display display, TreeItem item, FolderNode node) {
    operation.cancelFromWorker();
    collapseUnloaded(display, item, node);
  }

  /**
   * A folder whose listing did not finish keeps only its placeholder row. Collapse it, so expanding
   * it again lists it again.
   */
  private void collapseUnloaded(Display display, TreeItem item, FolderNode node) {
    if (display == null || display.isDisposed()) {
      return;
    }
    display.asyncExec(
        () -> {
          if (!isDisposed()) {
            explorer.refreshOperations();
          }
          if (item != null && !item.isDisposed() && !node.loaded) {
            FolderTreeIcons.setExpanded(item, false);
          }
        });
  }

  private void runChange(String description, String location, Change change) {
    runChange(description, location, true, change);
  }

  /**
   * Run a file change in the background. With {@code reload}, the folder being viewed is listed
   * again afterwards, or the folder the change returns is opened.
   */
  private void runChange(String description, String location, boolean reload, Change change) {
    TreeItem item = findTreeItem(currentFolderUri());
    VfsExplorerOperation operation = explorer.beginOperation(description, location);
    Display display = getDisplay();
    runInBackground(
        operation,
        () -> {
          String show = null;
          try {
            if (operation.isCancelled()) {
              operation.complete();
              refreshPanel(display);
              return;
            }
            show = change.run(operation);
            operation.complete();
          } catch (CancellationException e) {
            operation.cancelFromWorker();
          } catch (Throwable e) {
            fail(operation, e);
          }
          String folder = show;
          display.asyncExec(
              () -> {
                explorer.refreshOperations();
                if (isDisposed()) {
                  return;
                }
                if (!reload) {
                  updateToolbar();
                  return;
                }
                if (StringUtils.isNotEmpty(folder)) {
                  navigateTo(folder, true);
                  return;
                }
                TreeItem refreshItem = item;
                if (refreshItem == null || refreshItem.isDisposed()) {
                  refreshItem = findTreeItem(currentFolderUri());
                }
                FolderNode node = nodeOf(refreshItem);
                if (refreshItem != null && node != null) {
                  startList(refreshItem, node, true);
                }
                updateToolbar();
              });
        });
  }

  private void installRoot(
      String folderUri, List<VfsFileRow> rows, FileObject[] children, int read) {
    tree.removeAll();
    FolderNode node = new FolderNode(folderUri, titleOf(folderUri));
    node.loaded = true;
    node.children = rows;
    node.all = children;
    node.read = read;
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
    boolean showHiddenFolders = explorer.getViewState().isShowHiddenFolders();
    for (VfsFileRow row : VfsFileListing.foldersForTree(rows, showHiddenFolders)) {
      TreeItem child = new TreeItem(item, SWT.NONE);
      child.setText(row.getName());
      child.setImage(folderImage);
      child.setData(new FolderNode(row.getUri(), row.getName()));
      new TreeItem(child, SWT.NONE);
    }
  }

  private void refillTable() {
    FolderNode node = shown != null ? shown : selectedNode();
    if (node != null && node.loaded) {
      fillTable(node);
    }
  }

  private void fillTable(FolderNode node) {
    if (!columnsBuilt) {
      rebuildColumns();
    }
    shown = node;
    VfsExplorerViewState state = explorer.getViewState();
    List<VfsFileRow> rows =
        VfsFileListing.visible(
            node.children,
            state.isShowHiddenFolders(),
            state.isShowHiddenFiles(),
            filterText.getText(),
            state.getSortColumn(),
            state.isAscending());
    shownRows = rows;
    table.deselectAll();
    table.clearAll();
    table.setItemCount(rows.size());
    updateToolbar();
  }

  /** Virtual table: an item is filled when it is first shown. */
  private void fillItem(TableItem item) {
    int index = table.indexOf(item);
    if (index < 0 || index >= shownRows.size()) {
      return;
    }
    VfsFileRow row = shownRows.get(index);
    List<VfsFileColumn> columns = shownColumns();
    item.setData(row);
    item.setImage(imageFor(row));
    for (int i = 0; i < columns.size(); i++) {
      item.setText(i, textFor(row, columns.get(i)));
    }
  }

  private void refillTableIfAlive() {
    if (!isDisposed() && !table.isDisposed()) {
      refillTable();
    }
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
    for (int i = 0; i < shownRows.size(); i++) {
      if (name.equals(shownRows.get(i).getName())) {
        table.setSelection(i);
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
    if (isDisposed() || tree.isDisposed() || table.isDisposed()) {
      return;
    }
    navigateToolbar.enableToolbarItem(NAVIGATE_BACK, historyIndex > 0);
    navigateToolbar.enableToolbarItem(NAVIGATE_FORWARD, historyIndex + 1 < history.size());
    boolean canWrite = canWrite();
    List<VfsFileTransfer.Entry> folders = selectedFolderEntries();
    boolean folderSelected = !folders.isEmpty();
    boolean oneFolder = folders.size() == 1;
    boolean canPaste =
        canWrite && !clipboard().isEmpty() && StringUtils.isNotEmpty(currentFolderUri());
    treeToolbar.enableToolbarItem(TREE_CREATE, canWrite);
    treeToolbar.enableToolbarItem(TREE_COPY, folderSelected);
    treeToolbar.enableToolbarItem(TREE_CUT, canWrite && folderSelected);
    treeToolbar.enableToolbarItem(TREE_PASTE, canPaste);
    treeToolbar.enableToolbarItem(TREE_DELETE, canWrite && folderSelected);
    treeToolbar.enableToolbarItem(TREE_RENAME, canWrite && oneFolder);
    syncHiddenImage(treeToolbar, TREE_HIDDEN, explorer.getViewState().isShowHiddenFolders());

    VfsFileRow row = singleTableRow();
    VfsFileRow file = singleFileRow();
    List<VfsFileRow> tableRows = selectedTableRows();
    boolean tableSelected = !tableRows.isEmpty();
    detailsToolbar.enableToolbarItem(DETAILS_OPEN, row != null && canOpen(row));
    detailsToolbar.enableToolbarItem(DETAILS_OPEN_TEXT, file != null);
    detailsToolbar.enableToolbarItem(DETAILS_DOWNLOAD, file != null && canDownload());
    detailsToolbar.enableToolbarItem(DETAILS_UPLOAD, canUpload());
    detailsToolbar.enableToolbarItem(
        DETAILS_DRILL, file != null && HopVfsFileDialog.getArchiveScheme(file.getName()) != null);
    detailsToolbar.enableToolbarItem(DETAILS_COPY, tableSelected);
    detailsToolbar.enableToolbarItem(DETAILS_CUT, canWrite && tableSelected);
    detailsToolbar.enableToolbarItem(DETAILS_PASTE, canPaste);
    detailsToolbar.enableToolbarItem(DETAILS_RENAME, canWrite && file != null);
    detailsToolbar.enableToolbarItem(DETAILS_DELETE, canWrite && tableSelected);
    detailsToolbar.enableToolbarItem(DETAILS_MORE, shown != null && shown.unread() > 0);
    syncHiddenImage(detailsToolbar, DETAILS_HIDDEN, explorer.getViewState().isShowHiddenFiles());
    VfsPlaces.Place place = selectedPlace();
    bookmarksToolbar.enableToolbarItem(
        BOOKMARK_REMOVE, place != null && place.getKind() == VfsPlaces.Kind.BOOKMARK);
    publishCounts();
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

  private String currentFolderUri() {
    if (shown != null) {
      return shown.uri;
    }
    return getLocationText();
  }

  private void copyEntries(List<VfsFileTransfer.Entry> entries, VfsFileTransfer.Mode mode) {
    if (entries.isEmpty()) {
      return;
    }
    clipboard().set(mode, entries);
    updateToolbar();
  }

  void pasteClipboard() {
    if (!checkWrite()) {
      return;
    }
    VfsFileClipboard clipboard = clipboard();
    List<VfsFileTransfer.Entry> entries = clipboard.entries();
    if (entries.isEmpty()) {
      return;
    }
    String destinationUri = currentFolderUri();
    if (StringUtils.isBlank(destinationUri)) {
      return;
    }
    VfsFileTransfer.Mode mode = clipboard.mode();
    String operationKey =
        mode == VfsFileTransfer.Mode.MOVE
            ? "VfsFileExplorer.Operation.Moving"
            : "VfsFileExplorer.Operation.Copying";
    copyIntoFolder(
        destinationUri,
        entries,
        mode,
        BaseMessages.getString(PKG, operationKey, entries.get(0).getName()),
        clipboard);
  }

  private void copyIntoFolder(
      String destinationUri,
      List<VfsFileTransfer.Entry> entries,
      VfsFileTransfer.Mode mode,
      String description) {
    copyIntoFolder(destinationUri, entries, mode, description, null);
  }

  /**
   * Copy or move {@code entries} into a folder in the background. A move pasted from {@code
   * clipboard} empties it: the files are no longer where it points.
   */
  private void copyIntoFolder(
      String destinationUri,
      List<VfsFileTransfer.Entry> entries,
      VfsFileTransfer.Mode mode,
      String description,
      VfsFileClipboard clipboard) {
    if (StringUtils.isBlank(destinationUri) || entries.isEmpty()) {
      return;
    }
    runChange(
        description,
        destinationUri,
        operation -> {
          FileObject destination = HopVfs.getFileObject(destinationUri, variables);
          if (!destination.isFolder()) {
            throw new IllegalStateException(destinationUri);
          }
          VfsFileTransfer.BatchResult result =
              VfsFileTransfer.transferAll(
                  entries,
                  destination,
                  mode,
                  conflictResolver(),
                  operation::isCancelled,
                  operation.progress(),
                  uri -> HopVfs.getFileObject(uri, variables));
          if (clipboard != null && mode == VfsFileTransfer.Mode.MOVE) {
            // Moved files are gone from where the cut was. Skipped, refused and failed ones stay
            // on the clipboard, so the cut can be finished.
            clipboard.retain(entries, result.getRemaining());
          }
          if (result.isCancelled()) {
            throw new CancellationException();
          }
          if (!result.getFailures().isEmpty()) {
            List<String> errors = new ArrayList<>();
            for (VfsFileTransfer.Failure failure : result.getFailures()) {
              errors.add(failureMessage(failure));
            }
            throw new IllegalStateException(String.join("\n", errors));
          }
          return null;
        });
  }

  private String deleteEntries(List<VfsFileTransfer.Entry> entries, String shownUri)
      throws Exception {
    boolean removedShown = false;
    String parentOfShown = null;
    for (VfsFileTransfer.Entry entry : entries) {
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
      FileObject file = HopVfs.getFileObject(entry.getUri(), variables);
      boolean containsShown =
          VfsLocations.sameLocation(shownUri, entry.getUri())
              || VfsLocations.isUnder(entry.getUri(), shownUri);
      if (containsShown) {
        FileObject parent = file.getParent();
        if (parent == null) {
          throw new IllegalStateException(
              BaseMessages.getString(PKG, "VfsFileExplorer.Delete.NoParent", entry.getName()));
        }
        removedShown = true;
        parentOfShown = HopVfs.getFilename(parent);
      }
      VfsFileTransfer.delete(file);
    }
    return removedShown ? parentOfShown : null;
  }

  private String renameEntry(String uri, String newName) throws Exception {
    FileObject file = HopVfs.getFileObject(uri, variables);
    FileObject parent = file.getParent();
    if (parent == null) {
      throw new IllegalStateException(
          BaseMessages.getString(
              PKG, "VfsFileExplorer.Rename.NoParent", file.getName().getBaseName()));
    }
    FileObject destination = parent.resolveFile(newName);
    if (destination.exists() && !VfsFileTransfer.same(file, destination)) {
      throw new IllegalStateException(
          BaseMessages.getString(PKG, "VfsFileExplorer.Transfer.Exists", newName));
    }
    file.moveTo(destination);
    return HopVfs.getFilename(destination);
  }

  private void openWith(VfsFileRow row, boolean asText) {
    if (row == null || explorer.getHopGui() == null) {
      return;
    }
    try {
      IHopFileType type =
          asText ? new RawExplorerFileType() : VfsHopFileTypes.find(row.getUri(), false);
      if (type == null) {
        return;
      }
      type.openFile(explorer.getHopGui(), row.getUri(), variables);
    } catch (Throwable e) {
      failOnUi(e);
    }
  }

  private String askName(String current) {
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            Const.NVL(current, ""),
            BaseMessages.getString(PKG, "VfsFileExplorer.Rename.Header"),
            BaseMessages.getString(PKG, "VfsFileExplorer.Rename.Message"));
    String newName = dialog.open();
    if (StringUtils.isBlank(newName)
        || newName.equals(current)
        || newName.contains("/")
        || newName.contains("\\")) {
      return null;
    }
    return newName;
  }

  private boolean confirmDelete(List<String> names) {
    if (names == null || names.isEmpty()) {
      return false;
    }
    MessageBox box = new MessageBox(getShell(), SWT.YES | SWT.NO | SWT.ICON_QUESTION);
    box.setText(BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Title"));
    box.setMessage(
        names.size() == 1
            ? BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Message", names.get(0))
            : BaseMessages.getString(PKG, "VfsFileExplorer.Delete.Message.Many", names.size()));
    return box.open() == SWT.YES;
  }

  /**
   * Asks about each file that already exists, on the UI thread. Replace all and Skip all answer the
   * remaining files of the same paste.
   */
  private VfsFileTransfer.ConflictResolver conflictResolver() {
    VfsFileTransfer.Resolution[] always = {null};
    return (source, target) -> {
      if (always[0] != null) {
        return always[0];
      }
      int choice = askConflict(target);
      return switch (choice) {
        case 0 -> VfsFileTransfer.Resolution.REPLACE;
        case 1 -> always[0] = VfsFileTransfer.Resolution.REPLACE;
        case 2 -> VfsFileTransfer.Resolution.SKIP;
        case 3 -> always[0] = VfsFileTransfer.Resolution.SKIP;
        default -> VfsFileTransfer.Resolution.CANCEL;
      };
    };
  }

  /** Button index of the answer, or -1 when the dialog was closed. */
  private int askConflict(FileObject target) {
    int[] choice = {-1};
    Display display = getDisplay();
    if (display == null || display.isDisposed()) {
      return -1;
    }
    String name = target.getName().getBaseName();
    String folder = parentText(target);
    display.syncExec(
        () -> {
          if (isDisposed()) {
            return;
          }
          DialogBoxWithButtons box =
              new DialogBoxWithButtons(
                  getShell(),
                  BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.Title"),
                  BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.Message", name, folder),
                  new String[] {
                    BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.Replace"),
                    BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.ReplaceAll"),
                    BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.Skip"),
                    BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.SkipAll"),
                    BaseMessages.getString(PKG, "VfsFileExplorer.Conflict.Cancel")
                  });
          choice[0] = box.open();
        });
    return choice[0];
  }

  private static String parentText(FileObject file) {
    try {
      FileObject parent = file.getParent();
      return parent == null ? "" : HopVfs.getFilename(parent);
    } catch (Exception e) {
      return "";
    }
  }

  private void showFolderMenu() {
    if (tree.isDisposed()) {
      return;
    }
    List<VfsFileTransfer.Entry> folders = selectedFolderEntries();
    boolean any = !folders.isEmpty();
    boolean canWrite = canWrite();
    Menu menu = new Menu(tree);
    menuItem(
        menu, "VfsFileExplorer.Menu.Copy", "ui/images/copy.svg", any, this::copySelectedFolders);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Cut",
        "ui/images/cut.svg",
        canWrite && any,
        this::cutSelectedFolders);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Paste",
        "ui/images/paste.svg",
        canWrite && !clipboard().isEmpty(),
        this::pasteIntoFolder);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Rename",
        "ui/images/rename.svg",
        canWrite && folders.size() == 1,
        this::renameSelectedFolder);
    new MenuItem(menu, SWT.SEPARATOR);
    addCopyPathItems(menu, urisOf(folders));
    new MenuItem(menu, SWT.SEPARATOR);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Delete",
        "ui/images/delete.svg",
        canWrite && any,
        this::deleteSelectedFolders);
    replaceMenu(tree, menu);
  }

  private void showFileMenu() {
    if (table.isDisposed()) {
      return;
    }
    VfsFileRow row = singleTableRow();
    VfsFileRow file = singleFileRow();
    boolean any = !selectedTableRows().isEmpty();
    boolean canWrite = canWrite();
    Menu menu = new Menu(table);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Open",
        "ui/images/open.svg",
        row != null && canOpen(row),
        this::openSelected);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.OpenText",
        "ui/images/file.svg",
        file != null,
        this::openSelectedAsText);
    boolean web = BrowserFileTransferFacade.isSupported();
    menuItem(
        menu,
        web ? "VfsFileExplorer.Menu.Download.Web" : "VfsFileExplorer.Menu.Download",
        "ui/images/download.svg",
        file != null && canDownload(),
        this::downloadSelected);
    menuItem(
        menu,
        web ? "VfsFileExplorer.Menu.Upload.Web" : "VfsFileExplorer.Menu.Upload",
        "ui/images/upload.svg",
        canUpload(),
        this::uploadSelected);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Drill",
        "ui/images/zipfile.svg",
        file != null && HopVfsFileDialog.getArchiveScheme(file.getName()) != null,
        this::drillInto);
    new MenuItem(menu, SWT.SEPARATOR);
    menuItem(menu, "VfsFileExplorer.Menu.Copy", "ui/images/copy.svg", any, this::copySelectedFiles);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Cut",
        "ui/images/cut.svg",
        canWrite && any,
        this::cutSelectedFiles);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Paste",
        "ui/images/paste.svg",
        canWrite && !clipboard().isEmpty(),
        this::pasteIntoFileList);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Rename",
        "ui/images/rename.svg",
        canWrite && file != null,
        this::renameSelectedFile);
    new MenuItem(menu, SWT.SEPARATOR);
    addCopyPathItems(menu, urisOf(selectedFileEntries()));
    new MenuItem(menu, SWT.SEPARATOR);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.Delete",
        "ui/images/delete.svg",
        canWrite && any,
        this::deleteSelectedFiles);
    replaceMenu(table, menu);
  }

  /**
   * Copy the paths to the system clipboard, as they are or relative to the project home. One path
   * per line.
   */
  private void addCopyPathItems(Menu menu, List<String> uris) {
    menuItem(
        menu,
        "VfsFileExplorer.Menu.CopyPath",
        "ui/images/copy.svg",
        !uris.isEmpty(),
        () -> GuiResource.getInstance().toClipboard(String.join(Const.CR, uris)));
    List<String> relative = relativeToProject(uris);
    menuItem(
        menu,
        "VfsFileExplorer.Menu.CopyProjectPath",
        "ui/images/copy.svg",
        relative != null,
        () -> GuiResource.getInstance().toClipboard(String.join(Const.CR, relative)));
  }

  /** The paths relative to {@code PROJECT_HOME}, or null when one of them is outside it. */
  private List<String> relativeToProject(List<String> uris) {
    if (uris.isEmpty() || variables == null) {
      return null;
    }
    String projectHome = variables.getVariable(PROJECT_HOME);
    if (StringUtils.isBlank(projectHome)) {
      return null;
    }
    projectHome = variables.resolve(projectHome);
    List<String> relative = new ArrayList<>();
    for (String uri : uris) {
      String path = VfsLocations.relativeTo(uri, PROJECT_HOME, projectHome);
      if (path == null) {
        return null;
      }
      relative.add(path);
    }
    return relative;
  }

  private static List<String> urisOf(List<VfsFileTransfer.Entry> entries) {
    List<String> uris = new ArrayList<>();
    for (VfsFileTransfer.Entry entry : entries) {
      uris.add(entry.getUri());
    }
    return uris;
  }

  private void menuItem(Menu menu, String key, String image, boolean enabled, Runnable action) {
    MenuItem item = new MenuItem(menu, SWT.PUSH);
    item.setText(BaseMessages.getString(PKG, key));
    item.setImage(GuiResource.getInstance().getImage(image));
    item.setEnabled(enabled);
    item.addListener(SWT.Selection, e -> action.run());
  }

  private void replaceMenu(Control control, Menu menu) {
    Menu previous = control.getMenu();
    control.setMenu(menu);
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
    if (previous != null && previous != menu && !previous.isDisposed()) {
      previous
          .getDisplay()
          .asyncExec(
              () -> {
                if (!previous.isDisposed()) {
                  previous.dispose();
                }
              });
    }
  }

  private void refilterFolders() {
    for (TreeItem item : tree.getItems()) {
      refilterFolder(item);
    }
    refillTable();
  }

  private void refilterFolder(TreeItem item) {
    FolderNode node = nodeOf(item);
    if (node == null || !node.loaded) {
      return;
    }
    boolean expanded = item.getExpanded();
    replaceChildren(item, node.children);
    if (expanded) {
      FolderTreeIcons.setExpanded(item, true);
    }
  }

  private void syncHiddenImage(GuiToolbarWidgets toolbar, String id, boolean showing) {
    ToolItem toolItem = toolbar.findToolItem(id);
    if (toolItem == null || toolItem.isDisposed()) {
      return;
    }
    Image image =
        showing
            ? GuiResource.getInstance().getImageShow()
            : GuiResource.getInstance().getImageHide();
    if (toolItem.getImage() != image) {
      toolItem.setImage(image);
    }
  }

  private List<VfsFileTransfer.Entry> selectedFolderEntries() {
    List<VfsFileTransfer.Entry> entries = new ArrayList<>();
    if (tree.isDisposed()) {
      return entries;
    }
    for (TreeItem item : tree.getSelection()) {
      FolderNode node = nodeOf(item);
      if (node != null) {
        entries.add(new VfsFileTransfer.Entry(node.uri, node.name, true));
      }
    }
    return VfsFileTransfer.withoutNested(entries);
  }

  private boolean hasSelectedFiles() {
    for (VfsFileRow row : selectedTableRows()) {
      if (!row.isFolder()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Files in the list selection. Folders are left out: upload copies files into a chosen folder.
   */
  private List<VfsFileTransfer.Entry> selectedUploadEntries() {
    List<VfsFileTransfer.Entry> entries = new ArrayList<>();
    for (VfsFileRow row : selectedTableRows()) {
      if (!row.isFolder()) {
        entries.add(new VfsFileTransfer.Entry(row.getUri(), row.getName(), false));
      }
    }
    return entries;
  }

  private List<VfsFileTransfer.Entry> selectedFileEntries() {
    List<VfsFileTransfer.Entry> entries = new ArrayList<>();
    for (VfsFileRow row : selectedTableRows()) {
      entries.add(new VfsFileTransfer.Entry(row.getUri(), row.getName(), row.isFolder()));
    }
    return VfsFileTransfer.withoutNested(entries);
  }

  private static List<String> namesOf(List<VfsFileTransfer.Entry> entries) {
    List<String> names = new ArrayList<>();
    for (VfsFileTransfer.Entry entry : entries) {
      names.add(entry.getName());
    }
    return names;
  }

  private static String failureMessage(VfsFileTransfer.Failure failure) {
    if (failure.getRefusal() != null) {
      return transferMessage(failure.getRefusal(), failure.getName());
    }
    return BaseMessages.getString(
        PKG,
        "VfsFileExplorer.Transfer.Failed",
        failure.getName(),
        VfsExplorerOperation.describe(failure.getError()));
  }

  private static String transferMessage(VfsFileTransfer.Refusal refusal, String name) {
    String key =
        switch (refusal) {
          case SAME -> "VfsFileExplorer.Transfer.Same";
          case INTO_ITSELF -> "VfsFileExplorer.Transfer.IntoItself";
          case EXISTS -> "VfsFileExplorer.Transfer.Exists";
          case OTHER_KIND -> "VfsFileExplorer.Transfer.OtherKind";
          default -> "VfsFileExplorer.Error.Title";
        };
    return BaseMessages.getString(PKG, key, name);
  }

  private TreeItem findTreeItem(String uri) {
    if (StringUtils.isBlank(uri) || tree.isDisposed()) {
      return null;
    }
    return findTreeItem(tree.getItems(), uri);
  }

  private TreeItem findTreeItem(TreeItem[] items, String uri) {
    for (TreeItem item : items) {
      if (item.isDisposed()) {
        continue;
      }
      FolderNode node = nodeOf(item);
      if (node != null && VfsLocations.sameLocation(uri, node.uri)) {
        return item;
      }
      TreeItem nested = findTreeItem(item.getItems(), uri);
      if (nested != null) {
        return nested;
      }
    }
    return null;
  }

  private static boolean isUnder(Control parent, Control control) {
    Control cursor = control;
    while (cursor != null && !cursor.isDisposed()) {
      if (cursor == parent) {
        return true;
      }
      cursor = cursor.getParent();
    }
    return false;
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
    FolderNode node = nodeOf(item);
    return node != null && shown != null && VfsLocations.sameLocation(node.uri, shown.uri);
  }

  private List<VfsFileRow> selectedTableRows() {
    List<VfsFileRow> rows = new ArrayList<>();
    if (table.isDisposed()) {
      return rows;
    }
    // By index: a virtual table has no data on items that were never shown, e.g. after Ctrl+A.
    for (int index : table.getSelectionIndices()) {
      if (index >= 0 && index < shownRows.size()) {
        rows.add(shownRows.get(index));
      }
    }
    return rows;
  }

  private VfsFileRow singleTableRow() {
    List<VfsFileRow> rows = selectedTableRows();
    return rows.size() == 1 ? rows.get(0) : null;
  }

  private VfsFileRow singleFileRow() {
    VfsFileRow row = singleTableRow();
    return row != null && !row.isFolder() ? row : null;
  }

  private VfsFileClipboard clipboard() {
    return VfsFileClipboard.of(getDisplay());
  }

  /** Hop Web users without explorer write access may browse, but not change files. */
  private static boolean canWrite() {
    return HopSecurity.allows(Permission.EXPLORER_WRITE);
  }

  /** A browser download is an export on Hop Web. On the desktop it writes a copy. */
  private static boolean canDownload() {
    return BrowserFileTransferFacade.isSupported()
        ? HopSecurity.allows(Permission.FILE_EXPORT)
        : canWrite();
  }

  /** A browser upload goes into the folder being viewed. On the desktop it copies the selection. */
  private boolean canUpload() {
    if (!canWrite()) {
      return false;
    }
    return BrowserFileTransferFacade.isSupported()
        ? StringUtils.isNotEmpty(currentFolderUri())
        : hasSelectedFiles();
  }

  /** Shortcuts reach the actions even when their buttons are disabled. */
  private static boolean checkWrite() {
    return HopSecurityUi.check(Permission.EXPLORER_WRITE);
  }

  private boolean stopped(VfsExplorerOperation operation, int token) {
    return operation.isCancelled() || !generation.isCurrent(token);
  }

  private boolean canApply(int token) {
    return !isDisposed() && generation.isCurrent(token);
  }

  /**
   * Finish {@code operation} after a driver failure. {@link Error} (a missing library, a broken
   * static initializer) is not an {@link Exception}. Leaving it uncaught keeps the status line
   * ticking and prints the stack only on the Hop GUI console.
   */
  private void fail(VfsExplorerOperation operation, Throwable thrown) {
    operation.failUnlessCancelled(thrown);
    if (operation.getStatus() == VfsExplorerOperation.Status.FAILED
        && HopLogStore.isInitialized()) {
      LogChannel.UI.logError(Const.NVL(operation.getDescription(), ""), thrown);
    }
  }

  private void failOnUi(Throwable thrown) {
    VfsExplorerOperation operation =
        explorer.beginOperation(
            BaseMessages.getString(PKG, "VfsFileExplorer.Error.Title"), getLocationText());
    fail(operation, thrown);
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
    /**
     * Folder to open when the current one was renamed or removed, or null to reload it. Long
     * changes check {@code operation} to stop early.
     */
    String run(VfsExplorerOperation operation) throws Exception;
  }

  private static final class FolderNode {
    private final String uri;
    private final String name;
    private final VfsListingGeneration listing = new VfsListingGeneration();
    private boolean loaded;
    private List<VfsFileRow> children = List.of();

    /** Every child of the folder. The details of the first {@link #read} are in the rows. */
    private FileObject[] all = new FileObject[0];

    private int read;

    private FolderNode(String uri, String name) {
      this.uri = uri;
      this.name = StringUtils.isBlank(name) ? uri : name;
    }

    private int unread() {
      return Math.max(0, all.length - read);
    }
  }
}
