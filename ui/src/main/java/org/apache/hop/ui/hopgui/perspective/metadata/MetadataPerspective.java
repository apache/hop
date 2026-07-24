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

package org.apache.hop.ui.hopgui.perspective.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.extension.ExtensionPointHandler;
import org.apache.hop.core.extension.HopExtensionPoint;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.GuiRegistry;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchResult;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.history.AuditList;
import org.apache.hop.history.AuditManager;
import org.apache.hop.history.IAuditManager;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.metadata.api.IHopMetadataSerializer;
import org.apache.hop.metadata.api.MetadataRefactorUtil;
import org.apache.hop.metadata.plugin.MetadataPluginType;
import org.apache.hop.metadata.refactor.MetadataObjectReference;
import org.apache.hop.metadata.refactor.MetadataReferenceFinder;
import org.apache.hop.metadata.refactor.MetadataReferenceResult;
import org.apache.hop.metadata.util.HopMetadataUtil;
import org.apache.hop.ui.core.ConstUi;
import org.apache.hop.ui.core.FormDataBuilder;
import org.apache.hop.ui.core.PropsUi;
import org.apache.hop.ui.core.bus.HopGuiEvents;
import org.apache.hop.ui.core.dialog.BaseDialog;
import org.apache.hop.ui.core.dialog.DetailsDialog;
import org.apache.hop.ui.core.dialog.EnterStringDialog;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.gui.GuiMenuWidgets;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
import org.apache.hop.ui.core.gui.HopNamespace;
import org.apache.hop.ui.core.gui.IToolbarContainer;
import org.apache.hop.ui.core.metadata.MetadataEditor;
import org.apache.hop.ui.core.metadata.MetadataFileType;
import org.apache.hop.ui.core.metadata.MetadataManager;
import org.apache.hop.ui.core.widget.TreeMemory;
import org.apache.hop.ui.core.widget.TreeUtil;
import org.apache.hop.ui.hopgui.HopGui;
import org.apache.hop.ui.hopgui.HopGuiKeyHandler;
import org.apache.hop.ui.hopgui.ToolbarFacade;
import org.apache.hop.ui.hopgui.context.IGuiContextHandler;
import org.apache.hop.ui.hopgui.file.IHopFileType;
import org.apache.hop.ui.hopgui.file.IHopFileTypeHandler;
import org.apache.hop.ui.hopgui.file.empty.EmptyFileType;
import org.apache.hop.ui.hopgui.file.empty.EmptyHopFileTypeHandler;
import org.apache.hop.ui.hopgui.perspective.HopPerspectivePlugin;
import org.apache.hop.ui.hopgui.perspective.IHopPerspective;
import org.apache.hop.ui.hopgui.perspective.IMetadataDropReceiver;
import org.apache.hop.ui.hopgui.perspective.MetadataTransfer;
import org.apache.hop.ui.hopgui.perspective.TabClosable;
import org.apache.hop.ui.hopgui.perspective.TabCloseHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemHandler;
import org.apache.hop.ui.hopgui.perspective.TabItemReorder;
import org.apache.hop.ui.hopgui.perspective.explorer.ExplorerPerspective;
import org.apache.hop.ui.hopgui.search.ReferenceSearchResults;
import org.apache.hop.ui.hopgui.shared.SashFormMemory;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.StackLayout;
import org.eclipse.swt.custom.TreeEditor;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.DragSource;
import org.eclipse.swt.dnd.DragSourceAdapter;
import org.eclipse.swt.dnd.DragSourceEvent;
import org.eclipse.swt.dnd.DropTarget;
import org.eclipse.swt.dnd.DropTargetAdapter;
import org.eclipse.swt.dnd.DropTargetEvent;
import org.eclipse.swt.graphics.GC;
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
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.ToolBar;
import org.eclipse.swt.widgets.ToolItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.swt.widgets.Widget;

@HopPerspectivePlugin(
    id = "200-HopMetadataPerspective",
    name = "i18n::MetadataPerspective.Name",
    description = "i18n::MetadataPerspective.Description",
    image = "ui/images/metadata.svg",
    documentationUrl = "/hop-gui/perspective-metadata.html")
@GuiPlugin(
    name = "i18n::MetadataPerspective.Name",
    description = "i18n::MetadataPerspective.GuiPlugin.Description")
public class MetadataPerspective implements IHopPerspective, TabClosable, IMetadataDropReceiver {

  public static final Class<?> PKG = MetadataPerspective.class; // i18n
  private static final String METADATA_PERSPECTIVE_TREE = "Metadata perspective tree";

  /**
   * Audit-trail key under which the project's (possibly empty) virtual folders are stored so they
   * survive refresh and restart. Each entry is {@code typeKey + FOLDER_AUDIT_SEPARATOR + path}.
   */
  private static final String FOLDER_AUDIT_TYPE = "metadata-virtual-folders";

  private static final String FOLDER_AUDIT_SEPARATOR = "\t";

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataPerspective-Toolbar";

  /**
   * Parent id under which plugins can contribute right-click context-menu items (via {@link
   * org.apache.hop.core.gui.plugin.menu.GuiMenuElement}) for a selected metadata item. Contributed
   * items are appended below the perspective's own items when a metadata item ({@link #FILE}) node
   * is selected.
   */
  public static final String GUI_PLUGIN_CONTEXT_MENU_PARENT_ID = "MetadataPerspective-ContextMenu";

  public static final String TOOLBAR_ITEM_NEW_TYPE = "MetadataPerspective-Toolbar-09000-NewType";
  public static final String TOOLBAR_ITEM_NEW = "MetadataPerspective-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_EDIT = "MetadataPerspective-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE = "MetadataPerspective-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataPerspective-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_RENAME = "MetadataPerspective-Toolbar-10020-Rename";
  public static final String TOOLBAR_ITEM_EXPAND_ALL =
      "MetadataPerspective-Toolbar-10060-ExpandAll";
  public static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "MetadataPerspective-Toolbar-10070-CollapseAll";
  public static final String TOOLBAR_ITEM_SHOW_EMPTY =
      "MetadataPerspective-Toolbar-10080-ShowEmpty";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataPerspective-Toolbar-10100-Refresh";

  private static final String KEY_HELP = "Help";
  private static final String KEY_TYPE = "type";
  public static final String FILE = "File";
  public static final String FOLDER = "Folder";

  /** A metadata type node (groups the items of one {@code @HopMetadata} type). */
  public static final String TYPE = "MetadataItem";

  /** A category header node grouping several metadata types. */
  public static final String CATEGORY = "Category";

  public static final String VIRTUAL_PATH = "virtualPath";
  public static final String ERROR = "Error";

  /** Icons for the "show empty types" toggle button (icon reflects the action it will perform). */
  private static final String IMAGE_SHOW_ALL = "ui/images/show-all.svg";

  private static final String IMAGE_SHOW_SELECTED = "ui/images/show-selected.svg";

  private static final int FILTER_DEBOUNCE_MS = 250;

  @Getter private static MetadataPerspective instance;

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private Composite editorArea;
  private StackLayout editorStackLayout;
  private MetadataOverview overview;
  private GuiToolbarWidgets toolBarWidgets;
  private GuiMenuWidgets contextMenuWidgets;
  private Text searchText;
  private Label resultCountLabel;
  private String currentSearchFilter = "";

  /** When {@code false} (the default) metadata types and categories with no items are hidden. */
  private boolean showEmptyTypes = false;

  /**
   * In-memory view of all metadata types and their items; loaded once per {@link #reloadModel()}.
   */
  private final List<MetadataTypeModel> typeModels = new ArrayList<>();

  /** Stable node ids whose default expand state has been seeded into TreeMemory this session. */
  private final Set<String> treeStateSeeded = new HashSet<>();

  /** Debounced search action so we don't rebuild the tree on every keystroke. */
  private final Runnable filterRunnable = this::applyFilter;

  private final List<MetadataEditor<?>> editors = new ArrayList<>();

  private final MetadataFileType metadataFileType;

  public MetadataPerspective() {
    instance = this;

    this.metadataFileType = new MetadataFileType();
  }

  /**
   * When this perspective is disabled (an exclusion in disabledGuiElements.xml) HopGui skips it
   * while loading the perspectives, so initialize() never runs. The singleton still exists because
   * the class is instantiated to register the GUI elements it declares, so callers reaching it
   * through {@link #getInstance()} get an instance without a HopGui, tree or tab folder to work
   * with.
   */
  private boolean isInitialized() {
    return hopGui != null;
  }

  @Override
  public String getId() {
    return "metadata-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'm', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'm', global = true)
  @Override
  public void activate() {
    if (!isInitialized()) {
      return;
    }
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    this.updateSelection();
    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return isInitialized() && hopGui.isActivePerspective(this);
  }

  @Override
  public List<IHopFileType> getSupportedHopFileTypes() {
    return List.of(metadataFileType);
  }

  @Override
  public void initialize(HopGui hopGui, Composite parent) {
    this.hopGui = hopGui;

    // Split tree and editor
    //
    sash = new SashForm(parent, SWT.HORIZONTAL);
    sash.setLayoutData(new FormDataBuilder().fullSize().result());

    createTree(sash);

    // The right-hand side stacks the editor tab folder and an overview/landing page; the overview
    // is shown whenever no editor tab is open.
    editorArea = new Composite(sash, SWT.NONE);
    editorStackLayout = new StackLayout();
    editorArea.setLayout(editorStackLayout);
    PropsUi.setLook(editorArea);
    overview = new MetadataOverview(editorArea, this);
    createTabFolder(editorArea);
    editorStackLayout.topControl = overview;
    editorArea.layout();

    // Restore the saved tree-panel width and persist it whenever the divider is dragged.
    SashFormMemory.persist(sash, "metadata-perspective-tree-width");

    this.refresh();
    this.updateSelection();

    // refresh the metadata when it changes.
    //
    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName(), e -> refresh(), HopGuiEvents.MetadataChanged.name());

    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName() + "ProjectActivated",
            e -> hopGui.getDisplay().asyncExec(this::clearSearchFilters),
            HopGuiEvents.ProjectActivated.name());

    // Add key listeners
    HopGuiKeyHandler.getInstance().addParentObjectToHandle(this);
  }

  protected MetadataManager<IHopMetadata> getMetadataManager(String objectKey) throws HopException {
    IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
    Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
    return new MetadataManager<>(
        HopGui.getInstance().getVariables(), metadataProvider, metadataClass, hopGui.getShell());
  }

  protected void createTree(Composite parent) {
    // Create composite
    //
    Composite treeComposite = new Composite(parent, SWT.NONE);
    FormLayout layout = new FormLayout();
    layout.marginWidth = 0;
    layout.marginHeight = 0;
    treeComposite.setLayout(layout);

    // Create search/filter text box
    //
    searchText = new Text(treeComposite, SWT.SEARCH | SWT.ICON_CANCEL | SWT.ICON_SEARCH);
    searchText.setMessage(BaseMessages.getString(PKG, "MetadataPerspective.Search.Placeholder"));
    PropsUi.setLook(searchText);
    FormData searchFormData = new FormData();
    searchFormData.left = new FormAttachment(0, 0);
    searchFormData.top = new FormAttachment(0, 0);
    searchFormData.right = new FormAttachment(100, 0);
    searchText.setLayoutData(searchFormData);

    // Add search listener
    searchText.addListener(SWT.Modify, e -> filterTree());

    // Result count shown under the search box while a filter is active (empty otherwise)
    resultCountLabel = new Label(treeComposite, SWT.NONE);
    PropsUi.setLook(resultCountLabel);
    FormData countFormData = new FormData();
    countFormData.left = new FormAttachment(0, 0);
    countFormData.top = new FormAttachment(searchText, 0);
    countFormData.right = new FormAttachment(100, 0);
    countFormData.height = 0; // collapsed until a search filter is active (see updateResultCount)
    resultCountLabel.setLayoutData(countFormData);

    // Create a composite with toolbar and tree for the border
    Composite composite = new Composite(treeComposite, SWT.BORDER);
    composite.setLayout(new FormLayout());
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(resultCountLabel, PropsUi.getMargin());
    layoutData.right = new FormAttachment(100, 0);
    layoutData.bottom = new FormAttachment(100, 0);
    composite.setLayoutData(layoutData);

    // Create toolbar
    //
    IToolbarContainer toolBarContainer =
        ToolbarFacade.createToolbarContainer(composite, SWT.WRAP | SWT.LEFT | SWT.HORIZONTAL);
    Control toolBar = toolBarContainer.getControl();
    toolBarWidgets = new GuiToolbarWidgets();
    toolBarWidgets.registerGuiPluginObject(this);
    toolBarWidgets.createToolbarWidgets(toolBarContainer, GUI_PLUGIN_TOOLBAR_PARENT_ID);
    FormData toolBarFormData = new FormData();
    toolBarFormData.left = new FormAttachment(0, 0);
    toolBarFormData.top = new FormAttachment(0, 0);
    toolBarFormData.right = new FormAttachment(100, 0);
    toolBar.setLayoutData(toolBarFormData);
    toolBar.pack();
    PropsUi.setLook(toolBar, Props.WIDGET_STYLE_TOOLBAR);

    tree = new Tree(composite, SWT.SINGLE | SWT.H_SCROLL | SWT.V_SCROLL);
    tree.setHeaderVisible(false);
    tree.addListener(SWT.Selection, event -> this.updateSelection());
    tree.addListener(
        SWT.DefaultSelection,
        event -> {
          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null && FILE.equals(treeItem.getData(KEY_TYPE))) {
            onEditMetadata();
          }
        });

    tree.addMenuDetectListener(
        event -> {
          if (tree.getSelectionCount() < 1) {
            return;
          }

          TreeItem treeItem = tree.getSelection()[0];
          if (treeItem != null) {
            // Show the menu
            //
            Menu menu = new Menu(tree);
            MenuItem menuItem;

            switch ((String) treeItem.getData(KEY_TYPE)) {
              case TYPE:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.New"));
                menuItem.addListener(SWT.Selection, e -> onNewMetadata());
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.NewFolder"));
                menuItem.addListener(SWT.Selection, e -> createNewFolder());
                new MenuItem(menu, SWT.SEPARATOR);
                break;
              case FOLDER:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.New"));
                menuItem.addListener(SWT.Selection, e -> onNewMetadata());
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.NewFolder"));
                menuItem.addListener(SWT.Selection, e -> createNewFolder());
                new MenuItem(menu, SWT.SEPARATOR);
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(
                    BaseMessages.getString(PKG, "MetadataPerspective.Menu.DeleteFolder"));
                menuItem.setImage(GuiResource.getInstance().getImageDelete());
                menuItem.addListener(SWT.Selection, e -> onDeleteFolder());
                new MenuItem(menu, SWT.SEPARATOR);
                break;
              case CATEGORY:
                addNewTypeMenuItemsForCategory(menu, (String) treeItem.getData());
                break;
              case FILE:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Edit"));
                menuItem.setImage(GuiResource.getInstance().getImageEdit());
                menuItem.addListener(SWT.Selection, e -> onEditMetadata());

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Rename"));
                menuItem.setImage(GuiResource.getInstance().getImageRename());
                menuItem.addListener(SWT.Selection, e -> onRenameMetadata());

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Duplicate"));
                menuItem.setImage(GuiResource.getInstance().getImageDuplicate());
                menuItem.addListener(SWT.Selection, e -> duplicateMetadata());

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(
                    BaseMessages.getString(PKG, "MetadataPerspective.Menu.FindReferences"));
                menuItem.setImage(
                    GuiResource.getInstance().getImage("ui/images/search.svg", 16, 16));
                menuItem.addListener(SWT.Selection, e -> onFindReferences());

                new MenuItem(menu, SWT.SEPARATOR);

                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Delete"));
                menuItem.setImage(GuiResource.getInstance().getImageDelete());
                menuItem.addListener(SWT.Selection, e -> onDeleteMetadata());

                new MenuItem(menu, SWT.SEPARATOR);
                break;
              default:
                break;
            }

            // Help is not meaningful on a category header.
            if (!CATEGORY.equals(treeItem.getData(KEY_TYPE))) {
              menuItem = new MenuItem(menu, SWT.POP_UP);
              menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Help"));
              menuItem.setImage(GuiResource.getInstance().getImageHelp());
              menuItem.addListener(SWT.Selection, e -> onHelpMetadata());
            }

            // Let plugins contribute extra items for a selected metadata item.
            if (FILE.equals(treeItem.getData(KEY_TYPE))) {
              appendPluginContextMenuItems(menu);
            }

            tree.setMenu(menu);
            menu.setVisible(true);
          }
        });
    PropsUi.setLook(tree);

    FormData treeFormData = new FormData();
    treeFormData.left = new FormAttachment(0, 0);
    treeFormData.top = new FormAttachment(toolBar, PropsUi.getMargin());
    treeFormData.right = new FormAttachment(100, 0);
    treeFormData.bottom = new FormAttachment(100, 0);
    tree.setLayoutData(treeFormData);

    // Create Tree editor for rename
    treeEditor = new TreeEditor(tree);
    treeEditor.horizontalAlignment = SWT.LEFT;
    treeEditor.grabHorizontal = true;

    // Remember expand/collapse within the session (shared TreeMemory, keyed by stable node ids).
    tree.addListener(SWT.Expand, e -> recordTreeState((TreeItem) e.item, true));
    tree.addListener(SWT.Collapse, e -> recordTreeState((TreeItem) e.item, false));

    // Drag and drop: reorganize within tree (same type only) and drag to canvas to open
    createTreeDragSource(tree);
    createTreeDropTarget(tree);
  }

  /**
   * Returns the metadata type key (objectKey) for the given tree item by walking up to the root
   * class item.
   */
  private String getObjectKey(TreeItem item) {
    // Walk up to the nearest node that carries a metadata type key: a type node or a folder node
    // (both store the key in their data). Category and file nodes do not, so we keep climbing.
    // Returns null when no type ancestor exists (e.g. a category header is selected).
    TreeItem current = item;
    while (current != null) {
      String nodeType = (String) current.getData(KEY_TYPE);
      if (TYPE.equals(nodeType) || FOLDER.equals(nodeType)) {
        return (String) current.getData();
      }
      current = current.getParentItem();
    }
    return null;
  }

  private void createTreeDragSource(Tree tree) {
    DragSource dragSource = new DragSource(tree, DND.DROP_MOVE);
    dragSource.setTransfer(MetadataTransfer.INSTANCE);
    dragSource.addDragListener(
        new DragSourceAdapter() {
          private Image dragImage;

          @Override
          public void dragStart(DragSourceEvent event) {
            if (tree.getSelectionCount() != 1) {
              event.doit = false;
              return;
            }
            TreeItem item = tree.getSelection()[0];
            if (item == null || !FILE.equals(item.getData(KEY_TYPE))) {
              event.doit = false;
              return;
            }
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
            Rectangle bounds = item.getBounds();
            if (bounds.width > 0 && bounds.height > 0) {
              try {
                dragImage = new Image(Display.getCurrent(), bounds.width, bounds.height);
                GC gc = new GC(tree);
                try {
                  gc.copyArea(dragImage, bounds.x, bounds.y);
                } finally {
                  gc.dispose();
                }
                event.image = dragImage;
              } catch (Exception e) {
                LogChannel.GENERAL.logDebug("Could not create metadata drag image", e);
              }
            }
          }

          @Override
          public void dragSetData(DragSourceEvent event) {
            if (MetadataTransfer.INSTANCE.isSupportedType(event.dataType)) {
              TreeItem item = tree.getSelection()[0];
              String objectKey = getObjectKey(item);
              String name = item.getText(0);
              event.data = new String[] {objectKey, name};
            }
          }

          @Override
          public void dragFinished(DragSourceEvent event) {
            if (dragImage != null) {
              dragImage.dispose();
              dragImage = null;
            }
          }
        });
  }

  private void createTreeDropTarget(Tree tree) {
    DropTarget dropTarget = new DropTarget(tree, DND.DROP_MOVE);
    dropTarget.setTransfer(MetadataTransfer.INSTANCE);
    dropTarget.addDropListener(
        new DropTargetAdapter() {
          @Override
          public void dragOver(DropTargetEvent event) {
            if (event.item == null) {
              event.detail = DND.DROP_NONE;
              return;
            }
            if (!MetadataTransfer.INSTANCE.isSupportedType(event.currentDataType)) {
              return;
            }
            TreeItem targetItem = (TreeItem) event.item;
            String targetType = (String) targetItem.getData(KEY_TYPE);
            // Only allow drop on folder or on class root (MetadataItem); not on another file
            if (FILE.equals(targetType)) {
              event.detail = DND.DROP_NONE;
              event.feedback = DND.FEEDBACK_NONE;
              return;
            }
            // Same-type check is enforced in drop()
            event.feedback = DND.FEEDBACK_SELECT | DND.FEEDBACK_SCROLL | DND.FEEDBACK_EXPAND;
            event.detail = DND.DROP_MOVE;
          }

          @Override
          public void drop(DropTargetEvent event) {
            if (!MetadataTransfer.INSTANCE.isSupportedType(event.currentDataType)
                || event.data == null
                || !(event.data instanceof String[])) {
              return;
            }
            String[] payload = (String[]) event.data;
            if (payload.length < 2) {
              return;
            }
            String sourceObjectKey = payload[0];
            String name = payload[1];
            Widget item = event.item;
            if (!(item instanceof TreeItem targetItem)) {
              return;
            }
            String targetType = (String) targetItem.getData(KEY_TYPE);
            if (FILE.equals(targetType)) {
              return;
            }
            if (!sourceObjectKey.equals(getObjectKey(targetItem))) {
              return;
            }
            String newVirtualPath =
                FOLDER.equals(targetType) ? (String) targetItem.getData(VIRTUAL_PATH) : "";
            try {
              IHopMetadataProvider provider = hopGui.getMetadataProvider();
              IHopMetadataSerializer<IHopMetadata> serializer =
                  provider.getSerializer(provider.getMetadataClassForKey(sourceObjectKey));
              IHopMetadata metadata = serializer.load(name);
              metadata.setVirtualPath(Utils.isEmpty(newVirtualPath) ? "" : newVirtualPath);
              serializer.save(metadata);
              ExtensionPointHandler.callExtensionPoint(
                  hopGui.getLog(),
                  hopGui.getVariables(),
                  HopExtensionPoint.HopGuiMetadataObjectUpdated.id,
                  metadata);
              hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
              refresh();
            } catch (Exception e) {
              new ErrorDialog(
                  getShell(),
                  ERROR,
                  BaseMessages.getString(PKG, "MetadataPerspective.DragDropMove.Error"),
                  e);
            }
          }
        });
  }

  @Override
  public void openDroppedMetadata(String objectKey, String name) {
    try {
      MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
      manager.editWithEditor(name);
      hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          ERROR,
          BaseMessages.getString(PKG, "MetadataPerspective.DragDropOpen.Error"),
          e);
    }
  }

  protected void createTabFolder(Composite parent) {

    tabFolder = new CTabFolder(parent, SWT.MULTI | SWT.BORDER);
    tabFolder.addCTabFolder2Listener(
        new CTabFolder2Adapter() {
          @Override
          public void close(CTabFolderEvent event) {
            CTabItem tabItem = (CTabItem) event.item;
            closeTab(event, tabItem);
          }
        });
    tabFolder.addListener(
        SWT.Selection,
        event -> {
          MetadataEditor<?> active = getActiveEditor();
          if (active != null) {
            active.refreshOnDialogActivate();
          }
          updateGui();
        });
    PropsUi.setLook(tabFolder, Props.WIDGET_STYLE_TAB);

    // Show/Hide tree
    //
    ToolBar tabToolBar = new ToolBar(tabFolder, SWT.FLAT);
    tabFolder.setTopRight(tabToolBar, SWT.RIGHT);
    PropsUi.setLook(tabToolBar);

    final ToolItem item = new ToolItem(tabToolBar, SWT.PUSH);
    item.setImage(GuiResource.getInstance().getImageMaximizePanel());
    item.addListener(
        SWT.Selection,
        e -> {
          if (sash.getMaximizedControl() == null) {
            sash.setMaximizedControl(editorArea);
            item.setImage(GuiResource.getInstance().getImageMinimizePanel());
          } else {
            sash.setMaximizedControl(null);
            item.setImage(GuiResource.getInstance().getImageMaximizePanel());
          }
        });
    int height = tabToolBar.computeSize(SWT.DEFAULT, SWT.DEFAULT).y;
    tabFolder.setTabHeight(Math.max(height, tabFolder.getTabHeight()));

    new TabCloseHandler(this);

    // Support reorder tab item
    //
    new TabItemReorder(this, tabFolder);
  }

  public void addEditor(MetadataEditor<?> editor) {
    addEditor(editor, -1);
  }

  /**
   * Adds a metadata editor as a new tab. When {@code tabIndex} is non-negative the tab is inserted
   * at that position; otherwise it is appended at the end.
   */
  public void addEditor(MetadataEditor<?> editor, int tabIndex) {
    if (!isInitialized()) {
      // There is no tab folder to add the editor to.
      //
      return;
    }

    // Create tab item
    //
    CTabItem tabItem =
        tabIndex >= 0 && tabIndex < tabFolder.getItemCount()
            ? new CTabItem(tabFolder, SWT.CLOSE, tabIndex)
            : new CTabItem(tabFolder, SWT.CLOSE);
    tabItem.setFont(GuiResource.getInstance().getFontDefault());
    tabItem.setText(editor.getTitle());
    tabItem.setImage(editor.getTitleImage());
    tabItem.setToolTipText(editor.getTitleToolTip());

    // Create composite for editor and buttons
    //
    Composite composite = new Composite(tabFolder, SWT.NONE);
    FormLayout layoutComposite = new FormLayout();
    layoutComposite.marginWidth = PropsUi.getFormMargin();
    layoutComposite.marginHeight = PropsUi.getFormMargin();
    composite.setLayout(layoutComposite);
    PropsUi.setLook(composite);

    // Create buttons
    Button[] buttons = editor.createButtonsForButtonBar(composite);
    if (buttons != null) {
      BaseTransformDialog.positionBottomButtons(composite, buttons, PropsUi.getMargin(), null);
    }

    // Create editor content area
    //
    Composite area = new Composite(composite, SWT.NONE);
    FormLayout layoutArea = new FormLayout();
    layoutArea.marginWidth = 0;
    layoutArea.marginHeight = 0;
    area.setLayout(layoutArea);
    FormData fdArea = new FormData();
    fdArea.left = new FormAttachment(0, 0);
    fdArea.top = new FormAttachment(0, 0);
    fdArea.right = new FormAttachment(100, 0);
    if (buttons != null) {
      fdArea.bottom = new FormAttachment(buttons[0], -PropsUi.getMargin());
    } else {
      fdArea.bottom = new FormAttachment(100, -PropsUi.getMargin());
    }

    area.setLayoutData(fdArea);
    PropsUi.setLook(area);

    // Create editor controls
    //
    editor.createControl(area);

    tabItem.setControl(composite);
    tabItem.setData(editor);

    editors.add(editor);

    // Add listeners
    HopGuiKeyHandler keyHandler = HopGuiKeyHandler.getInstance();
    keyHandler.addParentObjectToHandle(this);
    HopGui.getInstance().replaceKeyboardShortcutListeners(this.getShell(), keyHandler);

    // Switch to the tab
    //
    tabFolder.setSelection(tabItem);

    editor.setFocus();

    // An editor is open: show the tab folder instead of the overview page.
    showEditors();
  }

  /** Shows the editor tab folder (used when at least one editor tab is open). */
  private void showEditors() {
    if (editorStackLayout == null || editorArea == null || editorArea.isDisposed()) {
      return;
    }
    if (editorStackLayout.topControl != tabFolder) {
      editorStackLayout.topControl = tabFolder;
      editorArea.layout();
    }
  }

  /**
   * Shows the overview/landing page (used when no editor tab is open) and refreshes its content.
   */
  private void showOverview() {
    if (editorStackLayout == null || editorArea == null || editorArea.isDisposed()) {
      return;
    }
    if (overview != null && !overview.isDisposed()) {
      overview.setCards(buildOverviewCards());
    }
    if (editorStackLayout.topControl != overview) {
      editorStackLayout.topControl = overview;
      editorArea.layout();
    }
  }

  /** Builds the overview cards (one per metadata type) from the cached model. */
  private List<MetadataOverview.TypeCard> buildOverviewCards() {
    List<MetadataOverview.TypeCard> cards = new ArrayList<>();
    for (MetadataTypeModel typeModel : typeModels) {
      cards.add(
          new MetadataOverview.TypeCard(
              typeModel.categoryId,
              typeModel.key,
              typeModel.typeName,
              typeModel.description,
              typeModel.image,
              typeModel.metadataClass.getClassLoader(),
              typeModel.items.size()));
    }
    return cards;
  }

  /** Selects (and reveals) the tree node for the given metadata type, if it is currently shown. */
  public void selectType(String key) {
    TreeItem typeItem = findTypeItem(key);
    if (typeItem != null) {
      typeItem.setExpanded(true);
      tree.setSelection(typeItem);
      tree.showSelection();
      updateSelection();
    }
  }

  /** Creates a new metadata item of the given type from the overview page. */
  public void createNewMetadataFromOverview(String key) {
    createMetadataOfType(key, "");
  }

  /**
   * Find a metadata editor
   *
   * @param objectKey the metadata annotation key
   * @param name the name of the metadata
   * @return the metadata editor or null if not found
   */
  public MetadataEditor<?> findEditor(String objectKey, String name) {
    if (objectKey == null || name == null) return null;

    for (MetadataEditor<?> editor : editors) {
      IHopMetadata metadata = editor.getMetadata();
      HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadata.getClass());
      if (annotation != null
          && annotation.key().equals(objectKey)
          && name.equals(metadata.getName())) {
        return editor;
      }
    }
    return null;
  }

  public void setActiveEditor(MetadataEditor<?> editor) {
    if (!isInitialized()) {
      return;
    }
    for (CTabItem item : tabFolder.getItems()) {
      if (item.getData().equals(editor)) {
        tabFolder.setSelection(item);
        tabFolder.showItem(item);

        editor.setFocus();

        HopGui.getInstance()
            .handleFileCapabilities(metadataFileType, editor.hasChanged(), false, false);
      }
    }
  }

  public MetadataEditor<?> getActiveEditor() {
    if (!isInitialized() || tabFolder.getSelectionIndex() < 0) {
      return null;
    }

    return (MetadataEditor<?>) tabFolder.getSelection().getData();
  }

  /**
   * The {@code @HopMetadata} type key and name of the currently selected metadata item, as a
   * 2-element array {@code {key, name}}, or {@code null} when the selection is not a single
   * metadata item. Intended for plugins contributing items under {@link
   * #GUI_PLUGIN_CONTEXT_MENU_PARENT_ID}.
   */
  public String[] getSelectedMetadataKeyAndName() {
    if (tree == null || tree.isDisposed() || tree.getSelectionCount() != 1) {
      return null;
    }
    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem == null
        || !FILE.equals(treeItem.getData(KEY_TYPE))
        || treeItem.getParentItem() == null) {
      return null;
    }
    Object key = treeItem.getParentItem().getData();
    String name = treeItem.getText(0);
    if (key == null || Utils.isEmpty(name)) {
      return null;
    }
    return new String[] {key.toString(), name};
  }

  /** Context-menu handler: find where the selected metadata item is referenced. */
  private void onFindReferences() {
    String[] keyAndName = getSelectedMetadataKeyAndName();
    if (keyAndName == null) {
      return;
    }
    findReferences(keyAndName[0], keyAndName[1]);
  }

  /**
   * Best-effort static scan (same engine as rename) for pipelines/workflows and metadata objects
   * that reference the given metadata element by name. Results are shown in the shared
   * search-results UI, in a bottom-dock tab, so each one can be opened by double-clicking it.
   */
  private void findReferences(String metadataKey, String name) {
    String projectHome = hopGui.getVariables().resolve(Const.VAR_PROJECT_HOME);
    if (Utils.isEmpty(projectHome) || Const.VAR_PROJECT_HOME.equals(projectHome)) {
      // Without an active project there is nothing to scan.
      showNoReferencesFound(name);
      return;
    }
    List<String> searchRoots = java.util.Collections.singletonList(projectHome);
    try {
      MetadataReferenceFinder finder = new MetadataReferenceFinder(hopGui.getMetadataProvider());
      List<MetadataReferenceResult> fileReferences =
          finder.findReferences(metadataKey, name, searchRoots);
      List<MetadataObjectReference> metadataReferences =
          finder.findReferencesInMetadata(metadataKey, name);

      List<ISearchResult> results =
          ReferenceSearchResults.build(hopGui, fileReferences, metadataReferences);
      if (results.isEmpty()) {
        showNoReferencesFound(name);
        return;
      }
      ReferenceSearchResults.showInBottomDock(
          hopGui,
          BaseMessages.getString(PKG, "MetadataPerspective.FindReferences.Tab.Title", name),
          name,
          results);
    } catch (HopException e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.FindReferences.Error.Title"),
          BaseMessages.getString(PKG, "MetadataPerspective.FindReferences.Error.Message", name),
          e);
    }
  }

  private void showNoReferencesFound(String name) {
    MessageBox box = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
    box.setText(
        BaseMessages.getString(PKG, "MetadataPerspective.FindReferences.NoReferences.Title"));
    box.setMessage(
        BaseMessages.getString(
            PKG, "MetadataPerspective.FindReferences.NoReferences.Message", name));
    box.open();
  }

  /**
   * Append context-menu items contributed by plugins (annotated with {@link
   * org.apache.hop.core.gui.plugin.menu.GuiMenuElement} under {@link
   * #GUI_PLUGIN_CONTEXT_MENU_PARENT_ID}) to the given menu. The menu is rebuilt on every
   * right-click, so the widgets are recreated each time. Does nothing when no plugin items are
   * registered.
   */
  private void appendPluginContextMenuItems(Menu menu) {
    if (GuiRegistry.getInstance()
        .findChildGuiMenuItems(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, GUI_PLUGIN_CONTEXT_MENU_PARENT_ID)
        .isEmpty()) {
      return;
    }
    try {
      contextMenuWidgets = new GuiMenuWidgets();
      contextMenuWidgets.registerGuiPluginObject(this);
      contextMenuWidgets.createMenuWidgets(GUI_PLUGIN_CONTEXT_MENU_PARENT_ID, getShell(), menu);
    } catch (Exception e) {
      LogChannel.UI.logError("Error adding plugin context menu items to metadata perspective", e);
    }
  }

  @Override
  public IHopFileTypeHandler getActiveFileTypeHandler() {
    MetadataEditor<?> editor = getActiveEditor();
    if (editor != null) {
      return editor;
    }

    // If all editors are closed
    //
    return new EmptyHopFileTypeHandler();
  }

  @Override
  public void setActiveFileTypeHandler(IHopFileTypeHandler fileTypeHandler) {
    if (fileTypeHandler instanceof MetadataEditor) {
      this.setActiveEditor((MetadataEditor<?>) fileTypeHandler);
    }
  }

  /**
   * Global "new" button: opens a category-grouped menu of every metadata type so a new item can be
   * created regardless of the current tree selection (including types whose node is currently
   * hidden because it has no items).
   */
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW_TYPE,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.NewType.Tooltip",
      image = "ui/images/add.svg")
  public void onNewMetadataType() {
    Menu menu = new Menu(tree);
    addNewTypeMenuItems(menu, null);
    // Position the drop-down just below the toolbar button.
    ToolItem toolItem = toolBarWidgets.findToolItem(TOOLBAR_ITEM_NEW_TYPE);
    if (toolItem != null && !toolItem.getParent().isDisposed()) {
      ToolBar toolBar = toolItem.getParent();
      Rectangle bounds = toolItem.getBounds();
      Point location = toolBar.toDisplay(bounds.x, bounds.y + bounds.height);
      menu.setLocation(location);
    }
    menu.setVisible(true);
  }

  /**
   * Adds "New &lt;type&gt;" items to {@code menu}, optionally restricted to a single category. When
   * a single category is requested (the category context menu) the type items are added directly,
   * since the category is already chosen. Otherwise each category becomes a cascade submenu so the
   * (long) list of metadata types stays compact. Categories follow the configured category order
   * and types keep the model's name order within a category.
   */
  private void addNewTypeMenuItems(Menu menu, String onlyCategoryId) {
    List<String> categoryIds = new ArrayList<>();
    for (MetadataTypeModel typeModel : typeModels) {
      if ((onlyCategoryId == null || onlyCategoryId.equals(typeModel.categoryId))
          && !categoryIds.contains(typeModel.categoryId)) {
        categoryIds.add(typeModel.categoryId);
      }
    }
    categoryIds.sort(
        Comparator.comparingInt(MetadataCategories::orderOf)
            .thenComparing(MetadataCategories::labelFor));

    for (String categoryId : categoryIds) {
      if (onlyCategoryId != null) {
        // Scoped to one category: add the type items straight into the menu.
        addTypeItems(menu, categoryId);
      } else {
        // Every category becomes a cascade submenu (a "sub folder").
        MenuItem categoryMenuItem = new MenuItem(menu, SWT.CASCADE);
        categoryMenuItem.setText(MetadataCategories.labelFor(categoryId));
        categoryMenuItem.setImage(
            GuiResource.getInstance()
                .getImage(
                    MetadataCategories.imageFor(categoryId),
                    getClass().getClassLoader(),
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE));
        Menu subMenu = new Menu(menu);
        categoryMenuItem.setMenu(subMenu);
        addTypeItems(subMenu, categoryId);
      }
    }
  }

  /**
   * Appends one "New &lt;type&gt;" push item per metadata type of {@code categoryId} to {@code
   * menu}.
   */
  private void addTypeItems(Menu menu, String categoryId) {
    for (MetadataTypeModel typeModel : typeModels) {
      if (!typeModel.categoryId.equals(categoryId)) {
        continue;
      }
      MenuItem typeMenuItem = new MenuItem(menu, SWT.POP_UP);
      typeMenuItem.setText(
          BaseMessages.getString(PKG, "MetadataPerspective.Menu.NewOfType", typeModel.typeName));
      typeMenuItem.setImage(
          GuiResource.getInstance()
              .getImage(
                  typeModel.image,
                  typeModel.metadataClass.getClassLoader(),
                  ConstUi.SMALL_ICON_SIZE,
                  ConstUi.SMALL_ICON_SIZE));
      String key = typeModel.key;
      typeMenuItem.addListener(SWT.Selection, e -> createMetadataOfType(key, ""));
    }
  }

  /** Adds "New &lt;type&gt;" items for a single category (used by the category context menu). */
  private void addNewTypeMenuItemsForCategory(Menu menu, String categoryId) {
    addNewTypeMenuItems(menu, categoryId);
  }

  /**
   * Appends one "new" item per metadata type to {@code menu}, grouped by category (in the
   * configured order, with a separator between groups) exactly like this perspective's global "new"
   * button. Used by the global HopGui "New" drop-down so both menus stay in sync. Returns the
   * number of items that were added.
   */
  public int addNewMetadataTypeMenuItems(Menu menu) {
    int before = menu.getItemCount();
    addNewTypeMenuItems(menu, null);
    return menu.getItemCount() - before;
  }

  /**
   * Toggle button controlling whether metadata types and categories with no items are shown in the
   * tree. The icon swaps to reflect the action a click will perform.
   */
  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_SHOW_EMPTY,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.ShowEmpty.Tooltip",
      image = IMAGE_SHOW_ALL)
  public void onToggleShowEmpty() {
    showEmptyTypes = !showEmptyTypes;
    toolBarWidgets.setToolbarItemImage(
        TOOLBAR_ITEM_SHOW_EMPTY, showEmptyTypes ? IMAGE_SHOW_SELECTED : IMAGE_SHOW_ALL);
    toolBarWidgets.setToolbarItemToolTip(
        TOOLBAR_ITEM_SHOW_EMPTY,
        BaseMessages.getString(
            PKG,
            showEmptyTypes
                ? "MetadataPerspective.ToolbarElement.HideEmpty.Tooltip"
                : "MetadataPerspective.ToolbarElement.ShowEmpty.Tooltip"));
    renderTree();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_NEW,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.New.Tooltip",
      image = "ui/images/new.svg")
  public void onNewMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem == null) {
      return;
    }
    String objectKey = getObjectKey(treeItem);
    if (objectKey == null) {
      // A category header (or label) is selected: there is no single type to create here. Use the
      // global "new" button (or its menu) to create an item of an arbitrary type.
      return;
    }
    createMetadataOfType(objectKey, Const.NVL((String) treeItem.getData(VIRTUAL_PATH), ""));
  }

  /**
   * Creates a new metadata item of the given type at the given virtual path and opens its editor.
   */
  private void createMetadataOfType(String objectKey, String virtualPath) {
    try {
      MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
      manager.newMetadataWithEditor(Const.NVL(virtualPath, ""));
      hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Header"),
          BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Message"),
          e);
    }
    refresh();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EDIT,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Edit.Tooltip",
      image = "ui/images/edit.svg")
  @GuiKeyboardShortcut(key = SWT.F3)
  @GuiOsxKeyboardShortcut(key = SWT.F3)
  public void onEditMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem.getData(KEY_TYPE).equals(FILE)) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      MetadataEditor<?> editor = this.findEditor(objectKey, objectName);
      if (editor != null) {
        this.setActiveEditor(editor);
      } else {
        try {
          MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
          manager.editWithEditor(objectName);

          hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
        } catch (Exception e) {
          new ErrorDialog(getShell(), ERROR, "Error editing metadata", e);
        }
      }

      this.updateSelection();
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_RENAME,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Rename.Tooltip",
      image = "ui/images/rename.svg")
  @GuiKeyboardShortcut(key = SWT.F2)
  @GuiOsxKeyboardShortcut(key = SWT.F2)
  public void onRenameMetadata() {

    if (tree.getSelectionCount() < 1) {
      return;
    }

    // Identify the selected item
    TreeItem item = tree.getSelection()[0];
    if (item != null) {
      if (item.getParentItem() == null) return;
      String objectKey = (String) item.getParentItem().getData();
      String objectName = item.getText(0);

      // The control that will be the editor must be a child of the Tree
      Text text = new Text(tree, SWT.BORDER);
      text.setText(item.getText());
      text.addListener(SWT.FocusOut, event -> text.dispose());
      text.addListener(
          SWT.KeyUp,
          event -> {
            switch (event.keyCode) {
              case SWT.CR, SWT.KEYPAD_CR:
                // If name changed
                if (!objectName.equals(text.getText())) {
                  String newName = text.getText().trim();
                  try {
                    MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
                    // If the renamed item's own tab has unsaved changes, ask to save first.
                    // This must happen before the rename so the save uses the original name.
                    MetadataEditor<?> ownEditor = findEditor(objectKey, objectName);
                    if (ownEditor != null
                        && ownEditor.hasChanged()
                        && !promptSaveBeforeRefactoring(ownEditor)) {
                      break; // user cancelled — abort rename
                    }
                    // Capture the currently active editor so we can restore focus after all
                    // close/reopen operations finish (tabs are reordered but focus should not move)
                    String[] activeLocation = captureActiveEditorLocation();
                    // Collect metadata refs whose tabs should be reloaded after the rename so that
                    // MetaSelectionLine dropdowns are already populated with the new name.
                    List<MetadataObjectReference> deferredMetadataRefs = new ArrayList<>();
                    // Check if this metadata type supports global replace
                    if (!MetadataRefactorUtil.supportsGlobalReplace(
                        hopGui.getMetadataProvider(), objectKey)) {
                      MessageBox msgDialog = new MessageBox(getShell(), SWT.ICON_WARNING | SWT.OK);
                      msgDialog.setText(
                          BaseMessages.getString(
                              PKG,
                              "MetadataPerspective.RenameMetadata.NoPropertyTypeWarning.Title"));
                      msgDialog.setMessage(
                          BaseMessages.getString(
                              PKG,
                              "MetadataPerspective.RenameMetadata.NoPropertyTypeWarning.Message"));
                      msgDialog.open();
                      // Proceed with rename only; no reference update
                    } else {
                      if (!performGlobalReplaceIfSupported(
                          objectKey, objectName, newName, deferredMetadataRefs)) {
                        break; // user cancelled a save dialog — abort rename
                      }
                    }
                    if (manager.rename(objectName, newName)) {
                      text.dispose();
                    }
                    // Reload affected metadata tabs now — after the rename so that
                    // MetaSelectionLine dropdowns already contain the new item name.
                    reloadEditorsForMetadata(deferredMetadataRefs);
                    // If the renamed item's tab is open, close and reopen it
                    reopenEditorIfOpen(objectKey, objectName, newName);
                    // Restore focus to the originally active editor. When the renamed item itself
                    // was active, focus follows it to its new name.
                    restoreActiveEditor(activeLocation, objectKey, objectName, newName);
                    hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
                  } catch (Exception e) {
                    new ErrorDialog(
                        getShell(),
                        BaseMessages.getString(
                            PKG, "MetadataPerspective.EditMetadata.Error.Header"),
                        BaseMessages.getString(
                            PKG, "MetadataPerspective.EditMetadata.Error.Message"),
                        e);
                  }
                }
                break;
              case SWT.ESC:
                text.dispose();
                break;
              default:
                break;
            }
          });
      text.selectAll();
      text.setFocus();
      PropsUi.setLook(text);
      treeEditor.setEditor(text, item);
    }
  }

  /**
   * Finds references to {@code oldName} in pipelines/workflows and other metadata, asks the user to
   * update them, and performs the replacement. Returns {@code true} if the operation completed (or
   * there was nothing to do), {@code false} if the user cancelled a save-changes dialog and the
   * caller should abort its own operation too.
   *
   * @param objectKey metadata type key (e.g. "rdbms", "reconnection")
   * @param oldName the name before rename
   * @param newName the name after rename
   */
  public boolean performGlobalReplaceIfSupported(String objectKey, String oldName, String newName)
      throws HopException {
    return performGlobalReplaceIfSupported(objectKey, oldName, newName, null);
  }

  /**
   * Like {@link #performGlobalReplaceIfSupported(String, String, String)} but populates {@code
   * metadataRefsToReload} (when non-null) with the metadata objects whose editors should be
   * refreshed after the caller completes the rename. This allows the caller to defer the
   * close+reopen until after {@code manager.rename()} so that MetaSelectionLine dropdowns are
   * populated with the already-renamed item's new name.
   */
  public boolean performGlobalReplaceIfSupported(
      String objectKey,
      String oldName,
      String newName,
      List<MetadataObjectReference> metadataRefsToReload)
      throws HopException {
    if (!MetadataRefactorUtil.supportsGlobalReplace(hopGui.getMetadataProvider(), objectKey)) {
      return true;
    }
    List<String> searchRoots = new ArrayList<>();
    String projectHome = hopGui.getVariables().resolve(Const.VAR_PROJECT_HOME);
    if (Utils.isEmpty(projectHome) || Const.VAR_PROJECT_HOME.equals(projectHome)) {
      return true;
    }
    searchRoots.add(projectHome);
    MetadataReferenceFinder finder = new MetadataReferenceFinder(hopGui.getMetadataProvider());
    List<MetadataReferenceResult> fileRefs = finder.findReferences(objectKey, oldName, searchRoots);
    List<MetadataObjectReference> metadataRefs =
        finder.findReferencesInMetadata(objectKey, oldName);
    if (fileRefs.isEmpty() && metadataRefs.isEmpty()) {
      return true;
    }
    // Before asking about updating references, let the user save any open pipeline/workflow tabs
    // or metadata tabs that would be affected. Abort the whole operation on cancel.
    Set<String> filePaths = new LinkedHashSet<>();
    for (MetadataReferenceResult r : fileRefs) {
      filePaths.add(r.getFilePath());
    }
    if (!saveChangedFileHandlersForRefs(filePaths)) {
      return false;
    }
    if (!saveChangedEditorsForRefs(metadataRefs)) {
      return false;
    }
    int fileTotal = fileRefs.stream().mapToInt(MetadataReferenceResult::getReferenceCount).sum();
    int total = fileTotal + metadataRefs.size();
    List<String> detailLines = new ArrayList<>();
    for (MetadataReferenceResult r : fileRefs) {
      detailLines.add(toDisplayPath(r.getFilePath(), projectHome));
    }
    for (MetadataObjectReference r : metadataRefs) {
      detailLines.add(
          BaseMessages.getString(
              PKG,
              "MetadataPerspective.RenameMetadata.UpdateReferences.Details.MetadataEntry",
              r.getContainerMetadataKey(),
              r.getContainerObjectName()));
    }
    boolean updateRefs =
        showUpdateMetadataReferencesDialog(
            objectKey, total, fileRefs.size(), metadataRefs.size(), detailLines);
    if (updateRefs) {
      finder.replaceReferences(objectKey, fileRefs, oldName, newName);
      finder.replaceReferencesInMetadata(objectKey, oldName, newName, metadataRefs);
      Set<String> updatedPaths = new LinkedHashSet<>();
      for (MetadataReferenceResult r : fileRefs) {
        updatedPaths.add(r.getFilePath());
      }
      // Capture the active file handler so reloading tabs doesn't steal focus
      ExplorerPerspective explorerPerspective = HopGui.getExplorerPerspective();
      IHopFileTypeHandler activeFileHandler = explorerPerspective.getActiveFileTypeHandler();
      explorerPerspective.reloadTabsForFilenames(updatedPaths);
      if (!(activeFileHandler instanceof EmptyHopFileTypeHandler)) {
        explorerPerspective.setActiveFileTypeHandler(activeFileHandler);
      }
      // Metadata tab reload is deferred: if the caller supplied an output list, populate it so
      // it can reload after manager.rename() — ensuring MetaSelectionLine dropdowns already
      // contain the new name when the tabs reopen. Otherwise reload immediately (backward compat).
      if (metadataRefsToReload != null) {
        metadataRefsToReload.addAll(metadataRefs);
      } else {
        reloadEditorsForMetadata(metadataRefs);
      }
    }
    return true;
  }

  /**
   * For each file path in {@code filePaths}, if an open pipeline/workflow tab has unsaved changes,
   * shows "save before refactoring?" dialog. Saves on Yes, discards on No, aborts on Cancel.
   */
  private boolean saveChangedFileHandlersForRefs(Set<String> filePaths) {
    List<IHopFileTypeHandler> changed =
        HopGui.getExplorerPerspective().getChangedHandlersForFilenames(filePaths);
    for (IHopFileTypeHandler handler : changed) {
      if (!promptSaveFileHandlerBeforeRefactoring(handler)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Shows "Do you want to save '{filename}' before refactoring?" for the given file handler. Saves
   * on Yes, discards on No, returns {@code false} (abort) on Cancel.
   */
  private boolean promptSaveFileHandlerBeforeRefactoring(IHopFileTypeHandler handler) {
    String filename = Const.NVL(handler.getFilename(), handler.toString());
    MessageBox dialog =
        new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
    dialog.setText(
        BaseMessages.getString(PKG, "MetadataPerspective.RenameMetadata.UpdateReferences.Title"));
    dialog.setMessage(
        BaseMessages.getString(
            PKG, "MetadataPerspective.RenameMetadata.SaveBeforeRefactoring.Message", filename));
    int answer = dialog.open();
    if ((answer & SWT.YES) != 0) {
      try {
        handler.save();
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error saving file before refactoring", e);
        return false;
      }
    }
    return (answer & SWT.CANCEL) == 0;
  }

  /**
   * For each metadata object in {@code refs}, if it has an open editor with unsaved changes, shows
   * "save before refactoring?" dialog. Saves if the user confirms, discards if the user declines.
   * Returns {@code false} (abort) if the user cancels any of the dialogs.
   */
  public boolean saveChangedEditorsForRefs(List<MetadataObjectReference> refs) {
    Set<MetadataObjectReference> seen = new LinkedHashSet<>();
    for (MetadataObjectReference ref : refs) {
      if (!seen.add(ref)) {
        continue;
      }
      MetadataEditor<?> editor =
          findEditor(ref.getContainerMetadataKey(), ref.getContainerObjectName());
      if (editor != null && editor.hasChanged()) {
        if (!promptSaveBeforeRefactoring(editor)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Shows "Do you want to save '{name}' before refactoring?" for the given editor. Saves on Yes,
   * discards on No, returns {@code false} (abort) on Cancel.
   */
  private boolean promptSaveBeforeRefactoring(MetadataEditor<?> editor) {
    Class<?> managedClass = editor.getManager().getManagedClass();
    HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);
    String managedName = annotation != null ? annotation.name() : managedClass.getSimpleName();

    MessageBox dialog =
        new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO | SWT.CANCEL);
    dialog.setText(TranslateUtil.translate(managedName, managedClass));
    dialog.setMessage(
        BaseMessages.getString(
            PKG,
            "MetadataPerspective.RenameMetadata.SaveBeforeRefactoring.Message",
            editor.getTitle()));
    int answer = dialog.open();
    if ((answer & SWT.YES) != 0) {
      try {
        editor.save();
      } catch (Exception e) {
        new ErrorDialog(getShell(), "Error", "Error saving metadata before refactoring", e);
        return false;
      }
    }
    return (answer & SWT.CANCEL) == 0;
  }

  /**
   * Closes and reopens any open metadata tabs whose underlying object was updated (e.g. a reference
   * inside was replaced). Each object is processed at most once.
   */
  public void reloadEditorsForMetadata(List<MetadataObjectReference> refs) {
    if (refs == null || refs.isEmpty()) {
      return;
    }
    Set<MetadataObjectReference> seen = new LinkedHashSet<>();
    for (MetadataObjectReference ref : refs) {
      if (!seen.add(ref)) {
        continue;
      }
      String name = ref.getContainerObjectName();
      reopenEditorIfOpen(ref.getContainerMetadataKey(), name, name);
    }
  }

  /**
   * If a metadata tab is open for the given key + currentName, force-closes it and reopens it with
   * newName at the same tab position. When the object was not renamed (only its content changed)
   * pass the same value for both parameters.
   */
  private void reopenEditorIfOpen(String objectKey, String currentName, String newName) {
    MetadataEditor<?> editor = findEditor(objectKey, currentName);
    if (editor == null) {
      return;
    }
    int tabIndex = -1;
    editors.remove(editor);
    for (CTabItem item : tabFolder.getItems()) {
      if (editor.equals(item.getData())) {
        tabIndex = tabFolder.indexOf(item);
        item.dispose();
        break;
      }
    }
    try {
      getMetadataManager(objectKey).editWithEditorAtIndex(newName, tabIndex);
    } catch (HopException e) {
      LogChannel.GENERAL.logError("Error reopening metadata tab: " + objectKey + "/" + newName, e);
    }
  }

  /**
   * Captures the active editor's (objectKey, name) as a 2-element array, or {@code null} if no
   * editor is active. Used to restore focus after close/reopen operations.
   */
  private String[] captureActiveEditorLocation() {
    MetadataEditor<?> active = getActiveEditor();
    if (active == null) {
      return null;
    }
    HopMetadata ann = HopMetadataUtil.getHopMetadataAnnotation(active.getMetadata().getClass());
    if (ann == null) {
      return null;
    }
    return new String[] {ann.key(), active.getMetadata().getName()};
  }

  /**
   * Restores focus to the editor that was active before a rename/refactor operation. When the
   * previously active editor was the item being renamed ({@code renamedKey}/{@code oldName}), focus
   * follows it to its new name ({@code newName}). Otherwise the original editor is re-activated.
   */
  private void restoreActiveEditor(
      String[] capturedLocation, String renamedKey, String oldName, String newName) {
    if (capturedLocation == null) {
      return;
    }
    String focusKey = capturedLocation[0];
    String focusName =
        (focusKey.equals(renamedKey) && capturedLocation[1].equals(oldName))
            ? newName
            : capturedLocation[1];
    MetadataEditor<?> editor = findEditor(focusKey, focusName);
    if (editor != null) {
      setActiveEditor(editor);
    }
  }

  /**
   * Returns a warning message for the update-references dialog when the given metadata type has
   * transforms or actions that are not yet updated by the refactor. Returns {@code null} when no
   * such warning applies.
   */
  private String getNotYetUpdatedWarning(String objectKey) {
    if (objectKey == null) {
      return null;
    }
    String key;
    switch (objectKey) {
      case "pipeline-run-configuration":
        key = "MetadataPerspective.RenameMetadata.NotYetUpdated.PipelineRunConfig";
        break;
      case "workflow-run-configuration":
        key = "MetadataPerspective.RenameMetadata.NotYetUpdated.WorkflowRunConfig";
        break;
      case "execution-info-location":
        key = "MetadataPerspective.RenameMetadata.NotYetUpdated.ExecutionInfoLocation";
        break;
      case "execution-data-profile":
        key = "MetadataPerspective.RenameMetadata.NotYetUpdated.ExecutionDataProfile";
        break;
      default:
        return null;
    }
    return BaseMessages.getString(PKG, key);
  }

  /**
   * Shows a dialog asking to update metadata references. Yes/No confirm; Details opens a list of
   * files and metadata objects that will be modified. When {@code objectKey} is one of the types
   * that have transforms/actions not yet updated, an extra warning line is shown. An experimental
   * feature note is always shown. Returns true if the user chose Yes.
   */
  private boolean showUpdateMetadataReferencesDialog(
      String objectKey,
      int totalRefCount,
      int fileCount,
      int metadataObjectCount,
      List<String> detailLines) {
    Shell shell =
        new Shell(hopGui.getShell(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setText(
        BaseMessages.getString(PKG, "MetadataPerspective.RenameMetadata.UpdateReferences.Title"));
    shell.setImage(GuiResource.getInstance().getImageHop());
    PropsUi.setLook(shell);
    FormLayout layout = new FormLayout();
    layout.marginLeft = PropsUi.getFormMargin();
    layout.marginRight = PropsUi.getFormMargin();
    layout.marginTop = PropsUi.getFormMargin();
    layout.marginBottom = PropsUi.getFormMargin();
    shell.setLayout(layout);
    int margin = PropsUi.getMargin();

    // Buttons created first so content labels can attach their bottom edge to them
    final boolean[] confirmed = new boolean[1];
    Button wYes = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wYes);
    wYes.setText(BaseMessages.getString("System.Button.Yes"));
    wYes.addListener(
        SWT.Selection,
        e -> {
          confirmed[0] = true;
          shell.dispose();
        });
    Button wDetails = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wDetails);
    wDetails.setText(
        BaseMessages.getString(
            PKG, "MetadataPerspective.RenameMetadata.UpdateReferences.Button.Details"));
    wDetails.addListener(
        SWT.Selection,
        e -> {
          DetailsDialog detailsDialog =
              new DetailsDialog(
                  shell,
                  BaseMessages.getString(
                      PKG, "MetadataPerspective.RenameMetadata.UpdateReferences.Details.Title"),
                  GuiResource.getInstance().getImageHop(),
                  BaseMessages.getString(
                      PKG, "MetadataPerspective.RenameMetadata.UpdateReferences.Details.Message"),
                  String.join(Const.CR, detailLines));
          detailsDialog.open();
        });
    Button wNo = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wNo);
    wNo.setText(BaseMessages.getString("System.Button.No"));
    wNo.addListener(
        SWT.Selection,
        e -> {
          confirmed[0] = false;
          shell.dispose();
        });
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wYes, wDetails, wNo}, margin, null);

    // Main message
    Label wMessage = new Label(shell, SWT.WRAP);
    PropsUi.setLook(wMessage);
    wMessage.setText(
        BaseMessages.getString(
            PKG,
            "MetadataPerspective.RenameMetadata.UpdateReferences.MessageWithMetadata",
            totalRefCount,
            fileCount,
            metadataObjectCount));
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, margin);
    fdMessage.right = new FormAttachment(100, -margin);
    fdMessage.top = new FormAttachment(0, margin);
    wMessage.setLayoutData(fdMessage);

    // Optional per-type warning (specific transforms/actions not yet supported)
    Control lastLabel = wMessage;
    String notYetUpdatedWarning = getNotYetUpdatedWarning(objectKey);
    if (notYetUpdatedWarning != null && !notYetUpdatedWarning.isEmpty()) {
      Label wWarning = new Label(shell, SWT.WRAP);
      PropsUi.setLook(wWarning);
      wWarning.setText(notYetUpdatedWarning);
      FormData fdWarning = new FormData();
      fdWarning.left = new FormAttachment(0, margin);
      fdWarning.right = new FormAttachment(100, -margin);
      fdWarning.top = new FormAttachment(lastLabel, margin);
      wWarning.setLayoutData(fdWarning);
      lastLabel = wWarning;
    }

    // Experimental feature note — always shown, fills remaining space above buttons
    Label wNote = new Label(shell, SWT.WRAP);
    PropsUi.setLook(wNote);
    wNote.setText(
        BaseMessages.getString(PKG, "MetadataPerspective.RenameMetadata.ExperimentalFeatureNote"));
    FormData fdNote = new FormData();
    fdNote.left = new FormAttachment(0, margin);
    fdNote.right = new FormAttachment(100, -margin);
    fdNote.top = new FormAttachment(lastLabel, margin);
    fdNote.bottom = new FormAttachment(wYes, -margin);
    wNote.setLayoutData(fdNote);

    shell.setDefaultButton(wYes);

    BaseDialog.defaultShellHandling(
        shell,
        c -> {
          /* enter in text field: no-op, buttons handle confirmation */
        },
        c -> confirmed[0] = false);
    return confirmed[0];
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DELETE,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Delete.Tooltip",
      image = "ui/images/delete.svg")
  @GuiKeyboardShortcut(key = SWT.DEL)
  @GuiOsxKeyboardShortcut(key = SWT.DEL)
  public void onDeleteMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];

    // Folders have their own delete handling; categories/labels can't be deleted.
    if (FOLDER.equals(treeItem.getData(KEY_TYPE))) {
      onDeleteFolder();
      return;
    }
    if (!FILE.equals(treeItem.getData(KEY_TYPE))) {
      return;
    }

    String objectKey = (String) treeItem.getParentItem().getData();
    String objectName = treeItem.getText(0);

    try {
      // Check for references in pipelines/workflows and other metadata objects
      MetadataReferenceFinder finder = new MetadataReferenceFinder(hopGui.getMetadataProvider());
      String projectHome = hopGui.getVariables().resolve(Const.VAR_PROJECT_HOME);
      List<MetadataReferenceResult> fileRefs = Collections.emptyList();
      if (MetadataRefactorUtil.supportsGlobalReplace(hopGui.getMetadataProvider(), objectKey)) {
        if (!Utils.isEmpty(projectHome) && !Const.VAR_PROJECT_HOME.equals(projectHome)) {
          fileRefs = finder.findReferences(objectKey, objectName, List.of(projectHome));
        }
      }
      List<MetadataObjectReference> metadataRefs =
          finder.findReferencesInMetadata(objectKey, objectName);

      int fileRefCount =
          fileRefs.stream().mapToInt(MetadataReferenceResult::getReferenceCount).sum();
      int totalRefCount = fileRefCount + metadataRefs.size();

      boolean confirmed;
      if (totalRefCount > 0) {
        List<String> detailLines = new ArrayList<>();
        for (MetadataReferenceResult r : fileRefs) {
          detailLines.add(toDisplayPath(r.getFilePath(), projectHome));
        }
        for (MetadataObjectReference r : metadataRefs) {
          detailLines.add(
              BaseMessages.getString(
                  PKG,
                  "MetadataPerspective.RenameMetadata.UpdateReferences.Details.MetadataEntry",
                  r.getContainerMetadataKey(),
                  r.getContainerObjectName()));
        }
        confirmed =
            showDeleteWithReferencesDialog(
                objectName, totalRefCount, fileRefs.size(), metadataRefs.size(), detailLines);
      } else {
        MessageBox confirmBox = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
        confirmBox.setText(BaseMessages.getString(PKG, "MetadataPerspective.DeleteMetadata.Title"));
        confirmBox.setMessage(
            BaseMessages.getString(
                PKG, "MetadataPerspective.DeleteMetadata.ConfirmNoRefs", objectName));
        confirmed = (confirmBox.open() & SWT.YES) != 0;
      }
      if (!confirmed) {
        return;
      }

      MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
      manager.deleteMetadata(objectName, true);

      refresh();
      updateSelection();

      hopGui.getEventsHandler().fire(HopGuiEvents.MetadataDeleted.name());
    } catch (Exception e) {
      new ErrorDialog(getShell(), ERROR, "Error delete metadata", e);
    }
  }

  /**
   * Replaces the resolved {@code projectHome} prefix in {@code path} with {@code ${PROJECT_HOME}}
   * so displayed paths are project-relative and shorter.
   */
  private static String toDisplayPath(String path, String projectHome) {
    if (!Utils.isEmpty(projectHome) && path.startsWith(projectHome)) {
      String rel = path.substring(projectHome.length());
      return Const.VAR_PROJECT_HOME + (rel.startsWith("/") ? rel : "/" + rel);
    }
    return path;
  }

  /**
   * Shows a confirmation dialog warning that the item being deleted has active references. Returns
   * {@code true} if the user confirms the deletion, {@code false} if they cancel.
   */
  private boolean showDeleteWithReferencesDialog(
      String objectName,
      int totalRefCount,
      int fileCount,
      int metadataObjectCount,
      List<String> detailLines) {
    Shell shell =
        new Shell(hopGui.getShell(), SWT.DIALOG_TRIM | SWT.RESIZE | SWT.APPLICATION_MODAL);
    shell.setText(BaseMessages.getString(PKG, "MetadataPerspective.DeleteMetadata.Title"));
    shell.setImage(GuiResource.getInstance().getImageHop());
    PropsUi.setLook(shell);
    FormLayout layout = new FormLayout();
    layout.marginLeft = PropsUi.getFormMargin();
    layout.marginRight = PropsUi.getFormMargin();
    layout.marginTop = PropsUi.getFormMargin();
    layout.marginBottom = PropsUi.getFormMargin();
    shell.setLayout(layout);
    int margin = PropsUi.getMargin();

    // Buttons first so the message label can attach its bottom to them
    final boolean[] confirmed = new boolean[1];
    Button wYes = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wYes);
    wYes.setText(BaseMessages.getString("System.Button.Yes"));
    wYes.addListener(
        SWT.Selection,
        e -> {
          confirmed[0] = true;
          shell.dispose();
        });
    Button wDetails = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wDetails);
    wDetails.setText(
        BaseMessages.getString(PKG, "MetadataPerspective.DeleteMetadata.Button.Details"));
    wDetails.addListener(
        SWT.Selection,
        e -> {
          new DetailsDialog(
                  shell,
                  BaseMessages.getString(
                      PKG, "MetadataPerspective.DeleteMetadata.Details.Title", objectName),
                  GuiResource.getInstance().getImageHop(),
                  BaseMessages.getString(
                      PKG, "MetadataPerspective.DeleteMetadata.Details.Message", objectName),
                  String.join(Const.CR, detailLines))
              .open();
        });
    Button wNo = new Button(shell, SWT.PUSH);
    PropsUi.setLook(wNo);
    wNo.setText(BaseMessages.getString("System.Button.No"));
    wNo.addListener(
        SWT.Selection,
        e -> {
          confirmed[0] = false;
          shell.dispose();
        });
    BaseTransformDialog.positionBottomButtons(
        shell, new Button[] {wYes, wDetails, wNo}, margin, null);

    Label wMessage = new Label(shell, SWT.WRAP);
    PropsUi.setLook(wMessage);
    wMessage.setText(
        BaseMessages.getString(
            PKG,
            "MetadataPerspective.DeleteMetadata.Message",
            totalRefCount,
            objectName,
            fileCount,
            metadataObjectCount));
    FormData fdMessage = new FormData();
    fdMessage.left = new FormAttachment(0, margin);
    fdMessage.right = new FormAttachment(100, -margin);
    fdMessage.top = new FormAttachment(0, margin);
    fdMessage.bottom = new FormAttachment(wYes, -margin);
    wMessage.setLayoutData(fdMessage);

    shell.setDefaultButton(wNo);
    BaseDialog.defaultShellHandling(
        shell,
        c -> {
          /* enter: no-op */
        },
        c -> confirmed[0] = false);
    return confirmed[0];
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_DUPLICATE,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.CreateCopy.Tooltip",
      image = "ui/images/duplicate.svg")
  public void duplicateMetadata() {

    if (tree.getSelectionCount() != 1) {
      return;
    }

    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem.getData(KEY_TYPE).equals(FILE)) {
      String objectKey = (String) treeItem.getParentItem().getData();
      String objectName = treeItem.getText(0);

      try {
        MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
        IHopMetadata metadata = manager.loadElement(objectName);

        int copyNr = 2;
        while (true) {
          String newName = objectName + " " + copyNr;
          if (!manager.getSerializer().exists(newName)) {
            metadata.setName(newName);
            manager.getSerializer().save(metadata);
            break;
          } else {
            copyNr++;
          }
        }
        refresh();

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "MetadataPerspective.DuplicateMetadata.Error.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.DuplicateMetadata.Error.Message"),
            e);
      }
    }
  }

  public void onHelpMetadata() {
    if (tree.getSelectionCount() != 1) {
      return;
    }
    String objectKey = getObjectKey(tree.getSelection()[0]);
    if (objectKey == null) {
      return;
    }
    try {
      MetadataManager<IHopMetadata> manager = getMetadataManager(objectKey);
      HopMetadata annotation = manager.getManagedClass().getAnnotation(HopMetadata.class);
      IPlugin plugin =
          PluginRegistry.getInstance().getPlugin(MetadataPluginType.class, annotation.key());
      HelpUtils.openHelp(getShell(), plugin);
    } catch (Exception ex) {
      new ErrorDialog(getShell(), ERROR, "Error opening URL", ex);
    }
  }

  public void updateEditor(MetadataEditor<?> editor) {

    if (editor == null || !isInitialized()) return;

    // Update TabItem
    //
    for (CTabItem item : tabFolder.getItems()) {
      if (editor.equals(item.getData())) {
        item.setText(editor.getTitle());
        if (editor.hasChanged()) item.setFont(GuiResource.getInstance().getFontBold());
        else item.setFont(tabFolder.getFont());
        break;
      }
    }

    // Update TreeItem
    //
    this.refresh();

    // Update HOP GUI menu and toolbar...
    //
    this.updateGui();
  }

  /** Update HopGui menu and toolbar... */
  public void updateGui() {
    final IHopFileTypeHandler activeHandler = getActiveFileTypeHandler();
    activeHandler.updateGui();
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_EXPAND_ALL,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.ExpandAll.Tooltip",
      image = "ui/images/expand-all.svg")
  @GuiKeyboardShortcut(control = true, key = '+')
  @GuiOsxKeyboardShortcut(command = true, key = '+')
  public void expandAll() {
    if (tree == null || tree.isDisposed()) {
      return;
    }

    tree.setRedraw(false);
    try {
      for (TreeItem item : tree.getItems()) {
        expandTreeItem(item, true);
        recordAllTreeState(item);
      }
    } finally {
      tree.setRedraw(true);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_COLLAPSE_ALL,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.CollapseAll.Tooltip",
      image = "ui/images/collapse-all.svg")
  @GuiKeyboardShortcut(control = true, key = '-')
  @GuiOsxKeyboardShortcut(command = true, key = '-')
  public void collapseAll() {
    if (tree == null || tree.isDisposed()) {
      return;
    }

    tree.setRedraw(false);
    try {
      for (TreeItem item : tree.getItems()) {
        expandTreeItem(item, false);
        recordAllTreeState(item);
      }
    } finally {
      tree.setRedraw(true);
    }
  }

  @GuiToolbarElement(
      root = GUI_PLUGIN_TOOLBAR_PARENT_ID,
      id = TOOLBAR_ITEM_REFRESH,
      toolTip = "i18n::MetadataPerspective.ToolbarElement.Refresh.Tooltip",
      image = "ui/images/refresh.svg")
  @GuiKeyboardShortcut(key = SWT.F5)
  @GuiOsxKeyboardShortcut(key = SWT.F5)
  public void refresh() {
    if (!isInitialized()) {
      // There is no tree to render the metadata into.
      //
      return;
    }
    reloadModel();
    renderTree();
    // Keep the overview catalog in sync when it is the visible panel (no editor open).
    if (overview != null
        && !overview.isDisposed()
        && editorStackLayout != null
        && editorStackLayout.topControl == overview) {
      overview.setCards(buildOverviewCards());
    }
  }

  /**
   * Loads the metadata model from disk into memory: one {@link MetadataTypeModel} per metadata type
   * with its items (name + virtual path). This is the only place that reads metadata objects from
   * the provider, so search filtering (handled entirely by {@link #renderTree()}) never touches
   * disk.
   */
  private void reloadModel() {
    typeModels.clear();
    try {
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      for (Class<IHopMetadata> metadataClass : metadataProvider.getMetadataClasses()) {
        HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
        if (annotation == null) {
          continue;
        }
        MetadataTypeModel typeModel =
            new MetadataTypeModel(
                annotation.key(),
                MetadataCategories.normalize(annotation.category()),
                Const.NVL(TranslateUtil.translate(annotation.name(), metadataClass), ""),
                Const.NVL(TranslateUtil.translate(annotation.description(), metadataClass), ""),
                annotation.image(),
                metadataClass);

        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = serializer.listObjectNames();
        Collections.sort(names);
        for (String name : names) {
          String virtualPath;
          try {
            virtualPath = Const.NVL(serializer.load(name).getVirtualPath(), "");
          } catch (HopException e) {
            // Ignore missing/corrupt metadata items
            LogChannel.GENERAL.logError("Error loading metadata object:" + name);
            continue;
          }
          typeModel.items.add(new MetadataItemModel(name, virtualPath));
        }
        typeModels.add(typeModel);
      }
      // Attach explicitly-created (persisted) virtual folders so empty ones survive
      // refresh/restart.
      loadPersistedFolders();
      // Sort the types by their (translated) display name.
      typeModels.sort(Comparator.comparing(typeModel -> typeModel.typeName));
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Header"),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Message"),
          e);
    }
  }

  /**
   * Reads the project's persisted virtual folders from the audit trail and attaches each path to
   * its metadata type model, so explicitly-created (and possibly empty) folders are rendered.
   */
  private void loadPersistedFolders() {
    Map<String, MetadataTypeModel> byKey = new LinkedHashMap<>();
    for (MetadataTypeModel typeModel : typeModels) {
      byKey.put(typeModel.key, typeModel);
    }
    try {
      AuditList list =
          AuditManager.getActive().retrieveList(getAuditNamespace(), FOLDER_AUDIT_TYPE);
      if (list == null || list.getNames() == null) {
        return;
      }
      for (String entry : list.getNames()) {
        int sep = entry.indexOf(FOLDER_AUDIT_SEPARATOR);
        if (sep < 0) {
          continue;
        }
        MetadataTypeModel typeModel = byKey.get(entry.substring(0, sep));
        String path = entry.substring(sep + FOLDER_AUDIT_SEPARATOR.length());
        if (typeModel != null
            && !Utils.isEmpty(path)
            && !typeModel.folderVirtualPaths.contains(path)) {
          typeModel.folderVirtualPaths.add(path);
        }
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error reading metadata virtual folders from the audit trail", e);
    }
  }

  /** Persists an (empty) virtual folder path for a metadata type in the project's audit trail. */
  private void persistFolder(String typeKey, String virtualPath) {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      String namespace = getAuditNamespace();
      AuditList list = auditManager.retrieveList(namespace, FOLDER_AUDIT_TYPE);
      if (list == null || list.getNames() == null) {
        list = new AuditList(new ArrayList<>());
      }
      String entry = typeKey + FOLDER_AUDIT_SEPARATOR + virtualPath;
      if (!list.getNames().contains(entry)) {
        list.getNames().add(entry);
        auditManager.storeList(namespace, FOLDER_AUDIT_TYPE, list);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error storing metadata virtual folder in the audit trail", e);
    }
  }

  /** The audit group for the current project (falls back to the default HopGui namespace). */
  private static String getAuditNamespace() {
    return Const.NVL(HopNamespace.getNamespace(), HopGui.DEFAULT_HOP_GUI_NAMESPACE);
  }

  /**
   * Deletes the selected virtual folder: any items inside it (or its sub-folders) are moved up to
   * the parent folder, and the folder (plus any persisted empty sub-folders) is removed from the
   * audit trail. Items are never deleted here.
   */
  public void onDeleteFolder() {
    if (tree.getSelectionCount() != 1) {
      return;
    }
    TreeItem treeItem = tree.getSelection()[0];
    if (!FOLDER.equals(treeItem.getData(KEY_TYPE))) {
      return;
    }
    String typeKey = getObjectKey(treeItem);
    String folderPath = Const.NVL((String) treeItem.getData(VIRTUAL_PATH), "");
    if (typeKey == null || folderPath.isEmpty()) {
      return;
    }

    String parentPath = parentPath(folderPath);
    List<MetadataItemModel> affected = new ArrayList<>();
    MetadataTypeModel typeModel = findTypeModel(typeKey);
    if (typeModel != null) {
      for (MetadataItemModel item : typeModel.items) {
        if (isUnderFolder(item.virtualPath, folderPath)) {
          affected.add(item);
        }
      }
    }

    MessageBox confirm = new MessageBox(getShell(), SWT.ICON_QUESTION | SWT.YES | SWT.NO);
    confirm.setText(BaseMessages.getString(PKG, "MetadataPerspective.DeleteFolder.Title"));
    confirm.setMessage(
        affected.isEmpty()
            ? BaseMessages.getString(
                PKG, "MetadataPerspective.DeleteFolder.Confirm", treeItem.getText())
            : BaseMessages.getString(
                PKG,
                "MetadataPerspective.DeleteFolder.ConfirmWithItems",
                treeItem.getText(),
                affected.size()));
    if ((confirm.open() & SWT.YES) == 0) {
      return;
    }

    try {
      if (!affected.isEmpty()) {
        IHopMetadataProvider provider = hopGui.getMetadataProvider();
        IHopMetadataSerializer<IHopMetadata> serializer =
            provider.getSerializer(provider.getMetadataClassForKey(typeKey));
        for (MetadataItemModel item : affected) {
          IHopMetadata metadata = serializer.load(item.name);
          // The affected paths all start with folderPath, so swap that prefix for the parent path.
          metadata.setVirtualPath(parentPath + item.virtualPath.substring(folderPath.length()));
          serializer.save(metadata);
        }
        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataChanged.name());
      }
      removePersistedFoldersUnder(typeKey, folderPath);
      refresh();
      updateSelection();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.DeleteFolder.Error.Header"),
          BaseMessages.getString(PKG, "MetadataPerspective.DeleteFolder.Error.Message"),
          e);
    }
  }

  /** Returns the parent of a virtual folder path ({@code "/a/b" -> "/a"}, {@code "/a" -> ""}). */
  private static String parentPath(String folderPath) {
    int idx = folderPath.lastIndexOf('/');
    return idx <= 0 ? "" : folderPath.substring(0, idx);
  }

  /**
   * True when {@code itemPath} is the folder itself or sits inside it (or one of its sub-folders).
   */
  private static boolean isUnderFolder(String itemPath, String folderPath) {
    String path = Const.NVL(itemPath, "");
    return path.equals(folderPath) || path.startsWith(folderPath + "/");
  }

  private MetadataTypeModel findTypeModel(String typeKey) {
    for (MetadataTypeModel typeModel : typeModels) {
      if (typeModel.key.equals(typeKey)) {
        return typeModel;
      }
    }
    return null;
  }

  /** Drops the given folder and any persisted sub-folder beneath it from the audit trail. */
  private void removePersistedFoldersUnder(String typeKey, String folderPath) {
    try {
      IAuditManager auditManager = AuditManager.getActive();
      String namespace = getAuditNamespace();
      AuditList list = auditManager.retrieveList(namespace, FOLDER_AUDIT_TYPE);
      if (list == null || list.getNames() == null || list.getNames().isEmpty()) {
        return;
      }
      String prefix = typeKey + FOLDER_AUDIT_SEPARATOR;
      boolean changed =
          list.getNames()
              .removeIf(
                  entry -> {
                    if (!entry.startsWith(prefix)) {
                      return false;
                    }
                    String path = entry.substring(prefix.length());
                    return path.equals(folderPath) || path.startsWith(folderPath + "/");
                  });
      if (changed) {
        auditManager.storeList(namespace, FOLDER_AUDIT_TYPE, list);
      }
    } catch (Exception e) {
      LogChannel.UI.logError("Error removing metadata virtual folder from the audit trail", e);
    }
  }

  /**
   * Rebuilds the tree from the in-memory model (see {@link #reloadModel()}), grouping types under
   * category headers and applying the current search filter and the hide-empty setting. Performs no
   * disk I/O, so it is cheap enough to run on every keystroke.
   */
  private void renderTree() {
    if (tree == null || tree.isDisposed()) {
      return;
    }
    try {
      // Capture scroll (top item) and selection so a rebuild doesn't jump back to the top.
      String topId = nodeIdentity(tree.getTopItem());
      TreeItem[] selection = tree.getSelection();
      String selectionId = selection.length > 0 ? nodeIdentity(selection[0]) : null;

      tree.setRedraw(false);
      tree.removeAll();

      boolean filtering = !Utils.isEmpty(currentSearchFilter);
      int totalMatches = 0;

      // Group the (name-sorted) types by their category id.
      Map<String, List<MetadataTypeModel>> typesByCategory = new LinkedHashMap<>();
      for (MetadataTypeModel typeModel : typeModels) {
        typesByCategory
            .computeIfAbsent(typeModel.categoryId, k -> new ArrayList<>())
            .add(typeModel);
      }
      // Order categories: known categories first (declaration order), then unknown, "Other" last.
      List<String> categoryIds = new ArrayList<>(typesByCategory.keySet());
      categoryIds.sort(
          Comparator.comparingInt(MetadataCategories::orderOf)
              .thenComparing(MetadataCategories::labelFor));

      for (String categoryId : categoryIds) {
        boolean categoryMatches = filtering && contains(MetadataCategories.labelFor(categoryId));
        TreeItem categoryItem = null;
        for (MetadataTypeModel typeModel : typesByCategory.get(categoryId)) {
          boolean typeMatches = filtering && (categoryMatches || contains(typeModel.typeName));
          List<MetadataItemModel> shownItems;
          if (!filtering || typeMatches) {
            shownItems = typeModel.items;
          } else {
            shownItems = new ArrayList<>();
            for (MetadataItemModel itemModel : typeModel.items) {
              if (contains(itemModel.name)) {
                shownItems.add(itemModel);
              }
            }
          }
          boolean showType =
              filtering
                  ? (typeMatches || !shownItems.isEmpty())
                  : (showEmptyTypes
                      || !typeModel.items.isEmpty()
                      || !typeModel.folderVirtualPaths.isEmpty());
          if (!showType) {
            continue;
          }
          totalMatches += shownItems.size();
          if (categoryItem == null) {
            categoryItem = createCategoryItem(categoryId);
          }
          buildTypeNode(categoryItem, typeModel, shownItems, filtering);
        }
      }

      updateResultCount(filtering, totalMatches);

      TreeUtil.setOptimalWidthOnColumns(tree);
      // Apply the remembered expand/collapse state (also force-expands everything while filtering).
      applyTreeMemory();

      tree.setRedraw(true);

      // Restore the selection and scroll position captured above.
      restoreTreeViewState(topId, selectionId);

      updateGui();
      updateSelection();
    } catch (Exception e) {
      new ErrorDialog(
          getShell(),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Header"),
          BaseMessages.getString(PKG, "MetadataPerspective.RefreshMetadata.Error.Message"),
          e);
    }
  }

  /**
   * Stable identity for any tree node (category, type, folder or item), independent of display
   * text, used to re-find a node after a rebuild. Returns {@code null} for unknown/disposed nodes.
   */
  private String nodeIdentity(TreeItem item) {
    if (item == null || item.isDisposed()) {
      return null;
    }
    String type = (String) item.getData(KEY_TYPE);
    if (CATEGORY.equals(type)) {
      return "C\t" + item.getData();
    }
    if (TYPE.equals(type)) {
      return "T\t" + item.getData();
    }
    if (FOLDER.equals(type)) {
      return "F\t" + item.getData() + "\t" + item.getData(VIRTUAL_PATH);
    }
    if (FILE.equals(type)) {
      return "I\t"
          + getObjectKey(item)
          + "\t"
          + item.getData(VIRTUAL_PATH)
          + "\t"
          + item.getText(0);
    }
    return null;
  }

  /**
   * Re-selects and re-scrolls to the nodes identified by {@code selectionId} / {@code topId} (as
   * produced by {@link #nodeIdentity}) after the tree has been rebuilt. Missing nodes are ignored.
   */
  private void restoreTreeViewState(String topId, String selectionId) {
    if (topId == null && selectionId == null) {
      return;
    }
    TreeItem[] found = new TreeItem[2]; // [0] = top item, [1] = selection
    findTreeItems(tree.getItems(), topId, selectionId, found);
    if (found[1] != null) {
      tree.setSelection(found[1]);
    }
    if (found[0] != null) {
      tree.setTopItem(found[0]);
    }
  }

  private void findTreeItems(TreeItem[] items, String topId, String selectionId, TreeItem[] found) {
    for (TreeItem item : items) {
      String id = nodeIdentity(item);
      if (id != null) {
        if (topId != null && topId.equals(id)) {
          found[0] = item;
        }
        if (selectionId != null && selectionId.equals(id)) {
          found[1] = item;
        }
      }
      findTreeItems(item.getItems(), topId, selectionId, found);
    }
  }

  /** Clears the search filter (e.g. when switching projects). */
  @Override
  public void clearSearchFilters() {
    currentSearchFilter = "";
    if (searchText != null && !searchText.isDisposed()) {
      searchText.setText("");
    }
  }

  private TreeItem createCategoryItem(String categoryId) {
    TreeItem categoryItem = new TreeItem(tree, SWT.NONE);
    categoryItem.setText(MetadataCategories.labelFor(categoryId));
    categoryItem.setImage(
        GuiResource.getInstance()
            .getImage(
                MetadataCategories.imageFor(categoryId),
                getClass().getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE));
    categoryItem.setData(categoryId);
    categoryItem.setData(KEY_TYPE, CATEGORY);
    categoryItem.setData(VIRTUAL_PATH, "");
    return categoryItem;
  }

  /** Builds a metadata type node (with item count) and its folder/item subtree under a category. */
  private void buildTypeNode(
      TreeItem categoryItem,
      MetadataTypeModel typeModel,
      List<MetadataItemModel> shownItems,
      boolean filtering) {
    Image image =
        GuiResource.getInstance()
            .getImage(
                typeModel.image,
                typeModel.metadataClass.getClassLoader(),
                ConstUi.SMALL_ICON_SIZE,
                ConstUi.SMALL_ICON_SIZE);

    int count = filtering ? shownItems.size() : typeModel.items.size();
    TreeItem classItem = new TreeItem(categoryItem, SWT.NONE);
    classItem.setText(0, typeModel.typeName + " (" + count + ")");
    classItem.setImage(image);
    classItem.setData(typeModel.key);
    classItem.setData(KEY_HELP, typeModel.description);
    classItem.setData(VIRTUAL_PATH, "");
    classItem.setData(KEY_TYPE, TYPE);

    // Materialize explicitly-created (possibly empty) folders first so they show without any items.
    // Skip while filtering: empty folders never match a search.
    if (!filtering) {
      for (String folderPath : typeModel.folderVirtualPaths) {
        resolveFolderItem(classItem, typeModel.key, folderPath);
      }
    }

    for (MetadataItemModel itemModel : shownItems) {
      TreeItem parentItem =
          Utils.isEmpty(itemModel.virtualPath)
              ? classItem
              : resolveFolderItem(classItem, typeModel.key, itemModel.virtualPath);

      TreeItem item = new TreeItem(parentItem, SWT.NONE);
      item.setText(0, Const.NVL(itemModel.name, ""));
      item.setData(VIRTUAL_PATH, parentItem.getData(VIRTUAL_PATH));
      item.setData(KEY_TYPE, FILE);
      MetadataEditor<?> editor = this.findEditor(typeModel.key, itemModel.name);
      if (editor != null && editor.hasChanged()) {
        item.setFont(GuiResource.getInstance().getFontBold());
      }
    }
  }

  /**
   * Ensures the folder chain for {@code virtualPath} exists under {@code typeItem}, creating folder
   * nodes as needed, and returns the deepest folder node (or {@code typeItem} for an empty path).
   */
  private TreeItem resolveFolderItem(TreeItem typeItem, String typeKey, String virtualPath) {
    TreeItem parentItem = typeItem;
    if (Utils.isEmpty(virtualPath)) {
      return parentItem;
    }
    List<String> folders = new ArrayList<>(Arrays.asList(virtualPath.split("/")));
    folders.removeAll(Arrays.asList("", null));
    for (String folder : folders) {
      if (folder.isEmpty()) {
        continue;
      }
      TreeItem alreadyExists = null;
      for (TreeItem childItem : parentItem.getItems()) {
        if (FOLDER.equals(childItem.getData(KEY_TYPE)) && childItem.getText().equals(folder)) {
          alreadyExists = childItem;
        }
      }
      if (alreadyExists != null) {
        parentItem = alreadyExists;
      } else {
        TreeItem folderItem = new TreeItem(parentItem, SWT.NONE);
        folderItem.setText(folder);
        folderItem.setData(typeKey);
        folderItem.setImage(GuiResource.getInstance().getImageFolder());
        folderItem.setData(
            VIRTUAL_PATH, folderItem.getParentItem().getData(VIRTUAL_PATH) + "/" + folder);
        folderItem.setData(KEY_TYPE, FOLDER);
        parentItem = folderItem;
      }
    }
    return parentItem;
  }

  /**
   * Stable TreeMemory key for an expandable node, independent of its display text so a changing
   * item count never invalidates a remembered state. Returns {@code null} for non-expandable nodes.
   */
  private String[] treeMemoryPath(TreeItem item) {
    String type = (String) item.getData(KEY_TYPE);
    if (CATEGORY.equals(type)) {
      return new String[] {"C", (String) item.getData()};
    }
    if (TYPE.equals(type)) {
      return new String[] {"T", (String) item.getData()};
    }
    if (FOLDER.equals(type)) {
      return new String[] {"F", (String) item.getData(), (String) item.getData(VIRTUAL_PATH)};
    }
    return null;
  }

  /** Applies the remembered expand/collapse state to the whole tree (see {@link #renderTree()}). */
  private void applyTreeMemory() {
    for (TreeItem item : tree.getItems()) {
      applyTreeMemory(item);
    }
  }

  private void applyTreeMemory(TreeItem item) {
    String[] path = treeMemoryPath(item);
    if (path != null) {
      if (!Utils.isEmpty(currentSearchFilter)) {
        // While searching, expand everything so matches are visible (not recorded as a choice).
        item.setExpanded(true);
      } else {
        // Categories expand by default; types and folders collapse by default. Seed each
        // default-expanded node once per session so the default holds until the user changes it.
        boolean defaultExpanded = "C".equals(path[0]);
        if (defaultExpanded && treeStateSeeded.add(String.join(" ", path))) {
          TreeMemory.getInstance().storeExpanded(METADATA_PERSPECTIVE_TREE, path, true);
        }
        item.setExpanded(TreeMemory.getInstance().isExpanded(METADATA_PERSPECTIVE_TREE, path));
      }
    }
    for (TreeItem child : item.getItems()) {
      applyTreeMemory(child);
    }
  }

  /** Records a user expand/collapse into the shared TreeMemory (ignored while searching). */
  private void recordTreeState(TreeItem item, boolean expanded) {
    if (item == null || item.isDisposed() || !Utils.isEmpty(currentSearchFilter)) {
      return;
    }
    String[] path = treeMemoryPath(item);
    if (path != null) {
      TreeMemory.getInstance().storeExpanded(METADATA_PERSPECTIVE_TREE, path, expanded);
      treeStateSeeded.add(String.join(" ", path));
    }
  }

  /**
   * Records the current expand state of a node and its children (used after expand/collapse all).
   */
  private void recordAllTreeState(TreeItem item) {
    recordTreeState(item, item.getExpanded());
    for (TreeItem child : item.getItems()) {
      recordAllTreeState(child);
    }
  }

  /** Case-insensitive containment test of {@code text} against the current search filter. */
  private boolean contains(String text) {
    return text != null && text.toLowerCase().contains(currentSearchFilter.toLowerCase());
  }

  /** Filter the tree based on search text, debounced so we don't rebuild on every keystroke. */
  protected void filterTree() {
    if (searchText == null || searchText.isDisposed()) {
      return;
    }
    Display display = searchText.getDisplay();
    display.timerExec(-1, filterRunnable);
    display.timerExec(FILTER_DEBOUNCE_MS, filterRunnable);
  }

  private void applyFilter() {
    if (searchText == null || searchText.isDisposed()) {
      return;
    }
    currentSearchFilter = searchText.getText();
    renderTree();
  }

  /** Updates the "n results" label shown under the search box while a filter is active. */
  private void updateResultCount(boolean filtering, int matches) {
    if (resultCountLabel == null || resultCountLabel.isDisposed()) {
      return;
    }
    resultCountLabel.setText(
        filtering
            ? BaseMessages.getString(PKG, "MetadataPerspective.Search.ResultCount", matches)
            : "");
    // Collapse the label's row when no filter is active so there is no gap above the tree.
    if (resultCountLabel.getLayoutData() instanceof FormData formData) {
      formData.height = filtering ? SWT.DEFAULT : 0;
    }
    resultCountLabel.getParent().layout();
  }

  /** Recursively expand or collapse a tree item and all its children */
  private void expandTreeItem(TreeItem item, boolean expand) {
    item.setExpanded(expand);
    for (TreeItem child : item.getItems()) {
      expandTreeItem(child, expand);
    }
  }

  protected void updateSelection() {

    boolean isMetadataSelected = false;
    boolean isFolderSelected = false;
    boolean canCreateHere = false;

    if (tree.getSelectionCount() > 0) {
      TreeItem treeItem = tree.getSelection()[0];
      String nodeType = (String) treeItem.getData(KEY_TYPE);
      isMetadataSelected = FILE.equals(nodeType);
      isFolderSelected = FOLDER.equals(nodeType);
      // The context "New" applies to a type, folder or file (all resolve to a type key), but not
      // to a category header or a plain label.
      canCreateHere = getObjectKey(treeItem) != null;
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_NEW, canCreateHere);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DUPLICATE, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, isMetadataSelected || isFolderSelected);
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
    if (!isInitialized()) {
      return false;
    }
    if (typeHandler instanceof MetadataEditor<?> editor && editor.isCloseable()) {

      editors.remove(editor);

      for (CTabItem item : tabFolder.getItems()) {
        if (editor.equals(item.getData())) {
          item.dispose();
        }
      }

      // Avoid refresh in a closing process (when switching project or exit)
      if (!hopGui.fileDelegate.isClosing()) {
        // Refresh tree to remove bold
        //
        this.refresh();

        // If all editors are closed
        //
        if (tabFolder.getItemCount() == 0) {
          HopGui.getInstance().handleFileCapabilities(new EmptyFileType(), false, false, false);
          // Bring back the overview/landing page.
          showOverview();
        }

        // Update Gui menu and toolbar
        this.updateGui();
      }
    }
    return false;
  }

  @Override
  public List<TabItemHandler> getItems() {
    List<TabItemHandler> items = new ArrayList<>();
    if (!isInitialized()) {
      return items;
    }
    for (CTabItem tabItem : tabFolder.getItems()) {
      for (MetadataEditor<?> editor : editors) {
        if (tabItem.getData().equals(editor)) {
          // This is the editor tabItem...
          //
          items.add(new TabItemHandler(tabItem, editor));
        }
      }
    }

    return items;
  }

  @Override
  public void navigateToPreviousFile() {
    if (hasNavigationPreviousFile()) {
      int index = tabFolder.getSelectionIndex() - 1;
      if (index < 0) {
        index = tabFolder.getItemCount() - 1;
      }
      tabFolder.setSelection(index);
      updateGui();
    }
  }

  @Override
  public void navigateToNextFile() {
    if (hasNavigationNextFile()) {
      int index = tabFolder.getSelectionIndex() + 1;
      if (index >= tabFolder.getItemCount()) {
        index = 0;
      }
      tabFolder.setSelection(index);
      updateGui();
    }
  }

  @Override
  public boolean hasNavigationPreviousFile() {
    return tabFolder.getItemCount() > 1;
  }

  @Override
  public boolean hasNavigationNextFile() {
    return tabFolder.getItemCount() > 1;
  }

  @Override
  public Control getControl() {
    return sash;
  }

  protected Shell getShell() {
    return hopGui.getShell();
  }

  @Override
  public List<IGuiContextHandler> getContextHandlers() {
    return new ArrayList<>();
  }

  @Override
  public List<ISearchable> getSearchables() {
    return new ArrayList<>();
  }

  @Override
  public void closeTab(CTabFolderEvent event, CTabItem tabItem) {
    MetadataEditor<?> editor = (MetadataEditor<?>) tabItem.getData();

    boolean isRemoved = remove(editor);
    if (isRemoved) {
      hopGui.auditDelegate.writeLastOpenFiles();
    }
    if (!isRemoved && event != null) {
      // Ignore event if canceled
      event.doit = false;
    }
  }

  @Override
  public CTabFolder getTabFolder() {
    return tabFolder;
  }

  private String getKeyOfMetadataClass(Class<? extends IHopMetadata> managedClass) {
    HopMetadata annotation = managedClass.getAnnotation(HopMetadata.class);
    assert annotation != null : "Metadata classes need to be annotated with @HopMetadata";
    return annotation.key();
  }

  /** Finds the tree node for a metadata type (by key), searching inside the category headers. */
  private TreeItem findTypeItem(String key) {
    if (!isInitialized()) {
      // There is no tree to navigate to.
      //
      return null;
    }
    for (TreeItem categoryItem : tree.getItems()) {
      for (TreeItem typeItem : categoryItem.getItems()) {
        if (TYPE.equals(typeItem.getData(KEY_TYPE)) && key.equals(typeItem.getData())) {
          return typeItem;
        }
      }
    }
    return null;
  }

  public void goToType(Class<? extends IHopMetadata> managedClass) {
    TreeItem typeItem = findTypeItem(getKeyOfMetadataClass(managedClass));
    if (typeItem != null) {
      tree.setSelection(typeItem);
      tree.showSelection();
    }
  }

  public void goToElement(Class<? extends IHopMetadata> managedClass, String elementName) {
    TreeItem typeItem = findTypeItem(getKeyOfMetadataClass(managedClass));
    if (typeItem == null) {
      return;
    }
    for (TreeItem elementItem : typeItem.getItems()) {
      if (elementName.equals(elementItem.getText())) {
        tree.setSelection(elementItem);
        tree.showSelection();
        onEditMetadata();
        return;
      }
    }
    // Element not directly under the type node (or not loaded): at least reveal the type.
    goToType(managedClass);
  }

  public void createNewFolder() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];
    String typeKey = getObjectKey(item);
    if (typeKey == null) {
      return; // A category header or label is selected: nothing to add a folder to.
    }
    String parentPath = Const.NVL((String) item.getData(VIRTUAL_PATH), "");
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Header"),
            BaseMessages.getString(
                PKG,
                "MetadataPerspective.CreateFolder.Message",
                parentPath.isEmpty() ? item.getText() : parentPath));
    String folder = dialog.open();
    if (Utils.isEmpty(folder)) {
      return;
    }
    // Reject a duplicate folder name on this level.
    for (TreeItem treeItem : item.getItems()) {
      if (folder.equals(treeItem.getText()) && FOLDER.equals(treeItem.getData(KEY_TYPE))) {
        MessageBox msgDialog = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
        msgDialog.setText(
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Header"));
        msgDialog.setMessage(
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Message"));
        msgDialog.open();
        return;
      }
    }
    // Persist the folder so it survives refresh/restart, then rebuild the tree from the model.
    persistFolder(typeKey, parentPath + "/" + folder);
    refresh();
  }

  /** In-memory view of one metadata type and its items, loaded once per {@link #reloadModel()}. */
  private static final class MetadataTypeModel {
    private final String key;
    private final String categoryId;
    private final String typeName;
    private final String description;
    private final String image;
    private final Class<IHopMetadata> metadataClass;
    private final List<MetadataItemModel> items = new ArrayList<>();

    /** Explicitly created virtual folder paths (persisted), shown even when they hold no items. */
    private final List<String> folderVirtualPaths = new ArrayList<>();

    private MetadataTypeModel(
        String key,
        String categoryId,
        String typeName,
        String description,
        String image,
        Class<IHopMetadata> metadataClass) {
      this.key = key;
      this.categoryId = categoryId;
      this.typeName = typeName;
      this.description = description;
      this.image = image;
      this.metadataClass = metadataClass;
    }
  }

  /** In-memory view of a single metadata item: its name and (optional) virtual folder path. */
  private static final class MetadataItemModel {
    private final String name;
    private final String virtualPath;

    private MetadataItemModel(String name, String virtualPath) {
      this.name = name;
      this.virtualPath = virtualPath;
    }
  }
}
