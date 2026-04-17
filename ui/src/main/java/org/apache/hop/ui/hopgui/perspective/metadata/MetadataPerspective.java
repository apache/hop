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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.gui.plugin.GuiPlugin;
import org.apache.hop.core.gui.plugin.key.GuiKeyboardShortcut;
import org.apache.hop.core.gui.plugin.key.GuiOsxKeyboardShortcut;
import org.apache.hop.core.gui.plugin.toolbar.GuiToolbarElement;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.search.ISearchable;
import org.apache.hop.core.util.TranslateUtil;
import org.apache.hop.core.util.Utils;
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
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.gui.GuiToolbarWidgets;
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
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.HelpUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabFolder2Adapter;
import org.eclipse.swt.custom.CTabFolderEvent;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.custom.SashForm;
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

  public static final String GUI_PLUGIN_TOOLBAR_PARENT_ID = "MetadataPerspective-Toolbar";

  public static final String TOOLBAR_ITEM_NEW = "MetadataPerspective-Toolbar-10000-New";
  public static final String TOOLBAR_ITEM_EDIT = "MetadataPerspective-Toolbar-10010-Edit";
  public static final String TOOLBAR_ITEM_DUPLICATE = "MetadataPerspective-Toolbar-10030-Duplicate";
  public static final String TOOLBAR_ITEM_DELETE = "MetadataPerspective-Toolbar-10040-Delete";
  public static final String TOOLBAR_ITEM_RENAME = "MetadataPerspective-Toolbar-10020-Rename";
  public static final String TOOLBAR_ITEM_EXPAND_ALL =
      "MetadataPerspective-Toolbar-10060-ExpandAll";
  public static final String TOOLBAR_ITEM_COLLAPSE_ALL =
      "MetadataPerspective-Toolbar-10070-CollapseAll";
  public static final String TOOLBAR_ITEM_REFRESH = "MetadataPerspective-Toolbar-10100-Refresh";

  private static final String KEY_HELP = "Help";
  private static final String KEY_TYPE = "type";
  public static final String FILE = "File";
  public static final String FOLDER = "Folder";
  public static final String VIRTUAL_PATH = "virtualPath";
  public static final String ERROR = "Error";

  @Getter private static MetadataPerspective instance;

  private HopGui hopGui;
  private SashForm sash;
  private Tree tree;
  private TreeEditor treeEditor;
  private CTabFolder tabFolder;
  private GuiToolbarWidgets toolBarWidgets;
  private Text searchText;
  private String currentSearchFilter = "";

  private final List<MetadataEditor<?>> editors = new ArrayList<>();

  private final MetadataFileType metadataFileType;

  public MetadataPerspective() {
    instance = this;

    this.metadataFileType = new MetadataFileType();
  }

  @Override
  public String getId() {
    return "metadata-perspective";
  }

  @GuiKeyboardShortcut(control = true, shift = true, key = 'm', global = true)
  @GuiOsxKeyboardShortcut(command = true, shift = true, key = 'm', global = true)
  @Override
  public void activate() {
    hopGui.setActivePerspective(this);
  }

  @Override
  public void perspectiveActivated() {
    this.updateSelection();
    this.updateGui();
  }

  @Override
  public boolean isActive() {
    return hopGui.isActivePerspective(this);
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
    createTabFolder(sash);

    sash.setWeights(new int[] {20, 80});

    this.refresh();
    this.updateSelection();

    // Set the top level items in the tree to be expanded
    //
    for (TreeItem item : tree.getItems()) {
      TreeMemory.getInstance().storeExpanded(METADATA_PERSPECTIVE_TREE, item, true);
    }

    // refresh the metadata when it changes.
    //
    hopGui
        .getEventsHandler()
        .addEventListener(
            getClass().getName(), e -> refresh(), HopGuiEvents.MetadataChanged.name());

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

    // Create a composite with toolbar and tree for the border
    Composite composite = new Composite(treeComposite, SWT.BORDER);
    composite.setLayout(new FormLayout());
    FormData layoutData = new FormData();
    layoutData.left = new FormAttachment(0, 0);
    layoutData.top = new FormAttachment(searchText, PropsUi.getMargin());
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
          if (treeItem != null && treeItem.getData(KEY_TYPE).equals(FILE)) {
            if (treeItem.getParentItem() == null) {
              onNewMetadata();
            } else {
              onEditMetadata();
            }
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
              case "MetadataItem", FOLDER:
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.New"));
                menuItem.addListener(SWT.Selection, e -> onNewMetadata());
                menuItem = new MenuItem(menu, SWT.POP_UP);
                menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.NewFolder"));
                menuItem.addListener(SWT.Selection, e -> createNewFolder());
                new MenuItem(menu, SWT.SEPARATOR);
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

            menuItem = new MenuItem(menu, SWT.POP_UP);
            menuItem.setText(BaseMessages.getString(PKG, "MetadataPerspective.Menu.Help"));
            menuItem.setImage(GuiResource.getInstance().getImageHelp());
            menuItem.addListener(SWT.Selection, e -> onHelpMetadata());

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

    // Remember tree node expanded/Collapsed
    TreeMemory.addTreeListener(tree, METADATA_PERSPECTIVE_TREE);

    // Drag and drop: reorganize within tree (same type only) and drag to canvas to open
    createTreeDragSource(tree);
    createTreeDropTarget(tree);
  }

  /**
   * Returns the metadata type key (objectKey) for the given tree item by walking up to the root
   * class item.
   */
  private String getObjectKey(TreeItem item) {
    TreeItem root = item;
    while (root.getParentItem() != null) {
      root = root.getParentItem();
    }
    return (String) root.getData();
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
            sash.setMaximizedControl(tabFolder);
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
    if (tabFolder.getSelectionIndex() < 0) {
      return null;
    }

    return (MetadataEditor<?>) tabFolder.getSelection().getData();
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
    if (treeItem != null) {
      String objectKey;
      if (treeItem.getParentItem() == null) {
        objectKey = (String) treeItem.getData();
      } else {
        objectKey = (String) treeItem.getParentItem().getData();
      }

      try {
        IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
        Class<IHopMetadata> metadataClass = metadataProvider.getMetadataClassForKey(objectKey);
        MetadataManager<IHopMetadata> manager =
            new MetadataManager<>(
                HopGui.getInstance().getVariables(),
                metadataProvider,
                metadataClass,
                hopGui.getShell());

        manager.newMetadataWithEditor((String) treeItem.getData(VIRTUAL_PATH));

        hopGui.getEventsHandler().fire(HopGuiEvents.MetadataCreated.name());
      } catch (Exception e) {
        new ErrorDialog(
            getShell(),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Header"),
            BaseMessages.getString(PKG, "MetadataPerspective.CreateMetadata.Error.Message"),
            e);
      }
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
   * If the metadata type supports global replace, finds references to oldName in pipeline/workflow
   * files, optionally shows the update dialog, and replaces references with newName. Reloads open
   * tabs for modified files. Call this when a metadata element is renamed (from tree or from
   * editor).
   *
   * @param objectKey metadata type key (e.g. "rdbms", "restconnection")
   * @param oldName the name before rename
   * @param newName the name after rename
   */
  /**
   * Finds references to {@code oldName} in pipelines/workflows and other metadata, asks the user to
   * update them, and performs the replacement. Returns {@code true} if the operation completed (or
   * there was nothing to do), {@code false} if the user cancelled a save-changes dialog and the
   * caller should abort its own operation too.
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
    String projectHome = hopGui.getVariables().resolve("${PROJECT_HOME}");
    if (Utils.isEmpty(projectHome) || "${PROJECT_HOME}".equals(projectHome)) {
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

    // No delete on folder
    if (!treeItem.getData(KEY_TYPE).equals(FILE)) {
      return;
    }

    String objectKey = (String) treeItem.getParentItem().getData();
    String objectName = treeItem.getText(0);

    try {
      // Check for references in pipelines/workflows and other metadata objects
      MetadataReferenceFinder finder = new MetadataReferenceFinder(hopGui.getMetadataProvider());
      String projectHome = hopGui.getVariables().resolve("${PROJECT_HOME}");
      List<MetadataReferenceResult> fileRefs = Collections.emptyList();
      if (MetadataRefactorUtil.supportsGlobalReplace(hopGui.getMetadataProvider(), objectKey)) {
        if (!Utils.isEmpty(projectHome) && !"${PROJECT_HOME}".equals(projectHome)) {
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
      return "${PROJECT_HOME}" + (rel.startsWith("/") ? rel : "/" + rel);
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
    String objectKey = null;
    TreeItem treeItem = tree.getSelection()[0];
    if (treeItem != null) {
      if (treeItem.getParentItem() != null) {
        treeItem = treeItem.getParentItem();
      }
      objectKey = (String) treeItem.getData();
    }

    if (objectKey != null) {
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
  }

  public void updateEditor(MetadataEditor<?> editor) {

    if (editor == null) return;

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
  public void expandAll() {
    if (tree == null || tree.isDisposed()) {
      return;
    }

    tree.setRedraw(false);
    try {
      for (TreeItem item : tree.getItems()) {
        expandTreeItem(item, true);
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
  public void collapseAll() {
    if (tree == null || tree.isDisposed()) {
      return;
    }

    tree.setRedraw(false);
    try {
      for (TreeItem item : tree.getItems()) {
        expandTreeItem(item, false);
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
    try {
      tree.setRedraw(false);
      tree.removeAll();

      // top level: object key
      //
      IHopMetadataProvider metadataProvider = hopGui.getMetadataProvider();
      List<Class<IHopMetadata>> metadataClasses = metadataProvider.getMetadataClasses();
      // Sort by name
      Collections.sort(
          metadataClasses,
          (cl1, cl2) -> {
            HopMetadata a1 = HopMetadataUtil.getHopMetadataAnnotation(cl1);
            HopMetadata a2 = HopMetadataUtil.getHopMetadataAnnotation(cl2);
            return a1.name().compareTo(a2.name());
          });

      for (Class<IHopMetadata> metadataClass : metadataClasses) {
        HopMetadata annotation = HopMetadataUtil.getHopMetadataAnnotation(metadataClass);
        Image image =
            GuiResource.getInstance()
                .getImage(
                    annotation.image(),
                    metadataClass.getClassLoader(),
                    ConstUi.SMALL_ICON_SIZE,
                    ConstUi.SMALL_ICON_SIZE);

        TreeItem classItem = new TreeItem(tree, SWT.NONE);
        classItem.setText(
            0, Const.NVL(TranslateUtil.translate(annotation.name(), metadataClass), ""));
        classItem.setImage(image);
        classItem.setExpanded(true);
        classItem.setData(annotation.key());
        classItem.setData(KEY_HELP, annotation.description());
        classItem.setData(VIRTUAL_PATH, "");
        classItem.setData(KEY_TYPE, "MetadataItem");

        // level 1: object names
        //
        IHopMetadataSerializer<IHopMetadata> serializer =
            metadataProvider.getSerializer(metadataClass);
        List<String> names = serializer.listObjectNames();
        Collections.sort(names);

        for (final String name : names) {
          // Apply filter - skip non-matching items
          if (!matchesFilter(name)) {
            continue;
          }

          IHopMetadata hopMetadata;
          try {
            hopMetadata = serializer.load(name);
          } catch (HopException e) {
            // Ignore missing metadata items
            LogChannel.GENERAL.logError("Error loading metadata object:" + name);
            continue;
          }
          TreeItem parentItem = classItem;

          if (!Utils.isEmpty(hopMetadata.getVirtualPath())) {
            List<String> folders =
                new ArrayList<>(Arrays.asList(hopMetadata.getVirtualPath().split("/")));
            // remove empty elements
            folders.removeAll(Arrays.asList("", null));

            for (String folder : folders) {
              TreeItem alreadyExists = null;
              if (!folder.isEmpty()) {
                // check if folder already exists on this level
                alreadyExists = null;
                for (TreeItem childItem : parentItem.getItems()) {
                  if (childItem.getData(KEY_TYPE).equals(FOLDER)
                      && childItem.getText().equals(folder)) {
                    alreadyExists = childItem;
                  }
                }

                if (alreadyExists != null) {
                  parentItem = alreadyExists;
                } else {
                  TreeItem folderItem = new TreeItem(parentItem, SWT.NONE);
                  folderItem.setText(folder);
                  folderItem.setData(annotation.key());
                  folderItem.setImage(GuiResource.getInstance().getImageFolder());
                  folderItem.setData(
                      VIRTUAL_PATH,
                      folderItem.getParentItem().getData(VIRTUAL_PATH) + "/" + folder);
                  folderItem.setData(KEY_TYPE, FOLDER);

                  // Expand folders when filtering to show matches
                  if (!Utils.isEmpty(currentSearchFilter)) {
                    folderItem.setExpanded(true);
                  }

                  parentItem = folderItem;
                }
              }
            }
          }

          TreeItem item = new TreeItem(parentItem, SWT.NONE);
          item.setText(0, Const.NVL(name, ""));
          item.setData(VIRTUAL_PATH, parentItem.getData(VIRTUAL_PATH));
          item.setData(KEY_TYPE, FILE);
          MetadataEditor<?> editor = this.findEditor(annotation.key(), name);
          if (editor != null && editor.hasChanged()) {
            item.setFont(GuiResource.getInstance().getFontBold());
          }
        }
      }

      // Remove empty class items (those with no children after filtering)
      if (!Utils.isEmpty(currentSearchFilter)) {
        List<TreeItem> itemsToRemove = new ArrayList<>();
        for (TreeItem classItem : tree.getItems()) {
          if (classItem.getItemCount() == 0) {
            itemsToRemove.add(classItem);
          } else {
            // Expand class items when filtering to show matches
            classItem.setExpanded(true);
          }
        }
        for (TreeItem item : itemsToRemove) {
          item.dispose();
        }
      }

      TreeUtil.setOptimalWidthOnColumns(tree);
      TreeMemory.setExpandedFromMemory(tree, METADATA_PERSPECTIVE_TREE);

      tree.setRedraw(true);

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

  /** Filter the tree based on search text */
  protected void filterTree() {
    if (searchText == null || searchText.isDisposed()) {
      return;
    }

    currentSearchFilter = searchText.getText();

    // Refresh to rebuild the tree with or without filter
    refresh();
  }

  /** Check if a metadata name matches the current filter */
  private boolean matchesFilter(String name) {
    if (Utils.isEmpty(currentSearchFilter)) {
      return true;
    }
    return name != null && name.toLowerCase().contains(currentSearchFilter.toLowerCase());
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
    boolean isAnythingSelected = false;

    if (tree.getSelectionCount() > 0) {
      isAnythingSelected = true;
      TreeItem treeItem = tree.getSelection()[0];
      if (treeItem.getData(KEY_TYPE).equals(FILE)) {
        isMetadataSelected = true;
      }
    }

    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_NEW, isAnythingSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_EDIT, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_RENAME, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DUPLICATE, isMetadataSelected);
    toolBarWidgets.enableToolbarItem(TOOLBAR_ITEM_DELETE, isMetadataSelected);
  }

  @Override
  public boolean remove(IHopFileTypeHandler typeHandler) {
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

  public void goToType(Class<? extends IHopMetadata> managedClass) {
    String key = getKeyOfMetadataClass(managedClass);
    // Look at all the top level items in the tree
    //
    for (TreeItem item : tree.getItems()) {
      String classKey = (String) item.getData();
      if (key.equals(classKey)) {
        // Found the item.
        //
        tree.setSelection(item);
        tree.showSelection();
        return;
      }
    }
  }

  public void goToElement(Class<? extends IHopMetadata> managedClass, String elementName) {
    String key = getKeyOfMetadataClass(managedClass);
    for (TreeItem item : tree.getItems()) {
      String classKey = (String) item.getData();
      if (key.equals(classKey)) {
        // Found the type.
        //
        for (TreeItem elementItem : item.getItems()) {
          if (elementName.equals(elementItem.getText())) {
            tree.setSelection(elementItem);
            tree.showSelection();
            onEditMetadata();
            return;
          }
        }
        goToType(managedClass);
      }
    }
  }

  public void createNewFolder() {
    TreeItem[] selection = tree.getSelection();
    if (selection == null || selection.length == 0) {
      return;
    }
    TreeItem item = selection[0];
    EnterStringDialog dialog =
        new EnterStringDialog(
            getShell(),
            "",
            BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Header"),
            BaseMessages.getString(
                PKG,
                "MetadataPerspective.CreateFolder.Message",
                ((String) item.getData(VIRTUAL_PATH)).isEmpty()
                    ? item.getText()
                    : (String) item.getData(VIRTUAL_PATH)));
    String folder = dialog.open();
    if (!Utils.isEmpty(folder)) {
      for (TreeItem treeItem : item.getItems()) {
        if (folder.equals(treeItem.getText()) && treeItem.getData("type").equals(FOLDER)) {
          MessageBox msgDialog = new MessageBox(getShell(), SWT.ICON_INFORMATION | SWT.OK);
          msgDialog.setText(
              BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Header"));
          msgDialog.setMessage(
              BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.Error.Message"));
          msgDialog.open();
          return;
        }
      }
      TreeItem newFolder = new TreeItem(item, SWT.NONE);
      newFolder.setText(folder);
      newFolder.setData(item.getData());
      newFolder.setImage(GuiResource.getInstance().getImageFolder());
      newFolder.setData(VIRTUAL_PATH, item.getData(VIRTUAL_PATH) + "/" + folder);
      newFolder.setData(KEY_TYPE, FOLDER);
      TreeItem emptyString = new TreeItem(newFolder, SWT.NONE);
      emptyString.setText(
          BaseMessages.getString(PKG, "MetadataPerspective.CreateFolder.EmptyFolder"));
      emptyString.setData(KEY_TYPE, "Label");
      emptyString.setForeground(tree.getDisplay().getSystemColor(SWT.COLOR_GRAY));
      newFolder.setExpanded(true);
    }
  }
}
